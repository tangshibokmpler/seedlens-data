import { createHash, randomUUID } from "node:crypto";
import { gunzipSync } from "node:zlib";

import { Pipeline } from "@agentnexus/lakecore/model";
import {
  assertTargetedRecordInputs,
  resolveRecordInputs,
  type PipelineRecordInputs,
} from "../record-inputs.js";
import {
  deleteSourceMaterialState,
  loadSourceMaterialStates,
  sourceMaterialRunActive,
  writeSourceMaterialState,
  type SourceMaterialReconcileState,
  type SourceMaterialStateRow,
} from "./state.js";
import type { ConnectorActiveConfig, PipelineRunContext } from "./types.js";

type BuiltinSourceType = "pg" | "mysql" | "sqlserver";
type ChangeType = "CREATED" | "UPDATED" | "DELETED";
const MAX_WRITE_BATCH_SIZE = 1000;
const CONTROL_QUERY_PAGE_SIZE = 5000;

interface SourceMaterialLoadConfig {
  sourceTable: string;
  materialTable: string;
  stateTable: string;
  maxPages: number;
  writeBatchSize: number;
}

interface DataSourceConfig {
  name: string;
  kind: string;
  builtinKind?: string;
  connection: Record<string, unknown>;
}

interface BuiltinDataSourceConfig extends DataSourceConfig {
  kind: "builtin";
  builtinKind: BuiltinSourceType;
}

interface MaterialColumnSchema {
  ordinal: number;
  name: string;
  data_type: string;
  nullable: boolean;
}

interface MaterialSchema {
  format_version: 1;
  table: {
    catalog: string;
    schema: string;
    name: string;
    type: string;
  };
  columns: readonly MaterialColumnSchema[];
}

interface ScannedTable {
  catalog: string;
  schema: string;
  name: string;
  type: string;
}

interface ExpectedMaterial {
  sourceMaterialKey: string;
  dataSourceNameKey: string;
  dataSourceType: string;
  sourceCatalogName: string;
  sourceSchemaName: string;
  sourceTableName: string;
  sourceTableType: string;
  materialSchema: MaterialSchema;
  schemaHash: string;
}

interface CurrentMaterial {
  sourceMaterialKey: string;
  dataSourceNameKey: string;
  dataSourceType: string;
  sourceCatalogName: string;
  sourceSchemaName: string;
  sourceTableName: string;
  sourceTableType: string;
  materialSchema: unknown;
  schemaHash: string;
  lastChangedRunId: string;
  createdAt: string;
  updatedAt: string;
}

interface ReconcileSummary {
  created: number;
  updated: number;
  unchanged: number;
  deleted: number;
}

interface MaterialChange {
  changeType: ChangeType;
  previous?: CurrentMaterial;
  expected?: ExpectedMaterial;
}

export class BuiltinSourceMaterialLoadPipeline {
  @Pipeline({ tag: "dataFactory", pipelineKey: "builtin_source_material_load" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：扫描当前租户环境已装载数据源配置 schema 下的全部表/视图结构，并维护版本化的最新原料清单。
 * 输入：默认读取 t_factory_config_source 完整配置；config.recordInputs 非空时只消费指定数据源记录，以及 pipelinePrelude 已创建的 Trino external catalog。
 * 逻辑：SQL Server 通过 system.query 将 sys.* 集合查询下推到源库，其余内置源读取 information_schema；规范化后调谐原料版本表并在状态中记录前后 snapshot。
 * 幂等：Schema 未变化不写版本表，失败运行复用 run_id；定向模式只清理指定数据源内部缺失原料，全量模式才清理已删除数据源。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = sourceMaterialLoadConfig(context.config);
  const recordInputs = resolveRecordInputs(context.config);
  assertTargetedRecordInputs(recordInputs, [config.sourceTable]);
  const targeted = recordInputs.size > 0;

  // 先读取状态和业务表，保证任何业务写入前所有前置表都已存在。
  const states = await loadSourceMaterialStates(context, config.stateTable);
  const sources = await loadDataSources(context, config.sourceTable, recordInputs);
  const currentMaterials = await loadCurrentMaterials(context, config.materialTable);
  const connectors = await context.services.lakehouse.managerConnectorsList({ provider: "trino" });
  const activeCatalogs = connectors.trino?.activeConfigs ?? [];

  const stateBySource = uniqueMap(states, (state) => state.data_source_name_key, "source material state");
  const materialsBySource = groupBy(currentMaterials, (material) => material.dataSourceNameKey);
  const expectedSourceKeys = new Set(sources.map((source) => source.name));
  for (const source of sources) {
    if (context.signal.aborted) {
      throw new Error("source material load aborted");
    }
    await reconcileDataSourceMaterials(context, config, {
      source,
      activeCatalogs,
      previousState: stateBySource.get(source.name),
      currentMaterials: materialsBySource.get(source.name) ?? [],
    });
  }

  if (!targeted) {
    const staleSourceKeys = new Set([
      ...states.map((state) => state.data_source_name_key),
      ...currentMaterials.map((material) => material.dataSourceNameKey),
    ].filter((key) => !expectedSourceKeys.has(key)));
    for (const sourceKey of [...staleSourceKeys].sort()) {
      if (context.signal.aborted) {
        throw new Error("source material load aborted");
      }
      await removeDeletedDataSourceMaterials(context, config, {
        sourceKey,
        previousState: stateBySource.get(sourceKey),
        currentMaterials: materialsBySource.get(sourceKey) ?? [],
      });
    }
  }
}

async function reconcileDataSourceMaterials(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  input: {
    source: DataSourceConfig;
    activeCatalogs: readonly ConnectorActiveConfig[];
    previousState?: SourceMaterialStateRow;
    currentMaterials: readonly CurrentMaterial[];
  },
): Promise<void> {
  const configHash = hashValue(input.source);
  if (sourceMaterialRunActive(input.previousState, configHash)) {
    context.logger.warn("source material scan skipped because another run holds the lease", {
      source: input.source.name,
      runId: input.previousState?.reconcile_state.run_id,
    });
    return;
  }

  const { runId, startedAt } = resumableRun(input.previousState, configHash);
  await writeSourceMaterialState(context, {
    table: config.stateTable,
    key: input.source.name,
    leaseMs: context.heartbeatLeaseMs,
    state: runningState(input.previousState, { runId, configHash, startedAt }),
  });

  try {
    const source = requireBuiltinDataSource(input.source);
    const catalog = activeCatalog(source, context, input.activeCatalogs);
    const beforeSnapshotId = await currentMaterialSnapshotId(context, config.materialTable);
    const expected = await scanSourceMaterials(context, config, source, catalog);
    const summary = await reconcileMaterialRows(context, config, {
      runId,
      changedAt: startedAt,
      expected,
      current: input.currentMaterials,
    });
    const afterSnapshotId = await currentMaterialSnapshotId(context, config.materialTable);
    const completedAt = new Date().toISOString();
    const materialSetHash = hashValue(expected.map((material) => ({
      key: material.sourceMaterialKey,
      schema_hash: material.schemaHash,
    })));
    await writeSourceMaterialState(context, {
      table: config.stateTable,
      key: source.name,
      leaseMs: context.heartbeatLeaseMs,
      state: {
        status: "ready",
        run_id: runId,
        config_hash: configHash,
        started_at: startedAt,
        updated_at: completedAt,
        material_set_hash: materialSetHash,
        table_count: expected.length,
        created_count: summary.created,
        updated_count: summary.updated,
        unchanged_count: summary.unchanged,
        deleted_count: summary.deleted,
        ...(beforeSnapshotId ? { before_snapshot_id: beforeSnapshotId } : {}),
        ...(afterSnapshotId ? { after_snapshot_id: afterSnapshotId } : {}),
        last_successful_run_id: runId,
        last_successful_material_set_hash: materialSetHash,
        ...(afterSnapshotId ? { last_successful_snapshot_id: afterSnapshotId } : {}),
        last_successful_at: completedAt,
      },
    });
    context.logger.info("source materials reconciled", {
      source: source.name,
      runId,
      tables: expected.length,
      ...summary,
    });
  } catch (error) {
    await writeFailedState(context, config, {
      key: input.source.name,
      previous: input.previousState,
      runId,
      configHash,
      startedAt,
      error,
    });
    context.logger.warn("source material reconcile failed", {
      source: input.source.name,
      error: errorMessage(error),
    });
  }
}

async function removeDeletedDataSourceMaterials(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  input: {
    sourceKey: string;
    previousState?: SourceMaterialStateRow;
    currentMaterials: readonly CurrentMaterial[];
  },
): Promise<void> {
  const configHash = "removed";
  if (sourceMaterialRunActive(input.previousState, configHash)) {
    return;
  }
  const { runId, startedAt } = resumableRun(input.previousState, configHash);
  await writeSourceMaterialState(context, {
    table: config.stateTable,
    key: input.sourceKey,
    leaseMs: context.heartbeatLeaseMs,
    state: runningState(input.previousState, { runId, configHash, startedAt }),
  });
  try {
    const changes = [...input.currentMaterials]
      .sort(compareCurrentMaterials)
      .map((previous): MaterialChange => ({ changeType: "DELETED", previous }));
    await writeMaterialChanges(context, config, {
      runId,
      changedAt: startedAt,
      changes,
    });
    await deleteSourceMaterialState(context, config.stateTable, input.sourceKey);
    context.logger.info("deleted data source materials removed", {
      source: input.sourceKey,
      runId,
      deleted: input.currentMaterials.length,
    });
  } catch (error) {
    await writeFailedState(context, config, {
      key: input.sourceKey,
      previous: input.previousState,
      runId,
      configHash,
      startedAt,
      error,
    });
    context.logger.warn("deleted data source material cleanup failed", {
      source: input.sourceKey,
      error: errorMessage(error),
    });
  }
}

async function reconcileMaterialRows(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  input: {
    runId: string;
    changedAt: string;
    expected: readonly ExpectedMaterial[];
    current: readonly CurrentMaterial[];
  },
): Promise<ReconcileSummary> {
  const currentByKey = uniqueMap(input.current, (material) => material.sourceMaterialKey, "source material");
  const expectedKeys = new Set(input.expected.map((material) => material.sourceMaterialKey));
  const summary: ReconcileSummary = { created: 0, updated: 0, unchanged: 0, deleted: 0 };
  const changes: MaterialChange[] = [];

  for (const material of input.expected) {
    const previous = currentByKey.get(material.sourceMaterialKey);
    if (!previous) {
      changes.push({ changeType: "CREATED", expected: material });
      summary.created += 1;
      continue;
    }
    if (!materialChanged(previous, material)) {
      summary.unchanged += 1;
      continue;
    }
    changes.push({ changeType: "UPDATED", previous, expected: material });
    summary.updated += 1;
  }

  for (const previous of [...input.current].sort(compareCurrentMaterials)) {
    if (expectedKeys.has(previous.sourceMaterialKey)) {
      continue;
    }
    changes.push({ changeType: "DELETED", previous });
    summary.deleted += 1;
  }
  await writeMaterialChanges(context, config, {
    runId: input.runId,
    changedAt: input.changedAt,
    changes,
  });
  return summary;
}

async function writeMaterialChanges(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  input: {
    runId: string;
    changedAt: string;
    changes: readonly MaterialChange[];
  },
): Promise<void> {
  for (const batch of chunks(input.changes, config.writeBatchSize)) {
    const currentRecords = batch.flatMap((change) => change.expected
      ? [currentMaterialRecord(
          change.expected,
          input.runId,
          input.changedAt,
          change.previous?.createdAt ?? input.changedAt,
        )]
      : []);
    await setBusinessTableRows(context, config.materialTable, currentRecords);
    const deletedKeys = batch.flatMap((change) =>
      change.changeType === "DELETED" && change.previous
        ? [change.previous.sourceMaterialKey]
        : []);
    await deleteBusinessTableRows(context, config.materialTable, deletedKeys);
  }
}

async function setBusinessTableRows(
  context: PipelineRunContext,
  table: string,
  records: readonly Record<string, unknown>[],
): Promise<void> {
  if (records.length === 0) {
    return;
  }
  await context.services.dataPlane.dataTableRowsSet({
    actor: context.actor,
    table,
    records,
  });
}

function currentMaterialRecord(
  material: ExpectedMaterial,
  runId: string,
  changedAt: string,
  createdAt: string,
): Record<string, unknown> {
  return {
    source_material_key: material.sourceMaterialKey,
    data_source_name_key: material.dataSourceNameKey,
    data_source_type: material.dataSourceType,
    source_catalog_name: material.sourceCatalogName,
    source_schema_name: material.sourceSchemaName,
    source_table_name: material.sourceTableName,
    source_table_type: material.sourceTableType,
    material_schema: material.materialSchema,
    schema_hash: material.schemaHash,
    last_changed_run_id: runId,
    created_at: createdAt,
    updated_at: changedAt,
  };
}

async function deleteBusinessTableRows(
  context: PipelineRunContext,
  table: string,
  keys: readonly string[],
): Promise<void> {
  if (keys.length === 0) {
    return;
  }
  await context.services.dataPlane.dataTableRowsDelete({
    actor: context.actor,
    table,
    keys,
  });
}

async function scanSourceMaterials(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  source: BuiltinDataSourceConfig,
  catalog: ConnectorActiveConfig,
): Promise<readonly ExpectedMaterial[]> {
  if (source.builtinKind === "sqlserver") {
    return scanSqlServerSourceMaterials(context, config, source, catalog);
  }
  return scanInformationSchemaSourceMaterials(context, config, source, catalog);
}

async function scanSqlServerSourceMaterials(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  source: BuiltinDataSourceConfig,
  catalog: ConnectorActiveConfig,
): Promise<readonly ExpectedMaterial[]> {
  const schema = sourceSchema(source);
  const nativeSql = [
    "SELECT",
    "  payload_rows.batch_id,",
    "  COMPRESS(CONCAT(",
    "    '[',",
    "    STRING_AGG(CAST(payload_rows.table_json AS nvarchar(max)), N',')",
    "      WITHIN GROUP (ORDER BY payload_rows.schema_name, payload_rows.table_name),",
    "    ']'",
    "  )) AS metadata_gzip",
    "FROM (",
    "  SELECT",
    "    ABS(CONVERT(bigint, CHECKSUM(table_rows.schema_name, table_rows.table_name))) % 32 AS batch_id,",
    "    table_rows.schema_name,",
    "    table_rows.table_name,",
    "    table_rows.table_json",
    "  FROM (",
    "    SELECT",
    "      column_rows.schema_name,",
    "      column_rows.table_name,",
    "      CONCAT(",
    "        '[\"', STRING_ESCAPE(column_rows.schema_name, 'json'),",
    "        '\",\"', STRING_ESCAPE(column_rows.table_name, 'json'),",
    "        '\",\"', CASE column_rows.object_type WHEN 'U' THEN 'BASE TABLE' ELSE 'VIEW' END,",
    "        '\",[',",
    "        STRING_AGG(column_rows.column_json, N',')",
    "          WITHIN GROUP (ORDER BY column_rows.ordinal),",
    "        ']]'",
    "      ) AS table_json",
    "    FROM (",
    "      SELECT",
    "        s.name AS schema_name,",
    "        o.name AS table_name,",
    "        o.type AS object_type,",
    "        c.column_id AS ordinal,",
    "        CAST(CONCAT(",
    "          '[', c.column_id,",
    "          ',\"', STRING_ESCAPE(c.name, 'json'), '\"',",
    "          ',\"', STRING_ESCAPE(formatted.data_type, 'json'), '\"',",
    "          ',', CASE WHEN c.is_nullable = 1 THEN '1' ELSE '0' END,",
    "          ']'",
    "        ) AS nvarchar(max)) AS column_json",
    "      FROM sys.objects o",
    "      JOIN sys.schemas s ON s.schema_id = o.schema_id",
    "      JOIN sys.columns c ON c.object_id = o.object_id",
    "      JOIN sys.types ty ON ty.user_type_id = c.user_type_id",
    "      CROSS APPLY (VALUES (",
    "        CASE",
    "          WHEN ty.name IN ('decimal', 'numeric')",
    "            THEN CONCAT(ty.name, '(', c.precision, ',', c.scale, ')')",
    "          WHEN ty.name IN ('varchar', 'char', 'varbinary', 'binary')",
    "            THEN CONCAT(ty.name, '(', CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(c.max_length AS varchar(10)) END, ')')",
    "          WHEN ty.name IN ('nvarchar', 'nchar')",
    "            THEN CONCAT(ty.name, '(', CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(c.max_length / 2 AS varchar(10)) END, ')')",
    "          WHEN ty.name IN ('datetime2', 'datetimeoffset', 'time')",
    "            THEN CONCAT(ty.name, '(', c.scale, ')')",
    "          WHEN ty.name = 'float'",
    "            THEN CONCAT(ty.name, '(', c.precision, ')')",
    "          ELSE ty.name",
    "        END",
    "      )) AS formatted(data_type)",
    `      WHERE s.name = ${quoteSqlString(schema)}`,
    "        AND o.type IN ('U', 'V')",
    "    ) AS column_rows",
    "    GROUP BY column_rows.schema_name, column_rows.table_name, column_rows.object_type",
    "  ) AS table_rows",
    ") AS payload_rows",
    "GROUP BY payload_rows.batch_id",
  ].join("\n");
  const result = await context.services.dataPlane.dataEnvironmentSqlQuery({
    actor: context.actor,
    catalog: catalog.name,
    schema,
    max_pages: config.maxPages,
    sql: [
      "SELECT",
      `  ${quoteSqlString(catalog.name)} AS table_catalog,`,
      "  metadata.batch_id,",
      "  to_base64(metadata.metadata_gzip) AS metadata_gzip_base64",
      `FROM TABLE(${quoteTrinoIdentifier(catalog.name, "trino catalog")}.system.query(`,
      `  query => ${quoteSqlString(nativeSql)}`,
      ")) AS metadata",
    ].join("\n"),
  });

  const tableByIdentity = new Map<string, ScannedTable>();
  const columnsByIdentity = new Map<string, MaterialColumnSchema[]>();
  for (const [index, row] of result.rows.entries()) {
    const catalogName = stringCell(row[0], `sqlserver[${index}].table_catalog`);
    const batchId = nonNegativeCell(row[1], `sqlserver[${index}].batch_id`);
    const rawTables = gunzipJsonArray(
      row[2],
      `sqlserver batch ${batchId}.metadata_gzip_base64`,
    );
    for (const [tableIndex, value] of rawTables.entries()) {
      const label = `sqlserver batch ${batchId}.tables[${tableIndex}]`;
      if (!Array.isArray(value) || value.length !== 4 || !Array.isArray(value[3])) {
        throw new Error(`${label} must be [schema, table, type, columns]`);
      }
      const table: ScannedTable = {
        catalog: catalogName,
        schema: stringCell(value[0], `${label}.schema`),
        name: stringCell(value[1], `${label}.table`),
        type: stringCell(value[2], `${label}.type`),
      };
      const identity = tableIdentity(table);
      if (tableByIdentity.has(identity)) {
        throw new Error(`source ${source.name} returned duplicate table ${table.schema}.${table.name}`);
      }
      tableByIdentity.set(identity, table);
      columnsByIdentity.set(identity, value[3].map((columnValue, columnIndex) => {
        const columnLabel = `${label}.columns[${columnIndex}]`;
        if (!Array.isArray(columnValue) || columnValue.length !== 4) {
          throw new Error(`${columnLabel} must be [ordinal, name, data_type, nullable]`);
        }
        return {
          ordinal: positiveCell(columnValue[0], `${columnLabel}.ordinal`),
          name: stringCell(columnValue[1], `${columnLabel}.name`),
          data_type: stringCell(columnValue[2], `${columnLabel}.data_type`),
          nullable: booleanCell(columnValue[3], `${columnLabel}.nullable`),
        };
      }));
    }
  }
  return expectedMaterialsFromMetadata(source, tableByIdentity, columnsByIdentity);
}

async function scanInformationSchemaSourceMaterials(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  source: BuiltinDataSourceConfig,
  catalog: ConnectorActiveConfig,
): Promise<readonly ExpectedMaterial[]> {
  const schema = sourceSchema(source);
  const [tableResult, columnResult] = await Promise.all([
    context.services.dataPlane.dataEnvironmentSqlQuery({
      actor: context.actor,
      catalog: catalog.name,
      schema,
      max_pages: config.maxPages,
      sql: [
        "SELECT table_catalog, table_schema, table_name, table_type",
        "FROM information_schema.tables",
        `WHERE table_schema = ${quoteSqlString(schema)}`,
        "ORDER BY table_catalog, table_schema, table_name",
      ].join("\n"),
    }),
    context.services.dataPlane.dataEnvironmentSqlQuery({
      actor: context.actor,
      catalog: catalog.name,
      schema,
      max_pages: config.maxPages,
      sql: [
        "SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, is_nullable",
        "FROM information_schema.columns",
        `WHERE table_schema = ${quoteSqlString(schema)}`,
        "ORDER BY table_catalog, table_schema, table_name, ordinal_position",
      ].join("\n"),
    }),
  ]);

  const tables: ScannedTable[] = tableResult.rows.map((row, index) => ({
    catalog: stringCell(row[0], `tables[${index}].table_catalog`),
    schema: stringCell(row[1], `tables[${index}].table_schema`),
    name: stringCell(row[2], `tables[${index}].table_name`),
    type: stringCell(row[3], `tables[${index}].table_type`),
  }));
  const tableByIdentity = uniqueMap(tables, tableIdentity, `source ${source.name} table`);
  const columnsByIdentity = new Map<string, MaterialColumnSchema[]>();
  for (const [index, row] of columnResult.rows.entries()) {
    const identity = tableIdentity({
      catalog: stringCell(row[0], `columns[${index}].table_catalog`),
      schema: stringCell(row[1], `columns[${index}].table_schema`),
      name: stringCell(row[2], `columns[${index}].table_name`),
    });
    if (!tableByIdentity.has(identity)) {
      throw new Error(`source ${source.name} changed while scanning; column metadata has no matching table`);
    }
    const ordinal = positiveCell(row[4], `columns[${index}].ordinal_position`);
    const list = columnsByIdentity.get(identity) ?? [];
    list.push({
      ordinal,
      name: stringCell(row[3], `columns[${index}].column_name`),
      data_type: stringCell(row[5], `columns[${index}].data_type`),
      nullable: String(row[6] ?? "").toUpperCase() !== "NO",
    });
    columnsByIdentity.set(identity, list);
  }

  return expectedMaterialsFromMetadata(source, tableByIdentity, columnsByIdentity);
}

function expectedMaterialsFromMetadata(
  source: BuiltinDataSourceConfig,
  tableByIdentity: ReadonlyMap<string, ScannedTable>,
  columnsByIdentity: ReadonlyMap<string, MaterialColumnSchema[]>,
): readonly ExpectedMaterial[] {
  return [...tableByIdentity.values()].map((table) => {
    const columns = columnsByIdentity.get(tableIdentity(table));
    if (!columns || columns.length === 0) {
      throw new Error(`source table ${table.catalog}.${table.schema}.${table.name} exposes no columns`);
    }
    columns.sort((left, right) => left.ordinal - right.ordinal || left.name.localeCompare(right.name));
    const materialSchema: MaterialSchema = {
      format_version: 1,
      table: {
        catalog: table.catalog,
        schema: table.schema,
        name: table.name,
        type: table.type,
      },
      columns,
    };
    return {
      sourceMaterialKey: hashValue([source.name, table.catalog, table.schema, table.name]),
      dataSourceNameKey: source.name,
      dataSourceType: source.builtinKind,
      sourceCatalogName: table.catalog,
      sourceSchemaName: table.schema,
      sourceTableName: table.name,
      sourceTableType: table.type,
      materialSchema,
      schemaHash: hashValue(materialSchema),
    };
  }).sort(compareExpectedMaterials);
}

async function loadDataSources(
  context: PipelineRunContext,
  table: string,
  recordInputs: PipelineRecordInputs,
): Promise<readonly DataSourceConfig[]> {
  const rows = recordInputs.get(table)
    ?? await loadAllControlTableRows(context, table, "name");
  const sources = rows.map((row, index) => {
    const name = textValue(row.name);
    if (!name) {
      throw new Error(`${table} row[${index}] requires name`);
    }
    return {
      name,
      kind: textValue(row.kind) ?? "",
      ...(textValue(row.builtin_kind) ? { builtinKind: textValue(row.builtin_kind) } : {}),
      connection: jsonObject(row.connection, `${table} row[${index}].connection`),
    };
  });
  uniqueMap(sources, (source) => source.name, "data source");
  return sources.sort((left, right) => left.name.localeCompare(right.name));
}

async function loadCurrentMaterials(
  context: PipelineRunContext,
  table: string,
): Promise<readonly CurrentMaterial[]> {
  const rows = await loadAllDataTableRows(context, table, "source_material_key");
  return rows.map((row, index) => ({
    sourceMaterialKey: requiredRowText(row, "source_material_key", table, index),
    dataSourceNameKey: requiredRowText(row, "data_source_name_key", table, index),
    dataSourceType: requiredRowText(row, "data_source_type", table, index),
    sourceCatalogName: requiredRowText(row, "source_catalog_name", table, index),
    sourceSchemaName: requiredRowText(row, "source_schema_name", table, index),
    sourceTableName: requiredRowText(row, "source_table_name", table, index),
    sourceTableType: requiredRowText(row, "source_table_type", table, index),
    materialSchema: jsonValue(row.material_schema, `${table} row[${index}].material_schema`),
    schemaHash: requiredRowText(row, "schema_hash", table, index),
    lastChangedRunId: requiredRowText(row, "last_changed_run_id", table, index),
    createdAt: timestampText(row.created_at, `${table} row[${index}].created_at`),
    updatedAt: timestampText(row.updated_at, `${table} row[${index}].updated_at`),
  }));
}

async function loadAllControlTableRows(
  context: PipelineRunContext,
  table: string,
  primaryKeyField: string,
): Promise<readonly Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for (let offset = 0; ; offset += CONTROL_QUERY_PAGE_SIZE) {
    const page = await context.services.controlPlane.businessTableRowsQuery({
      table,
      query: {
        order_by: [{ field: primaryKeyField, direction: "asc" }],
        limit: CONTROL_QUERY_PAGE_SIZE,
        offset,
        include_deleted: false,
      },
    });
    rows.push(...page.rows);
    if (page.rows.length < CONTROL_QUERY_PAGE_SIZE) {
      return rows;
    }
  }
}

async function loadAllDataTableRows(
  context: PipelineRunContext,
  table: string,
  primaryKeyField: string,
): Promise<readonly Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for (let offset = 0; ; offset += CONTROL_QUERY_PAGE_SIZE) {
    const page = await context.services.dataPlane.dataTableRowsQuery({
      table,
      query: {
        order_by: [{ field: primaryKeyField, direction: "asc" }],
        limit: CONTROL_QUERY_PAGE_SIZE,
        offset,
        include_deleted: false,
      },
    });
    rows.push(...page.rows);
    if (page.rows.length < CONTROL_QUERY_PAGE_SIZE) {
      return rows;
    }
  }
}

function requireBuiltinDataSource(source: DataSourceConfig): BuiltinDataSourceConfig {
  if (
    source.kind !== "builtin"
    || (source.builtinKind !== "pg" && source.builtinKind !== "mysql" && source.builtinKind !== "sqlserver")
  ) {
    throw new Error(`data source ${source.name} does not expose a supported builtin relational connector`);
  }
  return source as BuiltinDataSourceConfig;
}

function activeCatalog(
  source: BuiltinDataSourceConfig,
  context: PipelineRunContext,
  catalogs: readonly ConnectorActiveConfig[],
): ConnectorActiveConfig {
  const expected = externalCatalogName({
    tenantId: context.services.scope.tenantId,
    env: context.services.scope.env,
    sourceName: source.name,
  });
  const catalog = catalogs.find((entry) => entry.key === expected || entry.name === expected);
  if (!catalog) {
    throw new Error(`trino connector ${expected} for data source ${source.name} is not active; run data source load first`);
  }
  return catalog;
}

function sourceSchema(source: BuiltinDataSourceConfig): string {
  const configured = textValue(source.connection.schema);
  if (configured) {
    return configured;
  }
  if (source.builtinKind === "pg") {
    return "public";
  }
  if (source.builtinKind === "sqlserver") {
    return "dbo";
  }
  const database = textValue(source.connection.database);
  if (!database) {
    throw new Error(`data source ${source.name}.connection.database is required`);
  }
  return database;
}

function materialChanged(previous: CurrentMaterial, expected: ExpectedMaterial): boolean {
  return previous.schemaHash !== expected.schemaHash
    || previous.dataSourceType !== expected.dataSourceType
    || previous.sourceCatalogName !== expected.sourceCatalogName
    || previous.sourceSchemaName !== expected.sourceSchemaName
    || previous.sourceTableName !== expected.sourceTableName
    || previous.sourceTableType !== expected.sourceTableType;
}

async function currentMaterialSnapshotId(
  context: PipelineRunContext,
  table: string,
): Promise<string | undefined> {
  const version = await context.services.controlPlane.tableVersionGet({
    namespace_kind: "data",
    table,
  });
  return textValue(version.current_snapshot_id);
}

function runningState(
  previous: SourceMaterialStateRow | undefined,
  input: { runId: string; configHash: string; startedAt: string },
): SourceMaterialReconcileState {
  return {
    status: "running",
    run_id: input.runId,
    config_hash: input.configHash,
    started_at: input.startedAt,
    updated_at: new Date().toISOString(),
    ...lastSuccessfulState(previous),
  };
}

async function writeFailedState(
  context: PipelineRunContext,
  config: SourceMaterialLoadConfig,
  input: {
    key: string;
    previous?: SourceMaterialStateRow;
    runId: string;
    configHash: string;
    startedAt: string;
    error: unknown;
  },
): Promise<void> {
  try {
    await writeSourceMaterialState(context, {
      table: config.stateTable,
      key: input.key,
      leaseMs: context.heartbeatLeaseMs,
      state: {
        status: "failed",
        run_id: input.runId,
        config_hash: input.configHash,
        started_at: input.startedAt,
        updated_at: new Date().toISOString(),
        error: errorMessage(input.error),
        ...lastSuccessfulState(input.previous),
      },
    });
  } catch (stateError) {
    throw new AggregateError(
      [asError(input.error), asError(stateError)],
      `source material reconcile and failed-state write both failed for ${input.key}`,
    );
  }
}

function lastSuccessfulState(
  previous: SourceMaterialStateRow | undefined,
): Pick<SourceMaterialReconcileState,
  "last_successful_run_id" | "last_successful_material_set_hash" | "last_successful_snapshot_id" | "last_successful_at"> {
  const state = previous?.reconcile_state;
  const runId = state?.status === "ready" ? state.run_id : state?.last_successful_run_id;
  const materialSetHash = state?.status === "ready"
    ? state.material_set_hash
    : state?.last_successful_material_set_hash;
  const successfulAt = state?.status === "ready" ? state.updated_at : state?.last_successful_at;
  const snapshotId = state?.status === "ready"
    ? state.after_snapshot_id
    : state?.last_successful_snapshot_id;
  return {
    ...(runId ? { last_successful_run_id: runId } : {}),
    ...(materialSetHash ? { last_successful_material_set_hash: materialSetHash } : {}),
    ...(snapshotId ? { last_successful_snapshot_id: snapshotId } : {}),
    ...(successfulAt ? { last_successful_at: successfulAt } : {}),
  };
}

function resumableRun(
  previous: SourceMaterialStateRow | undefined,
  configHash: string,
): { runId: string; startedAt: string } {
  const state = previous?.reconcile_state;
  if (
    state
    && state.status !== "ready"
    && state.config_hash === configHash
    && state.run_id
    && state.started_at
  ) {
    return { runId: state.run_id, startedAt: state.started_at };
  }
  return { runId: randomUUID(), startedAt: new Date().toISOString() };
}

function sourceMaterialLoadConfig(value: unknown): SourceMaterialLoadConfig {
  const raw = value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
  return {
    sourceTable: configText(raw.sourceTable, "sourceTable", "t_factory_config_source"),
    materialTable: configText(raw.materialTable, "materialTable", "t_factory_config_source_materials"),
    stateTable: configText(raw.stateTable, "stateTable", "t_factory_state_source_material"),
    maxPages: positiveInteger(raw.maxPages, "maxPages") ?? 100,
    writeBatchSize: sourceMaterialWriteBatchSize(raw.writeBatchSize),
  };
}

function sourceMaterialWriteBatchSize(value: unknown): number {
  const size = positiveInteger(value, "writeBatchSize") ?? MAX_WRITE_BATCH_SIZE;
  if (size > MAX_WRITE_BATCH_SIZE) {
    throw new Error(`source material load config writeBatchSize must not exceed ${MAX_WRITE_BATCH_SIZE}`);
  }
  return size;
}

function configText(
  value: unknown,
  label: string,
  fallback: string,
): string {
  if (value === undefined) {
    return fallback;
  }
  const text = textValue(value);
  if (!text) {
    throw new Error(`source material load config ${label} must be a non-empty string`);
  }
  return text;
}

function positiveInteger(value: unknown, label: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error(`source material load config ${label} must be a positive integer`);
  }
  return value;
}

function chunks<T>(values: readonly T[], size: number): readonly (readonly T[])[] {
  const batches: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    batches.push(values.slice(index, index + size));
  }
  return batches;
}

function externalCatalogName(input: {
  tenantId: string;
  env: string;
  sourceName: string;
}): string {
  return [
    catalogToken(input.tenantId, "tenant"),
    catalogToken(input.env, "env"),
    catalogToken(input.sourceName, "source"),
  ].join("_");
}

function catalogToken(value: string, label: string): string {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!normalized) {
    throw new Error(`${label} must contain at least one catalog-safe character`);
  }
  return /^[a-z]/.test(normalized) ? normalized : `${label}_${normalized}`;
}

function tableIdentity(table: { catalog: string; schema: string; name: string }): string {
  return stableJson([table.catalog, table.schema, table.name]);
}

function compareExpectedMaterials(left: ExpectedMaterial, right: ExpectedMaterial): number {
  return left.sourceMaterialKey.localeCompare(right.sourceMaterialKey);
}

function compareCurrentMaterials(left: CurrentMaterial, right: CurrentMaterial): number {
  return left.sourceMaterialKey.localeCompare(right.sourceMaterialKey);
}

function uniqueMap<T>(
  values: readonly T[],
  keyOf: (value: T) => string,
  label: string,
): Map<string, T> {
  const result = new Map<string, T>();
  for (const value of values) {
    const key = keyOf(value);
    if (result.has(key)) {
      throw new Error(`duplicate ${label}: ${key}`);
    }
    result.set(key, value);
  }
  return result;
}

function groupBy<T>(values: readonly T[], keyOf: (value: T) => string): Map<string, T[]> {
  const result = new Map<string, T[]>();
  for (const value of values) {
    const key = keyOf(value);
    const group = result.get(key) ?? [];
    group.push(value);
    result.set(key, group);
  }
  return result;
}

function hashValue(value: unknown): string {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

function stableJson(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => stableValue(entry));
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => entry !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableValue(entry)]),
  );
}

function jsonObject(value: unknown, label: string): Record<string, unknown> {
  const parsed = jsonValue(value, label);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function jsonValue(value: unknown, label: string): unknown {
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw new Error(`${label} must contain valid JSON`);
  }
}

function requiredRowText(
  row: Record<string, unknown>,
  field: string,
  table: string,
  index: number,
): string {
  const value = textValue(row[field]);
  if (!value) {
    throw new Error(`${table} row[${index}] requires ${field}`);
  }
  return value;
}

function textValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function stringCell(value: unknown, label: string): string {
  const text = textValue(value);
  if (!text) {
    throw new Error(`source metadata ${label} must be a non-empty string`);
  }
  return text;
}

function positiveCell(value: unknown, label: string): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`source metadata ${label} must be a positive integer`);
  }
  return parsed;
}

function nonNegativeCell(value: unknown, label: string): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`source metadata ${label} must be a non-negative integer`);
  }
  return parsed;
}

function booleanCell(value: unknown, label: string): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (value === 0 || value === 1) {
    return value === 1;
  }
  throw new Error(`source metadata ${label} must be a boolean`);
}

function gunzipJsonArray(value: unknown, label: string): unknown[] {
  const encoded = stringCell(value, label);
  try {
    const parsed = JSON.parse(gunzipSync(Buffer.from(encoded, "base64")).toString("utf16le")) as unknown;
    if (!Array.isArray(parsed)) {
      throw new Error("decoded payload is not an array");
    }
    return parsed;
  } catch (error) {
    throw new Error(`source metadata ${label} must contain gzip-compressed JSON: ${errorMessage(error)}`);
  }
}

function timestampText(value: unknown, label: string): string {
  const date = value instanceof Date ? value : new Date(String(value ?? ""));
  if (Number.isNaN(date.getTime())) {
    throw new Error(`${label} must be a valid timestamp`);
  }
  return date.toISOString();
}

function quoteSqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function quoteTrinoIdentifier(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized || normalized.includes("\0")) {
    throw new Error(`${label} must be a non-empty SQL identifier`);
  }
  return `"${normalized.replace(/"/g, '""')}"`;
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("source material load requires a concrete tenantId/env context");
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function errorMessage(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, 2000);
}
