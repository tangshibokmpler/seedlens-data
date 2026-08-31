import {
  Pipeline,
  resolveSourceColumnType,
  trinoSourceColumnExpression,
} from "@agentnexus/lakecore/model";
import {
  assertTargetedRecordInputs,
  resolveRecordInputs,
  type PipelineRecordInputs,
} from "./record-inputs.js";
import type { PipelineRunContext } from "./types.js";

type BuiltinDataSourceKind = "pg" | "mysql" | "sqlserver";
type ImplementationKind = "builtin" | "custom";
type DatasetRow = Record<string, unknown>;
type PipelineKey = string | number | boolean;

interface DataPipelineConfig {
  pipelineTable: string;
  dataSourceTable: string;
  materialTable: string;
}

interface PipelineConfigRow {
  name: string;
  data_source_key: string;
  purpose: "physical_ingestion" | "virtual_projection";
  kind: ImplementationKind;
  pipeline_code_key: string;
  data_asset_name: string;
  processing_config: Record<string, unknown>;
}

interface BuiltinDataSource {
  name: string;
  kind: "builtin";
  builtin_kind: BuiltinDataSourceKind;
  connection: {
    host: string;
    database: string;
    schema?: string;
    user: string;
  };
}

interface FieldMapping {
  sourceField: string;
  targetField: string;
  targetType?: "string";
}

interface BuiltinProcessingConfig {
  source: {
    table: string;
    schema?: string;
  };
  fieldMappings: readonly FieldMapping[];
}

interface MatchedPipelineConfig {
  name: string;
  dataSourceKey: string;
  assetTable: string;
  processing: BuiltinProcessingConfig;
}

interface SourceRef {
  sourceKind: BuiltinDataSourceKind;
  catalog: string;
  schema: string;
  table: string;
  qualified: string;
  columns: readonly SourceColumnInfo[];
}

interface SourceColumnInfo {
  name: string;
  type: string;
}

interface DatasetSyncSummary {
  sourceRows: number;
  targetRows: number;
  inserted: number;
  updated: number;
  deleted: number;
  unchanged: number;
}

interface KeyedDatasetRow {
  key: PipelineKey;
  row: DatasetRow;
}

const MAX_WRITE_BATCH_SIZE = 1_000;

export class BuiltinPhysicalTableDataPipeline {
  @Pipeline({ tag: "dataFactory", pipelineKey: "builtin_physical_table_data" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：把外部源数据加工后写入 manifest 已独立定义和调谐的物理资产表。
 * 输入：物理管道配置、数据源配置、已登记的源物料及外部源表；pipeline 只读取 data_asset_name，不创建或修改目标表结构。
 * 逻辑：解析字段映射并读取源数据，通过 DataPlane 按目标表自身主键批量写入，再删除源中已不存在的目标记录。
 * 幂等：采用 reconcile/cleanup 策略；相同输入重复执行不产生重复记录，源中缺失的目标记录会被清理。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = dataPipelineConfig(context.config);
  const recordInputs = resolveRecordInputs(context.config);
  assertTargetedRecordInputs(recordInputs, [
    config.pipelineTable,
    config.dataSourceTable,
    config.materialTable,
  ]);

  const [pipelineRows, dataSources, sourceMaterials] = await Promise.all([
    loadPipelineConfigRows(context, config.pipelineTable, recordInputs),
    loadDataSources(context, config.dataSourceTable, recordInputs),
    loadSourceMaterials(context, config.materialTable, recordInputs),
  ]);
  const matched = matchedPipelineConfigs(context, pipelineRows);
  const dataSourcesByName = new Map(dataSources.map((source) => [source.name, source]));
  const expectedAssetTables = new Set<string>();

  for (const configRow of matched) {
    if (context.signal.aborted) {
      throw new Error(`${context.pipelineKey} aborted`);
    }
    if (expectedAssetTables.has(configRow.assetTable)) {
      throw new Error(
        `pipeline ${context.pipelineKey} has duplicate target asset table ${configRow.assetTable}`,
      );
    }
    expectedAssetTables.add(configRow.assetTable);

    const dataSource = dataSourcesByName.get(configRow.dataSourceKey);
    if (!dataSource) {
      throw new Error(
        `pipeline config ${configRow.name} references missing data source ${configRow.dataSourceKey}`,
      );
    }
    const source = resolveSourceRef(dataSource, configRow.processing, sourceMaterials);
    const sourceColumns = source.columns;
    assertSourceMappingsExist(configRow, sourceColumns);
    const sourceRows = await loadSourceDatasetRows(
      context,
      configRow,
      source,
      sourceColumns,
    );
    const summary = await reconcileDatasetRows(
      context,
      configRow,
      sourceRows,
      sourceColumns,
      source.sourceKind,
    );
    if (summary.inserted + summary.updated + summary.deleted > 0) {
      context.logger.info("physical asset data changed", {
        config: configRow.name,
        assetTable: configRow.assetTable,
        sourceRows: summary.sourceRows,
        targetRows: summary.targetRows,
        inserted: summary.inserted,
        updated: summary.updated,
        deleted: summary.deleted,
      });
    }
  }
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("builtin physical table data requires a concrete tenantId/env context");
  }
}

function dataPipelineConfig(config: unknown): DataPipelineConfig {
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    throw new Error(
      "builtin physical table data requires pipelineTable, dataSourceTable, and materialTable config",
    );
  }
  const raw = config as Record<string, unknown>;
  const pipelineTable = textField(raw, "pipelineTable");
  const dataSourceTable = textField(raw, "dataSourceTable");
  const materialTable = textField(raw, "materialTable");
  if (!pipelineTable || !dataSourceTable || !materialTable) {
    throw new Error(
      "builtin physical table data requires pipelineTable, dataSourceTable, and materialTable config",
    );
  }
  return { pipelineTable, dataSourceTable, materialTable };
}

async function loadPipelineConfigRows(
  context: PipelineRunContext,
  table: string,
  recordInputs: PipelineRecordInputs,
): Promise<readonly PipelineConfigRow[]> {
  const rows = await loadControlRows(context, table, "pipeline config", recordInputs);
  return rows.map((row, index) => pipelineConfigRow(row, index));
}

async function loadDataSources(
  context: PipelineRunContext,
  table: string,
  recordInputs: PipelineRecordInputs,
): Promise<readonly BuiltinDataSource[]> {
  const rows = await loadControlRows(context, table, "data source", recordInputs);
  const dataSources = rows
    .map((row, index) => dataSourceRow(row, index))
    .filter((source): source is BuiltinDataSource => Boolean(source));
  const skipped = rows.length - dataSources.length;
  if (skipped > 0) {
    context.logger.warn("builtin physical table data skipped unsupported data sources", {
      table,
      skipped,
    });
  }
  return dataSources;
}

async function loadSourceMaterials(
  context: PipelineRunContext,
  table: string,
  recordInputs: PipelineRecordInputs,
): Promise<readonly Record<string, unknown>[]> {
  const records = recordInputs.get(table);
  if (records) return records;
  return context.services.dataPlane.dataTableRowsList({
    table,
    include_deleted: false,
  });
}

async function loadControlRows(
  context: PipelineRunContext,
  table: string,
  label: string,
  recordInputs: PipelineRecordInputs,
): Promise<readonly Record<string, unknown>[]> {
  const records = recordInputs.get(table);
  if (records) {
    return records;
  }
  try {
    return await context.services.controlPlane.businessTableRowsList({
      table,
      include_deleted: false,
    });
  } catch (error) {
    if (isMissingTableError(error, table)) {
      context.logger.warn(`builtin physical table data skipped env without ${label} table`, {
        tenantId: context.services.scope.tenantId,
        env: context.services.scope.env,
        table,
        reason: errorMessage(error),
      });
      return [];
    }
    throw error;
  }
}

function pipelineConfigRow(row: Record<string, unknown>, index: number): PipelineConfigRow {
  const name = textField(row, "name");
  const dataSourceKey = textField(row, "data_source_key");
  const purpose = textField(row, "purpose");
  const implementationKind = textField(row, "kind");
  const pipelineCodeKey = textField(row, "pipeline_code_key");
  const dataAssetName = textField(row, "data_asset_name");
  if (!name || !dataSourceKey || !purpose || !implementationKind || !pipelineCodeKey || !dataAssetName) {
    throw new Error(
      `pipeline config row[${index}] requires name, data_source_key, purpose, kind, pipeline_code_key, and data_asset_name`,
    );
  }
  if (purpose !== "physical_ingestion" && purpose !== "virtual_projection") {
    throw new Error(
      `pipeline config row[${index}].purpose must be physical_ingestion or virtual_projection`,
    );
  }
  if (implementationKind !== "builtin" && implementationKind !== "custom") {
    throw new Error(`pipeline config row[${index}].kind must be builtin or custom`);
  }
  return {
    name,
    data_source_key: dataSourceKey,
    purpose,
    kind: implementationKind,
    pipeline_code_key: pipelineCodeKey,
    data_asset_name: dataAssetName,
    processing_config: jsonObjectField(
      row.processing_config,
      `pipeline config ${name}.processing_config`,
    ),
  };
}

function dataSourceRow(
  row: Record<string, unknown>,
  index: number,
): BuiltinDataSource | undefined {
  const name = textField(row, "name");
  const kind = textField(row, "kind");
  const builtinKind = textField(row, "builtin_kind");
  if (
    kind !== "builtin"
    || (builtinKind !== "pg" && builtinKind !== "mysql" && builtinKind !== "sqlserver")
  ) {
    return undefined;
  }
  const connection = jsonObjectField(row.connection, `data source row[${index}].connection`);
  const host = textField(connection, "host");
  const database = textField(connection, "database");
  const user = textField(connection, "user");
  if (!name || !host || !database || !user) {
    throw new Error(
      `builtin data source row[${index}] requires name, connection.host, connection.database, and connection.user`,
    );
  }
  return {
    name,
    kind: "builtin",
    builtin_kind: builtinKind,
    connection: {
      host,
      database,
      schema: textField(connection, "schema"),
      user,
    },
  };
}

function matchedPipelineConfigs(
  context: PipelineRunContext,
  rows: readonly PipelineConfigRow[],
): readonly MatchedPipelineConfig[] {
  return rows
    .filter((row) =>
      row.purpose === "physical_ingestion"
      && row.kind === "builtin"
      && row.pipeline_code_key === context.pipelineKey)
    .map((row) => ({
      name: row.name,
      dataSourceKey: row.data_source_key,
      assetTable: row.data_asset_name,
      processing: normalizeProcessingConfig(row),
    }));
}

function normalizeProcessingConfig(row: PipelineConfigRow): BuiltinProcessingConfig {
  const config = row.processing_config;
  const source = jsonObjectField(
    config.source,
    `pipeline config ${row.name}.processing_config.source`,
  );
  const sourceTable = textField(source, "table");
  const sourceSchema = textField(source, "schema");
  if (!sourceTable) {
    throw new Error(`pipeline config ${row.name} requires processing_config.source.table`);
  }
  const mappings = Array.isArray(config.field_mappings)
    ? config.field_mappings.map((entry, index) => fieldMapping(entry, row.name, index))
    : [];
  if (mappings.length === 0) {
    throw new Error(`pipeline config ${row.name} requires at least one field_mappings entry`);
  }
  const targetFields = new Set<string>();
  for (const mapping of mappings) {
    if (targetFields.has(mapping.targetField)) {
      throw new Error(`pipeline config ${row.name} has duplicate target field ${mapping.targetField}`);
    }
    targetFields.add(mapping.targetField);
  }
  return {
    source: {
      table: sourceTable,
      ...(sourceSchema ? { schema: sourceSchema } : {}),
    },
    fieldMappings: mappings,
  };
}

function fieldMapping(entry: unknown, configName: string, index: number): FieldMapping {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    throw new Error(`pipeline config ${configName} field_mappings[${index}] must be a JSON object`);
  }
  const raw = entry as Record<string, unknown>;
  const sourceField = textField(raw, "source_field");
  const targetField = textField(raw, "target_field");
  const targetType = textField(raw, "target_type");
  if (!sourceField || !targetField) {
    throw new Error(
      `pipeline config ${configName} field_mappings[${index}] requires source_field and target_field`,
    );
  }
  if (targetType && targetType !== "string") {
    throw new Error(
      `pipeline config ${configName} field_mappings[${index}].target_type only supports string`,
    );
  }
  return {
    sourceField,
    targetField,
    ...(targetType === "string" ? { targetType } : {}),
  };
}

function resolveSourceRef(
  dataSource: BuiltinDataSource,
  processing: BuiltinProcessingConfig,
  sourceMaterials: readonly Record<string, unknown>[],
): SourceRef {
  const schema = processing.source.schema;
  if (!schema) {
    throw new Error(`pipeline source ${dataSource.name} requires processing_config.source.schema`);
  }
  const matches = sourceMaterials.filter((row) =>
    textField(row, "data_source_name_key") === dataSource.name
    && textField(row, "source_schema_name")?.toLowerCase() === schema.toLowerCase()
    && textField(row, "source_table_name")?.toLowerCase() === processing.source.table.toLowerCase());
  if (matches.length !== 1) {
    throw new Error(
      `source material ${dataSource.name}/${schema}.${processing.source.table} must resolve exactly once; received ${matches.length}`,
    );
  }
  const material = matches[0]!;
  const catalog = requireNonEmpty(
    textField(material, "source_catalog_name"),
    "source material catalog",
  );
  const materialSchema = jsonObjectField(material.material_schema, "source material material_schema");
  if (!Array.isArray(materialSchema.columns) || materialSchema.columns.length === 0) {
    throw new Error("source material material_schema.columns must contain at least one column");
  }
  const columns = materialSchema.columns.map((value, index) => {
    const column = jsonObjectField(value, `source material column[${index}]`);
    return {
      name: requireNonEmpty(textField(column, "name"), `source material column[${index}].name`),
      type: requireNonEmpty(
        textField(column, "data_type"),
        `source material column[${index}].data_type`,
      ),
    };
  });
  return {
    sourceKind: dataSource.builtin_kind,
    catalog,
    schema,
    table: processing.source.table,
    qualified: qualifiedTrinoTable(catalog, schema, processing.source.table),
    columns,
  };
}

function assertSourceMappingsExist(
  config: MatchedPipelineConfig,
  sourceColumns: readonly SourceColumnInfo[],
): void {
  const sourceFieldNames = new Set(sourceColumns.map((column) => column.name.toLowerCase()));
  for (const mapping of config.processing.fieldMappings) {
    if (!sourceFieldNames.has(mapping.sourceField.toLowerCase())) {
      throw new Error(
        `pipeline config ${config.name} references missing source column ${mapping.sourceField}`,
      );
    }
  }
}

async function loadSourceDatasetRows(
  context: PipelineRunContext,
  config: MatchedPipelineConfig,
  source: SourceRef,
  sourceColumns: readonly SourceColumnInfo[],
): Promise<readonly DatasetRow[]> {
  const targetFields = config.processing.fieldMappings.map((mapping) => mapping.targetField);
  const sourceColumnsByName = new Map(
    sourceColumns.map((column) => [column.name.toLowerCase(), column]),
  );
  const selectList = config.processing.fieldMappings
    .map((mapping) => {
      const column = sourceColumnsByName.get(mapping.sourceField.toLowerCase());
      if (!column) {
        throw new Error(
          `pipeline config ${config.name} references missing source column ${mapping.sourceField}`,
        );
      }
      const sourceField = quoteTrinoIdentifier(mapping.sourceField, "source column");
      const conversion = mappedSourceColumnType(
        column.type,
        source.sourceKind,
        mapping.targetType,
      );
      const expression = trinoSourceColumnExpression(sourceField, conversion);
      return `${expression} AS ${quoteTrinoIdentifier(mapping.targetField, "target column")}`;
    })
    .join(", ");
  const result = await context.services.dataPlane.dataEnvironmentSqlQuery({
    actor: context.actor,
    catalog: source.catalog,
    schema: source.schema,
    max_pages: 1_000,
    sql: `SELECT ${selectList} FROM ${source.qualified}`,
  });
  return result.rows.map((row) => rowFromValues(targetFields, row));
}

async function reconcileDatasetRows(
  context: PipelineRunContext,
  config: MatchedPipelineConfig,
  sourceRows: readonly DatasetRow[],
  sourceColumns: readonly SourceColumnInfo[],
  sourceKind: BuiltinDataSourceKind,
): Promise<DatasetSyncSummary> {
  const targetRows = await context.services.dataPlane.dataTableRowsList({
    table: config.assetTable,
  });
  const firstWrite = sourceRows.length > 0
    ? sourceRows.slice(0, MAX_WRITE_BATCH_SIZE)
    : targetRows.slice(0, 1);
  if (firstWrite.length === 0) {
    return emptySummary(sourceRows.length, targetRows.length);
  }

  const firstResult = await context.services.dataPlane.dataTableRowsSet({
    actor: context.actor,
    table: config.assetTable,
    records: firstWrite,
  });
  const primaryKey = firstResult.primary_key_field;
  const fieldTypes = targetFieldTypes(config, sourceColumns, sourceKind);
  if (!fieldTypes.has(primaryKey)) {
    throw new Error(
      `pipeline config ${config.name} must map target table primary key ${primaryKey}`,
    );
  }
  const sourceByKey = rowsByPrimaryKey(
    config.name,
    "source",
    primaryKey,
    sourceRows,
    fieldTypes,
  );
  const targetByKey = rowsByPrimaryKey(
    config.name,
    "target",
    primaryKey,
    targetRows,
    fieldTypes,
  );
  const summary = emptySummary(sourceRows.length, targetRows.length);
  if (sourceRows.length > 0) {
    addSetResult(summary, firstResult);
    for (let offset = firstWrite.length; offset < sourceRows.length; offset += MAX_WRITE_BATCH_SIZE) {
      const result = await context.services.dataPlane.dataTableRowsSet({
        actor: context.actor,
        table: config.assetTable,
        records: sourceRows.slice(offset, offset + MAX_WRITE_BATCH_SIZE),
      });
      if (result.primary_key_field !== primaryKey) {
        throw new Error(`target table ${config.assetTable} primary key changed during pipeline run`);
      }
      addSetResult(summary, result);
    }
  }

  const staleKeys = [...targetByKey]
    .filter(([keyToken]) => !sourceByKey.has(keyToken))
    .map(([, entry]) => entry.key);
  for (let offset = 0; offset < staleKeys.length; offset += MAX_WRITE_BATCH_SIZE) {
    const result = await context.services.dataPlane.dataTableRowsDelete({
      actor: context.actor,
      table: config.assetTable,
      keys: staleKeys.slice(offset, offset + MAX_WRITE_BATCH_SIZE),
    });
    summary.deleted += result.deleted_rows;
  }
  return summary;
}

function emptySummary(sourceRows: number, targetRows: number): DatasetSyncSummary {
  return {
    sourceRows,
    targetRows,
    inserted: 0,
    updated: 0,
    deleted: 0,
    unchanged: 0,
  };
}

function addSetResult(
  summary: DatasetSyncSummary,
  result: {
    inserted_rows: number;
    updated_rows: number;
    unchanged_rows: number;
  },
): void {
  summary.inserted += result.inserted_rows;
  summary.updated += result.updated_rows;
  summary.unchanged += result.unchanged_rows;
}

function rowFromValues(fields: readonly string[], values: readonly unknown[]): DatasetRow {
  const row: DatasetRow = {};
  for (const [index, field] of fields.entries()) {
    row[field] = values[index] ?? null;
  }
  return row;
}

function rowsByPrimaryKey(
  configName: string,
  label: "source" | "target",
  primaryKey: string,
  rows: readonly DatasetRow[],
  fieldTypes: ReadonlyMap<string, string>,
): Map<string, KeyedDatasetRow> {
  const byKey = new Map<string, KeyedDatasetRow>();
  const keyType = fieldTypes.get(primaryKey);
  for (const row of rows) {
    const key = primaryKeyValue(configName, label, primaryKey, row);
    const keyToken = JSON.stringify(normalizeComparableValue(key, keyType));
    if (byKey.has(keyToken)) {
      throw new Error(
        `pipeline config ${configName} loaded duplicate ${label} primary key ${String(key)}`,
      );
    }
    byKey.set(keyToken, { key, row });
  }
  return byKey;
}

function primaryKeyValue(
  configName: string,
  label: "source" | "target",
  primaryKey: string,
  row: DatasetRow,
): PipelineKey {
  const value = row[primaryKey];
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  throw new Error(
    `pipeline config ${configName} loaded ${label} row without scalar primary key ${primaryKey}`,
  );
}

function targetFieldTypes(
  config: MatchedPipelineConfig,
  sourceColumns: readonly SourceColumnInfo[],
  sourceKind: BuiltinDataSourceKind,
): Map<string, string> {
  const sourceColumnsByName = new Map(
    sourceColumns.map((column) => [column.name.toLowerCase(), column]),
  );
  const fieldTypes = new Map<string, string>();
  for (const mapping of config.processing.fieldMappings) {
    const sourceColumn = sourceColumnsByName.get(mapping.sourceField.toLowerCase());
    if (sourceColumn) {
      fieldTypes.set(
        mapping.targetField,
        mappedSourceColumnType(sourceColumn.type, sourceKind, mapping.targetType).modelType,
      );
    }
  }
  return fieldTypes;
}

function normalizeComparableValue(value: unknown, type: string | undefined): unknown {
  if (value === undefined || value === null) {
    return null;
  }
  if (value instanceof Date) {
    return type === "date" ? value.toISOString().slice(0, 10) : value.toISOString();
  }
  switch (type) {
    case "integer":
    case "int":
    case "long":
    case "float":
    case "double": {
      const numeric = typeof value === "number" ? value : Number(value);
      return Number.isFinite(numeric) ? numeric : String(value);
    }
    case "boolean": {
      if (typeof value === "boolean") return value;
      if (typeof value === "string") {
        const normalized = value.trim().toLowerCase();
        if (["true", "1", "yes", "on"].includes(normalized)) return true;
        if (["false", "0", "no", "off"].includes(normalized)) return false;
      }
      return value;
    }
    default:
      return String(value);
  }
}

function mappedSourceColumnType(
  type: string,
  sourceKind: BuiltinDataSourceKind,
  targetType?: "string",
) {
  return resolveSourceColumnType(type, {
    sourceKind,
    targetType,
  });
}

function quoteTrinoIdentifier(value: string, label = "trino identifier"): string {
  const normalized = requireNonEmpty(value, label);
  return `"${normalized.replace(/"/g, "\"\"")}"`;
}

function qualifiedTrinoTable(catalog: string, schema: string, table: string): string {
  return [
    quoteTrinoIdentifier(catalog, "trino catalog"),
    quoteTrinoIdentifier(schema, "trino schema"),
    quoteTrinoIdentifier(table, "trino table"),
  ].join(".");
}

function textField(input: Record<string, unknown>, key: string): string | undefined {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function jsonObjectField(value: unknown, label: string): Record<string, unknown> {
  if (typeof value === "string") {
    return jsonObjectField(JSON.parse(value) as unknown, label);
  }
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  throw new Error(`${label} must be a JSON object`);
}

function isMissingTableError(error: unknown, table: string): boolean {
  const message = errorMessage(error);
  return (
    message.includes("no business tables")
    || message.includes(`business table ${table}`)
    || message.includes(table)
    || message.includes("NoSuchTable")
    || message.includes("NoSuchNamespace")
    || message.includes("does not exist")
    || message.includes("not found")
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function requireNonEmpty(value: string | undefined, label: string): string {
  const normalized = value?.trim();
  if (!normalized) {
    throw new Error(`${label} must not be empty`);
  }
  return normalized;
}
