import { createHash } from "node:crypto";

import { Pipeline } from "@agentnexus/lakecore/model";
import {
  assertTargetedRecordInputs,
  resolveRecordInputs,
  type PipelineRecordInputs,
} from "../record-inputs.js";
import type { PipelineRunContext } from "./types.js";
import {
  deleteTablePipelineState,
  loadTablePipelineState,
  reconcileStateValue,
  renewTableHeartbeat,
  tableReconcileLeaseReady,
  writeTablePipelineState,
} from "./state.js";

type TargetAssetType = "virtual";
type BuiltinDataSourceKind = "pg" | "mysql" | "sqlserver";
type ImplementationKind = "builtin" | "custom";

interface DataFactoryConfig {
  pipelineTable: string;
  dataSourceTable: string;
  stateTable: string;
}

interface PipelineConfigRow {
  name: string;
  data_source_key: string;
  target_asset_type: string;
  kind: ImplementationKind;
  pipeline_code_key: string;
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
  target: {
    assetTable: string;
    primaryKey: readonly string[];
  };
  fieldMappings: readonly FieldMapping[];
}

interface MatchedPipelineConfig {
  name: string;
  dataSourceKey: string;
  processing: BuiltinProcessingConfig;
}

interface SourceRef {
  sourceKind: string;
  catalog: string;
  schema: string;
  table: string;
  qualified: string;
}

interface SourceColumnInfo {
  name: string;
  type: string;
}

const DEFAULT_SOURCE_SCHEMA = "public";
const TARGET_ASSET_TYPE = "virtual" satisfies TargetAssetType;

export class BuiltinVirtualDatasetPipeline {
  @Pipeline({ tag: "dataFactory", pipelineKey: "builtin_virtual_dataset" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：根据数据加工配置为外部源表生成虚拟数据集视图。
 * 逻辑：默认读取完整 pipeline 和数据源配置；config.recordInputs 非空时只消费指定记录，校验映射并调谐视图语义。
 * 幂等：采用 reconcile/preserve 策略；定向模式不清理其他配置状态，有效租约只续心跳，成功同时记录表和实时数据状态。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = dataFactoryConfig(context.config);
  const pipelineTable = config.pipelineTable;
  const dataSourceTable = config.dataSourceTable;
  const recordInputs = resolveRecordInputs(context.config);
  assertTargetedRecordInputs(recordInputs, [pipelineTable, dataSourceTable]);
  const targeted = recordInputs.size > 0;
  const [pipelineRows, dataSources] = await Promise.all([
    loadPipelineConfigRows(context, pipelineTable, recordInputs),
    loadDataSources(context, dataSourceTable, recordInputs),
  ]);
  if (!targeted) {
    await removeMissingTablePipelineStates(
      context,
      config.stateTable,
      new Set(pipelineRows.map((row) => row.name)),
    );
  }
  const matched = matchedPipelineConfigs(context, pipelineRows);
  const dataSourcesByName = new Map(dataSources.map((source) => [source.name, source]));
  const expectedAssetTables = new Set<string>();

  for (const configRow of matched) {
    if (context.signal.aborted) {
      throw new Error(`${context.pipelineKey} aborted`);
    }
    if (expectedAssetTables.has(configRow.processing.target.assetTable)) {
      throw new Error(`pipeline ${context.pipelineKey} has duplicate target asset table ${configRow.processing.target.assetTable}`);
    }
    expectedAssetTables.add(configRow.processing.target.assetTable);

    let source: SourceRef;
    let sourceColumns: readonly SourceColumnInfo[];
    try {
      const dataSource = dataSourcesByName.get(configRow.dataSourceKey);
      if (!dataSource) {
        throw new Error(`pipeline config ${configRow.name} references missing data source ${configRow.dataSourceKey}`);
      }
      source = await resolveSourceRef(context, dataSource, configRow.processing);
      sourceColumns = await loadSourceColumns(context, source);
      assertSourceMappingsExist(configRow, sourceColumns);
    } catch (error) {
      const failed = reconcileStateValue("failed", stateHash({ config: configRow }), error);
      await writeTablePipelineState(context, {
        table: config.stateTable,
        key: configRow.name,
        leaseMs: context.heartbeatLeaseMs,
        tableState: failed,
        dataState: failed,
        success: false,
      });
      context.logger.warn("virtual dataset source resolution failed", {
        config: configRow.name,
        error: errorMessage(error),
      });
      continue;
    }
    await reconcileVirtualDataset(
      context,
      configRow,
      source,
      sourceColumns,
      config.stateTable,
      context.heartbeatLeaseMs,
    );
  }
}

async function removeMissingTablePipelineStates(
  context: PipelineRunContext,
  table: string,
  expectedKeys: ReadonlySet<string>,
): Promise<void> {
  const rows = await context.services.dataPlane.dataTableRowsList({ table });
  for (const row of rows) {
    const key = typeof row.config_key === "string" ? row.config_key.trim() : "";
    if (key && !expectedKeys.has(key)) {
      await deleteTablePipelineState(context, table, key);
    }
  }
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("builtin virtual dataset requires a concrete tenantId/env context");
  }
}

function dataFactoryConfig(config: unknown): DataFactoryConfig {
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    throw new Error("builtin virtual dataset requires pipelineTable and dataSourceTable config");
  }
  const raw = config as Record<string, unknown>;
  const pipelineTable = textField(raw, "pipelineTable");
  const dataSourceTable = textField(raw, "dataSourceTable");
  const stateTable = textField(raw, "stateTable");
  if (!pipelineTable || !dataSourceTable || !stateTable) {
    throw new Error("builtin virtual dataset requires pipelineTable, dataSourceTable, and stateTable config");
  }
  return {
    pipelineTable,
    dataSourceTable,
    stateTable,
  };
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
    context.logger.warn("builtin virtual dataset skipped unsupported data sources", {
      table,
      skipped,
    });
  }
  return dataSources;
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
      context.logger.warn(`builtin virtual dataset skipped env without ${label} table`, {
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
  const targetAssetType = textField(row, "target_asset_type");
  const implementationKind = textField(row, "kind");
  const pipelineCodeKey = textField(row, "pipeline_code_key");
  if (!name || !dataSourceKey || !targetAssetType || !implementationKind || !pipelineCodeKey) {
    throw new Error(`pipeline config row[${index}] requires name, data_source_key, target_asset_type, kind, and pipeline_code_key`);
  }
  if (implementationKind !== "builtin" && implementationKind !== "custom") {
    throw new Error(`pipeline config row[${index}].kind must be builtin or custom`);
  }
  return {
    name,
    data_source_key: dataSourceKey,
    target_asset_type: targetAssetType,
    kind: implementationKind,
    pipeline_code_key: pipelineCodeKey,
    processing_config: jsonObjectField(row.processing_config, `pipeline config ${name}.processing_config`),
  };
}

function dataSourceRow(row: Record<string, unknown>, index: number): BuiltinDataSource | undefined {
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
    throw new Error(`builtin data source row[${index}] requires name, connection.host, connection.database, and connection.user`);
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
      row.kind === "builtin"
      && row.pipeline_code_key === context.pipelineKey
      && row.target_asset_type === TARGET_ASSET_TYPE)
    .map((row) => ({
      name: row.name,
      dataSourceKey: row.data_source_key,
      processing: normalizeProcessingConfig(row),
    }));
}

function normalizeProcessingConfig(row: PipelineConfigRow): BuiltinProcessingConfig {
  const config = row.processing_config;
  const source = jsonObjectField(config.source, `pipeline config ${row.name}.processing_config.source`);
  const target = jsonObjectField(config.target, `pipeline config ${row.name}.processing_config.target`);
  const sourceTable = textField(source, "table");
  const sourceSchema = textField(source, "schema");
  const assetTable = textField(target, "asset_table");
  const primaryKey = stringArray(target.primary_key, `pipeline config ${row.name}.processing_config.target.primary_key`);
  if (!sourceTable || !assetTable) {
    throw new Error(`pipeline config ${row.name} requires processing_config.source.table and processing_config.target.asset_table`);
  }
  if (primaryKey.length !== 1) {
    throw new Error(`pipeline config ${row.name} target.primary_key must contain exactly one field`);
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
  if (!targetFields.has(primaryKey[0]!)) {
    throw new Error(`pipeline config ${row.name} primary key ${primaryKey[0]} is not in field_mappings target fields`);
  }
  return {
    source: {
      table: sourceTable,
      ...(sourceSchema ? { schema: sourceSchema } : {}),
    },
    target: {
      assetTable,
      primaryKey,
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
    throw new Error(`pipeline config ${configName} field_mappings[${index}] requires source_field and target_field`);
  }
  if (targetType && targetType !== "string") {
    throw new Error(`pipeline config ${configName} field_mappings[${index}].target_type only supports string`);
  }
  return {
    sourceField,
    targetField,
    ...(targetType === "string" ? { targetType } : {}),
  };
}

async function resolveSourceRef(
  context: PipelineRunContext,
  dataSource: BuiltinDataSource,
  processing: BuiltinProcessingConfig,
): Promise<SourceRef> {
  const expectedCatalog = externalCatalogName({
    tenantId: context.services.scope.tenantId,
    env: context.services.scope.env,
    sourceName: dataSource.name,
  });
  const connectors = await context.services.lakehouse.managerConnectorsList({
    provider: "trino",
  });
  const active = connectors.trino?.activeConfigs.find((entry) =>
    entry.key === expectedCatalog || entry.name === expectedCatalog);
  if (!active) {
    throw new Error(`trino connector ${expectedCatalog} for data source ${dataSource.name} is not active; run pipelinePrelude first`);
  }
  const schema = processing.source.schema
    ?? dataSource.connection.schema
    ?? defaultSourceSchema(dataSource.builtin_kind, dataSource.connection.database);
  return {
    sourceKind: dataSource.builtin_kind,
    catalog: active.name,
    schema,
    table: processing.source.table,
    qualified: qualifiedTrinoTable(active.name, schema, processing.source.table),
  };
}

async function loadSourceColumns(
  context: PipelineRunContext,
  source: SourceRef,
): Promise<readonly SourceColumnInfo[]> {
  const result = await context.services.dataPlane.dataEnvironmentSqlQuery({
    actor: context.actor,
    catalog: source.catalog,
    schema: source.schema,
    max_pages: 10,
    sql: [
      "SELECT column_name, data_type",
      "FROM information_schema.columns",
      `WHERE table_schema = ${quoteSqlString(source.schema)}`,
      `  AND table_name = ${quoteSqlString(source.table)}`,
      "ORDER BY ordinal_position",
    ].join("\n"),
  });
  const columns = result.rows.map((row) => ({
    name: stringCell(row[0], "column_name"),
    type: stringCell(row[1], "data_type"),
  }));
  if (columns.length === 0) {
    throw new Error(`source table ${source.catalog}.${source.schema}.${source.table} does not expose columns in information_schema`);
  }
  return columns;
}

function assertSourceMappingsExist(
  config: MatchedPipelineConfig,
  sourceColumns: readonly SourceColumnInfo[],
): void {
  const sourceFieldNames = new Set(sourceColumns.map((column) => column.name.toLowerCase()));
  for (const mapping of config.processing.fieldMappings) {
    if (!sourceFieldNames.has(mapping.sourceField.toLowerCase())) {
      throw new Error(`pipeline config ${config.name} references missing source column ${mapping.sourceField}`);
    }
  }
}

async function reconcileVirtualDataset(
  context: PipelineRunContext,
  config: MatchedPipelineConfig,
  source: SourceRef,
  sourceColumns: readonly SourceColumnInfo[],
  stateTable: string,
  leaseMs: number,
): Promise<void> {
  const expectedHash = stateHash({ config, source, sourceColumns });
  const previous = await loadTablePipelineState(context, stateTable, config.name);
  if (tableReconcileLeaseReady(previous, expectedHash)) {
    await renewTableHeartbeat(context, {
      table: stateTable,
      key: config.name,
      leaseMs,
    });
    return;
  }

  const sourceColumnsByName = new Map(sourceColumns.map((column) => [column.name.toLowerCase(), column]));
  try {
    const pending = reconcileStateValue("pending", expectedHash);
    await writeTablePipelineState(context, {
      table: stateTable,
      key: config.name,
      leaseMs,
      tableState: pending,
      dataState: pending,
      success: false,
    });
    const result = await context.services.dataPlane.dataEnvironmentViewReconcile({
      actor: context.actor,
      view: config.processing.target.assetTable,
      source: {
        kind: source.sourceKind,
        catalog: source.catalog,
        schema: source.schema,
        table: source.table,
      },
      field_mappings: config.processing.fieldMappings.map((mapping) => ({
        source_field: mapping.sourceField,
        target_field: mapping.targetField,
        source_type: sourceColumnsByName.get(mapping.sourceField.toLowerCase())?.type,
        target_type: mapping.targetType,
      })),
      marker: viewMarker(context, config),
    });
    const ready = reconcileStateValue("ready", expectedHash);
    await writeTablePipelineState(context, {
      table: stateTable,
      key: config.name,
      leaseMs,
      tableState: ready,
      dataState: ready,
      success: true,
    });
    if (result.changed) {
      context.logger.info("virtual dataset changed", {
        config: config.name,
        action: result.action,
        source: source.qualified,
        view: result.query_view_ref,
      });
    }
  } catch (error) {
    const failed = reconcileStateValue("failed", expectedHash, error);
    await writeTablePipelineState(context, {
      table: stateTable,
      key: config.name,
      leaseMs,
      tableState: failed,
      dataState: failed,
      success: false,
    });
    context.logger.warn("virtual dataset reconcile failed", {
      config: config.name,
      error: errorMessage(error),
    });
  }
}

function stateHash(value: unknown): string {
  return createHash("sha256")
    .update(JSON.stringify(stableValue(value)))
    .digest("hex");
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

function viewMarker(context: PipelineRunContext, config: MatchedPipelineConfig): string {
  return [
    "agentnexus.managed_by=lakectd",
    `pipeline_key=${context.pipelineKey}`,
    `pipeline_config=${config.name}`,
  ].join("; ");
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

function quoteSqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function stringCell(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`source information_schema ${label} must be a non-empty string`);
  }
  return value.trim();
}

function textField(input: Record<string, unknown>, key: string): string | undefined {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function stringArray(value: unknown, label: string): readonly string[] {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  return value.map((entry, index) => {
    if (typeof entry !== "string" || !entry.trim()) {
      throw new Error(`${label}[${index}] must be a non-empty string`);
    }
    return entry.trim();
  });
}

function jsonObjectField(value: unknown, label: string): Record<string, unknown> {
  const parsed = jsonObjectFieldOptional(value, label);
  if (!parsed) {
    throw new Error(`${label} must be a JSON object`);
  }
  return parsed;
}

function jsonObjectFieldOptional(value: unknown, label: string): Record<string, unknown> | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value === "string") {
    const parsed = JSON.parse(value) as unknown;
    return jsonObjectFieldOptional(parsed, label);
  }
  if (typeof value === "object" && !Array.isArray(value)) {
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

function defaultSourceSchema(kind: BuiltinDataSourceKind, database: string): string {
  if (kind === "mysql") return database;
  return kind === "sqlserver" ? "dbo" : DEFAULT_SOURCE_SCHEMA;
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
