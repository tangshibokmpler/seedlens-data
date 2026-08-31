import type {
  BusinessTableManifestColumn,
  TenantBusinessTableManifest,
} from "@agentnexus/lakecore/model";
import {
  Pipeline,
  resolveSourceColumnType,
} from "@agentnexus/lakecore/model";
import {
  assertTargetedRecordInputs,
  resolveRecordInputs,
  type PipelineRecordInputs,
} from "./record-inputs.js";
import type { PipelineRunContext } from "./types.js";

type BuiltinDataSourceKind = "pg" | "mysql" | "sqlserver";
type ImplementationKind = "builtin" | "custom";

interface PhysicalAssetConfig {
  pipelineTable: string;
  dataSourceTable: string;
}

interface PipelineConfigRow {
  name: string;
  data_source_key: string;
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

interface PhysicalAssetProcessingConfig {
  source: {
    table: string;
    schema?: string;
  };
  primaryKey: readonly string[];
  fieldMappings: readonly FieldMapping[];
}

interface MatchedPhysicalAssetConfig {
  name: string;
  dataSourceKey: string;
  assetTable: string;
  processing: PhysicalAssetProcessingConfig;
}

interface SourceRef {
  catalog: string;
  schema: string;
  table: string;
  qualified: string;
}

interface SourceColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
}

const DEFAULT_SOURCE_SCHEMA = "public";

export class BuiltinPhysicalDatasetPipeline {
  @Pipeline({ tag: "dataFactory", pipelineKey: "builtin_physical_dataset" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：根据源表结构和字段映射创建或调和 data_versioned 物理资产表 schema，不迁移业务数据。
 * 输入：t_factory_config_pipeline、t_factory_config_source 及外部源表结构；目标表名取自 data_asset_name。
 * 逻辑：解析源字段类型，生成单表 schema manifest，并通过 ControlPlane 使目标表定义收敛。
 * 幂等：采用 reconcile/preserve 策略；定义一致时不变更表，也不删除未出现在本次输入中的其他资产表。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = physicalAssetConfig(context.config);
  const recordInputs = resolveRecordInputs(context.config);
  assertTargetedRecordInputs(recordInputs, [config.pipelineTable, config.dataSourceTable]);
  const [pipelineRows, dataSources] = await Promise.all([
    loadPipelineConfigRows(context, config.pipelineTable, recordInputs),
    loadDataSources(context, config.dataSourceTable, recordInputs),
  ]);
  const matched = matchedPhysicalAssetConfigs(context, pipelineRows);
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

    try {
      const dataSource = dataSourcesByName.get(configRow.dataSourceKey);
      if (!dataSource) {
        throw new Error(
          `physical asset ${configRow.name} references missing data source ${configRow.dataSourceKey}`,
        );
      }
      const source = await resolveSourceRef(context, dataSource, configRow.processing);
      const sourceColumns = await loadSourceColumns(context, source);
      assertSourceMappingsExist(configRow, sourceColumns);
      const result = await context.services.controlPlane.businessSchemasReconcile({
        actor: context.actor,
        schema: physicalAssetManifest(configRow, sourceColumns),
      });
      const action = schemaAction(result, configRow.assetTable);
      if (action !== "unchanged") {
        context.logger.info("physical asset schema changed", {
          config: configRow.name,
          assetTable: configRow.assetTable,
          action,
          source: source.qualified,
        });
      }
    } catch (error) {
      context.logger.warn("physical asset schema reconcile failed", {
        config: configRow.name,
        assetTable: configRow.assetTable,
        error: errorMessage(error),
      });
    }
  }
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("builtin physical dataset requires a concrete tenantId/env context");
  }
}

function physicalAssetConfig(config: unknown): PhysicalAssetConfig {
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    throw new Error("builtin physical dataset requires pipelineTable and dataSourceTable config");
  }
  const raw = config as Record<string, unknown>;
  const pipelineTable = textField(raw, "pipelineTable");
  const dataSourceTable = textField(raw, "dataSourceTable");
  if (!pipelineTable || !dataSourceTable) {
    throw new Error("builtin physical dataset requires pipelineTable and dataSourceTable config");
  }
  return { pipelineTable, dataSourceTable };
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
    context.logger.warn("builtin physical dataset skipped unsupported data sources", {
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
      context.logger.warn(`builtin physical dataset skipped env without ${label} table`, {
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
  const implementationKind = textField(row, "kind");
  const pipelineCodeKey = textField(row, "pipeline_code_key");
  const dataAssetName = textField(row, "data_asset_name");
  if (!name || !dataSourceKey || !implementationKind || !pipelineCodeKey || !dataAssetName) {
    throw new Error(
      `pipeline config row[${index}] requires name, data_source_key, kind, pipeline_code_key, and data_asset_name`,
    );
  }
  if (implementationKind !== "builtin" && implementationKind !== "custom") {
    throw new Error(`pipeline config row[${index}].kind must be builtin or custom`);
  }
  return {
    name,
    data_source_key: dataSourceKey,
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

function matchedPhysicalAssetConfigs(
  context: PipelineRunContext,
  rows: readonly PipelineConfigRow[],
): readonly MatchedPhysicalAssetConfig[] {
  return rows
    .filter((row) =>
      row.kind === "builtin"
      && row.pipeline_code_key === context.pipelineKey)
    .map((row) => ({
      name: row.name,
      dataSourceKey: row.data_source_key,
      assetTable: row.data_asset_name,
      processing: normalizeProcessingConfig(row),
    }));
}

function normalizeProcessingConfig(row: PipelineConfigRow): PhysicalAssetProcessingConfig {
  const config = row.processing_config;
  const source = jsonObjectField(
    config.source,
    `pipeline config ${row.name}.processing_config.source`,
  );
  const target = jsonObjectField(
    config.target,
    `pipeline config ${row.name}.processing_config.target`,
  );
  const sourceTable = textField(source, "table");
  const sourceSchema = textField(source, "schema");
  const primaryKey = stringArray(
    target.primary_key,
    `pipeline config ${row.name}.processing_config.target.primary_key`,
  );
  if (!sourceTable) {
    throw new Error(`pipeline config ${row.name} requires processing_config.source.table`);
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
    throw new Error(
      `pipeline config ${row.name} primary key ${primaryKey[0]} is not in field_mappings target fields`,
    );
  }
  return {
    source: {
      table: sourceTable,
      ...(sourceSchema ? { schema: sourceSchema } : {}),
    },
    primaryKey,
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

async function resolveSourceRef(
  context: PipelineRunContext,
  dataSource: BuiltinDataSource,
  processing: PhysicalAssetProcessingConfig,
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
    throw new Error(
      `trino connector ${expectedCatalog} for data source ${dataSource.name} is not active; run pipelinePrelude first`,
    );
  }
  const schema = processing.source.schema
    ?? dataSource.connection.schema
    ?? defaultSourceSchema(dataSource.builtin_kind, dataSource.connection.database);
  return {
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
      "SELECT column_name, data_type, is_nullable",
      "FROM information_schema.columns",
      `WHERE table_schema = ${quoteSqlString(source.schema)}`,
      `  AND table_name = ${quoteSqlString(source.table)}`,
      "ORDER BY ordinal_position",
    ].join("\n"),
  });
  const columns = result.rows.map((row) => ({
    name: stringCell(row[0], "column_name"),
    type: stringCell(row[1], "data_type"),
    nullable: String(row[2] ?? "").toUpperCase() !== "NO",
  }));
  if (columns.length === 0) {
    throw new Error(
      `source table ${source.catalog}.${source.schema}.${source.table} does not expose columns in information_schema`,
    );
  }
  return columns;
}

function assertSourceMappingsExist(
  config: MatchedPhysicalAssetConfig,
  sourceColumns: readonly SourceColumnInfo[],
): void {
  const sourceFieldNames = new Set(sourceColumns.map((column) => column.name.toLowerCase()));
  for (const mapping of config.processing.fieldMappings) {
    if (!sourceFieldNames.has(mapping.sourceField.toLowerCase())) {
      throw new Error(
        `physical asset ${config.name} references missing source column ${mapping.sourceField}`,
      );
    }
  }
}

function physicalAssetManifest(
  config: MatchedPhysicalAssetConfig,
  sourceColumns: readonly SourceColumnInfo[],
): TenantBusinessTableManifest {
  const sourceColumnsByName = new Map(
    sourceColumns.map((column) => [column.name.toLowerCase(), column]),
  );
  const columns: Record<string, BusinessTableManifestColumn> = {};
  for (const mapping of config.processing.fieldMappings) {
    const sourceColumn = sourceColumnsByName.get(mapping.sourceField.toLowerCase());
    if (!sourceColumn) {
      throw new Error(
        `physical asset ${config.name} references missing source column ${mapping.sourceField}`,
      );
    }
    columns[mapping.targetField] = {
      type: mappedSourceColumnType(sourceColumn.type, mapping.targetType),
      nullable: !config.processing.primaryKey.includes(mapping.targetField) && sourceColumn.nullable,
    };
  }
  return {
    version: 1,
    tables: [{
      label: config.assetTable,
      kind: "data_versioned",
      table: config.assetTable,
      columns,
      primaryKey: config.processing.primaryKey,
    }],
  };
}

function schemaAction(
  result: {
    created_tables: readonly string[];
    updated_tables: readonly string[];
    unchanged_tables: readonly string[];
  },
  table: string,
): "created" | "updated" | "unchanged" {
  if (result.created_tables.includes(table)) return "created";
  if (result.updated_tables.includes(table)) return "updated";
  if (result.unchanged_tables.includes(table)) return "unchanged";
  throw new Error(`schema reconcile result does not include physical asset table ${table}`);
}

function mappedSourceColumnType(type: string, targetType?: "string"): string {
  const resolved = resolveSourceColumnType(type, {
    highPrecisionDecimal: targetType === "string" ? "string" : "reject",
  });
  return targetType === "string" ? "string" : resolved.modelType;
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
