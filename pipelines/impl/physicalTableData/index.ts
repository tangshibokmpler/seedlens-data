import {
  Pipeline,
  isModelDecimalType,
  normalizeModelDecimalValue,
} from "@agentnexus/lakecore/model";
import {
  assertTargetedRecordInputs,
  resolveRecordInputs,
} from "../record-inputs.js";
import {
  assertSourceMappingsExist,
  dataFactoryConfig,
  errorMessage,
  loadDataSources,
  loadPipelineConfigRows,
  loadSourceColumns,
  mappedSourceColumnType,
  matchedPhysicalDataConfigs,
  physicalDatasetStateHash,
  quoteTrinoIdentifier,
  resolveSourceRef,
  stateHash,
  type MatchedPipelineConfig,
  type SourceColumnInfo,
  type SourceRef,
} from "../physicalDataAssets/index.js";
import {
  loadTablePipelineState,
  reconcileStateValue,
  tableReconcileLeaseReady,
  writeTablePipelineState,
  type TablePipelineStateRow,
} from "../physicalDataAssets/state.js";
import type { PipelineRunContext } from "../physicalDataAssets/types.js";

type DatasetRow = Record<string, unknown>;

interface PhysicalDatasetSyncSummary {
  sourceRows: number;
  targetRows: number;
  inserted: number;
  updated: number;
  deleted: number;
  unchanged: number;
}

interface KeyedDatasetRow {
  key: string | number | boolean;
  row: DatasetRow;
}

export class BuiltinPhysicalTableDataPipeline {
  @Pipeline({ tag: "dataFactory", pipelineKey: "builtin_physical_table_data" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：把物理资产配置对应的源数据导入已创建的目标表。
 * 前置：同一配置的 table_reconcile_state 必须以相同定义摘要处于 ready。
 * 幂等：按业务主键比较源表和目标表，并通过 insert/update/delete 使目标数据收敛。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = dataFactoryConfig(context.config);
  const recordInputs = resolveRecordInputs(context.config);
  assertTargetedRecordInputs(recordInputs, [config.pipelineTable, config.dataSourceTable]);

  const [pipelineRows, dataSources] = await Promise.all([
    loadPipelineConfigRows(context, config.pipelineTable, recordInputs),
    loadDataSources(context, config.dataSourceTable, recordInputs),
  ]);
  const matched = matchedPhysicalDataConfigs(context, pipelineRows);
  const dataSourcesByName = new Map(dataSources.map((source) => [source.name, source]));
  const expectedAssetTables = new Set<string>();

  for (const configRow of matched) {
    if (context.signal.aborted) {
      throw new Error(`${context.pipelineKey} aborted`);
    }
    const assetTable = configRow.processing.target.assetTable;
    if (expectedAssetTables.has(assetTable)) {
      throw new Error(
        `pipeline ${context.pipelineKey} has duplicate target asset table ${assetTable}`,
      );
    }
    expectedAssetTables.add(assetTable);

    let source: SourceRef;
    let sourceColumns: readonly SourceColumnInfo[];
    try {
      const dataSource = dataSourcesByName.get(configRow.dataSourceKey);
      if (!dataSource) {
        throw new Error(
          `pipeline config ${configRow.name} references missing data source ${configRow.dataSourceKey}`,
        );
      }
      source = await resolveSourceRef(context, dataSource, configRow.processing);
      sourceColumns = await loadSourceColumns(context, source);
      assertSourceMappingsExist(configRow, sourceColumns);
    } catch (error) {
      await writeSourceResolutionFailure(
        context,
        config.stateTable,
        configRow,
        error,
      );
      continue;
    }

    await reconcilePhysicalTableData(
      context,
      configRow,
      source,
      sourceColumns,
      config.stateTable,
      context.heartbeatLeaseMs,
    );
  }
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("builtin physical table data requires a concrete tenantId/env context");
  }
}

async function writeSourceResolutionFailure(
  context: PipelineRunContext,
  stateTable: string,
  config: MatchedPipelineConfig,
  error: unknown,
): Promise<void> {
  const previous = await loadTablePipelineState(context, stateTable, config.name);
  if (previous) {
    await writeTablePipelineState(context, {
      table: stateTable,
      key: config.name,
      leaseMs: context.heartbeatLeaseMs,
      tableState: previous.table_reconcile_state,
      dataState: reconcileStateValue("failed", stateHash({ config }), error),
      success: false,
    });
  }
  context.logger.warn("physical table data source resolution failed", {
    config: config.name,
    error: errorMessage(error),
  });
}

async function reconcilePhysicalTableData(
  context: PipelineRunContext,
  config: MatchedPipelineConfig,
  source: SourceRef,
  sourceColumns: readonly SourceColumnInfo[],
  stateTable: string,
  leaseMs: number,
): Promise<void> {
  const expectedHash = physicalDatasetStateHash(config, source, sourceColumns);
  const previous = await loadTablePipelineState(context, stateTable, config.name);
  assertTableDefinitionReady(config, previous, expectedHash);
  const tableState = previous!.table_reconcile_state;
  let dataState = reconcileStateValue("pending", expectedHash);

  await writeTablePipelineState(context, {
    table: stateTable,
    key: config.name,
    leaseMs,
    tableState,
    dataState,
    success: false,
  });

  try {
    const summary = await syncPhysicalDatasetRows(context, config, source, sourceColumns);
    dataState = reconcileStateValue("ready", expectedHash);
    await writeTablePipelineState(context, {
      table: stateTable,
      key: config.name,
      leaseMs,
      tableState,
      dataState,
      success: true,
    });
    if (summary.inserted + summary.updated + summary.deleted > 0) {
      context.logger.info("physical table data changed", {
        config: config.name,
        sourceRows: summary.sourceRows,
        targetRows: summary.targetRows,
        inserted: summary.inserted,
        updated: summary.updated,
        deleted: summary.deleted,
      });
    }
  } catch (error) {
    dataState = reconcileStateValue("failed", expectedHash, error);
    await writeTablePipelineState(context, {
      table: stateTable,
      key: config.name,
      leaseMs,
      tableState,
      dataState,
      success: false,
    });
    context.logger.warn("physical table data reconcile failed", {
      config: config.name,
      error: errorMessage(error),
    });
  }
}

function assertTableDefinitionReady(
  config: MatchedPipelineConfig,
  state: TablePipelineStateRow | undefined,
  expectedHash: string,
): void {
  if (!state) {
    throw new Error(
      `physical table data ${config.name} requires table definition state; run builtin_physical_dataset first`,
    );
  }
  if (!tableReconcileLeaseReady(state, expectedHash)) {
    throw new Error(
      `physical table data ${config.name} requires a leased ready table definition with matching hash`,
    );
  }
}

async function syncPhysicalDatasetRows(
  context: PipelineRunContext,
  config: MatchedPipelineConfig,
  source: SourceRef,
  sourceColumns: readonly SourceColumnInfo[],
): Promise<PhysicalDatasetSyncSummary> {
  const [sourceRows, targetRows] = await Promise.all([
    loadSourceDatasetRows(context, config, source, sourceColumns),
    loadTargetDatasetRows(context, config),
  ]);
  const primaryKey = config.processing.target.primaryKey[0]!;
  const fieldTypes = targetFieldTypes(config, sourceColumns);
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
  const fields = config.processing.fieldMappings.map((mapping) => mapping.targetField);
  const summary: PhysicalDatasetSyncSummary = {
    sourceRows: sourceRows.length,
    targetRows: targetRows.length,
    inserted: 0,
    updated: 0,
    deleted: 0,
    unchanged: 0,
  };

  for (const [keyToken, sourceEntry] of sourceByKey) {
    const targetEntry = targetByKey.get(keyToken);
    if (!targetEntry) {
      await context.services.dataPlane.dataTableRowWrite({
        actor: context.actor,
        table: config.processing.target.assetTable,
        operation: "insert",
        record: sourceEntry.row,
      });
      summary.inserted += 1;
      continue;
    }
    if (sameDatasetRow(sourceEntry.row, targetEntry.row, fields, fieldTypes)) {
      summary.unchanged += 1;
      continue;
    }
    await context.services.dataPlane.dataTableRowWrite({
      actor: context.actor,
      table: config.processing.target.assetTable,
      operation: "update",
      key: sourceEntry.key,
      record: sourceEntry.row,
    });
    summary.updated += 1;
  }

  for (const [keyToken, targetEntry] of targetByKey) {
    if (sourceByKey.has(keyToken)) {
      continue;
    }
    await context.services.dataPlane.dataTableRowWrite({
      actor: context.actor,
      table: config.processing.target.assetTable,
      operation: "delete",
      key: targetEntry.key,
      record: { [primaryKey]: targetEntry.key },
    });
    summary.deleted += 1;
  }

  return summary;
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
      const castType = mappedSourceColumnType(
        column.type,
        mapping.targetType,
      ).trinoViewCastType;
      const expression = castType ? `CAST(${sourceField} AS ${castType})` : sourceField;
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

async function loadTargetDatasetRows(
  context: PipelineRunContext,
  config: MatchedPipelineConfig,
): Promise<readonly DatasetRow[]> {
  const targetFields = config.processing.fieldMappings.map((mapping) => mapping.targetField);
  const selectList = targetFields
    .map((field) => quoteTrinoIdentifier(field, "target column"))
    .join(", ");
  const result = await context.services.dataPlane.dataEnvironmentSqlQuery({
    actor: context.actor,
    max_pages: 1_000,
    sql: `SELECT ${selectList} FROM ${quoteTrinoIdentifier(
      config.processing.target.assetTable,
      "target table",
    )}`,
  });
  return result.rows.map((row) => rowFromValues(targetFields, row));
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
    const keyToken = stableKeyToken(key, keyType);
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
): string | number | boolean {
  const value = row[primaryKey];
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  throw new Error(
    `pipeline config ${configName} loaded ${label} row without scalar primary key ${primaryKey}`,
  );
}

function stableKeyToken(value: string | number | boolean, type: string | undefined): string {
  return JSON.stringify(normalizeComparableValue(value, type));
}

function sameDatasetRow(
  source: DatasetRow,
  target: DatasetRow,
  fields: readonly string[],
  fieldTypes: ReadonlyMap<string, string>,
): boolean {
  return fields.every((field) =>
    stableComparableJson(source[field], fieldTypes.get(field))
      === stableComparableJson(target[field], fieldTypes.get(field)));
}

function stableComparableJson(value: unknown, type: string | undefined): string {
  return JSON.stringify(stableValue(normalizeComparableValue(value, type)));
}

function normalizeComparableValue(value: unknown, type: string | undefined): unknown {
  if (value === undefined || value === null) {
    return null;
  }
  if (value instanceof Date) {
    return type === "date" ? value.toISOString().slice(0, 10) : value.toISOString();
  }
  if (type && isModelDecimalType(type)) {
    return normalizeModelDecimalValue(value, type);
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
      if (typeof value === "boolean") {
        return value;
      }
      if (typeof value === "string") {
        const normalized = value.trim().toLowerCase();
        if (["true", "1", "yes", "on"].includes(normalized)) {
          return true;
        }
        if (["false", "0", "no", "off"].includes(normalized)) {
          return false;
        }
      }
      return value;
    }
    case "date":
    case "timestamp":
    case "timestamptz": {
      if (typeof value === "string" || typeof value === "number") {
        const parsed = new Date(value);
        if (!Number.isNaN(parsed.getTime())) {
          return type === "date" ? parsed.toISOString().slice(0, 10) : parsed.toISOString();
        }
      }
      return String(value);
    }
    case "json":
    case "jsonb":
      if (typeof value === "string") {
        try {
          return JSON.parse(value) as unknown;
        } catch {
          return value;
        }
      }
      return value;
    default:
      return String(value);
  }
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => stableValue(entry));
  }
  if (!value || typeof value !== "object" || value instanceof Date) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableValue(entry)]),
  );
}

function targetFieldTypes(
  config: MatchedPipelineConfig,
  sourceColumns: readonly SourceColumnInfo[],
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
        mappedSourceColumnType(sourceColumn.type, mapping.targetType).modelType,
      );
    }
  }
  return fieldTypes;
}
