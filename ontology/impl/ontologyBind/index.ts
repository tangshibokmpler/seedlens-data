import { createHash } from "node:crypto";

import { Pipeline } from "@agentnexus/lakecore/model";
import {
  assertTargetedRecordInputs,
  resolveRecordInputs,
  type PipelineRecordInputs,
} from "./record-inputs.js";
import type { PipelineRunContext } from "./types.js";
import {
  deleteOntologyBindState,
  loadOntologyBindState,
  ontologyBindLeaseReady,
  renewOntologyBindHeartbeat,
  writeOntologyBindState,
} from "./state.js";

type DataAssetTargetType = "virtual" | "physical";
type ImplementationKind = "builtin" | "custom";

interface OntologyFactoryConfig {
  bindTable: string;
  pipelineTable: string;
  stateTable: string;
}

interface OntologyBindRow {
  object_table_name: string;
  data_asset_name: string;
  kind: ImplementationKind;
  bind_code_key: string;
  bind_config: Record<string, unknown>;
}

interface PipelineConfigRow {
  name: string;
  target_asset_type: DataAssetTargetType;
  kind: ImplementationKind;
  pipeline_code_key: string;
  data_asset_name: string;
  processing_config: Record<string, unknown>;
}

interface PipelineFieldMapping {
  sourceField: string;
  targetField: string;
}

interface PipelineTargetConfig {
  assetTable: string;
  primaryKey: readonly string[];
}

interface DataAssetExportConfig {
  configName: string;
  pipelineCodeKey: string;
  dataAssetName: string;
  assetTable: string;
  primaryKey: readonly string[];
  fieldNames: ReadonlySet<string>;
}

interface ViewsNamespaceRef {
  catalog: string;
  schema: string;
}

interface AssetColumnInfo {
  name: string;
  type: string;
}

interface BindProjectionConfig {
  mode: string;
  indexedProperties: readonly string[];
  displayProperties: readonly string[];
  materializedProperties: readonly string[];
  fallbackPolicy?: string;
}

interface BindFieldMapping {
  objectField: string;
  assetField: string;
  type: string;
  primaryKey: boolean;
  display: boolean;
  indexed: boolean;
  materialized: boolean;
}

interface ResolvedBindFieldMapping extends BindFieldMapping {
  sourceType: string;
}

interface BuiltinBindConfig {
  releaseId?: string;
  objectType: string;
  objectPrimaryKey: string;
  dataAssetTable: string;
  dataAssetPrimaryKey: readonly string[];
  projection: BindProjectionConfig;
  fieldMappings: readonly BindFieldMapping[];
}

interface MatchedOntologyBindConfig {
  objectTableName: string;
  bind: BuiltinBindConfig;
  dataAsset: DataAssetExportConfig;
}

const BIND_IMPLEMENTATION_KIND = "builtin" satisfies ImplementationKind;
const SOURCE_VIEW_KIND = "trino_view";

export class BuiltinOntologyBindPipeline {
  @Pipeline({ tag: "ontologyFactory", pipelineKey: "builtin_ontology_bind" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：根据本体绑定配置把数据资产字段投影为对象查询视图。
 * 逻辑：默认读取完整绑定与数据资产配置；config.recordInputs 非空时只消费指定记录，租约失效或配置变化时调谐对象查询视图。
 * 幂等：采用 reconcile/preserve 策略；定向模式不清理其他绑定状态，有效租约只续心跳，不主动清理旧视图。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = ontologyFactoryConfig(context.config);
  const bindTable = config.bindTable;
  const pipelineTable = config.pipelineTable;
  const recordInputs = resolveRecordInputs(context.config);
  assertTargetedRecordInputs(recordInputs, [bindTable, pipelineTable]);
  const targeted = recordInputs.size > 0;
  const [bindRows, pipelineRows] = await Promise.all([
    loadOntologyBindRows(context, bindTable, recordInputs),
    loadPipelineConfigRows(context, pipelineTable, recordInputs),
  ]);
  if (!targeted) {
    await removeMissingOntologyBindStates(
      context,
      config.stateTable,
      new Set(bindRows.map((row) => row.data_asset_name)),
    );
  }
  const dataAssets = dataAssetExportsByName(pipelineRows);
  const matched = matchedOntologyBindConfigs(context, bindRows, dataAssets);
  const expectedObjectTables = new Set<string>();
  const expectedAssetNames = new Set<string>();

  for (const bindConfig of matched) {
    if (context.signal.aborted) {
      throw new Error(`${context.pipelineKey} aborted`);
    }
    if (expectedObjectTables.has(bindConfig.objectTableName)) {
      throw new Error(`ontology bind pipeline ${context.pipelineKey} has duplicate object table ${bindConfig.objectTableName}`);
    }
    expectedObjectTables.add(bindConfig.objectTableName);
    if (expectedAssetNames.has(bindConfig.dataAsset.dataAssetName)) {
      throw new Error(`ontology bind pipeline ${context.pipelineKey} has duplicate asset state key ${bindConfig.dataAsset.dataAssetName}`);
    }
    expectedAssetNames.add(bindConfig.dataAsset.dataAssetName);
  }
  if (matched.length === 0) {
    return;
  }
  const namespace = await currentViewsNamespace(context);
  await Promise.all(matched.map((bindConfig) => reconcileOntologyBindView(
    context,
    bindConfig,
    namespace,
    config.stateTable,
    context.heartbeatLeaseMs,
  )));
}

async function removeMissingOntologyBindStates(
  context: PipelineRunContext,
  table: string,
  expectedKeys: ReadonlySet<string>,
): Promise<void> {
  const rows = await context.services.dataPlane.dataTableRowsList({ table });
  for (const row of rows) {
    const key = typeof row.asset_name_key === "string" ? row.asset_name_key.trim() : "";
    if (key && !expectedKeys.has(key)) {
      await deleteOntologyBindState(context, table, key);
    }
  }
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("builtin ontology bind requires a concrete tenantId/env context");
  }
}

function ontologyFactoryConfig(config: unknown): OntologyFactoryConfig {
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    throw new Error("builtin ontology bind requires bindTable and pipelineTable config");
  }
  const raw = config as Record<string, unknown>;
  const bindTable = textField(raw, "bindTable");
  const pipelineTable = textField(raw, "pipelineTable");
  const stateTable = textField(raw, "stateTable");
  if (!bindTable || !pipelineTable || !stateTable) {
    throw new Error("builtin ontology bind requires bindTable, pipelineTable, and stateTable config");
  }
  return {
    bindTable,
    pipelineTable,
    stateTable,
  };
}

async function loadOntologyBindRows(
  context: PipelineRunContext,
  table: string,
  recordInputs: PipelineRecordInputs,
): Promise<readonly OntologyBindRow[]> {
  const rows = await loadControlRows(context, table, "ontology bind", recordInputs);
  return rows.map((row, index) => ontologyBindRow(row, index));
}

async function loadPipelineConfigRows(
  context: PipelineRunContext,
  table: string,
  recordInputs: PipelineRecordInputs,
): Promise<readonly PipelineConfigRow[]> {
  const rows = await loadControlRows(context, table, "pipeline config", recordInputs);
  return rows.map((row, index) => pipelineConfigRow(row, index));
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
      context.logger.warn(`builtin ontology bind skipped env without ${label} table`, {
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

function ontologyBindRow(row: Record<string, unknown>, index: number): OntologyBindRow {
  const objectTableName = textField(row, "object_table_name");
  const dataAssetName = textField(row, "data_asset_name");
  const implementationKind = textField(row, "kind");
  const bindCodeKey = textField(row, "bind_code_key");
  if (!objectTableName || !dataAssetName || !implementationKind || !bindCodeKey) {
    throw new Error(`ontology bind row[${index}] requires object_table_name, data_asset_name, kind, and bind_code_key`);
  }
  if (implementationKind !== "builtin" && implementationKind !== "custom") {
    throw new Error(`ontology bind row[${index}].kind must be builtin or custom`);
  }
  return {
    object_table_name: objectTableName,
    data_asset_name: dataAssetName,
    kind: implementationKind,
    bind_code_key: bindCodeKey,
    bind_config: jsonObjectField(row.bind_config, `ontology bind ${objectTableName}.bind_config`),
  };
}

function pipelineConfigRow(row: Record<string, unknown>, index: number): PipelineConfigRow {
  const name = textField(row, "name");
  const targetAssetType = targetAssetTypeField(row, "target_asset_type");
  const implementationKind = textField(row, "kind");
  const pipelineCodeKey = textField(row, "pipeline_code_key");
  const dataAssetName = textField(row, "data_asset_name");
  if (!name || !targetAssetType || !implementationKind || !pipelineCodeKey || !dataAssetName) {
    throw new Error(`pipeline config row[${index}] requires name, target_asset_type, kind, pipeline_code_key, and data_asset_name`);
  }
  if (implementationKind !== "builtin" && implementationKind !== "custom") {
    throw new Error(`pipeline config row[${index}].kind must be builtin or custom`);
  }
  return {
    name,
    target_asset_type: targetAssetType,
    kind: implementationKind,
    pipeline_code_key: pipelineCodeKey,
    data_asset_name: dataAssetName,
    processing_config: jsonObjectField(row.processing_config, `pipeline config ${name}.processing_config`),
  };
}

function dataAssetExportsByName(
  pipelineRows: readonly PipelineConfigRow[],
): ReadonlyMap<string, DataAssetExportConfig> {
  const byTable = new Map<string, DataAssetExportConfig>();
  const byName = new Map<string, DataAssetExportConfig>();
  for (const row of pipelineRows) {
    const target = pipelineTargetConfig(row);
    const fields = pipelineFieldMappings(row);
    const dataAsset: DataAssetExportConfig = {
      configName: row.name,
      pipelineCodeKey: row.pipeline_code_key,
      dataAssetName: row.data_asset_name,
      assetTable: target.assetTable,
      primaryKey: target.primaryKey,
      fieldNames: new Set(fields.map((mapping) => mapping.targetField.toLowerCase())),
    };
    const previousByTable = byTable.get(dataAsset.assetTable);
    if (previousByTable) {
      throw new Error(`t_factory_config_pipeline exports duplicate data asset table ${dataAsset.assetTable}: ${previousByTable.configName}, ${dataAsset.configName}`);
    }
    const previousByName = byName.get(dataAsset.dataAssetName);
    if (previousByName) {
      throw new Error(`t_factory_config_pipeline exports duplicate data asset name ${dataAsset.dataAssetName}: ${previousByName.configName}, ${dataAsset.configName}`);
    }
    byTable.set(dataAsset.assetTable, dataAsset);
    byName.set(dataAsset.dataAssetName, dataAsset);
  }
  return byName;
}

function pipelineTargetConfig(row: PipelineConfigRow): PipelineTargetConfig {
  const target = jsonObjectField(row.processing_config.target, `pipeline config ${row.name}.processing_config.target`);
  const assetTable = textField(target, "asset_table");
  if (!assetTable) {
    throw new Error(`pipeline config ${row.name} requires processing_config.target.asset_table`);
  }
  return {
    assetTable,
    primaryKey: stringArray(target.primary_key, `pipeline config ${row.name}.processing_config.target.primary_key`),
  };
}

function pipelineFieldMappings(row: PipelineConfigRow): readonly PipelineFieldMapping[] {
  const value = row.processing_config.field_mappings;
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(`pipeline config ${row.name} requires at least one processing_config.field_mappings entry`);
  }
  return value.map((entry, index) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`pipeline config ${row.name}.processing_config.field_mappings[${index}] must be a JSON object`);
    }
    const raw = entry as Record<string, unknown>;
    const sourceField = textField(raw, "source_field");
    const targetField = textField(raw, "target_field");
    if (!sourceField || !targetField) {
      throw new Error(`pipeline config ${row.name}.processing_config.field_mappings[${index}] requires source_field and target_field`);
    }
    return {
      sourceField,
      targetField,
    };
  });
}

function matchedOntologyBindConfigs(
  context: PipelineRunContext,
  rows: readonly OntologyBindRow[],
  dataAssetsByName: ReadonlyMap<string, DataAssetExportConfig>,
): readonly MatchedOntologyBindConfig[] {
  return rows
    .filter((row) =>
      row.kind === BIND_IMPLEMENTATION_KIND
      && row.bind_code_key === context.pipelineKey)
    .map((row) => {
      const bind = normalizeBuiltinBindConfig(row);
      const dataAsset = dataAssetsByName.get(row.data_asset_name);
      if (!dataAsset) {
        throw new Error(`ontology bind ${row.object_table_name} references data asset ${row.data_asset_name}, but t_factory_config_pipeline does not export it`);
      }
      assertDataAssetCompatibility(row.object_table_name, bind, dataAsset);
      return {
        objectTableName: row.object_table_name,
        bind,
        dataAsset,
      };
    });
}

function normalizeBuiltinBindConfig(row: OntologyBindRow): BuiltinBindConfig {
  const config = row.bind_config;
  const objectType = textField(config, "object_type");
  const objectPrimaryKey = textField(config, "object_primary_key");
  const dataAssetTable = textField(config, "data_asset_table");
  const dataAssetPrimaryKey = stringArray(config.data_asset_primary_key, `ontology bind ${row.object_table_name}.bind_config.data_asset_primary_key`);
  const projection = projectionConfig(
    jsonObjectField(config.projection, `ontology bind ${row.object_table_name}.bind_config.projection`),
    row.object_table_name,
  );
  if (!objectType || !objectPrimaryKey || !dataAssetTable) {
    throw new Error(`ontology bind ${row.object_table_name} requires bind_config.object_type, object_primary_key, and data_asset_table`);
  }
  if (dataAssetPrimaryKey.length === 0) {
    throw new Error(`ontology bind ${row.object_table_name} data_asset_primary_key must contain at least one field`);
  }
  const mappings = Array.isArray(config.field_mappings)
    ? config.field_mappings.map((entry, index) =>
        fieldMapping(entry, row.object_table_name, index, objectPrimaryKey, projection))
    : [];
  if (mappings.length === 0) {
    throw new Error(`ontology bind ${row.object_table_name} requires at least one field_mappings entry`);
  }
  const objectFields = new Set<string>();
  for (const mapping of mappings) {
    const fieldKey = mapping.objectField.toLowerCase();
    if (objectFields.has(fieldKey)) {
      throw new Error(`ontology bind ${row.object_table_name} has duplicate object field ${mapping.objectField}`);
    }
    objectFields.add(fieldKey);
  }
  const primaryMapping = mappings.find((mapping) => mapping.objectField === objectPrimaryKey);
  if (!primaryMapping) {
    throw new Error(`ontology bind ${row.object_table_name} object primary key ${objectPrimaryKey} is not in field_mappings`);
  }
  if (!primaryMapping.materialized) {
    throw new Error(`ontology bind ${row.object_table_name} object primary key ${objectPrimaryKey} must be materialized`);
  }
  if (!sameStringSet([primaryMapping.assetField], dataAssetPrimaryKey)) {
    throw new Error(`ontology bind ${row.object_table_name} object primary key ${objectPrimaryKey} must map to data asset primary key ${dataAssetPrimaryKey.join(", ")}`);
  }
  return {
    releaseId: textField(config, "release_id"),
    objectType,
    objectPrimaryKey,
    dataAssetTable,
    dataAssetPrimaryKey,
    projection,
    fieldMappings: mappings,
  };
}

function projectionConfig(input: Record<string, unknown>, objectTableName: string): BindProjectionConfig {
  const mode = textField(input, "mode");
  if (!mode) {
    throw new Error(`ontology bind ${objectTableName} projection.mode is required`);
  }
  return {
    mode,
    indexedProperties: stringArray(input.indexed_properties ?? [], `ontology bind ${objectTableName}.projection.indexed_properties`),
    displayProperties: stringArray(input.display_properties ?? [], `ontology bind ${objectTableName}.projection.display_properties`),
    materializedProperties: stringArray(input.materialized_properties ?? [], `ontology bind ${objectTableName}.projection.materialized_properties`),
    fallbackPolicy: textField(input, "fallback_policy"),
  };
}

function fieldMapping(
  entry: unknown,
  objectTableName: string,
  index: number,
  objectPrimaryKey: string,
  projection: BindProjectionConfig,
): BindFieldMapping {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    throw new Error(`ontology bind ${objectTableName} field_mappings[${index}] must be a JSON object`);
  }
  const raw = entry as Record<string, unknown>;
  const objectField = textField(raw, "object_field");
  const assetField = textField(raw, "asset_field");
  const type = textField(raw, "type");
  if (!objectField || !assetField || !type) {
    throw new Error(`ontology bind ${objectTableName} field_mappings[${index}] requires object_field, asset_field, and type`);
  }
  const materializedByPolicy = projection.materializedProperties.includes("*")
    || projection.materializedProperties.includes(objectField)
    || objectField === objectPrimaryKey;
  return {
    objectField,
    assetField,
    type,
    primaryKey: booleanField(raw, "primary_key") ?? objectField === objectPrimaryKey,
    display: booleanField(raw, "display") ?? projection.displayProperties.includes(objectField),
    indexed: booleanField(raw, "indexed") ?? projection.indexedProperties.includes(objectField),
    materialized: booleanField(raw, "materialized") ?? materializedByPolicy,
  };
}

function assertDataAssetCompatibility(
  objectTableName: string,
  bind: BuiltinBindConfig,
  dataAsset: DataAssetExportConfig,
): void {
  if (bind.dataAssetTable !== dataAsset.assetTable) {
    throw new Error(`ontology bind ${objectTableName} data_asset_table ${bind.dataAssetTable} does not match t_factory_config_pipeline ${dataAsset.configName} target asset table ${dataAsset.assetTable}`);
  }
  if (!sameStringSet(bind.dataAssetPrimaryKey, dataAsset.primaryKey)) {
    throw new Error(`ontology bind ${objectTableName} data asset primary key ${bind.dataAssetPrimaryKey.join(", ")} does not match t_factory_config_pipeline ${dataAsset.configName} primary key ${dataAsset.primaryKey.join(", ")}`);
  }
  for (const mapping of bind.fieldMappings) {
    if (!dataAsset.fieldNames.has(mapping.assetField.toLowerCase())) {
      throw new Error(`ontology bind ${objectTableName} references data asset field ${mapping.assetField}, but t_factory_config_pipeline ${dataAsset.configName} does not export it`);
    }
  }
}

async function reconcileOntologyBindView(
  context: PipelineRunContext,
  bindConfig: MatchedOntologyBindConfig,
  namespace: ViewsNamespaceRef,
  stateTable: string,
  leaseMs: number,
): Promise<void> {
  const stateKey = bindConfig.dataAsset.dataAssetName;
  let expectedHash = ontologyBindStateHash(bindConfig);
  try {
    const mappings = materializedFieldMappings(bindConfig);
    const assetColumns = await loadAssetColumns(context, namespace, bindConfig.dataAsset.assetTable);
    const resolvedMappings = resolveAssetSourceTypes(bindConfig, mappings, assetColumns);
    expectedHash = ontologyBindStateHash(bindConfig, resolvedMappings);
    const previous = await loadOntologyBindState(context, stateTable, stateKey);
    if (ontologyBindLeaseReady(previous, expectedHash)) {
      await renewOntologyBindHeartbeat(context, {
        table: stateTable,
        key: stateKey,
        leaseMs,
      });
      return;
    }

    await writeOntologyBindState(context, {
      table: stateTable,
      key: stateKey,
      leaseMs,
      expectedHash,
      status: "reconciling",
    });
    const result = await context.services.dataPlane.dataEnvironmentViewReconcile({
      actor: context.actor,
      view: bindConfig.objectTableName,
      source: {
        kind: SOURCE_VIEW_KIND,
        catalog: namespace.catalog,
        schema: namespace.schema,
        table: bindConfig.dataAsset.assetTable,
      },
      field_mappings: resolvedMappings.map((mapping) => ({
        source_field: mapping.assetField,
        target_field: mapping.objectField,
        source_type: mapping.sourceType,
      })),
      marker: viewMarker(context, bindConfig),
    });
    await writeOntologyBindState(context, {
      table: stateTable,
      key: stateKey,
      leaseMs,
      expectedHash,
      status: "ready",
    });
    if (result.changed) {
      context.logger.info("ontology view changed", {
        objectTable: bindConfig.objectTableName,
        action: result.action,
        objectType: bindConfig.bind.objectType,
        dataAsset: bindConfig.dataAsset.dataAssetName,
        view: result.query_view_ref,
      });
    }
  } catch (error) {
    await writeOntologyBindState(context, {
      table: stateTable,
      key: stateKey,
      leaseMs,
      expectedHash,
      status: "failed",
      error,
    });
    context.logger.warn("ontology bind reconcile failed", {
      objectTable: bindConfig.objectTableName,
      dataAsset: bindConfig.dataAsset.dataAssetName,
      error: errorMessage(error),
    });
  }
}

function ontologyBindStateHash(
  bindConfig: MatchedOntologyBindConfig,
  resolvedMappings?: readonly ResolvedBindFieldMapping[],
): string {
  return createHash("sha256")
    .update(JSON.stringify(stableValue({
      objectTableName: bindConfig.objectTableName,
      bind: bindConfig.bind,
      dataAsset: {
        configName: bindConfig.dataAsset.configName,
        pipelineCodeKey: bindConfig.dataAsset.pipelineCodeKey,
        dataAssetName: bindConfig.dataAsset.dataAssetName,
        assetTable: bindConfig.dataAsset.assetTable,
        primaryKey: bindConfig.dataAsset.primaryKey,
        fieldNames: [...bindConfig.dataAsset.fieldNames].sort(),
      },
      ...(resolvedMappings
        ? { sourceTypes: resolvedMappings.map((mapping) => ({
            assetField: mapping.assetField,
            sourceType: mapping.sourceType,
          })) }
        : {}),
    })))
    .digest("hex");
}

async function loadAssetColumns(
  context: PipelineRunContext,
  namespace: ViewsNamespaceRef,
  assetTable: string,
): Promise<readonly AssetColumnInfo[]> {
  const result = await context.services.dataPlane.dataEnvironmentSqlQuery({
    actor: context.actor,
    catalog: namespace.catalog,
    schema: namespace.schema,
    max_pages: 10,
    sql: [
      "SELECT column_name, data_type",
      "FROM information_schema.columns",
      `WHERE table_schema = ${quoteSqlString(namespace.schema)}`,
      `  AND table_name = ${quoteSqlString(assetTable)}`,
      "ORDER BY ordinal_position",
    ].join("\n"),
  });
  const columns = result.rows.map((row) => ({
    name: stringCell(row[0], "column_name"),
    type: stringCell(row[1], "data_type"),
  }));
  if (columns.length === 0) {
    throw new Error(
      `data asset ${namespace.catalog}.${namespace.schema}.${assetTable} does not expose columns in information_schema`,
    );
  }
  return columns;
}

function resolveAssetSourceTypes(
  bindConfig: MatchedOntologyBindConfig,
  mappings: readonly BindFieldMapping[],
  columns: readonly AssetColumnInfo[],
): readonly ResolvedBindFieldMapping[] {
  const columnsByName = new Map<string, AssetColumnInfo>();
  for (const column of columns) {
    const key = column.name.toLowerCase();
    if (columnsByName.has(key)) {
      throw new Error(`data asset ${bindConfig.dataAsset.assetTable} exposes duplicate column ${column.name}`);
    }
    columnsByName.set(key, column);
  }
  return mappings.map((mapping) => {
    const column = columnsByName.get(mapping.assetField.toLowerCase());
    if (!column) {
      throw new Error(
        `ontology bind ${bindConfig.objectTableName} references missing data asset column ${mapping.assetField}`,
      );
    }
    return { ...mapping, sourceType: column.type };
  });
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

function materializedFieldMappings(
  bindConfig: MatchedOntologyBindConfig,
): readonly BindFieldMapping[] {
  const mappings = bindConfig.bind.fieldMappings.filter((mapping) =>
    mapping.primaryKey || mapping.materialized);
  if (mappings.length === 0) {
    throw new Error(`ontology bind ${bindConfig.objectTableName} has no materialized fields`);
  }
  if (!mappings.some((mapping) => mapping.objectField === bindConfig.bind.objectPrimaryKey)) {
    throw new Error(`ontology bind ${bindConfig.objectTableName} materialized fields must include ${bindConfig.bind.objectPrimaryKey}`);
  }
  return mappings;
}

async function currentViewsNamespace(
  context: PipelineRunContext,
): Promise<ViewsNamespaceRef> {
  const result = await context.services.dataPlane.dataEnvironmentSqlQuery({
    actor: context.actor,
    max_pages: 1,
    sql: "SELECT 1 AS seedlens_namespace_probe",
  });
  const catalog = result.catalog.trim();
  const schema = result.schema.trim();
  if (!catalog || !schema) {
    throw new Error(
      `cannot resolve views namespace for ${context.services.scope.tenantId}/${context.services.scope.env}`,
    );
  }
  return { catalog, schema };
}

function viewMarker(context: PipelineRunContext, bindConfig: MatchedOntologyBindConfig): string {
  return [
    "agentnexus.managed_by=lakectd",
    `agentnexus.pipeline_key=${context.pipelineKey}`,
    `agentnexus.bind_kind=${BIND_IMPLEMENTATION_KIND}`,
    `agentnexus.object_table=${bindConfig.objectTableName}`,
    `agentnexus.object_type=${bindConfig.bind.objectType}`,
    `agentnexus.data_asset_name=${bindConfig.dataAsset.dataAssetName}`,
    `agentnexus.data_asset_table=${bindConfig.dataAsset.assetTable}`,
    `agentnexus.data_asset_pipeline=${bindConfig.dataAsset.pipelineCodeKey}`,
    ...(bindConfig.bind.releaseId ? [`agentnexus.release_id=${bindConfig.bind.releaseId}`] : []),
  ].join("; ");
}

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) {
    return false;
  }
  const normalizedRight = new Set(right.map((value) => value.toLowerCase()));
  return left.every((value) => normalizedRight.has(value.toLowerCase()));
}

function targetAssetTypeField(
  input: Record<string, unknown>,
  key: string,
): DataAssetTargetType | undefined {
  const value = textField(input, key);
  if (!value) {
    return undefined;
  }
  if (value !== "virtual" && value !== "physical") {
    throw new Error(`${key} must be virtual or physical`);
  }
  return value;
}

function textField(input: Record<string, unknown>, key: string): string | undefined {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function booleanField(input: Record<string, unknown>, key: string): boolean | undefined {
  const value = input[key];
  return typeof value === "boolean" ? value : undefined;
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

function quoteSqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function stringCell(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value.trim();
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
