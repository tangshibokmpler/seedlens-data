import { createHash } from "node:crypto";

import { Pipeline, type ModelFieldType } from "@agentnexus/lakecore/model";
import {
  deleteGlobalViewState,
  globalViewLeaseReady,
  loadGlobalPlaneStates,
  renewGlobalViewHeartbeat,
  writeGlobalViewState,
  type GlobalPlaneStateRow,
} from "./state.js";
import type {
  GlobalTableField,
  GlobalTableSummary,
  PipelineRunContext,
} from "./types.js";

interface GlobalPlaneLoadConfig {
  stateTable: string;
}

interface GlobalViewSchema {
  version: 1;
  source: {
    catalog: string;
    schema: string;
    table: string;
    physical_table_ref: string;
  };
  target: {
    view: string;
  };
  primary_key: string;
  fields: readonly {
    name: string;
    type: ModelFieldType;
    source_type: string;
    required: boolean;
  }[];
}

interface RunSummary {
  created: number;
  updated: number;
  unchanged: number;
  leaseRenewed: number;
  dropped: number;
  failed: number;
}

export class BuiltinGlobalPlaneLoadPipeline {
  @Pipeline({ tag: "pipelinePrelude", pipelineKey: "builtin_global_plane_load" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：把租户级 Global 表以同名查询视图装载到当前 env，使所有环境共享同一份 Global 数据真相。
 * 输入：SharedPlane 返回的完整 Global 表清单，以及当前 env 的 t_global_state_views 状态行。
 * 逻辑：比较上次成功视图结构；结构变化或租约过期时调谐视图，Global 表删除时先删视图再删状态。
 * 幂等：采用 reconcile/cleanup 策略；结构一致且租约有效时只续心跳，失败保留上一次成功视图结构并在后续重试。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = globalPlaneLoadConfig(context.config);
  const listed = await context.services.globalPlane.globalTablesList({
    actor: context.actor,
  });
  if (listed.tenant_id !== context.services.scope.tenantId) {
    throw new Error(`global table list tenant mismatch: expected ${context.services.scope.tenantId}, received ${listed.tenant_id}`);
  }

  const tables = validateGlobalTables(listed.tables, config.stateTable);
  const states = await loadGlobalPlaneStates(context, config.stateTable);
  const stateByTable = new Map(states.map((state) => [state.global_table_name_key, state]));
  const expectedNames = new Set(tables.map((table) => table.table.table_name));
  const summary: RunSummary = {
    created: 0,
    updated: 0,
    unchanged: 0,
    leaseRenewed: 0,
    dropped: 0,
    failed: 0,
  };
  for (const table of tables) {
    if (context.signal.aborted) {
      throw new Error("builtin global plane load aborted");
    }
    const tableName = table.table.table_name;
    const outcome = await reconcileGlobalView(
      context,
      config,
      table,
      stateByTable.get(tableName),
    );
    if (outcome === "lease-renewed") {
      summary.leaseRenewed += 1;
    } else {
      summary[outcome] += 1;
    }
  }

  for (const state of states) {
    if (expectedNames.has(state.global_table_name_key)) {
      continue;
    }
    if (context.signal.aborted) {
      throw new Error("builtin global plane load aborted");
    }
    const outcome = await removeStaleGlobalView(context, config, state);
    if (outcome === "dropped") {
      summary.dropped += 1;
    } else {
      summary.failed += 1;
    }
  }

  context.logger.info("global plane views reconciled", { ...summary });
}

async function reconcileGlobalView(
  context: PipelineRunContext,
  config: GlobalPlaneLoadConfig,
  table: GlobalTableSummary,
  previous: GlobalPlaneStateRow | undefined,
): Promise<"created" | "updated" | "unchanged" | "lease-renewed" | "failed"> {
  const tableName = table.table.table_name;
  let expectedHash = hashValue({ version: 1, table });

  try {
    const viewSchema = globalViewSchema(table);
    const viewSchemaJson = stableJson(viewSchema);
    expectedHash = hashValue(viewSchema);
    if (globalViewLeaseReady(previous, expectedHash, viewSchemaJson)) {
      await renewGlobalViewHeartbeat(context, {
        table: config.stateTable,
        key: tableName,
        leaseMs: context.heartbeatLeaseMs,
      });
      return "lease-renewed";
    }

    await writeGlobalViewState(context, {
      table: config.stateTable,
      key: tableName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash,
      status: "reconciling",
      lastReconciledViewSchema: previous?.last_reconciled_view_schema,
    });
    const result = await context.services.dataPlane.dataEnvironmentViewReconcile({
      actor: context.actor,
      view: tableName,
      source: {
        kind: "global",
        catalog: viewSchema.source.catalog,
        schema: viewSchema.source.schema,
        table: viewSchema.source.table,
      },
      field_mappings: viewSchema.fields.map((field) => ({
        source_field: field.name,
        target_field: field.name,
        source_type: field.source_type,
      })),
      marker: viewMarker(context, tableName, expectedHash),
    });
    await writeGlobalViewState(context, {
      table: config.stateTable,
      key: tableName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash,
      status: "ready",
      lastReconciledViewSchema: viewSchemaJson,
      action: result.action,
      queryViewRef: result.query_view_ref,
    });
    if (result.changed) {
      context.logger.info("global view changed", {
        table: tableName,
        action: result.action,
        view: result.query_view_ref,
      });
    }
    return result.action;
  } catch (error) {
    await writeGlobalViewState(context, {
      table: config.stateTable,
      key: tableName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash,
      status: "failed",
      lastReconciledViewSchema: previous?.last_reconciled_view_schema,
      error,
    });
    context.logger.warn("global view reconcile failed", {
      table: tableName,
      error: errorMessage(error),
    });
    return "failed";
  }
}

async function removeStaleGlobalView(
  context: PipelineRunContext,
  config: GlobalPlaneLoadConfig,
  state: GlobalPlaneStateRow,
): Promise<"dropped" | "failed"> {
  const tableName = state.global_table_name_key;
  if (tableName === config.stateTable) {
    throw new Error(`global view state cannot manage reserved state table ${config.stateTable}`);
  }
  try {
    await writeGlobalViewState(context, {
      table: config.stateTable,
      key: tableName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash: "removed",
      status: "reconciling",
      lastReconciledViewSchema: state.last_reconciled_view_schema,
    });
    const result = await context.services.dataPlane.dataEnvironmentViewDrop({
      actor: context.actor,
      view: tableName,
    });
    await deleteGlobalViewState(context, config.stateTable, tableName);
    context.logger.info("stale global view removed", {
      table: tableName,
      view: result.query_view_ref,
      dropped: result.view_dropped,
    });
    return "dropped";
  } catch (error) {
    await writeGlobalViewState(context, {
      table: config.stateTable,
      key: tableName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash: "removed",
      status: "failed",
      lastReconciledViewSchema: state.last_reconciled_view_schema,
      error,
    });
    context.logger.warn("stale global view cleanup failed", {
      table: tableName,
      error: errorMessage(error),
    });
    return "failed";
  }
}

function validateGlobalTables(
  tables: readonly GlobalTableSummary[],
  stateTable: string,
): readonly GlobalTableSummary[] {
  const names = new Set<string>();
  for (const table of tables) {
    const name = requireNonEmpty(table.table.table_name, "global table name");
    if (name === stateTable) {
      throw new Error(`global table name ${name} is reserved for builtin_global_plane_load state`);
    }
    if (names.has(name)) {
      throw new Error(`duplicate global table from SharedPlane: ${name}`);
    }
    names.add(name);
  }
  return [...tables].sort((left, right) =>
    left.table.table_name.localeCompare(right.table.table_name));
}

function globalViewSchema(table: GlobalTableSummary): GlobalViewSchema {
  const tableName = requireNonEmpty(table.table.table_name, "global table name");
  const primaryKey = requireNonEmpty(table.table.primary_key_field, `global table ${tableName} primary key`);
  const fieldNames = new Set<string>();
  const fields = table.table.table_schema.fields.map((field, index) => {
    const normalized = globalViewField(tableName, field, index);
    if (fieldNames.has(normalized.name)) {
      throw new Error(`global table ${tableName} has duplicate field ${normalized.name}`);
    }
    fieldNames.add(normalized.name);
    return normalized;
  });
  if (fields.length === 0) {
    throw new Error(`global table ${tableName} must expose at least one field`);
  }
  if (!fieldNames.has(primaryKey)) {
    throw new Error(`global table ${tableName} primary key ${primaryKey} is not in its fields`);
  }
  return {
    version: 1,
    source: {
      catalog: requireNonEmpty(table.trino_catalog, `global table ${tableName} trino catalog`),
      schema: requireNonEmpty(table.schema, `global table ${tableName} schema`),
      table: tableName,
      physical_table_ref: requireNonEmpty(
        table.physical_table_ref,
        `global table ${tableName} physical table ref`,
      ),
    },
    target: { view: tableName },
    primary_key: primaryKey,
    fields,
  };
}

function globalViewField(
  tableName: string,
  field: GlobalTableField,
  index: number,
): GlobalViewSchema["fields"][number] {
  return {
    name: requireNonEmpty(field.name, `global table ${tableName} field[${index}].name`),
    type: field.type,
    source_type: postgresSourceType(field.type),
    required: field.required === true,
  };
}

function postgresSourceType(type: ModelFieldType): string {
  if (typeof type !== "string") {
    return "jsonb";
  }
  switch (type) {
    case "string":
    case "text":
      return "text";
    case "int":
      return "integer";
    case "long":
      return "bigint";
    case "float":
      return "real";
    case "double":
      return "double precision";
    case "json":
    case "jsonb":
      return "jsonb";
    default:
      return type;
  }
}

function globalPlaneLoadConfig(config: unknown): GlobalPlaneLoadConfig {
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    throw new Error("builtin global plane load requires stateTable config");
  }
  const raw = config as Record<string, unknown>;
  const stateTable = textField(raw, "stateTable");
  if (!stateTable) {
    throw new Error("builtin global plane load requires stateTable config");
  }
  return { stateTable };
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("builtin global plane load requires a concrete tenantId/env context");
  }
}

function viewMarker(context: PipelineRunContext, table: string, expectedHash: string): string {
  return [
    "agentnexus.managed_by=lakectd",
    `agentnexus.pipeline_key=${context.pipelineKey}`,
    "agentnexus.plane=global",
    `agentnexus.global_table=${table}`,
    `agentnexus.schema_hash=${expectedHash}`,
  ].join("; ");
}

function stableJson(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

function hashValue(value: unknown): string {
  return createHash("sha256").update(stableJson(value)).digest("hex");
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function textField(input: Record<string, unknown>, key: string): string | undefined {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function requireNonEmpty(value: string | undefined, label: string): string {
  const normalized = value?.trim();
  if (!normalized) {
    throw new Error(`${label} must not be empty`);
  }
  return normalized;
}
