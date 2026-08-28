import type { PipelineRunContext } from "./types.js";

export type SourceMaterialStateStatus = "running" | "ready" | "failed";

export interface SourceMaterialReconcileState {
  status: SourceMaterialStateStatus;
  run_id: string;
  config_hash: string;
  started_at: string;
  updated_at: string;
  material_set_hash?: string;
  table_count?: number;
  created_count?: number;
  updated_count?: number;
  unchanged_count?: number;
  deleted_count?: number;
  before_snapshot_id?: string;
  after_snapshot_id?: string;
  last_successful_run_id?: string;
  last_successful_material_set_hash?: string;
  last_successful_snapshot_id?: string;
  last_successful_at?: string;
  error?: string;
}

export interface SourceMaterialStateRow {
  data_source_name_key: string;
  heartbeat_lease_at: unknown;
  reconcile_state: SourceMaterialReconcileState;
}

export async function loadSourceMaterialStates(
  context: PipelineRunContext,
  table: string,
): Promise<readonly SourceMaterialStateRow[]> {
  const rows = await context.services.dataPlane.dataTableRowsList({
    table,
  });
  return rows.map((row, index) => {
    const key = textValue(row.data_source_name_key);
    if (!key) {
      throw new Error(`${table} row[${index}] requires data_source_name_key`);
    }
    return {
      data_source_name_key: key,
      heartbeat_lease_at: row.heartbeat_lease_at,
      reconcile_state: reconcileState(row.reconcile_state),
    };
  });
}

export function sourceMaterialRunActive(
  row: SourceMaterialStateRow | undefined,
  configHash: string,
  now = new Date(),
): boolean {
  if (
    !row
    || row.reconcile_state.status !== "running"
    || row.reconcile_state.config_hash !== configHash
  ) {
    return false;
  }
  const leaseAt = new Date(String(row.heartbeat_lease_at ?? ""));
  return !Number.isNaN(leaseAt.getTime()) && leaseAt.getTime() > now.getTime();
}

export async function writeSourceMaterialState(
  context: PipelineRunContext,
  input: {
    table: string;
    key: string;
    leaseMs: number;
    state: SourceMaterialReconcileState;
  },
): Promise<void> {
  const status = input.state.status === "running" ? "reconciling" : input.state.status;
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table: input.table,
    operation: "set",
    key: input.key,
    record: {
      data_source_name_key: input.key,
      heartbeat_lease_at: leaseAt(input.leaseMs),
      reconcile_status: status,
      status_updated_at: input.state.updated_at,
      reconcile_state: JSON.stringify(input.state),
    },
  });
}

export async function deleteSourceMaterialState(
  context: PipelineRunContext,
  table: string,
  key: string,
): Promise<void> {
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table,
    operation: "delete",
    key,
    record: { data_source_name_key: key },
  });
}

function reconcileState(value: unknown): SourceMaterialReconcileState {
  const parsed = parseJson(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return failedState();
  }
  const raw = parsed as Record<string, unknown>;
  const status = raw.status === "running" || raw.status === "ready" ? raw.status : "failed";
  return {
    status,
    run_id: textValue(raw.run_id) ?? "",
    config_hash: textValue(raw.config_hash) ?? "",
    started_at: textValue(raw.started_at) ?? "",
    updated_at: textValue(raw.updated_at) ?? "",
    ...optionalText("material_set_hash", raw.material_set_hash),
    ...optionalNumber("table_count", raw.table_count),
    ...optionalNumber("created_count", raw.created_count),
    ...optionalNumber("updated_count", raw.updated_count),
    ...optionalNumber("unchanged_count", raw.unchanged_count),
    ...optionalNumber("deleted_count", raw.deleted_count),
    ...optionalText("before_snapshot_id", raw.before_snapshot_id),
    ...optionalText("after_snapshot_id", raw.after_snapshot_id),
    ...optionalText("last_successful_run_id", raw.last_successful_run_id),
    ...optionalText("last_successful_material_set_hash", raw.last_successful_material_set_hash),
    ...optionalText("last_successful_snapshot_id", raw.last_successful_snapshot_id),
    ...optionalText("last_successful_at", raw.last_successful_at),
    ...optionalText("error", raw.error),
  };
}

function failedState(): SourceMaterialReconcileState {
  return {
    status: "failed",
    run_id: "",
    config_hash: "",
    started_at: "",
    updated_at: "",
  };
}

function parseJson(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return undefined;
  }
}

function textValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function optionalText<K extends string>(key: K, value: unknown): Partial<Record<K, string>> {
  const text = textValue(value);
  return text ? { [key]: text } as Record<K, string> : {};
}

function optionalNumber<K extends string>(key: K, value: unknown): Partial<Record<K, number>> {
  return typeof value === "number" && Number.isFinite(value)
    ? { [key]: value } as Record<K, number>
    : {};
}

function leaseAt(leaseMs: number): string {
  return new Date(Date.now() + Math.max(0, leaseMs)).toISOString();
}
