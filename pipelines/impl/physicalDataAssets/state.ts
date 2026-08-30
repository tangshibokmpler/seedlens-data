import type { PipelineRunContext } from "./types.js";

export interface ReconcileState {
  status: "pending" | "ready" | "failed";
  expected_hash: string;
  updated_at: string;
  error?: string;
}

export interface TablePipelineStateRow {
  config_key: string;
  heartbeat_lease_at: unknown;
  table_reconcile_state: ReconcileState;
  data_reconcile_state: ReconcileState;
}

export async function loadTablePipelineState(
  context: PipelineRunContext,
  table: string,
  key: string,
): Promise<TablePipelineStateRow | undefined> {
  const rows = await context.services.dataPlane.dataTableRowsList({
    table,
    filters: { config_key: key },
    limit: 1,
  });
  const row = rows[0];
  if (!row) {
    return undefined;
  }
  return {
    config_key: String(row.config_key ?? ""),
    heartbeat_lease_at: row.heartbeat_lease_at,
    table_reconcile_state: reconcileState(row.table_reconcile_state),
    data_reconcile_state: reconcileState(row.data_reconcile_state),
  };
}

export function tableReconcileLeaseReady(
  row: TablePipelineStateRow | undefined,
  expectedHash: string,
  now = new Date(),
): boolean {
  if (
    !row
    || row.table_reconcile_state.status !== "ready"
    || row.table_reconcile_state.expected_hash !== expectedHash
  ) {
    return false;
  }
  const leaseAt = new Date(String(row.heartbeat_lease_at ?? ""));
  return !Number.isNaN(leaseAt.getTime()) && leaseAt.getTime() > now.getTime();
}

export function reconcileStateValue(
  status: ReconcileState["status"],
  expectedHash: string,
  error?: unknown,
): ReconcileState {
  return {
    status,
    expected_hash: expectedHash,
    updated_at: new Date().toISOString(),
    ...(error === undefined ? {} : { error: errorMessage(error) }),
  };
}

export async function renewTableHeartbeat(
  context: PipelineRunContext,
  input: { table: string; key: string; leaseMs: number },
): Promise<void> {
  const updatedAt = new Date().toISOString();
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table: input.table,
    operation: "update",
    key: input.key,
    record: {
      config_key: input.key,
      heartbeat_lease_at: leaseAt(input.leaseMs),
      reconcile_status: "ready",
      status_updated_at: updatedAt,
    },
  });
}

export async function deleteTablePipelineState(
  context: PipelineRunContext,
  table: string,
  key: string,
): Promise<void> {
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table,
    operation: "delete",
    key,
    record: { config_key: key },
  });
}

export async function writeTablePipelineState(
  context: PipelineRunContext,
  input: {
    table: string;
    key: string;
    leaseMs: number;
    tableState: ReconcileState;
    dataState: ReconcileState;
    success: boolean;
  },
): Promise<void> {
  const status = pipelineStatus(input.tableState, input.dataState, input.success);
  const updatedAt = new Date().toISOString();
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table: input.table,
    operation: "set",
    key: input.key,
    record: {
      config_key: input.key,
      heartbeat_lease_at: leaseAt(input.leaseMs),
      reconcile_status: status,
      status_updated_at: updatedAt,
      table_reconcile_state: JSON.stringify(input.tableState),
      data_reconcile_state: JSON.stringify(input.dataState),
    },
  });
}

function pipelineStatus(
  tableState: ReconcileState,
  dataState: ReconcileState,
  success: boolean,
): "reconciling" | "ready" | "failed" {
  if (success && tableState.status === "ready" && dataState.status === "ready") {
    return "ready";
  }
  if (tableState.status === "failed" || dataState.status === "failed") {
    return "failed";
  }
  return "reconciling";
}

function reconcileState(value: unknown): ReconcileState {
  const parsed = typeof value === "string" ? JSON.parse(value) as unknown : value;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return reconcileStateValue("pending", "");
  }
  const raw = parsed as Record<string, unknown>;
  const status = raw.status === "ready" || raw.status === "failed" ? raw.status : "pending";
  return {
    status,
    expected_hash: typeof raw.expected_hash === "string" ? raw.expected_hash : "",
    updated_at: typeof raw.updated_at === "string" ? raw.updated_at : "",
    ...(typeof raw.error === "string" ? { error: raw.error } : {}),
  };
}

function leaseAt(leaseMs: number): string {
  return new Date(Date.now() + Math.max(0, leaseMs)).toISOString();
}

function errorMessage(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, 2000);
}
