import type { PipelineRunContext } from "./types.js";

interface StatePayload {
  status: "reconciling" | "ready" | "failed";
  expected_hash: string;
  updated_at: string;
  error?: string;
}

interface DataSourceStateRow {
  data_source_name_key: string;
  heartbeat_lease_at: unknown;
  state: StatePayload;
}

export async function loadDataSourceState(
  context: PipelineRunContext,
  table: string,
  key: string,
): Promise<DataSourceStateRow | undefined> {
  const rows = await context.services.dataPlane.dataTableRowsList({
    table,
    filters: { data_source_name_key: key },
    limit: 1,
  });
  const row = rows[0];
  if (!row) {
    return undefined;
  }
  return {
    data_source_name_key: String(row.data_source_name_key ?? ""),
    heartbeat_lease_at: row.heartbeat_lease_at,
    state: statePayload(row.state),
  };
}

export function dataSourceStateReady(
  row: DataSourceStateRow | undefined,
  expectedHash: string,
  now = new Date(),
): boolean {
  if (!row || row.state.status !== "ready" || row.state.expected_hash !== expectedHash) {
    return false;
  }
  const leaseAt = new Date(String(row.heartbeat_lease_at ?? ""));
  return !Number.isNaN(leaseAt.getTime()) && leaseAt.getTime() > now.getTime();
}

export async function renewDataSourceHeartbeat(
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
      data_source_name_key: input.key,
      heartbeat_lease_at: leaseAt(input.leaseMs),
      reconcile_status: "ready",
      status_updated_at: updatedAt,
    },
  });
}

export async function writeDataSourceState(
  context: PipelineRunContext,
  input: {
    table: string;
    key: string;
    leaseMs: number;
    expectedHash: string;
    status: "reconciling" | "ready" | "failed";
    error?: unknown;
  },
): Promise<void> {
  const updatedAt = new Date().toISOString();
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table: input.table,
    operation: "set",
    key: input.key,
    record: {
      data_source_name_key: input.key,
      heartbeat_lease_at: leaseAt(input.leaseMs),
      reconcile_status: input.status,
      status_updated_at: updatedAt,
      state: JSON.stringify({
        status: input.status,
        expected_hash: input.expectedHash,
        updated_at: updatedAt,
        ...(input.error === undefined ? {} : { error: errorMessage(input.error) }),
      } satisfies StatePayload),
    },
  });
}

export async function deleteDataSourceState(
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

function statePayload(value: unknown): StatePayload {
  const parsed = typeof value === "string" ? JSON.parse(value) as unknown : value;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { status: "failed", expected_hash: "", updated_at: "" };
  }
  const raw = parsed as Record<string, unknown>;
  return {
    status: raw.status === "ready"
      ? "ready"
      : raw.status === "reconciling"
      ? "reconciling"
      : "failed",
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
