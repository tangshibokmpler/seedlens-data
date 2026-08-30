import type { PipelineRunContext } from "./types.js";

export interface ViewReconcileState {
  status: "reconciling" | "ready" | "failed";
  expected_hash: string;
  updated_at: string;
  action?: "created" | "updated" | "unchanged";
  query_view_ref?: string;
  error?: string;
}

export interface GlobalPlaneStateRow {
  global_table_name_key: string;
  heartbeat_lease_at: unknown;
  view_reconcile_state: ViewReconcileState;
  last_reconciled_view_schema?: string;
}

export async function loadGlobalPlaneStates(
  context: PipelineRunContext,
  table: string,
): Promise<readonly GlobalPlaneStateRow[]> {
  const rows = await context.services.dataPlane.dataTableRowsList({
    table,
  });
  return rows.flatMap((row) => {
    const key = textValue(row.global_table_name_key);
    if (!key) {
      return [];
    }
    return [{
      global_table_name_key: key,
      heartbeat_lease_at: row.heartbeat_lease_at,
      view_reconcile_state: reconcileState(row.view_reconcile_state),
      last_reconciled_view_schema: canonicalJsonOptional(row.last_reconciled_view_schema),
    }];
  });
}

export function globalViewLeaseReady(
  row: GlobalPlaneStateRow | undefined,
  expectedHash: string,
  expectedViewSchema: string,
  now = new Date(),
): boolean {
  if (
    !row
    || row.view_reconcile_state.status !== "ready"
    || row.view_reconcile_state.expected_hash !== expectedHash
    || row.last_reconciled_view_schema !== expectedViewSchema
  ) {
    return false;
  }
  const leaseAt = new Date(String(row.heartbeat_lease_at ?? ""));
  return !Number.isNaN(leaseAt.getTime()) && leaseAt.getTime() > now.getTime();
}

export async function renewGlobalViewHeartbeat(
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
      global_table_name_key: input.key,
      heartbeat_lease_at: leaseAt(input.leaseMs),
      reconcile_status: "ready",
      status_updated_at: updatedAt,
    },
  });
}

export async function writeGlobalViewState(
  context: PipelineRunContext,
  input: {
    table: string;
    key: string;
    leaseMs: number;
    expectedHash: string;
    status: "reconciling" | "ready" | "failed";
    lastReconciledViewSchema?: string;
    action?: "created" | "updated" | "unchanged";
    queryViewRef?: string;
    error?: unknown;
  },
): Promise<void> {
  const state: ViewReconcileState = {
    status: input.status,
    expected_hash: input.expectedHash,
    updated_at: new Date().toISOString(),
    ...(input.action ? { action: input.action } : {}),
    ...(input.queryViewRef ? { query_view_ref: input.queryViewRef } : {}),
    ...(input.error === undefined ? {} : { error: errorMessage(input.error) }),
  };
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table: input.table,
    operation: "set",
    key: input.key,
    record: {
      global_table_name_key: input.key,
      heartbeat_lease_at: leaseAt(input.leaseMs),
      reconcile_status: input.status,
      status_updated_at: state.updated_at,
      view_reconcile_state: JSON.stringify(state),
      last_reconciled_view_schema: input.lastReconciledViewSchema ?? null,
    },
  });
}

export async function deleteGlobalViewState(
  context: PipelineRunContext,
  table: string,
  key: string,
): Promise<void> {
  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table,
    operation: "delete",
    key,
    record: { global_table_name_key: key },
  });
}

function reconcileState(value: unknown): ViewReconcileState {
  const parsed = parseJson(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { status: "failed", expected_hash: "", updated_at: "" };
  }
  const raw = parsed as Record<string, unknown>;
  const action = raw.action === "created" || raw.action === "updated" || raw.action === "unchanged"
    ? raw.action
    : undefined;
  const queryViewRef = textValue(raw.query_view_ref);
  const error = textValue(raw.error);
  return {
    status: raw.status === "ready"
      ? "ready"
      : raw.status === "reconciling"
      ? "reconciling"
      : "failed",
    expected_hash: textValue(raw.expected_hash) ?? "",
    updated_at: textValue(raw.updated_at) ?? "",
    ...(action ? { action } : {}),
    ...(queryViewRef ? { query_view_ref: queryViewRef } : {}),
    ...(error ? { error } : {}),
  };
}

function canonicalJsonOptional(value: unknown): string | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const parsed = parseJson(value);
  return parsed === undefined ? undefined : JSON.stringify(stableValue(parsed));
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

function textValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function leaseAt(leaseMs: number): string {
  return new Date(Date.now() + Math.max(0, leaseMs)).toISOString();
}

function errorMessage(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, 2000);
}
