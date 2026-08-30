import type { EnvironmentStateInputTableConfig } from "./config.js";

const ENVIRONMENT_HEALTH_SUMMARY_VERSION = 1 as const;

export type PipelineReconcileStatus = "ready" | "reconciling" | "failed" | "unknown";

export interface StateCounts {
  ready: number;
  reconciling: number;
  failed: number;
  unknown: number;
}

export interface StateTableSummary {
  step: string;
  table: string;
  role: "critical" | "reference";
  status: PipelineReconcileStatus;
  record_count: number;
  reconcile_source_record_count?: number;
  record_counts: StateCounts;
  earliest_lease_at?: string;
  error?: string;
}

export interface StateGroupSummary {
  version: typeof ENVIRONMENT_HEALTH_SUMMARY_VERSION;
  status: PipelineReconcileStatus;
  table_count: number;
  record_count: number;
  table_counts: StateCounts;
  record_counts: StateCounts;
  tables: readonly StateTableSummary[];
}

interface EffectiveRowState {
  status: PipelineReconcileStatus;
  leaseAt?: Date;
}

const STATUS_PRIORITY: Readonly<Record<PipelineReconcileStatus, number>> = {
  ready: 0,
  reconciling: 1,
  unknown: 2,
  failed: 3,
};

export function summarizeStateTable(
  config: EnvironmentStateInputTableConfig,
  rows: readonly Record<string, unknown>[],
  now: Date,
  reconcileSourceRecordCount?: number,
): StateTableSummary {
  if (rows.length === 0) {
    const status = reconcileSourceRecordCount === 0 ? "ready" : "unknown";
    return {
      step: config.step,
      table: config.table,
      role: config.role,
      status,
      record_count: 0,
      ...(reconcileSourceRecordCount !== undefined
        ? { reconcile_source_record_count: reconcileSourceRecordCount }
        : {}),
      record_counts: emptyCounts(),
    };
  }

  const states = rows.map((row) => effectiveRowState(row, now));
  const counts = emptyCounts();
  for (const state of states) {
    counts[state.status] += 1;
  }
  const validLeases = states.flatMap((state) => state.leaseAt ? [state.leaseAt] : []);
  const earliestLease = earliestDate(validLeases);
  return {
    step: config.step,
    table: config.table,
    role: config.role,
    status: aggregateStatus(states.map((state) => state.status)),
    record_count: rows.length,
    record_counts: counts,
    ...(earliestLease ? { earliest_lease_at: earliestLease.toISOString() } : {}),
  };
}

export function failedStateTableSummary(
  config: EnvironmentStateInputTableConfig,
  error: unknown,
): StateTableSummary {
  return {
    step: config.step,
    table: config.table,
    role: config.role,
    status: "unknown",
    record_count: 0,
    record_counts: emptyCounts(),
    error: errorMessage(error),
  };
}

export function summarizeStateGroup(
  tables: readonly StateTableSummary[],
): StateGroupSummary {
  const tableCounts = emptyCounts();
  const recordCounts = emptyCounts();
  let recordCount = 0;
  for (const table of tables) {
    recordCount += table.record_count;
    tableCounts[table.status] += 1;
    for (const status of statusValues()) {
      recordCounts[status] += table.record_counts[status];
    }
  }
  return {
    version: ENVIRONMENT_HEALTH_SUMMARY_VERSION,
    status: aggregateStatus(tables.map((table) => table.status)),
    table_count: tables.length,
    record_count: recordCount,
    table_counts: tableCounts,
    record_counts: recordCounts,
    tables,
  };
}

export function reconciledLeaseAt(
  now: Date,
  leaseMs: number,
  status: PipelineReconcileStatus,
  criticalTables: readonly StateTableSummary[],
): Date {
  const ownLease = new Date(now.getTime() + leaseMs);
  if (status === "unknown") {
    return ownLease;
  }
  const inputLeases = criticalTables.flatMap((table) => {
    if (!table.earliest_lease_at) {
      return [];
    }
    const leaseAt = new Date(table.earliest_lease_at);
    return Number.isNaN(leaseAt.getTime()) ? [] : [leaseAt];
  });
  return earliestDate([ownLease, ...inputLeases]) ?? ownLease;
}

function effectiveRowState(row: Record<string, unknown>, now: Date): EffectiveRowState {
  const status = reconcileStatus(row.reconcile_status);
  const leaseAt = timestamp(row.heartbeat_lease_at);
  const updatedAt = timestamp(row.status_updated_at);
  if (!status || !leaseAt || !updatedAt || leaseAt.getTime() <= now.getTime()) {
    return { status: "unknown" };
  }
  return { status, leaseAt };
}

function aggregateStatus(statuses: readonly PipelineReconcileStatus[]): PipelineReconcileStatus {
  if (statuses.length === 0) {
    return "ready";
  }
  return statuses.reduce((current, status) =>
    STATUS_PRIORITY[status] > STATUS_PRIORITY[current] ? status : current, "ready");
}

function reconcileStatus(value: unknown): PipelineReconcileStatus | undefined {
  return value === "ready" || value === "reconciling" || value === "failed" || value === "unknown"
    ? value
    : undefined;
}

function timestamp(value: unknown): Date | undefined {
  const parsed = new Date(String(value ?? ""));
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

function earliestDate(values: readonly Date[]): Date | undefined {
  return values.reduce<Date | undefined>((earliest, value) =>
    !earliest || value.getTime() < earliest.getTime() ? value : earliest, undefined);
}

function emptyCounts(): StateCounts {
  return { ready: 0, reconciling: 0, failed: 0, unknown: 0 };
}

function statusValues(): readonly PipelineReconcileStatus[] {
  return ["ready", "reconciling", "failed", "unknown"];
}

function errorMessage(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, 500);
}
