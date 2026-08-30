import { assertBusinessTableName } from "@agentnexus/lakecore/model";

export type StateTableRole = "critical" | "reference";

export type ReconcileSourceConfig =
  | { kind: "globalTables" }
  | { kind: "controlTable"; table: string };

export interface EnvironmentStateInputTableConfig {
  step: string;
  table: string;
  keyField: string;
  role: StateTableRole;
  reconcileSource: ReconcileSourceConfig;
}

export interface EnvironmentStateReconcilerConfig {
  environmentStateTable: string;
  stateTables: readonly EnvironmentStateInputTableConfig[];
}

export function defineEnvironmentStateReconcilerConfig(input: unknown): EnvironmentStateReconcilerConfig {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new Error("environmentStateReconciler config must be an object");
  }
  const raw = input as Record<string, unknown>;
  const environmentStateTable = tableName(
    raw.environmentStateTable,
    "environmentStateReconciler config.environmentStateTable",
  );
  if (!Array.isArray(raw.stateTables) || raw.stateTables.length === 0) {
    throw new Error("environmentStateReconciler config.stateTables must be a non-empty array");
  }

  const seen = new Set<string>();
  const stateTables = raw.stateTables.map((entry, index) => {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`environmentStateReconciler config.stateTables[${index}] must be an object`);
    }
    const item = entry as Record<string, unknown>;
    const step = identifier(
      item.step,
      `environmentStateReconciler config.stateTables[${index}].step`,
    );
    const table = tableName(
      item.table,
      `environmentStateReconciler config.stateTables[${index}].table`,
    );
    const keyField = identifier(
      item.keyField,
      `environmentStateReconciler config.stateTables[${index}].keyField`,
    );
    if (table === environmentStateTable) {
      throw new Error(
        `environmentStateReconciler environment state table ${table} cannot be an input state table`,
      );
    }
    if (seen.has(table)) {
      throw new Error(`environmentStateReconciler config contains duplicate state table ${table}`);
    }
    seen.add(table);
    if (item.role !== "critical" && item.role !== "reference") {
      throw new Error(
        `environmentStateReconciler config.stateTables[${index}].role must be critical or reference`,
      );
    }
    const role: StateTableRole = item.role;
    const reconcileSource = reconcileSourceConfig(item.reconcileSource, index);
    return { step, table, keyField, role, reconcileSource };
  });
  if (!stateTables.some((entry) => entry.role === "critical")) {
    throw new Error("environmentStateReconciler config requires at least one critical state table");
  }
  return { environmentStateTable, stateTables };
}

function identifier(value: unknown, label: string): string {
  if (typeof value !== "string" || !/^[a-z][a-z0-9_]*$/.test(value.trim())) {
    throw new Error(`${label} must be a lowercase ASCII identifier`);
  }
  return value.trim();
}

function reconcileSourceConfig(value: unknown, index: number): ReconcileSourceConfig {
  const label = `environmentStateReconciler config.stateTables[${index}].reconcileSource`;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  const raw = value as Record<string, unknown>;
  if (raw.kind === "globalTables") {
    return { kind: "globalTables" };
  }
  if (raw.kind === "controlTable") {
    return {
      kind: "controlTable",
      table: tableName(raw.table, `${label}.table`),
    };
  }
  throw new Error(`${label}.kind must be globalTables or controlTable`);
}

function tableName(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return assertBusinessTableName(value.trim(), label);
}
