import { Pipeline } from "@agentnexus/lakecore/model";
import { defineEnvironmentStateReconcilerConfig } from "./config.js";
import type {
  EnvironmentStateInputTableConfig,
  ReconcileSourceConfig,
} from "./config.js";
import {
  failedStateTableSummary,
  reconciledLeaseAt,
  summarizeStateGroup,
  summarizeStateTable,
} from "./state.js";
import type { PipelineRunContext } from "./types.js";

const STATE_PAGE_SIZE = 5_000;
const ENVIRONMENT_HEALTH_STATE_KEY = "environment";

export class BuiltinEnvironmentStateReconcilerPipeline {
  @Pipeline({
    tag: "pipelineFinalizer",
    pipelineKey: "builtin_environment_state_reconciler",
  })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：汇总当前 env 的关键路径与参考路径状态，并写入唯一的环境最终状态记录。
 * 输入：config 声明的状态表、各表公共调谐状态列和心跳租约。
 * 逻辑：逐表读取完整状态；状态为空时，仅当对应调协源也为空才归为 ready，否则归为 unknown。
 * 幂等：采用 reconcile 策略，以固定主键覆盖最终状态；参考路径只写汇总，不影响最终状态和租约。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  assertConcreteTarget(context);
  const config = defineEnvironmentStateReconcilerConfig(context.config);
  const now = new Date();
  const summaries = await Promise.all(config.stateTables.map(async (table) => {
    try {
      const rows = await loadStateRows(context, table);
      const reconcileSourceRecordCount = rows.length === 0
        ? await countReconcileSourceRecords(context, table.reconcileSource)
        : undefined;
      return summarizeStateTable(table, rows, now, reconcileSourceRecordCount);
    } catch (error) {
      context.logger.warn("environment state reconciler table read failed", {
        table: table.table,
        error: errorMessage(error),
      });
      return failedStateTableSummary(table, error);
    }
  }));
  const criticalTables = summaries.filter((table) => table.role === "critical");
  const referenceTables = summaries.filter((table) => table.role === "reference");
  const criticalSummary = summarizeStateGroup(criticalTables);
  const referenceSummary = summarizeStateGroup(referenceTables);
  const leaseAt = reconciledLeaseAt(
    now,
    context.heartbeatLeaseMs,
    criticalSummary.status,
    criticalTables,
  );

  await context.services.dataPlane.dataTableRowWrite({
    actor: context.actor,
    table: config.environmentStateTable,
    operation: "set",
    key: ENVIRONMENT_HEALTH_STATE_KEY,
    record: {
      state_key: ENVIRONMENT_HEALTH_STATE_KEY,
      heartbeat_lease_at: leaseAt.toISOString(),
      reconcile_status: criticalSummary.status,
      status_updated_at: now.toISOString(),
      critical_summary: JSON.stringify(criticalSummary),
      reference_summary: JSON.stringify(referenceSummary),
    },
  });
  context.logger.info("environment pipeline state reconciled", {
    status: criticalSummary.status,
    criticalTables: criticalSummary.table_count,
    referenceTables: referenceSummary.table_count,
    leaseAt: leaseAt.toISOString(),
  });
}

async function loadStateRows(
  context: PipelineRunContext,
  config: EnvironmentStateInputTableConfig,
): Promise<readonly Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for (let offset = 0; ; offset += STATE_PAGE_SIZE) {
    const page = await context.services.dataPlane.dataTableRowsQuery({
      table: config.table,
      query: {
        fields: [
          config.keyField,
          "reconcile_status",
          "heartbeat_lease_at",
          "status_updated_at",
        ],
        order_by: [{ field: config.keyField, direction: "asc" }],
        limit: STATE_PAGE_SIZE,
        offset,
      },
    });
    rows.push(...page.rows);
    if (page.rows.length < STATE_PAGE_SIZE) {
      return rows;
    }
  }
}

async function countReconcileSourceRecords(
  context: PipelineRunContext,
  source: ReconcileSourceConfig,
): Promise<number> {
  if (source.kind === "globalTables") {
    const listed = await context.services.globalPlane.globalTablesList({
      actor: context.actor,
    });
    return listed.tables.length;
  }
  try {
    const result = await context.services.controlPlane.businessTableRowsQuery({
      table: source.table,
      query: { operation: "count" },
    });
    return result.count;
  } catch (error) {
    if (isMissingBusinessTableError(error, source.table)) {
      return 0;
    }
    throw error;
  }
}

function assertConcreteTarget(context: PipelineRunContext): void {
  if (context.services.scope.tenantId === "manager" || context.services.scope.env === "all") {
    throw new Error("environment state reconciler requires a concrete tenantId/env context");
  }
}

function errorMessage(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, 500);
}

function isMissingBusinessTableError(error: unknown, table: string): boolean {
  const message = errorMessage(error);
  return message.includes(`unknown business table ${table}`)
    || message.includes(`business table ${table} is missing`);
}
