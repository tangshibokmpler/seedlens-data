import {
  Pipeline,
  type TenantBusinessTableManifest,
} from "@agentnexus/lakecore/model";
import schema from "./schema.json" with { type: "json" };
import type { PipelineRunContext } from "./types.js";

export class BuiltinPipelineStateSchemaPipeline {
  @Pipeline({ tag: "pipelinePrelude", pipelineKey: "builtin_pipeline_state_schema" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

/**
 * 功能：为当前租户环境一次性调谐所有 lakectd 状态管理表。
 * 逻辑：从同目录 schema.json 读取完整 manifest，创建或更新 env 独立 PostgreSQL 表与查询 view。
 * 幂等：采用 reconcile 策略；结构一致时跳过，结构变化时收敛，不读取、复制或清理任何业务状态行。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  const schema = await loadPipelineStateManifest();
  const result = await context.services.controlPlane.businessSchemasReconcile({
    actor: context.actor,
    schema,
  });
  context.logger.info("pipeline state tables reconciled", {
    created: result.created_tables,
    updated: result.updated_tables,
    unchanged: result.unchanged_tables,
  });
}

export async function loadPipelineStateManifest(): Promise<TenantBusinessTableManifest> {
  const parsed: unknown = schema;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("pipeline state schema must be a JSON object");
  }
  const manifest = parsed as Record<string, unknown>;
  if (manifest.version !== 1 || !Array.isArray(manifest.tables)) {
    throw new Error("pipeline state schema must contain version 1 and a tables array");
  }
  return parsed as TenantBusinessTableManifest;
}
