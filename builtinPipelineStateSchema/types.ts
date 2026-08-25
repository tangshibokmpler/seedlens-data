import type { TenantBusinessTableManifest } from "@agentnexus/lakecore/model";
import type { PipelineRunContext as LakecorePipelineRunContext } from "@agentnexus/lakecore/model/pipeline";

interface PipelineServices {
  controlPlane: {
    businessSchemasReconcile(input: {
      actor: string;
      schema: TenantBusinessTableManifest;
    }): Promise<{
      created_tables: readonly string[];
      updated_tables: readonly string[];
      unchanged_tables: readonly string[];
    }>;
  };
}

export type PipelineRunContext = LakecorePipelineRunContext<PipelineServices>;
