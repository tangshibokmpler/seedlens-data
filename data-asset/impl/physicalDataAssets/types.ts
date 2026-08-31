import type {
  TenantBusinessTableManifest,
} from "@agentnexus/lakecore/model";
import type {
  PipelineRunContext as LakecorePipelineRunContext,
} from "@agentnexus/lakecore/model/pipeline";

type PipelineKey = string | number | boolean;
type PipelineRow = Record<string, unknown>;

interface ConnectorActiveConfig {
  key: string;
  name: string;
}

interface PipelineServices {
  scope: {
    readonly tenantId: string;
    readonly env: string;
  };
  lakehouse: {
    managerConnectorsList(input?: {
      provider?: "trino";
    }): Promise<{
      trino?: {
        activeConfigs: readonly ConnectorActiveConfig[];
      };
    }>;
  };
  controlPlane: {
    businessTableRowsList(input: {
      table: string;
      filters?: Readonly<Record<string, PipelineKey | undefined>>;
      limit?: number;
      include_deleted?: boolean;
    }): Promise<readonly PipelineRow[]>;
    businessSchemasReconcile(input: {
      actor: string;
      schema: TenantBusinessTableManifest;
    }): Promise<{
      created_tables: readonly string[];
      updated_tables: readonly string[];
      unchanged_tables: readonly string[];
    }>;
  };
  dataPlane: {
    dataEnvironmentSqlQuery(input: {
      actor: string;
      sql: string;
      catalog?: string;
      schema?: string;
      max_pages?: number;
    }): Promise<{
      rows: readonly unknown[][];
    }>;
  };
}

export type PipelineRunContext = LakecorePipelineRunContext<PipelineServices>;
