import type {
  TenantBusinessTableManifest,
} from "@agentnexus/lakecore/model";
import type {
  PipelineRunContext as LakecorePipelineRunContext,
} from "@agentnexus/lakecore/model/pipeline";

type PipelineRow = Record<string, unknown>;
type PipelineKey = string | number | boolean;

interface ConnectorActiveConfig {
  key: string;
  name: string;
  connector: string;
  displayName: string;
  purpose?: string;
  config: Record<string, unknown>;
}

interface BusinessSchemasMutationResult {
  created_tables: readonly string[];
  updated_tables: readonly string[];
  unchanged_tables: readonly string[];
  environments: readonly {
    env: string;
    data_plane_unversioned_tables?: readonly {
      table: string;
      physical_table_ref: string;
      query_catalog: string;
      query_namespace: readonly string[];
      query_view_ref: string;
      changed?: boolean;
      table_action?: "created" | "updated" | "unchanged";
      view_action?: "created" | "updated" | "unchanged";
    }[];
  }[];
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
    }): Promise<BusinessSchemasMutationResult>;
  };
  dataPlane: {
    dataTableRowsList(input: {
      table: string;
      filters?: Readonly<Record<string, PipelineKey | undefined>>;
      limit?: number;
    }): Promise<readonly PipelineRow[]>;
    dataEnvironmentSqlQuery(input: {
      actor: string;
      sql: string;
      catalog?: string;
      schema?: string;
      max_pages?: number;
    }): Promise<{
      rows: readonly unknown[][];
    }>;
    dataTableRowWrite(input: {
      actor: string;
      table: string;
      operation: "insert" | "update" | "delete" | "set";
      record: PipelineRow;
      key?: PipelineKey;
    }): Promise<unknown>;
  };
}

export type PipelineRunContext = LakecorePipelineRunContext<PipelineServices>;
