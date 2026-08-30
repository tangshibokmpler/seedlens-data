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
  };
  dataPlane: {
    dataTableRowsList(input: {
      table: string;
      filters?: Readonly<Record<string, PipelineKey | undefined>>;
      limit?: number;
    }): Promise<readonly PipelineRow[]>;
    dataTableRowWrite(input: {
      actor: string;
      table: string;
      operation: "insert" | "update" | "delete" | "set";
      record: PipelineRow;
      key?: PipelineKey;
    }): Promise<unknown>;
    dataEnvironmentSqlQuery(input: {
      actor: string;
      sql: string;
      catalog?: string;
      schema?: string;
      max_pages?: number;
    }): Promise<{
      rows: readonly unknown[][];
    }>;
    dataEnvironmentViewReconcile(input: {
      actor: string;
      view: string;
      source: {
        kind?: string;
        catalog: string;
        schema: string;
        table: string;
      };
      field_mappings: readonly {
        source_field: string;
        target_field: string;
        source_type?: string;
        target_type?: "string";
      }[];
      marker?: string;
    }): Promise<{
      query_view_ref: string;
      action: "created" | "updated" | "unchanged";
      changed: boolean;
    }>;
  };
}

export type PipelineRunContext = LakecorePipelineRunContext<PipelineServices>;
