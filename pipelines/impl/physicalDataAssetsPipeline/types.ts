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
  };
  dataPlane: {
    dataTableRowsList(input: {
      table: string;
      filters?: Readonly<Record<string, PipelineKey | undefined>>;
      limit?: number;
    }): Promise<readonly PipelineRow[]>;
    dataTableRowsSet(input: {
      actor: string;
      table: string;
      records: readonly PipelineRow[];
    }): Promise<{
      primary_key_field: string;
      inserted_rows: number;
      updated_rows: number;
      unchanged_rows: number;
    }>;
    dataTableRowsDelete(input: {
      actor: string;
      table: string;
      keys: readonly PipelineKey[];
    }): Promise<{
      deleted_rows: number;
    }>;
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
