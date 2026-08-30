import type {
  PipelineRunContext as LakecorePipelineRunContext,
} from "@agentnexus/lakecore/model/pipeline";

type PipelineKey = string | number | boolean;
type PipelineRow = Record<string, unknown>;

export interface ConnectorActiveConfig {
  key: string;
  name: string;
  connector?: string;
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
    tableVersionGet(input: {
      namespace_kind: "data";
      table: string;
    }): Promise<{
      current_snapshot_id?: string;
    }>;
    businessTableRowsQuery(input: {
      table: string;
      query: {
        order_by: readonly {
          field: string;
          direction: "asc" | "desc";
        }[];
        limit: number;
        offset: number;
        include_deleted: boolean;
      };
    }): Promise<{
      rows: readonly PipelineRow[];
      count: number;
    }>;
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
    dataTableRowsQuery(input: {
      table: string;
      query: {
        order_by: readonly {
          field: string;
          direction: "asc" | "desc";
        }[];
        limit: number;
        offset: number;
        include_deleted: boolean;
      };
    }): Promise<{
      rows: readonly PipelineRow[];
      count: number;
    }>;
    dataTableRowsSet(input: {
      actor: string;
      table: string;
      records: readonly PipelineRow[];
    }): Promise<unknown>;
    dataTableRowsDelete(input: {
      actor: string;
      table: string;
      keys: readonly PipelineKey[];
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
  };
}

export type PipelineRunContext = LakecorePipelineRunContext<PipelineServices>;
