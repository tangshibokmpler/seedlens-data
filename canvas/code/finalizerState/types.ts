import type {
  PipelineRunContext as LakecorePipelineRunContext,
} from "@agentnexus/lakecore/model/pipeline";

type PipelineKey = string | number | boolean;
type PipelineRow = Record<string, unknown>;

interface PipelineServices {
  scope: {
    readonly tenantId: string;
    readonly env: string;
  };
  globalPlane: {
    globalTablesList(input: {
      actor?: string;
    }): Promise<{
      tables: readonly unknown[];
    }>;
  };
  controlPlane: {
    businessTableRowsQuery(input: {
      table: string;
      query: { operation: "count" };
    }): Promise<{ rows: readonly PipelineRow[]; count: number }>;
  };
  dataPlane: {
    dataTableRowsQuery(input: {
      table: string;
      query: {
        fields: readonly string[];
        order_by: readonly { field: string; direction: "asc" | "desc" }[];
        limit: number;
        offset: number;
      };
    }): Promise<{ rows: readonly PipelineRow[]; count: number }>;
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
