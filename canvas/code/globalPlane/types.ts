import type { ModelFieldType } from "@agentnexus/lakecore/model";
import type {
  PipelineRunContext as LakecorePipelineRunContext,
} from "@agentnexus/lakecore/model/pipeline";

type PipelineKey = string | number | boolean;
type PipelineRow = Record<string, unknown>;

export interface GlobalTableField {
  id: number;
  name: string;
  required: boolean;
  type: ModelFieldType;
}

export interface GlobalTableSummary {
  tenant_id: string;
  env: string;
  database: string;
  schema: string;
  trino_catalog: string;
  physical_table_ref: string;
  table: {
    table_name: string;
    primary_key_field: string;
    table_schema: {
      fields: readonly GlobalTableField[];
    };
  };
}

interface PipelineServices {
  scope: {
    readonly tenantId: string;
    readonly env: string;
  };
  globalPlane: {
    globalTablesList(input: {
      actor?: string;
      search?: string;
    }): Promise<{
      tenant_id: string;
      env: string;
      database: string;
      schema: string;
      trino_catalog: string;
      tables: readonly GlobalTableSummary[];
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
    dataEnvironmentViewDrop(input: {
      actor: string;
      view: string;
    }): Promise<{
      query_view_ref: string;
      view_dropped: boolean;
    }>;
  };
}

export type PipelineRunContext = LakecorePipelineRunContext<PipelineServices>;
