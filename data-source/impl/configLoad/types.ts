import type { PipelineRunContext as LakecorePipelineRunContext } from "@agentnexus/lakecore/model/pipeline";

export interface ConnectorActiveConfig {
  key: string;
  name: string;
  tenantId?: string;
  connector?: string;
  connectorClass?: string;
  displayName?: string;
  purpose?: string;
  config: Record<string, unknown>;
}

interface ConnectorMutationResult {
  catalog?: string;
  connector?: string;
  status: string;
  materialized?: {
    message?: string;
  };
  runtime?: {
    connectorState?: string;
    taskStatus?: string;
    restartRequested?: boolean;
    warning?: string;
  };
}

interface PipelineServices {
  scope: {
    readonly tenantId: string;
    readonly env: string;
  };
  lakehouse: {
    managerConnectorsList(input?: {
      provider?: "kafka" | "trino";
    }): Promise<{
      kafka?: {
        kafkaConnect: {
          activeConfigs: readonly ConnectorActiveConfig[];
        };
      };
      trino?: {
        activeConfigs: readonly ConnectorActiveConfig[];
      };
    }>;
    managerConnectorImport(input: {
      provider: "kafka" | "trino";
      config: Record<string, unknown>;
    }): Promise<ConnectorMutationResult>;
    managerConnectorRemove(input: {
      provider: "kafka" | "trino";
      key: string;
    }): Promise<ConnectorMutationResult>;
  };
  controlPlane: {
    businessTableRowsList(input: {
      table: string;
      include_deleted?: boolean;
    }): Promise<readonly Record<string, unknown>[]>;
  };
  dataPlane: {
    dataTableRowsList(input: {
      table: string;
      filters?: Readonly<Record<string, string | number | boolean | undefined>>;
      limit?: number;
    }): Promise<readonly Record<string, unknown>[]>;
    dataTableRowWrite(input: {
      actor: string;
      table: string;
      operation: "insert" | "update" | "delete" | "set";
      record: Record<string, unknown>;
      key?: string | number | boolean;
    }): Promise<unknown>;
  };
}

export type PipelineRunContext = LakecorePipelineRunContext<PipelineServices>;
