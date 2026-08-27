import { createHash } from "node:crypto";

import { Pipeline } from "@agentnexus/lakecore/model";
import type { ConnectorActiveConfig, PipelineRunContext } from "./types.js";
import {
  cdcTopicName,
  resolveCdcStreamConfig,
  resolveTrinoSourceConfig,
  type BuiltinDataSourceKind,
  type CdcStreamConfig,
} from "./source-config.js";
import {
  dataSourceStateReady,
  deleteDataSourceState,
  loadDataSourceState,
  renewDataSourceHeartbeat,
  writeDataSourceState,
} from "./state.js";

export class BuiltinDataSourceLoadPipeline {
  @Pipeline({ tag: "pipelinePrelude", pipelineKey: "builtin_data_source_load" })
  static Run(context: PipelineRunContext): Promise<void> {
    return run(context);
  }
}

interface PipelinePreludeOptions {
  table?: string;
  stateTable?: string;
}

interface BuiltinDataSource {
  name: string;
  kind: "builtin";
  builtin_kind: BuiltinDataSourceKind;
  connection: {
    host: string;
    port?: number;
    database: string;
    schema?: string;
    user: string;
    password?: string;
    encrypt?: boolean;
    trustServerCertificate?: boolean;
  };
  stream?: unknown;
}

type BuiltinPgDataSource = BuiltinDataSource & { builtin_kind: "pg" };

interface LoadedDataSource<TSource = unknown> {
  tenantId: string;
  env: string;
  source: TSource;
}

type LoadedPgDataSource = LoadedDataSource<BuiltinPgDataSource>;
type LoadedBuiltinDataSource = LoadedDataSource<BuiltinDataSource>;

interface ExpectedTrinoSourceCatalog extends TrinoSourceImportConfig {
  tenantId: string;
  env: string;
  sourceName: string;
}

interface ExpectedKafkaSourceConnector extends KafkaSourceImportConfig {
  tenantId: string;
  env: string;
  sourceName: string;
  topic: string;
}

/**
 * 功能：将当前租户环境的数据源配置装载为 Trino catalog，并为启用 stream 的 PostgreSQL 源维护 Debezium connector。
 * 逻辑：读取完整数据源事实和 datasource name 状态，租约失效或配置变化时收敛 Trino catalog 与 Kafka connector，并清理已移除资源。
 * 幂等：采用 reconcile/cleanup 策略；有效租约只续心跳，成功写 ready，失败写 failed 并等待后续重试。
 */
export async function run(context: PipelineRunContext): Promise<void> {
  const options = pipelinePreludeOptions(context.config);
  const stateTable = pipelinePreludeStateTableName(options);
  const sources = await loadDataSources(context);
  const builtinSources = sources.filter(isLoadedBuiltinDataSource);
  const skipped = sources.length - builtinSources.length;
  if (skipped > 0) {
    context.logger.warn("pipelinePrelude skipped unsupported data sources", {
      skipped,
    });
  }

  const expectedCatalogs = builtinSources.map((loaded) => expectedTrinoSourceCatalog(loaded));
  const expectedStreams = builtinSources.filter(isLoadedBuiltinPgDataSource).flatMap((loaded) => {
    const stream = resolveCdcStreamConfig(
      loaded.source.stream,
      `data source ${loaded.source.name}.stream`,
    );
    return stream ? [expectedKafkaSourceConnector(loaded, stream)] : [];
  });
  assertUniqueCdcTopics(expectedStreams);
  const streamBySource = new Map(expectedStreams.map((stream) => [stream.sourceName, stream]));
  const expectedByTarget = expectedCatalogsByTarget(context, expectedCatalogs);
  for (const target of expectedByTarget.values()) {
    await removeCatalogsMissingFromDataSources(context, {
      tenantId: target.tenantId,
      env: target.env,
      expected: target.expected,
    });
  }

  await removeKafkaConnectorsMissingFromDataSources(context, expectedStreams);
  for (const expected of expectedCatalogs) {
    if (context.signal.aborted) {
      throw new Error("pipelinePrelude aborted");
    }
    await reconcileDataSource(context, {
      expected,
      stream: streamBySource.get(expected.sourceName),
      stateTable,
    });
  }
  await removeMissingDataSourceStates(
    context,
    stateTable,
    new Set(sources.flatMap((loaded) => {
      if (!loaded.source || typeof loaded.source !== "object" || Array.isArray(loaded.source)) {
        return [];
      }
      const name = textField(loaded.source as Record<string, unknown>, "name");
      return name ? [name] : [];
    })),
  );
}

async function reconcileDataSource(
  context: PipelineRunContext,
  input: {
    expected: ExpectedTrinoSourceCatalog;
    stream?: ExpectedKafkaSourceConnector;
    stateTable: string;
  },
): Promise<void> {
  const expectedHash = stateHash({ catalog: input.expected, stream: input.stream });
  const state = await loadDataSourceState(context, input.stateTable, input.expected.sourceName);
  if (dataSourceStateReady(state, expectedHash)) {
    await renewDataSourceHeartbeat(context, {
      table: input.stateTable,
      key: input.expected.sourceName,
      leaseMs: context.heartbeatLeaseMs,
    });
    return;
  }

  try {
    await writeDataSourceState(context, {
      table: input.stateTable,
      key: input.expected.sourceName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash,
      status: "reconciling",
    });
    await removeStaleCatalogsForSource(context, input.expected);
    const active = await activeCatalogForKey(context, input.expected.key);
    if (!active || !activeCatalogMatchesConfig(active, input.expected)) {
      const result = await context.services.lakehouse.managerConnectorImport({
        provider: "trino",
        config: input.expected,
      });
      context.logger.info("data source loaded", {
        source: input.expected.sourceName,
        catalog: result.catalog,
        status: result.status,
      });
    }
    if (input.stream) {
      await reconcileKafkaSource(context, input.stream);
    }
    await writeDataSourceState(context, {
      table: input.stateTable,
      key: input.expected.sourceName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash,
      status: "ready",
    });
  } catch (error) {
    await writeDataSourceState(context, {
      table: input.stateTable,
      key: input.expected.sourceName,
      leaseMs: context.heartbeatLeaseMs,
      expectedHash,
      status: "failed",
      error,
    });
    context.logger.warn("data source reconcile failed", {
      source: input.expected.sourceName,
      error: errorMessage(error),
    });
  }
}

async function reconcileKafkaSource(
  context: PipelineRunContext,
  expected: ExpectedKafkaSourceConnector,
): Promise<void> {
  const result = await context.services.lakehouse.managerConnectorImport({
    provider: "kafka",
    config: expected,
  });
  if (
    result.runtime
    && (result.runtime.connectorState !== "RUNNING" || result.runtime.taskStatus !== "RUNNING")
  ) {
    context.logger.warn("CDC source task not running", {
      source: expected.sourceName,
      connector: expected.name,
      connectorState: result.runtime.connectorState,
      taskStatus: result.runtime.taskStatus,
      restartRequested: result.runtime.restartRequested,
      ...(result.runtime.warning ? { warning: result.runtime.warning } : {}),
    });
  }
  context.logger.info("CDC source reconciled", {
    source: expected.sourceName,
    topic: expected.topic,
  });
}

async function loadDataSources(context: PipelineRunContext): Promise<LoadedDataSource[]> {
  const options = pipelinePreludeOptions(context.config);
  return loadControlDataSources(context, options);
}

async function loadControlDataSources(
  context: PipelineRunContext,
  options: PipelinePreludeOptions,
): Promise<LoadedDataSource[]> {
  const table = pipelinePreludeTableName(options);
  const { tenantId, env } = context.services.scope;
  const viewNamespace = `views_${tenantId}_${env}`;

  let rows: Awaited<ReturnType<PipelineRunContext["services"]["controlPlane"]["businessTableRowsList"]>>;
  try {
    rows = await context.services.controlPlane.businessTableRowsList({
      table,
      include_deleted: false,
    });
  } catch (error) {
    if (isMissingDataSourceControlTableError(error, table)) {
      context.logger.warn("pipelinePrelude skipped env without configured data source control table", {
        tenantId,
        env,
        namespace: viewNamespace,
        table,
        reason: errorMessage(error),
      });
      return [];
    }
    throw error;
  }
  return rows.map((row) => ({
    tenantId,
    env,
    source: {
      name: row.name,
      kind: row.kind,
      builtin_kind: row.builtin_kind,
      connection: jsonObjectField(row.connection, "connection"),
      stream: jsonObjectField(row.stream, "stream"),
    },
  }));
}

function expectedTrinoSourceCatalog(loaded: LoadedBuiltinDataSource): ExpectedTrinoSourceCatalog {
  const { tenantId, env, source } = loaded;
  const catalog = externalCatalogName({
    tenantId,
    env,
    sourceName: source.name,
  });
  const resolved = resolveTrinoSourceConfig({
    kind: source.builtin_kind,
    host: source.connection.host,
    port: source.connection.port,
    database: source.connection.database,
    user: source.connection.user,
    encrypt: source.connection.encrypt,
    trustServerCertificate: source.connection.trustServerCertificate,
  });
  return {
    tenantId,
    env,
    sourceName: source.name,
    key: catalog,
    name: source.name,
    connector: resolved.connector,
    catalog,
    displayName: resolved.displayName,
    purpose: "external-source",
    source: {
      kind: resolved.connector,
      host: source.connection.host,
      port: source.connection.port ?? defaultSourcePort(source.builtin_kind),
      database: source.connection.database,
      schema: source.connection.schema,
      user: source.connection.user,
      password: source.connection.password,
    },
    config: {
      ...resolved.properties,
      ...(source.connection.password
        ? { "connection-password": source.connection.password }
        : {}),
    },
  };
}

function expectedKafkaSourceConnector(
  loaded: LoadedPgDataSource,
  stream: CdcStreamConfig,
): ExpectedKafkaSourceConnector {
  const { tenantId, env, source } = loaded;
  const key = externalCatalogName({ tenantId, env, sourceName: source.name });
  const name = externalConnectorName({ tenantId, env, sourceName: source.name });
  const topic = cdcTopicName({ tenantId, env, topicSuffix: stream.topicSuffix });
  const identity = createHash("sha256")
    .update(`${tenantId}\0${env}\0${source.name}`)
    .digest("hex")
    .slice(0, 24);
  return {
    tenantId,
    env,
    sourceName: source.name,
    topic,
    key,
    name,
    connectorArtifact: "debezium-connector-postgres",
    connectorClass: "io.debezium.connector.postgresql.PostgresConnector",
    type: "source",
    purpose: "cdc",
    config: {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": source.connection.host,
      "database.port": String(source.connection.port ?? 5432),
      "database.user": source.connection.user,
      "database.password": source.connection.password ?? "",
      "database.dbname": source.connection.database,
      "topic.prefix": topic,
      "plugin.name": "pgoutput",
      "slot.name": `anx_${identity}`,
      "publication.name": `anx_pub_${identity}`,
      "publication.autocreate.mode": "filtered",
      "table.include.list": stream.tables.join(","),
      "snapshot.mode": stream.snapshotMode,
      "tombstones.on.delete": "false",
      "transforms": "route",
      "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
      "transforms.route.regex": ".*",
      "transforms.route.replacement": topic,
    },
  };
}

function assertUniqueCdcTopics(expected: readonly ExpectedKafkaSourceConnector[]): void {
  const sourceByTopic = new Map<string, string>();
  for (const connector of expected) {
    const existing = sourceByTopic.get(connector.topic);
    if (existing) {
      throw new Error(`CDC topic ${connector.topic} is shared by data sources ${existing} and ${connector.sourceName}`);
    }
    sourceByTopic.set(connector.topic, connector.sourceName);
  }
}

function expectedCatalogsByTarget(
  context: PipelineRunContext,
  expectedCatalogs: readonly ExpectedTrinoSourceCatalog[],
): Map<string, {
  tenantId: string;
  env: string;
  expected: ExpectedTrinoSourceCatalog[];
}> {
  const groups = new Map<string, {
    tenantId: string;
    env: string;
    expected: ExpectedTrinoSourceCatalog[];
  }>();
  const { tenantId, env } = context.services.scope;
  groups.set(targetKey(tenantId, env), {
    tenantId,
    env,
    expected: [],
  });
  for (const expected of expectedCatalogs) {
    const key = targetKey(expected.tenantId, expected.env);
    const group = groups.get(key) ?? {
      tenantId: expected.tenantId,
      env: expected.env,
      expected: [],
    };
    group.expected.push(expected);
    groups.set(key, group);
  }
  return groups;
}

function targetKey(tenantId: string, env: string): string {
  return `${tenantId}\0${env}`;
}

function pipelinePreludeOptions(config: unknown): PipelinePreludeOptions {
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    return {};
  }
  const raw = config as Record<string, unknown>;
  return {
    table: textField(raw, "table"),
    stateTable: textField(raw, "stateTable"),
  };
}

function pipelinePreludeStateTableName(options: PipelinePreludeOptions): string {
  const table = options.stateTable?.trim();
  if (!table) {
    throw new Error("pipelinePrelude requires config.stateTable");
  }
  return table;
}

function pipelinePreludeTableName(options: PipelinePreludeOptions): string {
  const table = options.table?.trim();
  if (!table) {
    throw new Error("pipelinePrelude requires config.table when control table loading is enabled");
  }
  return table;
}

function jsonObjectField(value: unknown, label: string): Record<string, unknown> | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value === "string") {
    const parsed = JSON.parse(value) as unknown;
    return jsonObjectField(parsed, label);
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  throw new Error(`data source row ${label} must be a JSON object`);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isMissingDataSourceControlTableError(error: unknown, table: string): boolean {
  const message = errorMessage(error);
  return (
    message.includes("no business tables")
    || message.includes(`business table ${table}`)
    || message.includes(table)
    || message.includes("NoSuchTable")
    || message.includes("NoSuchNamespace")
    || message.includes("does not exist")
    || message.includes("not found")
  );
}

function textField(input: Record<string, unknown>, key: string): string | undefined {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function isBuiltinDataSource(value: unknown): value is BuiltinDataSource {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const raw = value as Record<string, unknown>;
  const connection = raw.connection;
  return (
    typeof raw.name === "string"
    && raw.kind === "builtin"
    && (raw.builtin_kind === "pg" || raw.builtin_kind === "mysql" || raw.builtin_kind === "sqlserver")
    && Boolean(connection)
    && typeof connection === "object"
    && !Array.isArray(connection)
    && typeof (connection as Record<string, unknown>).host === "string"
    && typeof (connection as Record<string, unknown>).database === "string"
    && typeof (connection as Record<string, unknown>).user === "string"
  );
}

function isBuiltinPgDataSource(value: unknown): value is BuiltinPgDataSource {
  return isBuiltinDataSource(value) && value.builtin_kind === "pg";
}

function isLoadedBuiltinDataSource(value: LoadedDataSource): value is LoadedBuiltinDataSource {
  return isBuiltinDataSource(value.source);
}

function isLoadedBuiltinPgDataSource(value: LoadedDataSource): value is LoadedPgDataSource {
  return isBuiltinPgDataSource(value.source);
}

function defaultSourcePort(kind: BuiltinDataSourceKind): number {
  if (kind === "pg") return 5432;
  return kind === "mysql" ? 3306 : 1433;
}

interface TrinoSourceImportConfig {
  [key: string]: unknown;
  key: string;
  name: string;
  connector: string;
  catalog: string;
  displayName: string;
  purpose: string;
  source: Record<string, unknown>;
  config: Record<string, unknown>;
}

interface KafkaSourceImportConfig {
  [key: string]: unknown;
  key: string;
  name: string;
  connectorArtifact: string;
  connectorClass: string;
  type: string;
  purpose: string;
  config: Record<string, unknown>;
}

async function removeKafkaConnectorsMissingFromDataSources(
  context: PipelineRunContext,
  expected: readonly ExpectedKafkaSourceConnector[],
): Promise<void> {
  const expectedKeys = new Set(expected.map((entry) => entry.key));
  const connectors = await context.services.lakehouse.managerConnectorsList({ provider: "kafka" });
  const { tenantId, env } = context.services.scope;
  const orphaned = (connectors.kafka?.kafkaConnect.activeConfigs ?? []).filter((entry) =>
    isKafkaConnectorManagedByPipelinePrelude(entry, tenantId, env)
    && !expectedKeys.has(entry.key));
  for (const connector of orphaned) {
    await context.services.lakehouse.managerConnectorRemove({
      provider: "kafka",
      key: connector.key,
    });
    context.logger.info("CDC source removed", {
      connector: connector.name,
      reason: "removed",
    });
  }
}

async function removeStaleCatalogsForSource(
  context: PipelineRunContext,
  expected: ExpectedTrinoSourceCatalog,
): Promise<void> {
  const connectors = await context.services.lakehouse.managerConnectorsList({
    provider: "trino",
  });
  const staleConfigs = (connectors.trino?.activeConfigs ?? []).filter((entry) =>
    isCatalogForSource(entry, expected)
    && (entry.key !== expected.key || entry.name !== expected.catalog));

  for (const stale of staleConfigs) {
    const removed = await context.services.lakehouse.managerConnectorRemove({
      provider: "trino",
      key: stale.key,
    });
    context.logger.info("data source unloaded", {
      source: expected.sourceName,
      catalog: stale.name,
      status: removed.status,
      reason: "stale",
    });
  }
}

async function removeCatalogsMissingFromDataSources(
  context: PipelineRunContext,
  input: {
    tenantId: string;
    env: string;
    expected: readonly ExpectedTrinoSourceCatalog[];
  },
): Promise<void> {
  const expectedKeys = new Set(input.expected.map((expected) => expected.key));
  const connectors = await context.services.lakehouse.managerConnectorsList({
    provider: "trino",
  });
  const orphanedConfigs = (connectors.trino?.activeConfigs ?? []).filter((entry) =>
    isCatalogManagedByPipelinePrelude(entry, input.tenantId, input.env)
    && !expectedKeys.has(entry.key));

  for (const orphaned of orphanedConfigs) {
    const removed = await context.services.lakehouse.managerConnectorRemove({
      provider: "trino",
      key: orphaned.key,
    });
    context.logger.info("data source unloaded", {
      source: textConfigField(orphaned.config, "lakehouse.external.name"),
      catalog: orphaned.name,
      status: removed.status,
      reason: "removed",
    });
  }
}

async function activeCatalogForKey(
  context: PipelineRunContext,
  key: string,
): Promise<ConnectorActiveConfig | undefined> {
  const connectors = await context.services.lakehouse.managerConnectorsList({
    provider: "trino",
  });
  return connectors.trino?.activeConfigs.find((entry) => entry.key === key);
}

function isCatalogForSource(
  active: ConnectorActiveConfig,
  expected: ExpectedTrinoSourceCatalog,
): boolean {
  return (
    isCatalogManagedByPipelinePrelude(active, expected.tenantId, expected.env)
    && (
      active.key === expected.key
      || active.key === expected.sourceName
      || active.config["lakehouse.external.name"] === expected.sourceName
    )
  );
}

function isCatalogManagedByPipelinePrelude(
  active: ConnectorActiveConfig,
  tenantId: string,
  env: string,
): boolean {
  if (active.purpose !== "external-source" || active.config["lakehouse.external.tenant"] !== tenantId) {
    return false;
  }
  const sourceName = textConfigField(active.config, "lakehouse.external.name");
  if (!sourceName) {
    return false;
  }
  const scopedPrefix = pipelinePreludeScopePrefix(tenantId, env);
  return active.key.startsWith(scopedPrefix) || active.name.startsWith(scopedPrefix);
}

function isKafkaConnectorManagedByPipelinePrelude(
  active: {
    key: string;
    tenantId?: string;
    purpose?: string;
  },
  tenantId: string,
  env: string,
): boolean {
  if (active.purpose !== "cdc") {
    return false;
  }
  const scopedPrefix = pipelinePreludeScopePrefix(tenantId, env);
  return active.key.startsWith(scopedPrefix) || active.key.startsWith(`cdc_${scopedPrefix}`);
}

function pipelinePreludeScopePrefix(tenantId: string, env: string): string {
  return `${catalogToken(tenantId, "tenant")}_${catalogToken(env, "env")}_`;
}

function textConfigField(input: Record<string, unknown>, key: string): string | undefined {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function activeCatalogMatchesConfig(
  active: ConnectorActiveConfig,
  expected: TrinoSourceImportConfig,
): boolean {
  const actualState = {
    key: active.key,
    name: active.config["lakehouse.external.name"],
    connector: active.connector,
    catalog: active.name,
    displayName: active.displayName,
    purpose: active.purpose,
    source: active.config["lakehouse.external.source"],
    config: externalCatalogProperties(active.config),
  };
  const expectedState = {
    key: expected.key,
    name: expected.name,
    connector: expected.connector,
    catalog: expected.catalog,
    displayName: expected.displayName,
    purpose: expected.purpose,
    source: expected.source,
    config: {
      "connector.name": expected.connector,
      ...expected.config,
    },
  };
  return stableJson(actualState) === stableJson(expectedState);
}

function externalCatalogProperties(config: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(config).filter(([key]) => !key.startsWith("lakehouse.external.")),
  );
}

function stableJson(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

function stateHash(value: unknown): string {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

async function removeMissingDataSourceStates(
  context: PipelineRunContext,
  table: string,
  expectedKeys: ReadonlySet<string>,
): Promise<void> {
  const rows = await context.services.dataPlane.dataTableRowsList({ table });
  for (const row of rows) {
    const key = typeof row.data_source_name_key === "string" ? row.data_source_name_key.trim() : "";
    if (key && !expectedKeys.has(key)) {
      await deleteDataSourceState(context, table, key);
    }
  }
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => stableValue(entry));
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => entry !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableValue(entry)]),
  );
}

function externalCatalogName(input: {
  tenantId: string;
  env: string;
  sourceName: string;
}): string {
  return [
    catalogToken(input.tenantId, "tenant"),
    catalogToken(input.env, "env"),
    catalogToken(input.sourceName, "source"),
  ].join("_");
}

function externalConnectorName(input: {
  tenantId: string;
  env: string;
  sourceName: string;
}): string {
  return `cdc_${[
    catalogToken(input.tenantId, "tenant"),
    catalogToken(input.env, "env"),
    catalogToken(input.sourceName, "source"),
  ].join("_")}`;
}

function catalogToken(value: string, label: string): string {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!normalized) {
    throw new Error(`${label} must contain at least one catalog-safe character`);
  }
  return /^[a-z]/.test(normalized) ? normalized : `${label}_${normalized}`;
}
