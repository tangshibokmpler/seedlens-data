export type BuiltinDataSourceKind = "pg" | "mysql" | "sqlserver";

type TrinoSourceConnector = "postgresql" | "mysql" | "sqlserver";

export interface CdcStreamConfig {
  enabled: true;
  topicSuffix: string;
  tables: readonly string[];
  snapshotMode: string;
}

export function resolveTrinoSourceConfig(input: {
  kind: BuiltinDataSourceKind;
  host: string;
  port?: number;
  database: string;
  user: string;
  encrypt?: boolean;
  trustServerCertificate?: boolean;
}): {
  connector: TrinoSourceConnector;
  displayName: string;
  properties: Record<string, string>;
} {
  const connector = input.kind === "pg" ? "postgresql" : input.kind;
  const port = input.port ?? defaultPort(input.kind);
  return {
    connector,
    displayName: input.kind === "pg"
      ? "PostgreSQL"
      : input.kind === "mysql"
      ? "MySQL"
      : "SQL Server",
    properties: {
      "connection-url": connectionUrl(input, connector, port),
      "connection-user": input.user,
      ...(input.kind === "pg" || input.kind === "sqlserver"
        ? { "case-insensitive-name-matching": "true" }
        : {}),
    },
  };
}

export function resolveCdcStreamConfig(
  value: unknown,
  label: string,
): CdcStreamConfig | undefined {
  if (value === undefined || value === null) return undefined;
  const raw = jsonObject(value, label);
  if (raw.enabled === false) return undefined;
  if (raw.enabled !== true) {
    throw new Error(`${label}.enabled must be true or false`);
  }
  const topicSuffix = kafkaToken(requiredText(raw.topicSuffix, `${label}.topicSuffix`), label);
  if (!Array.isArray(raw.tables) || raw.tables.length === 0) {
    throw new Error(`${label}.tables must be a non-empty array`);
  }
  const tables = raw.tables.map((entry, index) => {
    const table = requiredText(entry, `${label}.tables[${index}]`);
    if (!/^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$/.test(table)) {
      throw new Error(`${label}.tables[${index}] must use schema.table format`);
    }
    return table;
  });
  if (new Set(tables).size !== tables.length) {
    throw new Error(`${label}.tables must not contain duplicates`);
  }
  const snapshotMode = raw.snapshotMode === undefined
    ? "no_data"
    : requiredText(raw.snapshotMode, `${label}.snapshotMode`);
  if (!new Set(["always", "initial", "no_data", "when_needed"]).has(snapshotMode)) {
    throw new Error(`${label}.snapshotMode is not supported: ${snapshotMode}`);
  }
  return { enabled: true, topicSuffix, tables, snapshotMode };
}

export function cdcTopicName(input: {
  tenantId: string;
  env: string;
  topicSuffix: string;
}): string {
  const topic = [
    kafkaToken(input.tenantId, "tenantId"),
    kafkaToken(input.env, "env"),
    kafkaToken(input.topicSuffix, "topicSuffix"),
  ].join(".");
  if (topic.length > 249 || topic === "." || topic === "..") {
    throw new Error("CDC topic must be a valid Kafka topic name");
  }
  return topic;
}

function defaultPort(kind: BuiltinDataSourceKind): number {
  if (kind === "pg") return 5432;
  return kind === "mysql" ? 3306 : 1433;
}

function connectionUrl(
  input: Parameters<typeof resolveTrinoSourceConfig>[0],
  connector: TrinoSourceConnector,
  port: number,
): string {
  if (input.kind !== "sqlserver") {
    return `jdbc:${connector}://${input.host}:${port}/${input.database}`;
  }
  for (const [field, value] of [["host", input.host], ["database", input.database]] as const) {
    if (!value.trim() || value.includes(";")) {
      throw new Error(`SQL Server ${field} must be non-empty and cannot contain semicolons`);
    }
  }
  return [
    `jdbc:sqlserver://${input.host}:${port}`,
    `databaseName=${input.database}`,
    `encrypt=${input.encrypt ?? true}`,
    `trustServerCertificate=${input.trustServerCertificate ?? false}`,
  ].join(";");
}

function jsonObject(value: unknown, label: string): Record<string, unknown> {
  if (typeof value === "string") {
    return jsonObject(JSON.parse(value) as unknown, label);
  }
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  throw new Error(`${label} must be a JSON object`);
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value.trim();
}

function kafkaToken(value: string, label: string): string {
  const token = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (!token) {
    throw new Error(`${label} must contain a Kafka-safe character`);
  }
  return token;
}
