export type PipelineRecord = Record<string, unknown>;

export type PipelineRecordInputs = ReadonlyMap<string, readonly PipelineRecord[]>;

export function resolveRecordInputs(config: unknown): PipelineRecordInputs {
  if (!config || typeof config !== "object" || Array.isArray(config)) return new Map();
  const value = (config as Record<string, unknown>).recordInputs;
  if (value === undefined) return new Map();
  if (!Array.isArray(value)) throw new Error("pipeline config recordInputs must be an array");

  const inputs = new Map<string, readonly PipelineRecord[]>();
  for (const [inputIndex, input] of value.entries()) {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      throw new Error(`pipeline config recordInputs[${inputIndex}] must be an object`);
    }
    const raw = input as Record<string, unknown>;
    const table = typeof raw.table === "string" ? raw.table.trim() : "";
    if (!table) {
      throw new Error(`pipeline config recordInputs[${inputIndex}].table must be a non-empty string`);
    }
    if (inputs.has(table)) {
      throw new Error(`pipeline config recordInputs contains duplicate table ${table}`);
    }
    if (!Array.isArray(raw.records) || raw.records.length === 0) {
      throw new Error(`pipeline config recordInputs for ${table} must contain records`);
    }
    const records = raw.records.map((record, recordIndex) => {
      if (!record || typeof record !== "object" || Array.isArray(record)) {
        throw new Error(
          `pipeline config recordInputs for ${table} records[${recordIndex}] must be an object`,
        );
      }
      return record as PipelineRecord;
    });
    inputs.set(table, records);
  }
  return inputs;
}

export function assertTargetedRecordInputs(
  inputs: PipelineRecordInputs,
  requiredTables: readonly string[],
): void {
  if (inputs.size === 0) return;
  const required = new Set(requiredTables);
  const missing = [...required].filter((table) => !inputs.has(table));
  const unexpected = [...inputs.keys()].filter((table) => !required.has(table));
  if (missing.length > 0 || unexpected.length > 0) {
    throw new Error([
      "pipeline config recordInputs must exactly match required tables",
      ...(missing.length > 0 ? [`missing: ${missing.join(", ")}`] : []),
      ...(unexpected.length > 0 ? [`unexpected: ${unexpected.join(", ")}`] : []),
    ].join("; "));
  }
}
