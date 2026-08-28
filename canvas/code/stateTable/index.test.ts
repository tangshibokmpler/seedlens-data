import assert from "node:assert/strict";
import test from "node:test";

import { pipelines } from "../index.js";
import { loadPipelineStateManifest } from "./index.js";

test("global plane load follows the pipeline state schema", () => {
  assert.deepEqual(pipelines.slice(0, 2).map((pipeline) => pipeline.pipelineKey), [
    "builtin_pipeline_state_schema",
    "builtin_global_plane_load",
  ]);
});

test("pipeline state manifest creates pipeline and environment state tables", async () => {
  const manifest = await loadPipelineStateManifest();

  assert.deepEqual(manifest.tables.map((table) => ({
    table: table.table,
    kind: table.kind,
    primaryKey: table.primaryKey,
    columns: Object.keys(table.columns),
  })), [{
    table: "t_global_state_views",
    kind: "data_unversioned",
    primaryKey: ["global_table_name_key"],
    columns: [
      "global_table_name_key",
      "heartbeat_lease_at",
      "reconcile_status",
      "status_updated_at",
      "view_reconcile_state",
      "last_reconciled_view_schema",
    ],
  }, {
    table: "t_factory_state_source",
    kind: "data_unversioned",
    primaryKey: ["data_source_name_key"],
    columns: ["data_source_name_key", "heartbeat_lease_at", "reconcile_status", "status_updated_at", "state"],
  }, {
    table: "t_factory_state_source_material",
    kind: "data_unversioned",
    primaryKey: ["data_source_name_key"],
    columns: ["data_source_name_key", "heartbeat_lease_at", "reconcile_status", "status_updated_at", "reconcile_state"],
  }, {
    table: "t_factory_state_asset",
    kind: "data_unversioned",
    primaryKey: ["config_key"],
    columns: ["config_key", "heartbeat_lease_at", "reconcile_status", "status_updated_at", "table_reconcile_state", "data_reconcile_state"],
  }, {
    table: "t_ontology_state_bind",
    kind: "data_unversioned",
    primaryKey: ["asset_name_key"],
    columns: ["asset_name_key", "heartbeat_lease_at", "reconcile_status", "status_updated_at", "object_table_reconcile_state", "data_reconcile_state"],
  }, {
    table: "t_system_state_overview",
    kind: "data_unversioned",
    primaryKey: ["state_key"],
    columns: ["state_key", "heartbeat_lease_at", "reconcile_status", "status_updated_at", "critical_summary", "reference_summary"],
  }]);
});
