// Migration 0110 backfills existing sidecar rows once, against real data, so
// this replays it over seeded rows.

import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { sql } from "drizzle-orm";

import { sidecar, sidecarAllocation, workflowProbe } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const MIGRATION_SQL = readFileSync(
  join(
    import.meta.dir,
    "../../packages/db/migrations/0110_sidecar_provisioner_binding.sql",
  ),
  "utf8",
);
const TENANT = "tnt-migration-0110";
const ASSET = "asset-migration-0110";
const DIGEST = "d".repeat(64);

describe.skipIf(!harnessDbEnvAvailable())("migration 0110 backfill", () => {
  let h: TestDb;
  beforeAll(async () => {
    h = await createTestDb();
  });
  afterAll(async () => {
    await h.close();
  });
  beforeEach(async () => {
    await h.reset();
    await seedTenants(h.db, [{ id: TENANT }]);
    await seedAsset(h.db, {
      id: ASSET,
      tenantId: TENANT,
      kind: "workflow",
      name: "migration-0110",
    });
  });

  async function seedHosted(
    sidecarId: string,
    status: "allocated" | "released" | "destroy_failed",
    binding: string,
    createdAt: Date,
  ) {
    const run = `run-${sidecarId}-${binding}`;
    await seedWorkflowRun(h.db, {
      id: run,
      anchorRunId: run,
      tenantId: TENANT,
    });
    await h.db.insert(sidecarAllocation).values({
      id: `alloc-${sidecarId}-${binding}`,
      anchorRunId: run,
      tenantId: TENANT,
      sidecarId,
      status,
      generation: 1,
      maxDisconnectedMs: 900_000,
      provisionerId: "p",
      provisionerApiVersion: 2,
      provisionerBindingFingerprint: binding,
      adapterManifestDigest: DIGEST,
      createdAt,
    });
  }

  async function seedProbe(sidecarId: string) {
    await h.db.insert(workflowProbe).values({
      id: `probe-${sidecarId}`,
      tenantId: TENANT,
      definitionAssetId: ASSET,
      sidecarId,
      source: { kind: "registry", registry: "npmjs" },
      entry: "./workflow.js",
      status: "probing",
      provisionerId: "p",
      provisionerApiVersion: 2,
      provisionerBindingFingerprint: "probe",
      adapterManifestDigest: DIGEST,
    });
  }

  async function insertSidecar(id: string) {
    await h.db.insert(sidecar).values({
      id,
      tokenHashSha256: new TextEncoder().encode(id),
      provisionerId: "unset",
      provisionerApiVersion: 2,
      provisionerBindingFingerprint: "unset",
      adapterManifestDigest: "unset",
    });
  }

  async function rerunMigration() {
    await h.db.execute(
      sql.raw(`ALTER TABLE "sidecar"
      DROP COLUMN "provisioner_id",
      DROP COLUMN "provisioner_api_version",
      DROP COLUMN "provisioner_binding_fingerprint",
      DROP COLUMN "adapter_manifest_digest",
      DROP COLUMN "retired_at",
      DROP COLUMN "reconnect_window_ended_at"`),
    );
    for (const statement of MIGRATION_SQL.split("--> statement-breakpoint"))
      await h.db.execute(sql.raw(statement));
  }

  test("backfills from the earliest hosted row, retires idle rows, deletes orphans", async () => {
    await insertSidecar("sc-live");
    await insertSidecar("sc-idle");
    await insertSidecar("sc-cleanup");
    await insertSidecar("sc-orphan");
    await insertSidecar("sc-probe");
    await seedHosted("sc-live", "allocated", "first", new Date(1000));
    await seedHosted("sc-live", "allocated", "second", new Date(2000));
    await seedHosted("sc-idle", "released", "idle", new Date(1000));
    await seedHosted("sc-cleanup", "destroy_failed", "cleanup", new Date(1000));
    await seedProbe("sc-probe");

    await rerunMigration();

    const rows = await h.db.execute<{
      id: string;
      provisioner_binding_fingerprint: string;
      adapter_manifest_digest: string;
      retired: boolean;
    }>(
      sql.raw(
        `SELECT id, provisioner_binding_fingerprint, adapter_manifest_digest, retired_at IS NOT NULL AS retired FROM sidecar ORDER BY id`,
      ),
    );
    expect([...rows]).toEqual([
      {
        id: "sc-cleanup",
        provisioner_binding_fingerprint: "cleanup",
        adapter_manifest_digest: DIGEST,
        retired: false,
      },
      {
        id: "sc-idle",
        provisioner_binding_fingerprint: "idle",
        adapter_manifest_digest: DIGEST,
        retired: true,
      },
      {
        id: "sc-live",
        provisioner_binding_fingerprint: "first",
        adapter_manifest_digest: DIGEST,
        retired: false,
      },
      {
        id: "sc-probe",
        provisioner_binding_fingerprint: "probe",
        adapter_manifest_digest: DIGEST,
        retired: false,
      },
    ]);
  });
});
