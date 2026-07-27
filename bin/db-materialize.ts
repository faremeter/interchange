#!/usr/bin/env bun
/* eslint-disable no-console */
// bin/db-materialize -- run-once: freeze each folded agent's body into a
// workflow asset.
//
// The one-time cliff run that must precede dropping the `agent` table. For
// every folded `workflow_definition` whose `asset_id` is still null, it
// synthesizes the agent's `workflow.json`, writes it into a `workflow`-kind
// asset repo, and points the definition at that asset -- so the body survives
// the agent row's retirement and a folded definition hydrates exactly like a
// native one.
//
// Unlike `bin/db-backfill` (rows only, `db`-only, re-run freely during the
// transition), this writes asset repos, so it needs the on-disk data dir the
// hub reads its bodies from. It writes under HUB_DATA_DIR for exactly that
// reason: a hub reading a materialized definition resolves the body from a repo
// under this dir. In-process hydrate reads the blob without verifying the
// commit signature, so the signing key is a fresh per-run pair -- only the data
// dir must match the hub's. Run this while the hub is down (a maintenance
// window), so the two never write the same repos concurrently.
//
// Safe to re-run: a definition that already has an `asset_id` is skipped, and a
// partially-materialized definition is recovered by finding its asset by name.
// If any folded definition cannot be materialized, the run reports the complete
// manifest and exits non-zero; the ones that did materialize stay committed.

import { generateKeyPair } from "@intx/crypto";
import { createDB } from "@intx/db";
import {
  createAgentRepoStore,
  createAssetService,
  materializeFoldedBodies,
  MaterializeError,
} from "@intx/hub-sessions";

import {
  resolveBackfillDbConfig,
  type BackfillDbConfig,
} from "./lib/db-config";

function fail(err: unknown): never {
  console.error(
    `db-materialize: ${err instanceof Error ? err.message : String(err)}`,
  );
  process.exit(1);
}

async function main(): Promise<void> {
  let config: BackfillDbConfig;
  try {
    config = resolveBackfillDbConfig(process.env);
  } catch (err) {
    fail(err);
  }

  const dataDir = process.env["HUB_DATA_DIR"];
  if (dataDir === undefined || dataDir === "") {
    fail(new Error("HUB_DATA_DIR is required"));
  }

  let handle: ReturnType<typeof createDB>;
  try {
    handle = createDB(config);
  } catch (err) {
    fail(err);
  }
  const { db, close } = handle;

  const signingKey = await generateKeyPair();
  const agentRepoStore = createAgentRepoStore({ dataDir, signingKey });
  const assetService = createAssetService({
    db,
    repoStore: agentRepoStore.repoStore,
  });

  try {
    const summary = await materializeFoldedBodies(db, assetService);
    await close();
    console.log(
      `db-materialize: materialized ${summary.bodiesMaterialized} body(ies) ` +
        `(${summary.bodiesSkipped} already materialized)`,
    );
  } catch (err) {
    await close();
    if (err instanceof MaterializeError) {
      console.error(
        `db-materialize: ${err.unmaterializable.length} folded ` +
          `definition(s) could not be materialized:`,
      );
      for (const u of err.unmaterializable) {
        console.error(
          `  - ${u.definitionId} (agent ${u.agentId}, ${u.name}): ${u.reason}`,
        );
      }
      process.exit(1);
    }
    fail(err);
  }
}

if (import.meta.main) {
  await main();
}
