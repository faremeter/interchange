#!/usr/bin/env bun
/* eslint-disable no-console */
// bin/db-backfill -- run-once: fold legacy agents onto workflow definitions.
//
// Constructs a `db` handle from the `DB_*` environment and runs the rows-only
// backfill: every legacy `agent` and native `workflow`-kind asset gains a
// `workflow_definition` (+ version) row, with no body persisted -- an
// agent-origin body is synthesized at deploy time from the live agent row.
//
// Safe to re-run: agents/assets that already have a definition are skipped
// (`origin_agent_id` / `asset_id` guards plus their unique indexes). If any
// agent is undeployable (null prompt, or model requirements that resolve to no
// source), the preflight aborts having written NOTHING and prints the complete
// manifest, so the whole failing set is fixed in one pass rather than one
// re-run at a time.

import { createDB } from "@intx/db";
import { BackfillPreflightError, runBackfill } from "@intx/hub-sessions";

import {
  resolveBackfillDbConfig,
  type BackfillDbConfig,
} from "./lib/db-config";

function fail(err: unknown): never {
  console.error(
    `db-backfill: ${err instanceof Error ? err.message : String(err)}`,
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

  let handle: ReturnType<typeof createDB>;
  try {
    handle = createDB(config);
  } catch (err) {
    fail(err);
  }
  const { db, close } = handle;

  try {
    const summary = await runBackfill(db);
    await close();
    console.log(
      `db-backfill: folded ${summary.agentsFolded} agent(s) ` +
        `(${summary.agentsSkipped} already folded), ` +
        `${summary.workflowAssetsFolded} workflow asset(s) ` +
        `(${summary.workflowAssetsSkipped} already folded); ` +
        `anchored ${summary.nativeRunsAnchored} deployment run(s) to a definition`,
    );
  } catch (err) {
    await close();
    if (err instanceof BackfillPreflightError) {
      console.error(
        `db-backfill: preflight aborted -- ${err.undeployable.length} ` +
          `agent(s) cannot be folded; no rows written:`,
      );
      for (const u of err.undeployable) {
        console.error(`  - ${u.agentId} (${u.name}): ${u.reason}`);
      }
      process.exit(1);
    }
    fail(err);
  }
}

if (import.meta.main) {
  await main();
}
