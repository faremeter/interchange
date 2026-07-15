#!/usr/bin/env bun
/* eslint-disable no-console */

// Provisions the local development sidecar's identity row.
//
// The hub authenticates a sidecar's WebSocket handshake against the
// per-sidecar token hash stored on the `sidecar` table, so the dev
// sidecar cannot connect until a row exists whose `token_hash_sha256`
// matches the `SIDECAR_TOKEN` it presents. This script writes that row
// from the resolved `SIDECAR_ID`/`SIDECAR_TOKEN` the orchestrator hands
// the sidecar process, keeping the two in agreement by construction.
//
// Run as a blocking pre-flight step before the sidecar spawns. It is a
// dev-only convenience: production sidecars are provisioned by a
// separate operator mechanism, never self-provisioned.

import { createDB } from "@intx/db";
import { sidecar } from "@intx/db/schema";
import { sha256 } from "@intx/crypto";

import {
  resolveProvisionConfig,
  type ProvisionConfig,
} from "./lib/provision-config";

async function main(): Promise<void> {
  let config: ProvisionConfig;
  try {
    config = resolveProvisionConfig(process.env);
  } catch (err) {
    console.error(
      `provision-sidecar: ${err instanceof Error ? err.message : String(err)}`,
    );
    process.exit(1);
  }

  const { db, close } = createDB(config.db);
  // Close the connection before exiting on either path, rather than in a
  // `finally`: `process.exit` on the failure path would preempt a `finally`
  // and abandon the connection, so capture the failure and exit after the
  // close instead.
  let failure: string | null = null;
  try {
    const tokenHashSha256 = await sha256(config.sidecarToken);
    // `url` is not read by the handshake or routing today; it names the
    // sidecar's own address on the row, so a dev placeholder is honest
    // rather than borrowing the hub's URL. Upsert on the id so a restart
    // without a database reset re-points the token instead of colliding.
    await db
      .insert(sidecar)
      .values({
        id: config.sidecarId,
        url: "ws://dev-sidecar",
        tokenHashSha256,
      })
      .onConflictDoUpdate({
        target: sidecar.id,
        set: { url: "ws://dev-sidecar", tokenHashSha256 },
      });
  } catch (err) {
    failure = err instanceof Error ? err.message : String(err);
  }
  // A failure closing the connection must not mask a write failure: log it
  // but let the write outcome decide the exit.
  try {
    await close();
  } catch (err) {
    console.error(
      `provision-sidecar: failed to close database connection: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  if (failure !== null) {
    console.error(`provision-sidecar: failed to write sidecar row: ${failure}`);
    process.exit(1);
  }
  console.log(`Provisioned sidecar "${config.sidecarId}".`);
}

if (import.meta.main) {
  await main();
}
