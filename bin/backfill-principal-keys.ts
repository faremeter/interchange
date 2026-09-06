#!/usr/bin/env bun

// Backfill per-principal signing keys onto principals that predate the
// per-principal-key feature.
//
// Run ONCE as the last step of the deploy that introduces per-principal keys,
// AFTER the key-minting hub is live. It is safe against a live hub: nothing
// reads principal keys yet, the hub only mints for newly-created principals,
// and the active-key unique index blocks a double key. Idempotent -- a
// principal that already holds an active key is skipped -- so it is safe to
// re-run or resume after a partial failure. On a database where every principal
// was created after the feature landed (e.g. local dev), it does nothing.
//
//   set -a; . .env; . .env.hub; set +a
//   bun run --conditions=intx-src bin/backfill-principal-keys.ts
//
// It reads the same DB_* and PRINCIPAL_KEY_ENCRYPTION_KEY the hub uses. The key
// is required: the seeds must be sealed under the same cipher the hub decrypts
// with, so there is no noop fallback.

import { setup, getLogger } from "@intx/log";
import {
  backfillPrincipalKeys,
  createDB,
  createPrincipalKeyStore,
} from "@intx/db";
import { createEnvKeyCredentialCipher } from "@intx/crypto";
import { hexDecode } from "@intx/types";

import { resolveDbConfig } from "./lib/db-config";

await setup({ dev: true });
const log = getLogger(["backfill-principal-keys"]);

const keyHex = process.env["PRINCIPAL_KEY_ENCRYPTION_KEY"];
if (keyHex === undefined || keyHex.trim() === "") {
  throw new Error(
    "PRINCIPAL_KEY_ENCRYPTION_KEY environment variable is required",
  );
}
const cipher = createEnvKeyCredentialCipher(hexDecode(keyHex));

const { db, close } = createDB(resolveDbConfig(process.env));
try {
  const store = createPrincipalKeyStore({ db, cipher });
  const report = await backfillPrincipalKeys(db, store);
  log.info(
    "Backfill complete: minted {keysGenerated} new signing key(s); " +
      "{alreadyKeyed} principal(s) already keyed.",
    report,
  );
} finally {
  await close();
}
