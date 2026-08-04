#!/usr/bin/env bun

// Re-key existing plaintext credential secrets to the encrypted-at-rest form.
//
// Run ONCE, against a STOPPED hub, during the deploy that introduces credential
// encryption-at-rest. With the hub down there is no write to race an un-re-keyed
// row, so the strict read path never trips on legacy plaintext mid-migration.
//
//   set -a; . .env; . .env.hub; set +a
//   bun run --conditions=intx-src bin/rekey-credential-secrets.ts
//
// It reads the same DB_* and CREDENTIAL_ENCRYPTION_KEY the hub uses, and is
// idempotent -- already-encrypted rows are skipped -- so it is safe to re-run
// or resume after a partial failure. On a database created after encryption
// landed, every row is already encrypted and it does nothing.

import { setup, getLogger } from "@intx/log";
import { createDB, rekeyCredentialSecrets } from "@intx/db";
import { createEnvKeyCredentialCipher } from "@intx/crypto";
import { hexDecode } from "@intx/types";

import { resolveDbConfig } from "./lib/db-config";

await setup({ dev: true });
const log = getLogger(["rekey-credential-secrets"]);

const keyHex = process.env["CREDENTIAL_ENCRYPTION_KEY"];
if (keyHex === undefined || keyHex.trim() === "") {
  throw new Error("CREDENTIAL_ENCRYPTION_KEY environment variable is required");
}
const cipher = createEnvKeyCredentialCipher(hexDecode(keyHex));

const { db, close } = createDB(resolveDbConfig(process.env));
try {
  const report = await rekeyCredentialSecrets(db, cipher);
  log.info(
    "Re-key complete: encrypted {credentialSecrets} credential secret(s), " +
      "{credentialRefreshSecrets} refresh secret(s), and {oauthClientSecrets} " +
      "oauth-client secret(s); {alreadyEncrypted} column(s) already encrypted.",
    report,
  );
} finally {
  await close();
}
