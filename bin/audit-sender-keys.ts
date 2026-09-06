#!/usr/bin/env bun

// Audit that every mail sender which could sign resolves to a durable public
// key, so a durable-signature verifier sees no unresolvable signed senders.
//
// Read-only. Sweeps every workflow run past the deploy-ack window that carries a
// sending address, and every active user principal, resolving each against the
// hub's own key storage -- the same resolver the verifier uses. Run it before
// turning on signature enforcement: it names any sender whose address does not
// resolve so the hole is fixed before that sender's mail would be dropped. It
// writes nothing.
//
//   set -a; . .env; . .env.hub; set +a
//   bun run --conditions=intx-src bin/audit-sender-keys.ts
//
// It reads the same DB_* and PRINCIPAL_KEY_ENCRYPTION_KEY the hub uses. Exits
// non-zero when any sender is unresolvable.

import { setup, getLogger } from "@intx/log";
import { auditSenderKeys, createDB, createPrincipalKeyStore } from "@intx/db";
import { createEnvKeyCredentialCipher } from "@intx/crypto";
import { hexDecode } from "@intx/types";

import { resolveDbConfig } from "./lib/db-config";

await setup({ dev: true });
const log = getLogger(["audit-sender-keys"]);

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
  const report = await auditSenderKeys(db, store);
  log.info(
    "Audit complete: checked {runsChecked} run sender(s) and " +
      "{usersChecked} user sender(s); {unresolvedCount} unresolvable.",
    { ...report, unresolvedCount: report.unresolved.length },
  );
  for (const sender of report.unresolved) {
    log.error("Unresolvable {kind} sender: {address}", sender);
  }
  if (report.unresolved.length > 0) {
    process.exitCode = 1;
  }
} finally {
  await close();
}
