// Re-key existing credential secrets: encrypt any secret still stored as
// plaintext, in place, under the given cipher.
//
// Ordinarily every secret is encrypted at its write site, so on a database
// created after encryption-at-rest landed there is nothing to do. This pass
// exists for the transition: a database whose rows predate encryption still
// holds plaintext, and the strict read path throws on a non-ciphertext value.
// Running this once (against a stopped hub, so no write races an un-re-keyed
// row) brings every row to the encrypted form.
//
// Idempotent: a value already a ciphertext is skipped, so the pass is safe to
// re-run or resume after a partial failure.

import { eq } from "drizzle-orm";

import { credentialAad, type CredentialCipher } from "@intx/types";
import { isCiphertext } from "@intx/crypto";

import type { DB } from "./client";
import { credential } from "./schema/credentials";
import { oauthClient } from "./schema/oauth-clients";

export type RekeyReport = {
  /** Credential `secret` columns newly encrypted. */
  credentialSecrets: number;
  /** Credential `refresh_secret` columns newly encrypted. */
  credentialRefreshSecrets: number;
  /** OAuth-client `client_secret` columns newly encrypted. */
  oauthClientSecrets: number;
  /** Columns already encrypted and left unchanged. */
  alreadyEncrypted: number;
};

export async function rekeyCredentialSecrets(
  db: DB["db"],
  cipher: CredentialCipher,
): Promise<RekeyReport> {
  const report: RekeyReport = {
    credentialSecrets: 0,
    credentialRefreshSecrets: 0,
    oauthClientSecrets: 0,
    alreadyEncrypted: 0,
  };

  for (const row of await db.select().from(credential)) {
    const updates: { secret?: string; refreshSecret?: string } = {};
    if (isCiphertext(row.secret)) {
      report.alreadyEncrypted += 1;
    } else {
      updates.secret = await cipher.encrypt(
        row.secret,
        credentialAad(row.id, "secret"),
      );
      report.credentialSecrets += 1;
    }
    if (row.refreshSecret !== null) {
      if (isCiphertext(row.refreshSecret)) {
        report.alreadyEncrypted += 1;
      } else {
        updates.refreshSecret = await cipher.encrypt(
          row.refreshSecret,
          credentialAad(row.id, "refreshSecret"),
        );
        report.credentialRefreshSecrets += 1;
      }
    }
    if (updates.secret !== undefined || updates.refreshSecret !== undefined) {
      await db.update(credential).set(updates).where(eq(credential.id, row.id));
    }
  }

  for (const row of await db.select().from(oauthClient)) {
    if (isCiphertext(row.clientSecret)) {
      report.alreadyEncrypted += 1;
      continue;
    }
    await db
      .update(oauthClient)
      .set({
        clientSecret: await cipher.encrypt(
          row.clientSecret,
          credentialAad(row.id, "clientSecret"),
        ),
      })
      .where(eq(oauthClient.id, row.id));
    report.oauthClientSecrets += 1;
  }

  return report;
}
