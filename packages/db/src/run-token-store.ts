import { asc, eq, inArray } from "drizzle-orm";

import { sha256 } from "@intx/crypto";
import { actionConsumer, toolConsumer } from "@intx/authz";
import { runTokenAad, type CredentialCipher } from "@intx/types";
import {
  generateBearerSecret,
  generateId,
  RUN_TOKEN_PREFIX,
} from "@intx/hub-common";

import type { DB, DBExecutor } from "./client";
import { runToken } from "./schema/run-tokens";

type DBHandle = DB["db"];

const SEALED_TOKEN_COLUMN = "sealed_token";

/**
 * Whether `value` names a workflow consumer that can hold a run token: a tool
 * package or an action handler, in the identity form the credential-use gate
 * and the `{tool}` grant condition use.
 */
function isRunTokenConsumer(value: string): boolean {
  return [toolConsumer(""), actionConsumer("")].some(
    (prefix) => value.startsWith(prefix) && value.length > prefix.length,
  );
}

/** A run token as the authentication lookup sees it; never the secret. */
export type RunTokenRow = {
  id: string;
  principalId: string;
  consumer: string;
};

/** A run token unsealed for delivery to the run's sidecar. */
export type RunTokenDelivery = {
  id: string;
  consumer: string;
  token: string;
};

/**
 * Delete every token the principals hold, so no sealed secret outlives the
 * run that could use it. Needs no cipher, so a path that deactivates run
 * principals calls it in the same transaction.
 */
export async function deleteRunTokensForPrincipals(
  tx: DBExecutor,
  principalIds: readonly string[],
): Promise<void> {
  if (principalIds.length === 0) return;
  await tx
    .delete(runToken)
    .where(inArray(runToken.principalId, [...principalIds]));
}

export type CreateRunTokenStoreDeps = {
  db: DBHandle;
  /**
   * Seals each token's plaintext at rest. Production supplies the credential
   * cipher under `CREDENTIAL_ENCRYPTION_KEY`; tests and local dev may supply
   * the noop cipher.
   */
  cipher: CredentialCipher;
};

/**
 * Store for the `run_token` table -- the bearers a workflow run's consumers
 * present to the hub REST API as the run principal.
 *
 * The store owns the token's whole secret lifecycle: it mints the plaintext,
 * stores its hash for lookup and a sealed copy for delivery, and unseals only
 * for delivery. No method other than `loadDelivery` returns a secret.
 */
export function createRunTokenStore({ db, cipher }: CreateRunTokenStoreDeps) {
  return {
    /**
     * Mint a token for each consumer the principal does not hold one for yet.
     * A consumer that already has a token keeps it, so a retry or a race on
     * the same principal never replaces a token already delivered.
     */
    async mintIfAbsent(
      args: { principalId: string; consumers: readonly string[] },
      tx: DBExecutor,
    ): Promise<void> {
      for (const consumer of args.consumers) {
        if (!isRunTokenConsumer(consumer)) {
          throw new Error(
            `runTokenStore.mintIfAbsent: ${JSON.stringify(consumer)} is not a tool or action consumer`,
          );
        }
        const id = generateId("runToken");
        const token = generateBearerSecret(RUN_TOKEN_PREFIX);
        await tx
          .insert(runToken)
          .values({
            id,
            principalId: args.principalId,
            consumer,
            tokenHashSha256: await sha256(token),
            sealedToken: await cipher.encrypt(
              token,
              runTokenAad(id, SEALED_TOKEN_COLUMN),
            ),
          })
          .onConflictDoNothing({
            target: [runToken.principalId, runToken.consumer],
          });
      }
    },

    /** Find the token whose plaintext hashes to `tokenHash`. */
    async findByHash(tokenHash: Uint8Array): Promise<RunTokenRow | undefined> {
      const [row] = await db
        .select({
          id: runToken.id,
          principalId: runToken.principalId,
          consumer: runToken.consumer,
        })
        .from(runToken)
        .where(eq(runToken.tokenHashSha256, tokenHash))
        .limit(1);
      return row;
    },

    /** Unseal every token the principal holds, ordered by consumer. */
    async loadDelivery(principalId: string): Promise<RunTokenDelivery[]> {
      const rows = await db
        .select()
        .from(runToken)
        .where(eq(runToken.principalId, principalId))
        .orderBy(asc(runToken.consumer));
      const delivery: RunTokenDelivery[] = [];
      for (const row of rows) {
        delivery.push({
          id: row.id,
          consumer: row.consumer,
          token: await cipher.decrypt(
            row.sealedToken,
            runTokenAad(row.id, SEALED_TOKEN_COLUMN),
          ),
        });
      }
      return delivery;
    },
  };
}

export type RunTokenStore = ReturnType<typeof createRunTokenStore>;
