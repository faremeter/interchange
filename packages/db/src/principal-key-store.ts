import { and, eq } from "drizzle-orm";

import { generateKeyPair, signEd25519 } from "@intx/crypto";
import { hexDecode, hexEncode, principalKeyAad } from "@intx/types";
import type { CredentialCipher } from "@intx/types";
import { generateId } from "@intx/hub-common";

import type { DB, DBExecutor } from "./client";
import { principalKey } from "./schema/principal-keys";

type DBHandle = DB["db"];

export type CreatePrincipalKeyStoreDeps = {
  db: DBHandle;
  /**
   * Seals the private key seed at rest. Production supplies a real env-key
   * cipher under `PRINCIPAL_KEY_ENCRYPTION_KEY`; tests and local dev may supply
   * the noop cipher, which stores the seed as plaintext.
   */
  cipher: CredentialCipher;
};

// The `principal_key` table holds a principal's Ed25519 signing key. A key is
// either hub-custodied or public-only. For a hub-custodied key the hub mints,
// seals, and signs with the private key on the principal's behalf, so a
// signature ATTRIBUTES an action to a principal but is NOT non-repudiable
// against the hub operator, who holds the key. For a public-only key another
// party holds the private key: the hub records the public key it is given and
// refuses to sign. See docs/AUTH.md.
//
// This module owns the one-active-key-per-principal invariant end to end:
// `sign` and `getPublicKey` resolve the single active row; `generate` and
// `recordPublicKey` make a key active, which the table's partial unique index
// rejects if the principal already has one; and `retireActiveKey` retires the
// active key so another can take its place. The private seed never leaves
// this module -- `sign` decrypts, signs, and discards it; no function returns
// private material.

/**
 * Store for a principal's hub-custodied key: it mints, signs with, and reads
 * the active key. The public-only functions below need no cipher, so they are
 * module functions that a caller without a key store can use.
 */
export function createPrincipalKeyStore({
  db,
  cipher,
}: CreatePrincipalKeyStoreDeps) {
  const loadActive = (principalId: string, tx?: DBExecutor) =>
    loadActiveKey(tx ?? db, principalId);

  return {
    /**
     * Mint a fresh active signing key for a principal. Generates an Ed25519 key
     * pair, seals the hex-encoded 32-byte seed with the cipher bound to the
     * key's row and column, and inserts it. A principal that already holds an
     * active key trips the partial unique index and this throws. Returns the
     * hex-encoded public key; never returns private material.
     */
    async generate(principalId: string, tx?: DBExecutor): Promise<string> {
      const id = generateId("principalKey");
      const keyPair = await generateKeyPair();
      const sealedPrivateKey = await cipher.encrypt(
        hexEncode(keyPair.privateKey),
        principalKeyAad(id, "private_key"),
      );
      const publicKey = hexEncode(keyPair.publicKey);
      const now = new Date();
      await (tx ?? db).insert(principalKey).values({
        id,
        principalId,
        publicKey,
        privateKey: sealedPrivateKey,
        status: "active",
        createdAt: now,
        updatedAt: now,
      });
      return publicKey;
    },

    /**
     * Sign a message with the principal's active key. Decrypts the sealed seed,
     * produces the raw 64-byte Ed25519 signature, and discards the seed. Throws
     * when the principal has no active key, or when its active key is
     * public-only. The seed stays a local that goes out
     * of scope; it is never returned or logged.
     */
    async sign(
      principalId: string,
      message: Uint8Array,
      tx?: DBExecutor,
    ): Promise<Uint8Array> {
      const row = await loadActive(principalId, tx);
      if (row === undefined) {
        throw new Error(
          `principalKeyStore.sign: principal ${principalId} has no active key`,
        );
      }
      if (row.privateKey === null) {
        throw new Error(
          `principalKeyStore.sign: the active key of principal ${principalId} has no private key on the hub`,
        );
      }
      const seedHex = await cipher.decrypt(
        row.privateKey,
        principalKeyAad(row.id, "private_key"),
      );
      const seed = hexDecode(seedHex);
      return signEd25519(seed, message);
    },

    /**
     * The hex-encoded public key of a principal's active signing key. Throws
     * when the principal has no active key rather than defaulting, so a missing
     * key surfaces at the call site instead of downstream.
     */
    async getPublicKey(principalId: string, tx?: DBExecutor): Promise<string> {
      const row = await loadActive(principalId, tx);
      if (row === undefined) {
        throw new Error(
          `principalKeyStore.getPublicKey: principal ${principalId} has no active key`,
        );
      }
      return row.publicKey;
    },
  };
}

export type PrincipalKeyStore = ReturnType<typeof createPrincipalKeyStore>;

async function loadActiveKey(executor: DBExecutor, principalId: string) {
  const [row] = await executor
    .select()
    .from(principalKey)
    .where(
      and(
        eq(principalKey.principalId, principalId),
        eq(principalKey.status, "active"),
      ),
    )
    .limit(1);
  return row;
}

/**
 * Make `publicKey` the principal's active key when another party holds the
 * private key. The row holds no private key, so `sign` refuses it.
 *
 * Recording the public-only key that is already active is a no-op, and
 * recording a public-only key the principal held before and retired makes
 * that row active again. The call throws when the key belongs to another
 * principal, when the key is hub-custodied, or when the principal already
 * has a different active key: retire that key first.
 *
 * The caller serializes writers for this principal. The function reads the
 * current rows and then writes, so it takes the transaction that holds the
 * caller's lock rather than opening its own.
 */
export async function recordPublicKey(
  tx: DBExecutor,
  principalId: string,
  publicKey: string,
): Promise<void> {
  const [existing] = await tx
    .select()
    .from(principalKey)
    .where(eq(principalKey.publicKey, publicKey))
    .limit(1);
  if (existing !== undefined && existing.principalId !== principalId) {
    throw new Error(
      `recordPublicKey: key ${publicKey} belongs to principal ${existing.principalId}, not ${principalId}`,
    );
  }
  if (existing !== undefined && existing.privateKey !== null) {
    throw new Error(
      `recordPublicKey: key ${publicKey} of principal ${principalId} is hub-custodied, not public-only`,
    );
  }
  if (existing?.status === "active") return;

  const active = await loadActiveKey(tx, principalId);
  if (active !== undefined) {
    throw new Error(
      `recordPublicKey: principal ${principalId} already has active key ${active.publicKey}; retire it first`,
    );
  }
  const now = new Date();
  if (existing !== undefined) {
    await tx
      .update(principalKey)
      .set({ status: "active", updatedAt: now })
      .where(eq(principalKey.id, existing.id));
    return;
  }
  await tx.insert(principalKey).values({
    id: generateId("principalKey"),
    principalId,
    publicKey,
    privateKey: null,
    status: "active",
    createdAt: now,
    updatedAt: now,
  });
}

/**
 * Retire the principal's active key, so it no longer signs or answers for
 * the principal and another key can become active. A principal with no
 * active key is left as it is.
 */
export async function retireActiveKey(
  executor: DBExecutor,
  principalId: string,
): Promise<void> {
  await executor
    .update(principalKey)
    .set({ status: "retired", updatedAt: new Date() })
    .where(
      and(
        eq(principalKey.principalId, principalId),
        eq(principalKey.status, "active"),
      ),
    );
}
