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

/**
 * Store for the `principal_key` table -- a principal's Ed25519 signing key.
 *
 * The hub custodies the private key: it mints, seals, and signs with it on the
 * principal's behalf. A signature therefore ATTRIBUTES an action to a principal
 * but is NOT non-repudiable against the hub operator, who holds the key. See
 * docs/AUTH.md.
 *
 * The store owns the one-active-key-per-principal invariant end to end: `sign`
 * and `getPublicKey` resolve the single active row, and `generate` inserts a new
 * active key that the table's partial unique index rejects if the principal
 * already has one. The private seed never leaves this module -- `sign` decrypts,
 * signs, and discards it; no method returns private material.
 */
export function createPrincipalKeyStore({
  db,
  cipher,
}: CreatePrincipalKeyStoreDeps) {
  async function loadActive(principalId: string, tx?: DBExecutor) {
    const [row] = await (tx ?? db)
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
     * when the principal has no active key. The seed stays a local that goes out
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
