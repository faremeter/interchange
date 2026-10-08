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
   * Seals the private key seed at rest: the real env-key cipher under
   * `PRINCIPAL_KEY_ENCRYPTION_KEY`, or the noop cipher for tests and local dev.
   */
  cipher: CredentialCipher;
};

/**
 * Store for the `principal_key` table -- a principal's Ed25519 signing key.
 *
 * The hub custodies the private key: a signature attributes an action to a
 * principal but is not non-repudiable against the hub operator, who holds the
 * key. See docs/AUTH.md.
 *
 * One active key per principal: `sign`/`getPublicKey` resolve the single active
 * row, and `generate` relies on the table's partial unique index to reject a
 * second. The private seed never leaves this module; no method returns it.
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
     * Mint a fresh active signing key: generate an Ed25519 pair, seal the
     * hex-encoded seed with the row-bound cipher, insert, and return the
     * hex-encoded public key. A second active key trips the partial unique
     * index and throws.
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
     * Sign with the principal's active key: decrypt the sealed seed, sign, and
     * drop the seed. Throws when the principal has no active key.
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
     * The hex-encoded public key of the principal's active key. Throws when
     * there is none, so a missing key surfaces at the call site.
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
