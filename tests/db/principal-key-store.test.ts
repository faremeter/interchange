import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import {
  createPrincipalKeyStore,
  parsePrincipalKeyRow,
  pgErrorCode,
  PG_UNIQUE_VIOLATION,
} from "@intx/db";
import { principalKey } from "@intx/db/schema";
import { isCiphertext, verifyEd25519 } from "@intx/crypto";
import { hexDecode } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt_pk";
const PRINCIPAL = "prn_pk";
// A real env-key cipher, not the noop cipher: the AEAD and its AAD binding must
// actually run so a bug in principalKeyAad or the seal/unseal path fails a test
// rather than passing silently through an identity cipher.
const cipher = createTestCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())(
  "createPrincipalKeyStore (real DB)",
  () => {
    let h: TestDb;

    beforeAll(async () => {
      h = await createTestDb();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT }]);
      await seedPrincipal(h.db, { id: PRINCIPAL, tenantId: TENANT });
    });

    test("generate then getPublicKey then sign round-trips through verify", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });

      const publicKey = await store.generate(PRINCIPAL);
      expect(await store.getPublicKey(PRINCIPAL)).toBe(publicKey);

      const message = new TextEncoder().encode("attribute this action");
      const signature = await store.sign(PRINCIPAL, message);
      expect(signature).toHaveLength(64);
      expect(
        await verifyEd25519(message, signature, hexDecode(publicKey)),
      ).toBe(true);
    });

    test("the private seed is sealed at rest and never exposed on the interface", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      const publicKey = await store.generate(PRINCIPAL);

      const [row] = await h.db
        .select()
        .from(principalKey)
        .where(eq(principalKey.principalId, PRINCIPAL));
      // The stored private key is AEAD ciphertext, not the raw hex seed.
      expect(isCiphertext(row?.privateKey ?? "")).toBe(true);
      // The only key material the store hands back is the public key.
      expect(publicKey).not.toBe(row?.privateKey);
      expect(isCiphertext(publicKey)).toBe(false);
    });

    test("a principal may hold only one active key", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await store.generate(PRINCIPAL);

      let code: string | undefined;
      try {
        await store.generate(PRINCIPAL);
      } catch (err) {
        code = pgErrorCode(err);
      }
      expect(code).toBe(PG_UNIQUE_VIOLATION);
    });

    test("sign and getPublicKey throw for a principal with no active key", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await expect(store.getPublicKey(PRINCIPAL)).rejects.toThrow(
        /no active key/,
      );
      await expect(
        store.sign(PRINCIPAL, new Uint8Array([1, 2, 3])),
      ).rejects.toThrow(/no active key/);
    });

    test("parsePrincipalKeyRow validates the status enum", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await store.generate(PRINCIPAL);

      const [row] = await h.db
        .select()
        .from(principalKey)
        .where(eq(principalKey.principalId, PRINCIPAL));
      if (row === undefined) throw new Error("expected a minted key row");
      expect(parsePrincipalKeyRow(row).status).toBe("active");
    });
  },
);
