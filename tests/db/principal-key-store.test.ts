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
  recordPublicKey,
  retireActiveKey,
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
const OTHER_PRINCIPAL = "prn_pk_other";
const KEY_A = "aa".repeat(32);
const KEY_B = "bb".repeat(32);
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

    function keyRows(principalId: string) {
      return h.db
        .select()
        .from(principalKey)
        .where(eq(principalKey.principalId, principalId));
    }

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

    test("recordPublicKey makes a public-only key active, and sign refuses it", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A));

      expect(await store.getPublicKey(PRINCIPAL)).toBe(KEY_A);
      const rows = await keyRows(PRINCIPAL);
      expect(rows).toHaveLength(1);
      expect(rows[0]?.privateKey).toBeNull();
      await expect(
        store.sign(PRINCIPAL, new Uint8Array([1, 2, 3])),
      ).rejects.toThrow(/has no private key on the hub/);
    });

    test("recordPublicKey of the active key is a no-op", async () => {
      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A));
      const [before] = await keyRows(PRINCIPAL);

      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A));

      expect(await keyRows(PRINCIPAL)).toEqual(before ? [before] : []);
    });

    test("recordPublicKey makes a retired key of the same principal active again", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A));
      await retireActiveKey(h.db, PRINCIPAL);
      const retiredAt = new Date("2020-01-01T00:00:00Z");
      await h.db
        .update(principalKey)
        .set({ updatedAt: retiredAt })
        .where(eq(principalKey.principalId, PRINCIPAL));
      const [retired] = await keyRows(PRINCIPAL);
      if (retired === undefined) throw new Error("expected a key row");

      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A));

      const rows = await keyRows(PRINCIPAL);
      expect(rows).toHaveLength(1);
      expect(rows[0]?.id).toBe(retired.id);
      expect(rows[0]?.status).toBe("active");
      expect(rows[0]?.updatedAt.getTime()).toBeGreaterThan(retiredAt.getTime());
      expect(await store.getPublicKey(PRINCIPAL)).toBe(KEY_A);
    });

    test("recordPublicKey throws when the principal has a different active key", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A));

      await expect(
        h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_B)),
      ).rejects.toThrow(/already has active key .*; retire it first/);
      expect(await store.getPublicKey(PRINCIPAL)).toBe(KEY_A);
    });

    test("recordPublicKey of a retired key throws when a different key is active", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A));
      await retireActiveKey(h.db, PRINCIPAL);
      await h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_B));

      await expect(
        h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A)),
      ).rejects.toThrow(/already has active key .*; retire it first/);
      expect(await store.getPublicKey(PRINCIPAL)).toBe(KEY_B);
    });

    test("recordPublicKey throws for a key that belongs to another principal", async () => {
      await seedPrincipal(h.db, { id: OTHER_PRINCIPAL, tenantId: TENANT });
      await h.db.transaction((tx) =>
        recordPublicKey(tx, OTHER_PRINCIPAL, KEY_A),
      );

      await expect(
        h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, KEY_A)),
      ).rejects.toThrow(/belongs to principal prn_pk_other/);
      expect(await keyRows(PRINCIPAL)).toHaveLength(0);
    });

    test("recordPublicKey refuses the key of a hub-custodied row, active or retired", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      const minted = await store.generate(PRINCIPAL);

      await expect(
        h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, minted)),
      ).rejects.toThrow(/is hub-custodied, not public-only/);

      await retireActiveKey(h.db, PRINCIPAL);
      await expect(
        h.db.transaction((tx) => recordPublicKey(tx, PRINCIPAL, minted)),
      ).rejects.toThrow(/is hub-custodied, not public-only/);
      await expect(store.getPublicKey(PRINCIPAL)).rejects.toThrow(
        /no active key/,
      );
    });

    test("recordPublicKey in a rolled-back transaction leaves no row", async () => {
      const rollback = new Error("roll back");

      await expect(
        h.db.transaction(async (tx) => {
          await recordPublicKey(tx, PRINCIPAL, KEY_A);
          throw rollback;
        }),
      ).rejects.toBe(rollback);

      expect(await keyRows(PRINCIPAL)).toHaveLength(0);
    });

    test("retireActiveKey leaves a principal with no active key as it is", async () => {
      await retireActiveKey(h.db, PRINCIPAL);
      expect(await keyRows(PRINCIPAL)).toHaveLength(0);
    });

    test("retireActiveKey retires a hub-minted key so a new key can be minted", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      const first = await store.generate(PRINCIPAL);

      await retireActiveKey(h.db, PRINCIPAL);
      await expect(store.getPublicKey(PRINCIPAL)).rejects.toThrow(
        /no active key/,
      );
      const second = await store.generate(PRINCIPAL);

      expect(second).not.toBe(first);
      expect(await store.getPublicKey(PRINCIPAL)).toBe(second);
      const statuses = (await keyRows(PRINCIPAL))
        .map((row) => [row.publicKey, row.status])
        .sort();
      expect(statuses).toEqual(
        [
          [first, "retired"],
          [second, "active"],
        ].sort(),
      );
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
