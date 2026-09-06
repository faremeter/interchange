import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { type } from "arktype";

import { createPrincipalKeyStore, lookupLocalPrincipalSigner } from "@intx/db";
import { SignerIdentity } from "@intx/types";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

const TENANT = "tnt_si";
const PRINCIPAL = "prn_si";
const cipher = createTestCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())(
  "lookupLocalPrincipalSigner (real DB)",
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

    test("returns the local-principal variant carrying the resolved public key", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      const mintedPublicKey = await store.generate(PRINCIPAL);

      const signer = await lookupLocalPrincipalSigner(store, PRINCIPAL);
      expect(signer).toEqual({
        kind: "local-principal",
        principalId: PRINCIPAL,
        publicKey: mintedPublicKey,
      });
      // Secondary: the resolved shape validates against the SignerIdentity type.
      expect(SignerIdentity(signer) instanceof type.errors).toBe(false);
    });

    test("throws for a principal with no active key", async () => {
      const store = createPrincipalKeyStore({ db: h.db, cipher });
      await expect(
        lookupLocalPrincipalSigner(store, PRINCIPAL),
      ).rejects.toThrow(/no active key/);
    });
  },
);
