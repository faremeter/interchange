import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { rekeyCredentialSecrets } from "@intx/db";
import { credential, oauthClient } from "@intx/db/schema";
import { credentialAad } from "@intx/types";
import { isCiphertext } from "@intx/crypto";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedCredential,
  seedOAuthClient,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT = "tnt_rk";
const PROVIDER = "prv_rk";
const cipher = createTestCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())(
  "rekeyCredentialSecrets (real DB)",
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
      await seedProvider(h.db, { id: PROVIDER, tenantId: TENANT, name: "gh" });
      // A plaintext credential (secret + refreshSecret) predating encryption.
      await seedCredential(h.db, {
        id: "cred_plain",
        tenantId: TENANT,
        providerId: PROVIDER,
        name: "plain",
        secret: "sk-plain",
        refreshSecret: "rk-plain",
      });
      // A credential already stored encrypted -- must be left untouched.
      await seedCredential(h.db, {
        id: "cred_enc",
        tenantId: TENANT,
        providerId: PROVIDER,
        name: "enc",
        secret: await cipher.encrypt(
          "sk-enc",
          credentialAad("cred_enc", "secret"),
        ),
      });
      // A plaintext oauth-client secret predating encryption.
      await seedOAuthClient(h.db, {
        id: "oac_plain",
        tenantId: TENANT,
        providerId: PROVIDER,
        clientSecret: "cs-plain",
      });
    });

    async function credentialRow(id: string) {
      const [row] = await h.db
        .select()
        .from(credential)
        .where(eq(credential.id, id));
      if (row === undefined) throw new Error(`credential ${id} not found`);
      return row;
    }

    test("encrypts plaintext rows in place and leaves encrypted rows untouched", async () => {
      const before = await credentialRow("cred_enc");

      const report = await rekeyCredentialSecrets(h.db, cipher);
      expect(report).toEqual({
        credentialSecrets: 1,
        credentialRefreshSecrets: 1,
        oauthClientSecrets: 1,
        alreadyEncrypted: 1,
      });

      // The plaintext credential is now ciphertext that decrypts to the
      // original values, bound to (id, column).
      const plain = await credentialRow("cred_plain");
      expect(isCiphertext(plain.secret)).toBe(true);
      expect(
        await cipher.decrypt(
          plain.secret,
          credentialAad("cred_plain", "secret"),
        ),
      ).toBe("sk-plain");
      if (plain.refreshSecret === null) {
        throw new Error("expected a re-keyed refresh secret");
      }
      expect(isCiphertext(plain.refreshSecret)).toBe(true);
      expect(
        await cipher.decrypt(
          plain.refreshSecret,
          credentialAad("cred_plain", "refreshSecret"),
        ),
      ).toBe("rk-plain");

      // The already-encrypted credential is byte-for-byte unchanged.
      const enc = await credentialRow("cred_enc");
      expect(enc.secret).toBe(before.secret);

      // The oauth-client secret is now ciphertext decrypting to the original.
      const [oac] = await h.db
        .select()
        .from(oauthClient)
        .where(eq(oauthClient.id, "oac_plain"));
      if (oac === undefined) throw new Error("oauth client not found");
      expect(isCiphertext(oac.clientSecret)).toBe(true);
      expect(
        await cipher.decrypt(
          oac.clientSecret,
          credentialAad("oac_plain", "clientSecret"),
        ),
      ).toBe("cs-plain");
    });

    test("a second run is a no-op (idempotent)", async () => {
      await rekeyCredentialSecrets(h.db, cipher);
      const secondRun = await rekeyCredentialSecrets(h.db, cipher);
      expect(secondRun).toEqual({
        credentialSecrets: 0,
        credentialRefreshSecrets: 0,
        oauthClientSecrets: 0,
        // cred_plain.secret, cred_plain.refreshSecret, cred_enc.secret, oac.
        alreadyEncrypted: 4,
      });
    });
  },
);
