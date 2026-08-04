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
import { credential } from "@intx/db/schema";
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
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT = "tnt_rkp";
const PROVIDER = "prv_rkp";
const cipher = createTestCredentialCipher();

describe.skipIf(!harnessDbEnvAvailable())("rekey resume-after-partial", () => {
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
  });

  async function row(id: string) {
    const [r] = await h.db
      .select()
      .from(credential)
      .where(eq(credential.id, id));
    if (r === undefined) throw new Error(`credential ${id} not found`);
    return r;
  }

  test("mixed row: plaintext secret + already-encrypted refreshSecret", async () => {
    // Simulates a crash after refreshSecret was encrypted but before secret was.
    await seedCredential(h.db, {
      id: "cred_mixed",
      tenantId: TENANT,
      providerId: PROVIDER,
      name: "mixed",
      secret: "sk-plain",
      refreshSecret: await cipher.encrypt(
        "rk-plain",
        credentialAad("cred_mixed", "refreshSecret"),
      ),
    });
    const before = await row("cred_mixed");

    const report = await rekeyCredentialSecrets(h.db, cipher);
    expect(report).toEqual({
      credentialSecrets: 1,
      credentialRefreshSecrets: 0,
      oauthClientSecrets: 0,
      alreadyEncrypted: 1,
    });

    const after = await row("cred_mixed");
    // secret got encrypted and decrypts back
    expect(isCiphertext(after.secret)).toBe(true);
    expect(
      await cipher.decrypt(after.secret, credentialAad("cred_mixed", "secret")),
    ).toBe("sk-plain");
    // the already-encrypted refreshSecret is byte-for-byte untouched and still decrypts
    expect(after.refreshSecret).toBe(before.refreshSecret);
    if (after.refreshSecret === null) throw new Error("refreshSecret vanished");
    expect(
      await cipher.decrypt(
        after.refreshSecret,
        credentialAad("cred_mixed", "refreshSecret"),
      ),
    ).toBe("rk-plain");
  });

  test("a plaintext secret literally starting with enc: is skipped (false positive)", async () => {
    // isCiphertext is a family match on "enc:"; a genuine plaintext beginning
    // "enc:" is treated as already-encrypted and left plaintext.
    await seedCredential(h.db, {
      id: "cred_encprefix",
      tenantId: TENANT,
      providerId: PROVIDER,
      name: "encprefix",
      secret: "enc:this-is-actually-plaintext",
    });

    const report = await rekeyCredentialSecrets(h.db, cipher);
    expect(report.credentialSecrets).toBe(0);
    expect(report.alreadyEncrypted).toBeGreaterThanOrEqual(1);

    const after = await row("cred_encprefix");
    // Left as plaintext (not encrypted). At read time the strict decrypt throws.
    expect(after.secret).toBe("enc:this-is-actually-plaintext");
    await expect(
      cipher.decrypt(after.secret, credentialAad("cred_encprefix", "secret")),
    ).rejects.toThrow();
  });
});
