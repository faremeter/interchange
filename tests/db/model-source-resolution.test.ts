import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createNoopCredentialCipher } from "@intx/crypto";
import { eq } from "drizzle-orm";

import { credentialSecretColumns, resolveModelSources } from "@intx/db";
import { credential } from "@intx/db/schema";
import { credentialAad, type ModelRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { createTestCredentialCipher } from "@intx/test-harness/crypto";
import {
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedProvider,
  seedTenants,
  seedWallet,
} from "@intx/test-harness/seed";

const REQ_OPUS: ModelRequirement[] = [{ model: "opus" }];

// These direct-call tests seed PLAINTEXT secrets, so they resolve through an
// explicit noop cipher (which passes a plaintext value through). The two tests
// that seed real ciphertext -- or that assert the strict decrypt rejects a
// plaintext -- pass a real `createTestCredentialCipher()` instead.
const noopCipher = createNoopCredentialCipher();

// Credential use is authorized by tenant ownership within the hierarchy (see
// buildSource), so these direct-call tests seed the credential on the resolving
// tenant (or an ancestor) and pass no grants: a tenant-owned credential in the
// chain is usable by ownership alone. The out-of-chain case below proves the
// ownership boundary.

describe.skipIf(!harnessDbEnvAvailable())(
  "model-source-resolution (real DB)",
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
    });

    // A single credential-backed offering for model "opus" via provider
    // "anthropic". The credential's secret is what a built source carries as
    // its apiKey.
    async function seedBase(opts?: {
      offeringPriority?: number;
      offeringCapabilities?: string[];
      offeringQuirks?: Record<string, unknown>;
    }): Promise<void> {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await seedProvider(h.db, {
        id: "prv_x",
        tenantId: "tnt_root",
        name: "prv-x",
      });
      await seedCredential(h.db, {
        id: "cred_a",
        tenantId: "tnt_root",
        providerId: "prv_x",
        name: "cred-a",
        secret: "sk-anthropic",
      });
      await seedModel(h.db, {
        id: "mdl_opus",
        tenantId: "tnt_root",
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv_anthropic",
        tenantId: "tnt_root",
        name: "anthropic",
        credentialId: "cred_a",
      });
      await seedModelOffering(h.db, {
        id: "mof_a",
        tenantId: "tnt_root",
        modelId: "mdl_opus",
        providerId: "mpv_anthropic",
        priority: opts?.offeringPriority ?? 0,
        capabilities: opts?.offeringCapabilities ?? [],
        ...(opts?.offeringQuirks !== undefined
          ? { quirks: opts.offeringQuirks }
          : {}),
      });
    }

    // Add a second offering for "opus" through provider "relay".
    async function addRelay(priority: number): Promise<void> {
      await seedCredential(h.db, {
        id: "cred_r",
        tenantId: "tnt_root",
        providerId: "prv_x",
        name: "cred-r",
        secret: "sk-relay",
      });
      await seedModelProvider(h.db, {
        id: "mpv_relay",
        tenantId: "tnt_root",
        name: "relay",
        credentialId: "cred_r",
      });
      await seedModelOffering(h.db, {
        id: "mof_relay",
        tenantId: "tnt_root",
        modelId: "mdl_opus",
        providerId: "mpv_relay",
        priority,
      });
    }

    describe("resolveModelSources", () => {
      test("returns no_requirements for an empty requirement list", async () => {
        await seedBase();
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          [],
          noopCipher,
        );
        expect(result).toEqual({ ok: false, reason: "no_requirements" });
      });

      test("builds a credential-backed source from the catalog", async () => {
        await seedBase();
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources).toEqual([
          {
            id: "mof_a",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            credentialId: "cred_a",
            model: "opus",
            capabilities: [],
          },
        ]);
      });

      test("decrypts an encrypted credential secret through the cipher", async () => {
        const cipher = createTestCredentialCipher();
        await seedTenants(h.db, [{ id: "tnt_root" }]);
        await seedProvider(h.db, {
          id: "prv_x",
          tenantId: "tnt_root",
          name: "prv-x",
        });
        await seedCredential(h.db, {
          id: "cred_enc",
          tenantId: "tnt_root",
          providerId: "prv_x",
          name: "cred-enc",
          secret: await cipher.encrypt(
            "sk-seeded",
            credentialAad("cred_enc", "secret"),
          ),
        });
        await h.db
          .update(credential)
          .set(await credentialSecretColumns(cipher, "cred_enc", "sk-real"))
          .where(eq(credential.id, "cred_enc"));
        await seedModel(h.db, {
          id: "mdl_opus",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModelProvider(h.db, {
          id: "mpv_anthropic",
          tenantId: "tnt_root",
          name: "anthropic",
          credentialId: "cred_enc",
        });
        await seedModelOffering(h.db, {
          id: "mof_a",
          tenantId: "tnt_root",
          modelId: "mdl_opus",
          providerId: "mpv_anthropic",
        });

        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          cipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        // The delivered secret is the decrypted plaintext, not the stored blob.
        expect(result.materials[0]?.secret).toBe("sk-real");
        // The material carries the row's secret version from the same read.
        expect(result.materials[0]?.secretVersion).toBe(2);
      });

      test("fails closed when a stored secret is not a ciphertext", async () => {
        // seedBase stores a plaintext secret; a real cipher's strict decrypt
        // rejects it rather than delivering a bad key -- the un-re-keyed-row
        // guard.
        await seedBase();
        const cipher = createTestCredentialCipher();
        await expect(
          resolveModelSources(h.db, "tnt_root", REQ_OPUS, cipher),
        ).rejects.toThrow(/not an enc:aead/);
      });

      test("carries the offering row's quirks bag on the resolved source", async () => {
        await seedBase({
          offeringQuirks: { forceAssistantReasoningContent: true },
        });
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        const [source] = result.sources;
        expect(source?.quirks).toEqual({
          forceAssistantReasoningContent: true,
        });
      });

      test("omits quirks on the resolved source when the row has none", async () => {
        await seedBase();
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        const [source] = result.sources;
        if (source === undefined) throw new Error("expected one source");
        expect("quirks" in source).toBe(false);
      });

      test("orders sources by ascending priority", async () => {
        await seedBase();
        await addRelay(5);
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.id)).toEqual(["mof_a", "mof_relay"]);
      });

      test("matches when an offering carries the required capability", async () => {
        await seedBase({ offeringCapabilities: ["vision-input"] });
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          [{ model: "opus", capabilities: ["vision-input"] }],
          noopCipher,
        );
        expect(result.ok).toBe(true);
      });

      test("is unavailable when no offering carries the required capability", async () => {
        await seedBase();
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          [{ model: "opus", capabilities: ["vision-input"] }],
          noopCipher,
        );
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
        });
      });

      test("hard-pin restricts to the named providers in order", async () => {
        await seedBase();
        await addRelay(0);
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          [{ model: "opus", providers: { mode: "pin", order: ["relay"] } }],
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.id)).toEqual(["mof_relay"]);
      });

      test("soft-prefer fronts the named provider and keeps the rest", async () => {
        await seedBase({ offeringPriority: 1 });
        await addRelay(0);
        // relay has the better catalog priority, but the creator prefers
        // anthropic.
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          [
            {
              model: "opus",
              providers: { mode: "prefer", order: ["anthropic"] },
            },
          ],
          noopCipher,
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.id)).toEqual(["mof_a", "mof_relay"]);
      });

      test("a wallet-backed provider is skipped, leaving the model unavailable when it is the only one", async () => {
        await seedTenants(h.db, [{ id: "tnt_root" }]);
        await seedWallet(h.db, { id: "wal_1", tenantId: "tnt_root" });
        await seedModel(h.db, {
          id: "mdl_opus",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModelProvider(h.db, {
          id: "mpv_anthropic",
          tenantId: "tnt_root",
          name: "anthropic",
          walletId: "wal_1",
        });
        await seedModelOffering(h.db, {
          id: "mof_a",
          tenantId: "tnt_root",
          modelId: "mdl_opus",
          providerId: "mpv_anthropic",
        });
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          noopCipher,
        );
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
          model: "opus",
          skips: [{ reason: "wallet_backed", provider: "anthropic" }],
        });
      });

      for (const status of ["expired", "revoked", "error"] as const) {
        test(`skips a credential with status ${status} as credential_inactive`, async () => {
          await seedTenants(h.db, [{ id: "tnt_root" }]);
          await seedProvider(h.db, {
            id: "prv_x",
            tenantId: "tnt_root",
            name: "prv-x",
          });
          await seedCredential(h.db, {
            id: "cred_a",
            tenantId: "tnt_root",
            providerId: "prv_x",
            name: "cred-a",
            secret: "sk-inactive",
            status,
          });
          await seedModel(h.db, {
            id: "mdl_opus",
            tenantId: "tnt_root",
            canonicalName: "opus",
          });
          await seedModelProvider(h.db, {
            id: "mpv_anthropic",
            tenantId: "tnt_root",
            name: "anthropic",
            credentialId: "cred_a",
          });
          await seedModelOffering(h.db, {
            id: "mof_a",
            tenantId: "tnt_root",
            modelId: "mdl_opus",
            providerId: "mpv_anthropic",
          });
          const result = await resolveModelSources(
            h.db,
            "tnt_root",
            REQ_OPUS,
            noopCipher,
          );
          expect(result).toMatchObject({
            ok: false,
            reason: "model_unavailable",
            skips: [{ reason: "credential_inactive", provider: "anthropic" }],
          });
          expect(JSON.stringify(result)).not.toContain("sk-inactive");
        });
      }

      test("refuses a credential on a tenant outside the ancestor chain", async () => {
        // The provider references a real credential, but the credential lives
        // on a sibling tenant outside the resolving chain. resolveCredentialById
        // refuses it so its secret is never emitted, and the offering is
        // skipped as credential_unresolved. The real foreign key requires the
        // credential to exist, so an off-chain row replaces the old "no row at
        // all" fixture.
        await seedTenants(h.db, [{ id: "tnt_root" }, { id: "tnt_sibling" }]);
        await seedProvider(h.db, {
          id: "prv_x",
          tenantId: "tnt_sibling",
          name: "prv-x",
        });
        await seedCredential(h.db, {
          id: "cred_a",
          tenantId: "tnt_sibling",
          providerId: "prv_x",
          name: "cred-a",
          secret: "sk-sibling",
        });
        await seedModel(h.db, {
          id: "mdl_opus",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModelProvider(h.db, {
          id: "mpv_anthropic",
          tenantId: "tnt_root",
          name: "anthropic",
          credentialId: "cred_a",
        });
        await seedModelOffering(h.db, {
          id: "mof_a",
          tenantId: "tnt_root",
          modelId: "mdl_opus",
          providerId: "mpv_anthropic",
        });
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          noopCipher,
        );
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
          skips: [{ reason: "credential_unresolved", provider: "anthropic" }],
        });
        expect(JSON.stringify(result)).not.toContain("sk-sibling");
      });

      test("invoker preference reorders after the creator preference", async () => {
        await seedBase();
        await addRelay(0);
        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          [
            {
              model: "opus",
              providers: { mode: "prefer", order: ["anthropic"] },
            },
          ],
          noopCipher,
          { invokerPreferences: { opus: { mode: "pin", order: ["relay"] } } },
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        // The invoker pins relay, overriding the creator's anthropic
        // preference.
        expect(result.sources.map((s) => s.id)).toEqual(["mof_relay"]);
      });
    });
  },
);
