import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { resolveInstanceModelSources, resolveModelSources } from "@intx/db";
import type { ModelRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAgent,
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
  seedWallet,
} from "@intx/test-harness/seed";

const REQ_OPUS: ModelRequirement[] = [{ model: "opus" }];

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
        const result = await resolveModelSources(h.db, "tnt_root", []);
        expect(result).toEqual({ ok: false, reason: "no_requirements" });
      });

      test("builds a credential-backed source from the catalog", async () => {
        await seedBase();
        const result = await resolveModelSources(h.db, "tnt_root", REQ_OPUS);
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources).toEqual([
          {
            id: "mof_a",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: "sk-anthropic",
            model: "opus",
            capabilities: [],
          },
        ]);
      });

      test("orders sources by ascending priority", async () => {
        await seedBase();
        await addRelay(5);
        const result = await resolveModelSources(h.db, "tnt_root", REQ_OPUS);
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.id)).toEqual(["mof_a", "mof_relay"]);
      });

      test("matches when an offering carries the required capability", async () => {
        await seedBase({ offeringCapabilities: ["vision"] });
        const result = await resolveModelSources(h.db, "tnt_root", [
          { model: "opus", capabilities: ["vision"] },
        ]);
        expect(result.ok).toBe(true);
      });

      test("is unavailable when no offering carries the required capability", async () => {
        await seedBase();
        const result = await resolveModelSources(h.db, "tnt_root", [
          { model: "opus", capabilities: ["vision"] },
        ]);
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
        });
      });

      test("hard-pin restricts to the named providers in order", async () => {
        await seedBase();
        await addRelay(0);
        const result = await resolveModelSources(h.db, "tnt_root", [
          { model: "opus", providers: { mode: "pin", order: ["relay"] } },
        ]);
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.id)).toEqual(["mof_relay"]);
      });

      test("soft-prefer fronts the named provider and keeps the rest", async () => {
        await seedBase({ offeringPriority: 1 });
        await addRelay(0);
        // relay has the better catalog priority, but the creator prefers
        // anthropic.
        const result = await resolveModelSources(h.db, "tnt_root", [
          {
            model: "opus",
            providers: { mode: "prefer", order: ["anthropic"] },
          },
        ]);
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
        const result = await resolveModelSources(h.db, "tnt_root", REQ_OPUS);
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
          model: "opus",
          skips: [{ reason: "wallet_backed", provider: "anthropic" }],
        });
      });

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
        const result = await resolveModelSources(h.db, "tnt_root", REQ_OPUS);
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
          { invokerPreferences: { opus: { mode: "pin", order: ["relay"] } } },
        );
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        // The invoker pins relay, overriding the creator's anthropic
        // preference.
        expect(result.sources.map((s) => s.id)).toEqual(["mof_relay"]);
      });
    });

    describe("resolveInstanceModelSources", () => {
      async function seedAgentWithRelay(
        modelRequirements: unknown,
      ): Promise<void> {
        await seedBase();
        await addRelay(1);
        await seedPrincipal(h.db, {
          id: "prn_creator",
          tenantId: "tnt_root",
        });
        await seedAgent(h.db, {
          id: "agt_1",
          tenantId: "tnt_root",
          creatorPrincipalId: "prn_creator",
          modelRequirements,
        });
      }

      test("resolves from the agent's persisted modelRequirements", async () => {
        await seedAgentWithRelay([{ model: "opus" }]);
        const result = await resolveInstanceModelSources(h.db, "tnt_root", {
          agentId: "agt_1",
          modelPreferences: null,
        });
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        // mof_a (priority 0) before mof_relay (priority 1).
        expect(result.sources.map((s) => s.id)).toEqual(["mof_a", "mof_relay"]);
      });

      test("applies the invoker preferences persisted on the instance", async () => {
        await seedAgentWithRelay([{ model: "opus" }]);
        const result = await resolveInstanceModelSources(h.db, "tnt_root", {
          agentId: "agt_1",
          modelPreferences: [
            { model: "opus", providers: { mode: "pin", order: ["relay"] } },
          ],
        });
        expect(result.ok).toBe(true);
        if (!result.ok) return;
        // The invoker pin restricts to relay despite mof_a's better priority.
        expect(result.sources.map((s) => s.id)).toEqual(["mof_relay"]);
      });

      test("returns no_requirements when the agent has none", async () => {
        await seedAgentWithRelay(null);
        const result = await resolveInstanceModelSources(h.db, "tnt_root", {
          agentId: "agt_1",
          modelPreferences: null,
        });
        expect(result).toEqual({ ok: false, reason: "no_requirements" });
      });

      test("returns no_requirements when the agent is absent from the tenant", async () => {
        await seedAgentWithRelay([{ model: "opus" }]);
        const result = await resolveInstanceModelSources(h.db, "tnt_root", {
          agentId: "agt_missing",
          modelPreferences: null,
        });
        expect(result).toEqual({ ok: false, reason: "no_requirements" });
      });

      test("throws on malformed persisted modelPreferences", async () => {
        await seedAgentWithRelay([{ model: "opus" }]);
        await expect(
          resolveInstanceModelSources(h.db, "tnt_root", {
            agentId: "agt_1",
            modelPreferences: [{ model: "opus", providers: { mode: "force" } }],
          }),
        ).rejects.toThrow();
      });
    });
  },
);
