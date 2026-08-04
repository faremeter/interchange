import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { resolveDefinitionSources } from "@intx/hub-api";
import type { ModelRequirement } from "@intx/types";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

// resolveDefinitionSources turns a definition's requirements into the ordered,
// credential-bearing source chain a run launches against. It resolves the
// requirements from the `modelRequirements` manifest when set, else derives
// them from a single step's declared model, and stamps a credential secret only
// where the launching tenant owns the credential within its hierarchy --
// fail-closed.

const SECRET = "sk-secret";

describe.skipIf(!harnessDbEnvAvailable())(
  "resolveDefinitionSources (real DB)",
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

    // A single credential-backed offering for model "opus". The credential is
    // tenant-owned by default (usable by ownership); a case can make it
    // principal-owned to exercise the ownership boundary.
    async function seedCatalog(
      opts: { credentialPrincipalId?: string } = {},
    ): Promise<void> {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      if (opts.credentialPrincipalId !== undefined) {
        await seedPrincipal(h.db, {
          id: opts.credentialPrincipalId,
          tenantId: "tnt_root",
        });
      }
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
        secret: SECRET,
        ...(opts.credentialPrincipalId !== undefined
          ? { principalId: opts.credentialPrincipalId }
          : {}),
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
    }

    function resolve(args: {
      modelRequirements: ModelRequirement[] | null;
      fallbackModel: string | null;
    }) {
      return resolveDefinitionSources({
        db: h.db,
        tenantId: "tnt_root",
        modelRequirements: args.modelRequirements,
        fallbackModel: args.fallbackModel,
        invokerPreferences: {},
      });
    }

    test("resolves the chain from the modelRequirements manifest", async () => {
      await seedCatalog();
      const res = await resolve({
        modelRequirements: [{ model: "opus" }],
        fallbackModel: null,
      });
      expect(res.ok).toBe(true);
      if (!res.ok) return;
      expect(res.sources.map((s) => s.apiKey)).toEqual([SECRET]);
      const [head] = res.sources;
      if (head === undefined) throw new Error("expected a source");
      expect(res.defaultSource).toBe(head.id);
    });

    test("derives the chain from the fallback model when no manifest is set", async () => {
      await seedCatalog();
      const res = await resolve({
        modelRequirements: null,
        fallbackModel: "opus",
      });
      expect(res.ok).toBe(true);
      if (!res.ok) return;
      expect(res.sources.map((s) => s.model)).toEqual(["opus"]);
      const [head] = res.sources;
      if (head === undefined) throw new Error("expected a source");
      expect(res.defaultSource).toBe(head.id);
    });

    test("fails when neither a manifest nor a fallback model is present", async () => {
      await seedCatalog();
      const res = await resolve({
        modelRequirements: null,
        fallbackModel: null,
      });
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.message).toContain("no model requirements");
    });

    test("withholds a principal-owned credential (not usable via the catalog)", async () => {
      await seedCatalog({ credentialPrincipalId: "prn_owner" });
      const res = await resolve({
        modelRequirements: [{ model: "opus" }],
        fallbackModel: null,
      });
      expect(res.ok).toBe(false);
      if (res.ok) return;
      expect(res.message).toContain("credential_unauthorized");
    });
  },
);
