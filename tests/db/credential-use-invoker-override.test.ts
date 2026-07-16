import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { resolveModelSources } from "@intx/db";
import type { ModelRequirement, ProviderPreference } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
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

// The invoker's launch-time provider preference reorders and restricts the
// tenant catalog, but it must not become a way to spend a credential the
// agent's creator is not authorized for. The credential-use gate keys on the
// creator's `credential:{id}` / `use` grant, and it sits below preference
// application in buildSource — so an invoker `pin` onto an offering the creator
// cannot use skips as `credential_unauthorized` rather than emitting the
// secret. This test pins that boundary: same model, two providers backed by
// distinct credentials, creator authorized for one credential only.

const REQ_OPUS: ModelRequirement[] = [{ model: "opus" }];
const SECRET_A = "sk-provider-a-credential";
const SECRET_B = "sk-provider-b-credential";

// The creator holds `credential:{id}` / `use` for provider A's credential
// only. Provider B's credential is deliberately absent from this grant list.
const CREATOR_GRANTS_A_ONLY: GrantRule[] = [
  {
    id: "grt_use_cred_a",
    resource: "credential:cred_a",
    action: "use",
    effect: "allow",
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: "prn_creator",
  },
];

function pin(provider: string): Record<string, ProviderPreference> {
  return { opus: { mode: "pin", order: [provider] } };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "credential-use enforcement under invoker override (real DB)",
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

    // Two catalog offerings for model "opus" from providers "provider-a" and
    // "provider-b", each backed by its own tenant credential. The creator is
    // authorized for provider A's credential (cred_a) only.
    async function seedTwoProviderCatalog(): Promise<void> {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await seedPrincipal(h.db, { id: "prn_creator", tenantId: "tnt_root" });
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
        secret: SECRET_A,
      });
      await seedCredential(h.db, {
        id: "cred_b",
        tenantId: "tnt_root",
        providerId: "prv_x",
        name: "cred-b",
        secret: SECRET_B,
      });
      await seedModel(h.db, {
        id: "mdl_opus",
        tenantId: "tnt_root",
        canonicalName: "opus",
      });
      await seedModelProvider(h.db, {
        id: "mpv_a",
        tenantId: "tnt_root",
        name: "provider-a",
        credentialId: "cred_a",
      });
      await seedModelProvider(h.db, {
        id: "mpv_b",
        tenantId: "tnt_root",
        name: "provider-b",
        credentialId: "cred_b",
      });
      await seedModelOffering(h.db, {
        id: "mof_a",
        tenantId: "tnt_root",
        modelId: "mdl_opus",
        providerId: "mpv_a",
      });
      await seedModelOffering(h.db, {
        id: "mof_b",
        tenantId: "tnt_root",
        modelId: "mdl_opus",
        providerId: "mpv_b",
      });
    }

    test("invoker pinning the authorized provider emits its secret", async () => {
      await seedTwoProviderCatalog();

      const result = await resolveModelSources(
        h.db,
        "tnt_root",
        REQ_OPUS,
        CREATOR_GRANTS_A_ONLY,
        { invokerPreferences: pin("provider-a") },
      );

      expect(result.ok).toBe(true);
      if (!result.ok) return;
      // The emitted source's `provider` is the offering's plugin (the runtime
      // adapter), not the catalog provider name the invoker pinned on; both
      // seeded providers default to the "anthropic" plugin. The load-bearing
      // assertion is that provider A's secret is what reaches the source.
      expect(result.sources).toEqual([
        {
          id: "mof_a",
          provider: "anthropic",
          baseURL: "https://api.anthropic.com",
          apiKey: SECRET_A,
          model: "opus",
          capabilities: [],
        },
      ]);
    });

    test("invoker cannot pin a creator-unauthorized provider to spend its credential", async () => {
      await seedTwoProviderCatalog();

      const result = await resolveModelSources(
        h.db,
        "tnt_root",
        REQ_OPUS,
        CREATOR_GRANTS_A_ONLY,
        { invokerPreferences: pin("provider-b") },
      );

      // The invoker restricted resolution to provider B, which the creator is
      // not authorized to spend. The offering is skipped as
      // credential_unauthorized rather than falling back to provider A, and
      // provider B's secret never reaches the resolution.
      expect(JSON.stringify(result)).not.toContain(SECRET_B);
      expect(result).toMatchObject({
        ok: false,
        reason: "model_unavailable",
        model: "opus",
        skips: [{ reason: "credential_unauthorized", provider: "provider-b" }],
      });
    });
  },
);
