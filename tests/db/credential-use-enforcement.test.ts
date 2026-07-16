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
import type { GrantRule } from "@intx/types/authz";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAgent,
  seedCredential,
  seedGrant,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

// The catalog resolver dereferences a provider's credential reference to the
// secret and places it on a launchable source. That secret may only enter the
// source when the agent's creator holds `credential:{id}` / `use` for it. These
// tests pin the fail-closed contract on both paths that inject secrets: agent
// launch (resolveModelSources) and rotation/reconnect re-resolution
// (resolveInstanceModelSources). A non-owner creator lacking the grant must
// never see the secret in the resolved sources.

const REQ_OPUS: ModelRequirement[] = [{ model: "opus" }];
const SECRET = "sk-tenant-credential";

// A non-owner creator with no `credential:{id}` / `use` grant. The empty grant
// list is the vulnerable case: nothing authorizes credential use, so the
// resolver must withhold the secret.
const UNAUTHORIZED_CREATOR_GRANTS: GrantRule[] = [];

// A creator holding an explicit `credential:{id}` / `use` allow grant for the
// tenant credential — the authorized case.
const AUTHORIZED_CREATOR_GRANTS: GrantRule[] = [
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

describe.skipIf(!harnessDbEnvAvailable())(
  "credential-use enforcement (real DB)",
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

    // A single credential-backed offering for model "opus" through provider
    // "anthropic", carrying an organizational credential (principalId null) on
    // the resolving tenant. The credential's secret is what a built source
    // carries as its apiKey.
    async function seedTenantCredentialOffering(): Promise<void> {
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
        secret: SECRET,
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

    // Seed an agent whose creator's credential-use authorization the caller
    // controls via `authorized`. The credential-using offering is the same
    // organizational credential as the launch fixture.
    async function seedAgentWithCreator(authorized: boolean): Promise<void> {
      await seedTenantCredentialOffering();
      await seedPrincipal(h.db, {
        id: "prn_creator",
        tenantId: "tnt_root",
      });
      if (authorized) {
        await seedGrant(h.db, {
          id: "grt_creator_use",
          tenantId: "tnt_root",
          principalId: "prn_creator",
          resource: "credential:cred_a",
          action: "use",
        });
      }
      await seedAgent(h.db, {
        id: "agt_1",
        tenantId: "tnt_root",
        creatorPrincipalId: "prn_creator",
        modelRequirements: [{ model: "opus" }],
      });
    }

    describe("launch path (resolveModelSources)", () => {
      test("withholds the secret from a creator lacking credential/use", async () => {
        await seedTenantCredentialOffering();

        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          UNAUTHORIZED_CREATOR_GRANTS,
        );

        // The secret must not reach any launchable source, and the whole
        // resolution must not carry it anywhere.
        expect(JSON.stringify(result)).not.toContain(SECRET);
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
          model: "opus",
          skips: [{ reason: "credential_unauthorized", provider: "anthropic" }],
        });
      });

      test("emits the secret to a creator holding credential/use", async () => {
        await seedTenantCredentialOffering();

        const result = await resolveModelSources(
          h.db,
          "tnt_root",
          REQ_OPUS,
          AUTHORIZED_CREATOR_GRANTS,
        );

        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources).toEqual([
          {
            id: "mof_a",
            provider: "anthropic",
            baseURL: "https://api.anthropic.com",
            apiKey: SECRET,
            model: "opus",
            capabilities: [],
          },
        ]);
      });
    });

    describe("rotation path (resolveInstanceModelSources)", () => {
      test("withholds the secret when the instance creator lacks credential/use", async () => {
        await seedAgentWithCreator(false);

        const result = await resolveInstanceModelSources(h.db, "tnt_root", {
          agentId: "agt_1",
          modelPreferences: null,
        });

        // The rotation push ships resolution.sources to the running sidecar.
        // An unauthorized creator must never have the rotated secret pushed.
        expect(JSON.stringify(result)).not.toContain(SECRET);
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
          model: "opus",
          skips: [{ reason: "credential_unauthorized", provider: "anthropic" }],
        });
      });

      test("pushes the secret when the instance creator holds credential/use", async () => {
        await seedAgentWithCreator(true);

        const result = await resolveInstanceModelSources(h.db, "tnt_root", {
          agentId: "agt_1",
          modelPreferences: null,
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.apiKey)).toEqual([SECRET]);
      });
    });
  },
);
