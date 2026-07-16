import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createGrantStore,
  resolveInstanceModelSources,
  resolveModelSources,
} from "@intx/db";
import type { ModelRequirement } from "@intx/types";
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

// Credential resolution walks the tenant ancestor chain: a child tenant
// reaches a credential owned by a parent through inheritance. The
// credential-use grant check must widen to the same chain, because the owner's
// authorizing `credential:{id}` / `use` grant is stamped with the credential's
// own (parent) tenant, exactly as the mint path and the 0037 backfill produce
// it. These tests pin the multi-tenant contract on both secret-injecting
// paths — launch (resolveModelSources) and rotation/reconnect re-resolution
// (resolveInstanceModelSources) — against a parent tenant `tnt_root` and a
// child tenant `tnt_child` that inherits from it. The single-tenant suite in
// credential-use-enforcement.test.ts does not exercise inheritance, which is
// the gap that let the fail-closed asymmetry through.

const SECRET = "sk-parent-personal-credential";
const REQ_OPUS: ModelRequirement[] = [{ model: "opus" }];

describe.skipIf(!harnessDbEnvAvailable())(
  "credential-use enforcement across the tenant chain (real DB)",
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

    // Parent tenant `tnt_root` owns a personal credential (principalId set),
    // the credential-backed offering for model "opus", and the model catalog.
    // Child tenant `tnt_child` inherits all of it through the ancestor chain
    // and hosts an agent whose creator is the given principal. The credential's
    // owner is always `prn_owner` in the parent; the creator principal (which
    // may differ) and its tenant are supplied per case. The owner's
    // `credential:{id}` / `use` grant, when a case seeds one, is stamped with
    // the credential's tenant (tnt_root) — the mint/backfill behavior.
    async function seedInheritedCredential(opts: {
      creatorPrincipalId: string;
      creatorTenantId: string;
    }): Promise<void> {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_child", parentId: "tnt_root" },
      ]);
      await seedPrincipal(h.db, { id: "prn_owner", tenantId: "tnt_root" });
      if (opts.creatorPrincipalId !== "prn_owner") {
        await seedPrincipal(h.db, {
          id: opts.creatorPrincipalId,
          tenantId: opts.creatorTenantId,
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
        principalId: "prn_owner",
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
      await seedAgent(h.db, {
        id: "agt_1",
        tenantId: "tnt_child",
        creatorPrincipalId: opts.creatorPrincipalId,
        modelRequirements: [{ model: "opus" }],
      });
    }

    // POSITIVE (the fix): the credential owner holds a specifically-scoped
    // `credential:cred_a` / `use` grant in the PARENT tenant, and drives a
    // child-tenant agent. The child inherits the credential; the owner's grant
    // lives up the chain. Chain-aware collection must find it and emit the
    // secret. This case fails before the fix.
    describe("owner grant in the parent tenant (inherited credential)", () => {
      async function seedOwnerWithParentGrant(): Promise<void> {
        await seedInheritedCredential({
          creatorPrincipalId: "prn_owner",
          creatorTenantId: "tnt_root",
        });
        await seedGrant(h.db, {
          id: "grt_owner_use",
          tenantId: "tnt_root",
          principalId: "prn_owner",
          resource: "credential:cred_a",
          action: "use",
        });
      }

      test("launch resolution emits the inherited secret", async () => {
        await seedOwnerWithParentGrant();

        const creatorGrants = await createGrantStore(h.db).collectGrantsInChain(
          "prn_owner",
          "tnt_child",
        );
        const result = await resolveModelSources(
          h.db,
          "tnt_child",
          REQ_OPUS,
          creatorGrants,
        );

        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.apiKey)).toEqual([SECRET]);
      });

      test("rotation re-resolution emits the inherited secret", async () => {
        await seedOwnerWithParentGrant();

        const result = await resolveInstanceModelSources(h.db, "tnt_child", {
          agentId: "agt_1",
          modelPreferences: null,
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.apiKey)).toEqual([SECRET]);
      });
    });

    // REGRESSION GUARD: a child-tenant owner holding `*` / `*` in the CHILD
    // tenant uses the inherited credential. This works today (the grant lives
    // in the resolving tenant) and must keep working: chain-aware collection
    // still includes the acting tenant, so it must find the child's `*` / `*`.
    describe("child-tenant owner with wildcard grant in the child", () => {
      async function seedChildOwnerWithWildcard(): Promise<void> {
        await seedInheritedCredential({
          creatorPrincipalId: "prn_child_owner",
          creatorTenantId: "tnt_child",
        });
        await seedGrant(h.db, {
          id: "grt_child_wildcard",
          tenantId: "tnt_child",
          principalId: "prn_child_owner",
          resource: "*",
          action: "*",
        });
      }

      test("launch resolution emits the inherited secret", async () => {
        await seedChildOwnerWithWildcard();

        const creatorGrants = await createGrantStore(h.db).collectGrantsInChain(
          "prn_child_owner",
          "tnt_child",
        );
        const result = await resolveModelSources(
          h.db,
          "tnt_child",
          REQ_OPUS,
          creatorGrants,
        );

        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.apiKey)).toEqual([SECRET]);
      });

      test("rotation re-resolution emits the inherited secret", async () => {
        await seedChildOwnerWithWildcard();

        const result = await resolveInstanceModelSources(h.db, "tnt_child", {
          agentId: "agt_1",
          modelPreferences: null,
        });

        expect(result.ok).toBe(true);
        if (!result.ok) return;
        expect(result.sources.map((s) => s.apiKey)).toEqual([SECRET]);
      });
    });

    // NEGATIVE (no over-broadening): a child-tenant creator holding NO
    // `credential:cred_a` / `use` grant anywhere in the chain must be denied.
    // Widening collection to the chain must not turn into "everyone up the
    // chain can spend it" — a creator with no authorizing grant still fails
    // closed and never sees the secret.
    describe("child-tenant creator with no credential-use grant", () => {
      async function seedUnauthorizedCreator(): Promise<void> {
        await seedInheritedCredential({
          creatorPrincipalId: "prn_stranger",
          creatorTenantId: "tnt_child",
        });
      }

      test("launch resolution withholds the secret", async () => {
        await seedUnauthorizedCreator();

        const creatorGrants = await createGrantStore(h.db).collectGrantsInChain(
          "prn_stranger",
          "tnt_child",
        );
        const result = await resolveModelSources(
          h.db,
          "tnt_child",
          REQ_OPUS,
          creatorGrants,
        );

        expect(JSON.stringify(result)).not.toContain(SECRET);
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
          model: "opus",
          skips: [{ reason: "credential_unauthorized", provider: "anthropic" }],
        });
      });

      test("rotation re-resolution withholds the secret", async () => {
        await seedUnauthorizedCreator();

        const result = await resolveInstanceModelSources(h.db, "tnt_child", {
          agentId: "agt_1",
          modelPreferences: null,
        });

        expect(JSON.stringify(result)).not.toContain(SECRET);
        expect(result).toMatchObject({
          ok: false,
          reason: "model_unavailable",
          model: "opus",
          skips: [{ reason: "credential_unauthorized", provider: "anthropic" }],
        });
      });
    });
  },
);
