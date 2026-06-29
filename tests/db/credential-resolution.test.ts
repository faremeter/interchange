import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  resolveCredentialById,
  resolveCredentialByName,
  resolveCredentialRequirement,
  resolveOAuthClient,
  resolveProviderByName,
} from "@intx/db";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedCredential,
  seedOAuthClient,
  seedPrincipal,
  seedProvider,
  seedTenants,
} from "@intx/test-harness/seed";

describe.skipIf(!harnessDbEnvAvailable())(
  "credential-resolution (real DB)",
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

    describe("resolveProviderByName", () => {
      test("returns the provider declared on the input tenant", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        const got = await resolveProviderByName(h.db, "tnt_leaf", "github");
        expect(got?.id).toBe("prv_1");
      });

      test("inherits an ancestor provider", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_leaf", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_root",
          tenantId: "tnt_root",
          name: "github",
        });
        const got = await resolveProviderByName(h.db, "tnt_leaf", "github");
        expect(got?.id).toBe("prv_root");
      });

      test("child shadows an ancestor provider with the same name", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_leaf", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_root",
          tenantId: "tnt_root",
          name: "github",
        });
        await seedProvider(h.db, {
          id: "prv_leaf",
          tenantId: "tnt_leaf",
          name: "github",
        });
        const got = await resolveProviderByName(h.db, "tnt_leaf", "github");
        expect(got?.id).toBe("prv_leaf");
      });

      test("returns null when no provider matches", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        const got = await resolveProviderByName(h.db, "tnt_leaf", "github");
        expect(got).toBeNull();
      });
    });

    describe("resolveOAuthClient", () => {
      test("returns the client declared on the input tenant", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedOAuthClient(h.db, {
          id: "oac_1",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
        });
        const got = await resolveOAuthClient(h.db, "tnt_leaf", "prv_1");
        expect(got?.id).toBe("oac_1");
      });

      test("inherits an ancestor client", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_leaf", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_root",
          tenantId: "tnt_root",
          name: "github",
        });
        await seedOAuthClient(h.db, {
          id: "oac_root",
          tenantId: "tnt_root",
          providerId: "prv_root",
        });
        const got = await resolveOAuthClient(h.db, "tnt_leaf", "prv_root");
        expect(got?.id).toBe("oac_root");
      });

      test("returns null when no client matches the provider", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        const got = await resolveOAuthClient(h.db, "tnt_leaf", "prv_1");
        expect(got).toBeNull();
      });
    });

    describe("resolveCredentialByName", () => {
      test("returns the credential declared on the input tenant", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_1",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "ci-token",
        });
        const got = await resolveCredentialByName(h.db, "tnt_leaf", "ci-token");
        expect(got?.id).toBe("cred_1");
      });

      test("inherits an ancestor credential", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_leaf", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_root",
          tenantId: "tnt_root",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_root",
          tenantId: "tnt_root",
          providerId: "prv_root",
          name: "ci-token",
        });
        const got = await resolveCredentialByName(h.db, "tnt_leaf", "ci-token");
        expect(got?.id).toBe("cred_root");
      });

      test("child shadows an ancestor credential with the same name", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_leaf", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_root",
          tenantId: "tnt_root",
          name: "github",
        });
        await seedProvider(h.db, {
          id: "prv_leaf",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_root",
          tenantId: "tnt_root",
          providerId: "prv_root",
          name: "ci-token",
        });
        await seedCredential(h.db, {
          id: "cred_leaf",
          tenantId: "tnt_leaf",
          providerId: "prv_leaf",
          name: "ci-token",
        });
        const got = await resolveCredentialByName(h.db, "tnt_leaf", "ci-token");
        expect(got?.id).toBe("cred_leaf");
      });

      test("returns null when no credential matches", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        const got = await resolveCredentialByName(h.db, "tnt_leaf", "ci-token");
        expect(got).toBeNull();
      });
    });

    describe("resolveCredentialById", () => {
      test("returns the credential when it belongs to the input tenant", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_1",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "ci-token",
        });
        const got = await resolveCredentialById(h.db, "tnt_leaf", "cred_1");
        expect(got?.id).toBe("cred_1");
      });

      test("returns the credential when it belongs to an ancestor", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_leaf", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_root",
          tenantId: "tnt_root",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_root",
          tenantId: "tnt_root",
          providerId: "prv_root",
          name: "ci-token",
        });
        const got = await resolveCredentialById(h.db, "tnt_leaf", "cred_root");
        expect(got?.id).toBe("cred_root");
      });

      test("returns null for a sibling-tenant credential", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_a", parentId: "tnt_root" },
          { id: "tnt_b", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_b",
          tenantId: "tnt_b",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_b",
          tenantId: "tnt_b",
          providerId: "prv_b",
          name: "ci-token",
        });
        const got = await resolveCredentialById(h.db, "tnt_a", "cred_b");
        expect(got).toBeNull();
      });

      test("returns null when the credential does not exist", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        const got = await resolveCredentialById(
          h.db,
          "tnt_leaf",
          "cred_missing",
        );
        expect(got).toBeNull();
      });
    });

    describe("resolveCredentialRequirement", () => {
      test("returns null when the provider is unknown", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "tenant" },
          null,
          null,
        );
        expect(got).toBeNull();
      });

      test("source tenant matches a tenant-level credential and ignores principal-scoped ones", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedPrincipal(h.db, { id: "prn_user", tenantId: "tnt_leaf" });
        await seedCredential(h.db, {
          id: "cred_tenant",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "tenant-cred",
          principalId: null,
        });
        await seedCredential(h.db, {
          id: "cred_principal",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "principal-cred",
          principalId: "prn_user",
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "tenant" },
          null,
          null,
        );
        expect(got?.id).toBe("cred_tenant");
      });

      test("source creator matches the creator's credential", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedPrincipal(h.db, { id: "prn_creator", tenantId: "tnt_leaf" });
        await seedPrincipal(h.db, { id: "prn_invoker", tenantId: "tnt_leaf" });
        await seedCredential(h.db, {
          id: "cred_creator",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "creator-cred",
          principalId: "prn_creator",
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "creator" },
          "prn_creator",
          "prn_invoker",
        );
        expect(got?.id).toBe("cred_creator");
      });

      test("source invoker matches the invoker's credential", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedPrincipal(h.db, { id: "prn_creator", tenantId: "tnt_leaf" });
        await seedPrincipal(h.db, { id: "prn_invoker", tenantId: "tnt_leaf" });
        await seedCredential(h.db, {
          id: "cred_invoker",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "invoker-cred",
          principalId: "prn_invoker",
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "invoker" },
          "prn_creator",
          "prn_invoker",
        );
        expect(got?.id).toBe("cred_invoker");
      });

      test("matches when the credential covers every required scope", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_1",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "scoped",
          principalId: null,
          scopes: ["repo:read", "repo:write"],
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "tenant", scopes: ["repo:read"] },
          null,
          null,
        );
        expect(got?.id).toBe("cred_1");
      });

      test("returns null when the credential lacks a required scope", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_1",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "scoped",
          principalId: null,
          scopes: ["repo:read"],
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "tenant", scopes: ["repo:admin"] },
          null,
          null,
        );
        expect(got).toBeNull();
      });

      test("ignores a non-active credential", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_revoked",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "revoked-cred",
          principalId: null,
          status: "revoked",
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "tenant" },
          null,
          null,
        );
        expect(got).toBeNull();
      });

      test("throws when more than one credential matches", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_a",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "cred-a",
          principalId: null,
        });
        await seedCredential(h.db, {
          id: "cred_b",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "cred-b",
          principalId: null,
        });
        await expect(
          resolveCredentialRequirement(
            h.db,
            "tnt_leaf",
            { providerName: "github", source: "tenant" },
            null,
            null,
          ),
        ).rejects.toThrow(/Ambiguous credential match/);
      });

      test("disambiguates by name when supplied", async () => {
        await seedTenants(h.db, [{ id: "tnt_leaf" }]);
        await seedProvider(h.db, {
          id: "prv_1",
          tenantId: "tnt_leaf",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_a",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "cred-a",
          principalId: null,
        });
        await seedCredential(h.db, {
          id: "cred_b",
          tenantId: "tnt_leaf",
          providerId: "prv_1",
          name: "cred-b",
          principalId: null,
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "tenant", name: "cred-b" },
          null,
          null,
        );
        expect(got?.id).toBe("cred_b");
      });

      test("inherits a matching credential from an ancestor tenant", async () => {
        await seedTenants(h.db, [
          { id: "tnt_root" },
          { id: "tnt_leaf", parentId: "tnt_root" },
        ]);
        await seedProvider(h.db, {
          id: "prv_root",
          tenantId: "tnt_root",
          name: "github",
        });
        await seedCredential(h.db, {
          id: "cred_root",
          tenantId: "tnt_root",
          providerId: "prv_root",
          name: "ci-token",
          principalId: null,
        });
        const got = await resolveCredentialRequirement(
          h.db,
          "tnt_leaf",
          { providerName: "github", source: "tenant" },
          null,
          null,
        );
        expect(got?.id).toBe("cred_root");
      });
    });
  },
);
