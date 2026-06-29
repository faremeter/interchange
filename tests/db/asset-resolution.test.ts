import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  listAssetsForTenant,
  resolveAssetById,
  resolveAssetByName,
} from "@intx/db";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedTenants } from "@intx/test-harness/seed";

describe.skipIf(!harnessDbEnvAvailable())("asset-resolution (real DB)", () => {
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

  describe("resolveAssetByName", () => {
    test("returns null when the chain has no matching asset", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      const got = await resolveAssetByName(h.db, "tnt_leaf", "skill", "greet");
      expect(got).toBeNull();
    });

    test("returns the asset declared on the input tenant", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedAsset(h.db, {
        id: "ast_1",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "greet",
      });
      const got = await resolveAssetByName(h.db, "tnt_leaf", "skill", "greet");
      expect(got?.id).toBe("ast_1");
    });

    test("returns the ancestor asset when the input tenant has none", async () => {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_leaf", parentId: "tnt_root" },
      ]);
      await seedAsset(h.db, {
        id: "ast_root",
        tenantId: "tnt_root",
        kind: "skill",
        name: "greet",
      });
      const got = await resolveAssetByName(h.db, "tnt_leaf", "skill", "greet");
      expect(got?.id).toBe("ast_root");
    });

    test("child shadows ancestor when both declare the same (kind, name)", async () => {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_leaf", parentId: "tnt_root" },
      ]);
      await seedAsset(h.db, {
        id: "ast_root",
        tenantId: "tnt_root",
        kind: "skill",
        name: "greet",
      });
      await seedAsset(h.db, {
        id: "ast_leaf",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "greet",
      });
      const got = await resolveAssetByName(h.db, "tnt_leaf", "skill", "greet");
      expect(got?.id).toBe("ast_leaf");
    });

    test("filters by kind so distinct kinds with the same name do not collide", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedAsset(h.db, {
        id: "ast_skill",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "shared",
      });
      await seedAsset(h.db, {
        id: "ast_pkg",
        tenantId: "tnt_leaf",
        kind: "package-registry",
        name: "shared",
      });
      const skill = await resolveAssetByName(
        h.db,
        "tnt_leaf",
        "skill",
        "shared",
      );
      const pkg = await resolveAssetByName(
        h.db,
        "tnt_leaf",
        "package-registry",
        "shared",
      );
      expect(skill?.id).toBe("ast_skill");
      expect(pkg?.id).toBe("ast_pkg");
    });
  });

  describe("resolveAssetById", () => {
    test("returns the asset when it belongs to the input tenant", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedAsset(h.db, {
        id: "ast_1",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "greet",
      });
      const got = await resolveAssetById(h.db, "tnt_leaf", "ast_1");
      expect(got?.id).toBe("ast_1");
    });

    test("returns the asset when it belongs to an ancestor tenant", async () => {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_leaf", parentId: "tnt_root" },
      ]);
      await seedAsset(h.db, {
        id: "ast_root",
        tenantId: "tnt_root",
        kind: "skill",
        name: "greet",
      });
      const got = await resolveAssetById(h.db, "tnt_leaf", "ast_root");
      expect(got?.id).toBe("ast_root");
    });

    test("returns null when the asset belongs to a sibling tenant", async () => {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_a", parentId: "tnt_root" },
        { id: "tnt_b", parentId: "tnt_root" },
      ]);
      await seedAsset(h.db, {
        id: "ast_b",
        tenantId: "tnt_b",
        kind: "skill",
        name: "greet",
      });
      const got = await resolveAssetById(h.db, "tnt_a", "ast_b");
      expect(got).toBeNull();
    });

    test("returns null when the asset does not exist", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      const got = await resolveAssetById(h.db, "tnt_leaf", "ast_missing");
      expect(got).toBeNull();
    });
  });

  describe("listAssetsForTenant", () => {
    test("returns an empty list when the chain has no assets", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      const got = await listAssetsForTenant(h.db, "tnt_leaf");
      expect(got).toEqual([]);
    });

    test("tags direct assets with origin.direct = true", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedAsset(h.db, {
        id: "ast_1",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "greet",
      });
      const [only] = await listAssetsForTenant(h.db, "tnt_leaf");
      expect(only?.id).toBe("ast_1");
      expect(only?.origin).toEqual({ tenantId: "tnt_leaf", direct: true });
    });

    test("tags inherited assets with the ancestor tenant id and direct = false", async () => {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_leaf", parentId: "tnt_root" },
      ]);
      await seedAsset(h.db, {
        id: "ast_root",
        tenantId: "tnt_root",
        kind: "skill",
        name: "greet",
      });
      const [only] = await listAssetsForTenant(h.db, "tnt_leaf");
      expect(only?.id).toBe("ast_root");
      expect(only?.origin).toEqual({ tenantId: "tnt_root", direct: false });
    });

    test("child overrides parent which overrides grandparent for matching (kind, name)", async () => {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_mid", parentId: "tnt_root" },
        { id: "tnt_leaf", parentId: "tnt_mid" },
      ]);
      await seedAsset(h.db, {
        id: "ast_root",
        tenantId: "tnt_root",
        kind: "skill",
        name: "greet",
      });
      await seedAsset(h.db, {
        id: "ast_mid",
        tenantId: "tnt_mid",
        kind: "skill",
        name: "greet",
      });
      await seedAsset(h.db, {
        id: "ast_leaf",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "greet",
      });
      const list = await listAssetsForTenant(h.db, "tnt_leaf");
      expect(list).toHaveLength(1);
      expect(list[0]?.id).toBe("ast_leaf");
      expect(list[0]?.origin).toEqual({ tenantId: "tnt_leaf", direct: true });

      const fromMid = await listAssetsForTenant(h.db, "tnt_mid");
      expect(fromMid).toHaveLength(1);
      expect(fromMid[0]?.id).toBe("ast_mid");
      expect(fromMid[0]?.origin).toEqual({ tenantId: "tnt_mid", direct: true });
    });

    test("inner package-registry shadows outer when both share the same name", async () => {
      // Two `package-registry` assets at different tenancy levels with
      // the same name resolve to the inner one. The session service's
      // resolver walks `listAssetsForTenant(..., "package-registry")`
      // and trusts the dedup, so a regression here would let an outer
      // registry leak into the inner tenant's resolver map.
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_leaf", parentId: "tnt_root" },
      ]);
      await seedAsset(h.db, {
        id: "ast_pkg_outer",
        tenantId: "tnt_root",
        kind: "package-registry",
        name: "workspace-builtins",
      });
      await seedAsset(h.db, {
        id: "ast_pkg_inner",
        tenantId: "tnt_leaf",
        kind: "package-registry",
        name: "workspace-builtins",
      });
      const list = await listAssetsForTenant(
        h.db,
        "tnt_leaf",
        "package-registry",
      );
      expect(list).toHaveLength(1);
      expect(list[0]?.id).toBe("ast_pkg_inner");
      expect(list[0]?.origin).toEqual({ tenantId: "tnt_leaf", direct: true });
    });

    test("filters by kind when supplied", async () => {
      await seedTenants(h.db, [{ id: "tnt_leaf" }]);
      await seedAsset(h.db, {
        id: "ast_skill",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "greet",
      });
      await seedAsset(h.db, {
        id: "ast_pkg",
        tenantId: "tnt_leaf",
        kind: "package-registry",
        name: "registry",
      });
      const onlyPkg = await listAssetsForTenant(
        h.db,
        "tnt_leaf",
        "package-registry",
      );
      expect(onlyPkg).toHaveLength(1);
      expect(onlyPkg[0]?.id).toBe("ast_pkg");
    });

    test("merges distinct (kind, name) pairs across the chain", async () => {
      await seedTenants(h.db, [
        { id: "tnt_root" },
        { id: "tnt_leaf", parentId: "tnt_root" },
      ]);
      await seedAsset(h.db, {
        id: "ast_root",
        tenantId: "tnt_root",
        kind: "skill",
        name: "greet",
      });
      await seedAsset(h.db, {
        id: "ast_leaf",
        tenantId: "tnt_leaf",
        kind: "skill",
        name: "search",
      });
      const list = await listAssetsForTenant(h.db, "tnt_leaf");
      expect(list).toHaveLength(2);
      const byId = new Map(list.map((row) => [row.id, row]));
      expect(byId.get("ast_leaf")?.origin).toEqual({
        tenantId: "tnt_leaf",
        direct: true,
      });
      expect(byId.get("ast_root")?.origin).toEqual({
        tenantId: "tnt_root",
        direct: false,
      });
    });
  });

  describe("ancestor-chain depth guard", () => {
    test("walker terminates when the chain exceeds the MAX_DEPTH guard", async () => {
      // Build a 30-link chain: tnt_0 -> tnt_1 -> ... -> tnt_29, where
      // tnt_29 is the root. getAncestorChain caps traversal at
      // MAX_DEPTH=20, so resolving from tnt_0 cannot reach an asset
      // parked at tnt_25; the walker must terminate and return null
      // rather than looping or throwing.
      const tenants: { id: string; parentId: string | null }[] = [];
      for (let i = 0; i < 30; i++) {
        tenants.push({
          id: `tnt_${i}`,
          parentId: i === 29 ? null : `tnt_${i + 1}`,
        });
      }
      await seedTenants(h.db, tenants);
      await seedAsset(h.db, {
        id: "ast_far",
        tenantId: "tnt_25",
        kind: "skill",
        name: "greet",
      });
      const got = await resolveAssetByName(h.db, "tnt_0", "skill", "greet");
      expect(got).toBeNull();
    });
  });
});
