import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  listVisibleModels,
  listVisibleOfferings,
  listVisibleProviders,
} from "@intx/db";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedTenants,
  seedWallet,
} from "@intx/test-harness/seed";

describe.skipIf(!harnessDbEnvAvailable())(
  "catalog-resolution (real DB)",
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

    // A single wallet backs every provider in these tests; the catalog
    // resolvers never read the auth side, only the XOR constraint requires
    // one to be present.
    async function seedChainWithWallet(
      tenants: { id: string; parentId?: string | null }[],
      walletTenantId: string,
    ): Promise<void> {
      await seedTenants(h.db, tenants);
      await seedWallet(h.db, { id: "wal_1", tenantId: walletTenantId });
    }

    describe("listVisibleModels", () => {
      test("inherits an ancestor model the child does not redefine", async () => {
        await seedChainWithWallet(
          [{ id: "tnt_root" }, { id: "tnt_child", parentId: "tnt_root" }],
          "tnt_root",
        );
        await seedModel(h.db, {
          id: "mdl_r",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        const visible = await listVisibleModels(h.db, "tnt_child");
        expect(visible.map((m) => m.row.id)).toEqual(["mdl_r"]);
        expect(visible[0]?.origin).toEqual({
          tenantId: "tnt_root",
          direct: false,
        });
      });

      test("a child row shadows the ancestor with the same canonicalName", async () => {
        await seedChainWithWallet(
          [{ id: "tnt_root" }, { id: "tnt_child", parentId: "tnt_root" }],
          "tnt_root",
        );
        await seedModel(h.db, {
          id: "mdl_r",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModel(h.db, {
          id: "mdl_c",
          tenantId: "tnt_child",
          canonicalName: "opus",
        });
        const visible = await listVisibleModels(h.db, "tnt_child");
        expect(visible.map((m) => m.row.id)).toEqual(["mdl_c"]);
        expect(visible[0]?.origin.direct).toBe(true);
      });

      test("a disabled child row suppresses the inherited model", async () => {
        await seedChainWithWallet(
          [{ id: "tnt_root" }, { id: "tnt_child", parentId: "tnt_root" }],
          "tnt_root",
        );
        await seedModel(h.db, {
          id: "mdl_r",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModel(h.db, {
          id: "mdl_c",
          tenantId: "tnt_child",
          canonicalName: "opus",
          disabled: true,
        });
        const visible = await listVisibleModels(h.db, "tnt_child");
        expect(visible).toEqual([]);
      });
    });

    describe("listVisibleProviders", () => {
      test("a disabled child provider suppresses the inherited provider", async () => {
        await seedChainWithWallet(
          [{ id: "tnt_root" }, { id: "tnt_child", parentId: "tnt_root" }],
          "tnt_root",
        );
        await seedModelProvider(h.db, {
          id: "mpv_r",
          tenantId: "tnt_root",
          name: "anthropic",
          walletId: "wal_1",
        });
        await seedModelProvider(h.db, {
          id: "mpv_c",
          tenantId: "tnt_child",
          name: "anthropic",
          walletId: "wal_1",
          disabled: true,
        });
        const visible = await listVisibleProviders(h.db, "tnt_child");
        expect(visible).toEqual([]);
      });
    });

    describe("listVisibleOfferings", () => {
      async function seedBaseOffering(): Promise<void> {
        await seedChainWithWallet(
          [{ id: "tnt_root" }, { id: "tnt_child", parentId: "tnt_root" }],
          "tnt_root",
        );
        await seedModel(h.db, {
          id: "mdl_r",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModelProvider(h.db, {
          id: "mpv_r",
          tenantId: "tnt_root",
          name: "anthropic",
          walletId: "wal_1",
        });
        await seedModelOffering(h.db, {
          id: "mof_r",
          tenantId: "tnt_root",
          modelId: "mdl_r",
          providerId: "mpv_r",
        });
      }

      test("inherits an offering by (canonicalName, providerName)", async () => {
        await seedBaseOffering();
        const visible = await listVisibleOfferings(h.db, "tnt_child");
        expect(visible.map((o) => o.offering.id)).toEqual(["mof_r"]);
      });

      test("a child provider shadow routes the inherited offering through the child's provider config", async () => {
        await seedBaseOffering();
        await seedModelProvider(h.db, {
          id: "mpv_c",
          tenantId: "tnt_child",
          name: "anthropic",
          baseURL: "https://proxy.child.example",
          walletId: "wal_1",
        });
        const visible = await listVisibleOfferings(h.db, "tnt_child");
        expect(visible).toHaveLength(1);
        // The offering is inherited from root, but its provider config is the
        // child's shadow — shadowed configuration applies to inherited
        // offerings.
        expect(visible[0]?.provider.id).toBe("mpv_c");
        expect(visible[0]?.provider.baseURL).toBe(
          "https://proxy.child.example",
        );
      });

      test("disabling the provider cascades to its inherited offerings", async () => {
        await seedBaseOffering();
        await seedModelProvider(h.db, {
          id: "mpv_c",
          tenantId: "tnt_child",
          name: "anthropic",
          walletId: "wal_1",
          disabled: true,
        });
        const visible = await listVisibleOfferings(h.db, "tnt_child");
        expect(visible).toEqual([]);
      });

      test("disabling the model cascades to its inherited offerings", async () => {
        await seedBaseOffering();
        await seedModel(h.db, {
          id: "mdl_c",
          tenantId: "tnt_child",
          canonicalName: "opus",
          disabled: true,
        });
        const visible = await listVisibleOfferings(h.db, "tnt_child");
        expect(visible).toEqual([]);
      });

      test("a disabled child offering suppresses the inherited one", async () => {
        await seedBaseOffering();
        // The child localizes the model and provider (same names) so its
        // disable-offering row references its own rows, then disables the pair.
        await seedModel(h.db, {
          id: "mdl_c",
          tenantId: "tnt_child",
          canonicalName: "opus",
        });
        await seedModelProvider(h.db, {
          id: "mpv_c",
          tenantId: "tnt_child",
          name: "anthropic",
          walletId: "wal_1",
        });
        await seedModelOffering(h.db, {
          id: "mof_c",
          tenantId: "tnt_child",
          modelId: "mdl_c",
          providerId: "mpv_c",
          disabled: true,
        });
        const visible = await listVisibleOfferings(h.db, "tnt_child");
        expect(visible).toEqual([]);
      });

      test("a child model shadow routes the inherited offering through the child's model row", async () => {
        await seedBaseOffering();
        await seedModel(h.db, {
          id: "mdl_c",
          tenantId: "tnt_child",
          canonicalName: "opus",
          displayName: "Opus (child)",
        });
        const visible = await listVisibleOfferings(h.db, "tnt_child");
        expect(visible).toHaveLength(1);
        expect(visible[0]?.model.id).toBe("mdl_c");
        expect(visible[0]?.model.displayName).toBe("Opus (child)");
      });

      test("drops an offering whose model referent is not visible in the chain", async () => {
        // The offering's model lives on a sibling tenant outside the
        // resolving chain, so the dereference to a canonical identity finds
        // nothing and the offering is dropped. The real foreign key requires
        // the referent to exist somewhere, so it sits off-chain rather than
        // dangling.
        await seedChainWithWallet(
          [
            { id: "tnt_root" },
            { id: "tnt_child", parentId: "tnt_root" },
            { id: "tnt_sibling", parentId: "tnt_root" },
          ],
          "tnt_root",
        );
        await seedModel(h.db, {
          id: "mdl_r",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModelProvider(h.db, {
          id: "mpv_r",
          tenantId: "tnt_root",
          name: "anthropic",
          walletId: "wal_1",
        });
        await seedModelOffering(h.db, {
          id: "mof_r",
          tenantId: "tnt_root",
          modelId: "mdl_r",
          providerId: "mpv_r",
        });
        await seedModel(h.db, {
          id: "mdl_offchain",
          tenantId: "tnt_sibling",
          canonicalName: "ghost",
        });
        await seedModelOffering(h.db, {
          id: "mof_dangling",
          tenantId: "tnt_root",
          modelId: "mdl_offchain",
          providerId: "mpv_r",
        });
        const visible = await listVisibleOfferings(h.db, "tnt_child");
        expect(visible.map((o) => o.offering.id)).toEqual(["mof_r"]);
      });
    });

    describe("listVisibleOfferings across a three-level chain", () => {
      test("a mid-tenant provider shadow applies to a root offering for a leaf", async () => {
        await seedChainWithWallet(
          [
            { id: "tnt_root" },
            { id: "tnt_mid", parentId: "tnt_root" },
            { id: "tnt_leaf", parentId: "tnt_mid" },
          ],
          "tnt_root",
        );
        await seedModel(h.db, {
          id: "mdl_r",
          tenantId: "tnt_root",
          canonicalName: "opus",
        });
        await seedModelProvider(h.db, {
          id: "mpv_r",
          tenantId: "tnt_root",
          name: "anthropic",
          walletId: "wal_1",
        });
        await seedModelProvider(h.db, {
          id: "mpv_m",
          tenantId: "tnt_mid",
          name: "anthropic",
          baseURL: "https://proxy.mid.example",
          walletId: "wal_1",
        });
        await seedModelOffering(h.db, {
          id: "mof_r",
          tenantId: "tnt_root",
          modelId: "mdl_r",
          providerId: "mpv_r",
        });
        const visible = await listVisibleOfferings(h.db, "tnt_leaf");
        expect(visible).toHaveLength(1);
        expect(visible[0]?.provider.id).toBe("mpv_m");
        expect(visible[0]?.origin).toEqual({
          tenantId: "tnt_root",
          direct: false,
        });
      });
    });

    describe("offeringKey collision safety", () => {
      test("names that would collide under a space separator stay distinct", async () => {
        // ("a", "b c") and ("a b", "c") both render as "a b c" under a space
        // separator; the NUL separator keeps them distinct.
        await seedChainWithWallet([{ id: "tnt_root" }], "tnt_root");
        await seedModel(h.db, {
          id: "mdl_1",
          tenantId: "tnt_root",
          canonicalName: "a",
        });
        await seedModel(h.db, {
          id: "mdl_2",
          tenantId: "tnt_root",
          canonicalName: "a b",
        });
        await seedModelProvider(h.db, {
          id: "mpv_1",
          tenantId: "tnt_root",
          name: "b c",
          walletId: "wal_1",
        });
        await seedModelProvider(h.db, {
          id: "mpv_2",
          tenantId: "tnt_root",
          name: "c",
          walletId: "wal_1",
        });
        await seedModelOffering(h.db, {
          id: "mof_1",
          tenantId: "tnt_root",
          modelId: "mdl_1",
          providerId: "mpv_1",
        });
        await seedModelOffering(h.db, {
          id: "mof_2",
          tenantId: "tnt_root",
          modelId: "mdl_2",
          providerId: "mpv_2",
        });
        const visible = await listVisibleOfferings(h.db, "tnt_root");
        expect(visible.map((o) => o.offering.id).sort()).toEqual([
          "mof_1",
          "mof_2",
        ]);
      });
    });
  },
);
