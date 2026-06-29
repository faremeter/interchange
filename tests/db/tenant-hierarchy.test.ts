import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";
import { eq } from "drizzle-orm";

import { getAncestorChain, getDescendantTenants } from "@intx/db";
import { tenant } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants } from "@intx/test-harness/seed";

describe.skipIf(!harnessDbEnvAvailable())("tenant-hierarchy (real DB)", () => {
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

  describe("getDescendantTenants", () => {
    test("returns just the tenant when it has no children", async () => {
      await seedTenants(h.db, [{ id: "root" }]);
      expect(await getDescendantTenants(h.db, "root")).toEqual(["root"]);
    });

    test("collects every tenant across multiple levels, breadth-first", async () => {
      // root -> {a, b}; a -> {a1, a2}; b -> {b1}; a1 -> {a1x}. A separate
      // `elsewhere -> unrelated` subtree exists so the exclusion of a
      // disjoint subtree is exercised; the real parent FK requires that
      // disjoint parent to exist rather than dangle.
      await seedTenants(h.db, [
        { id: "root" },
        { id: "a", parentId: "root" },
        { id: "b", parentId: "root" },
        { id: "a1", parentId: "a" },
        { id: "a2", parentId: "a" },
        { id: "b1", parentId: "b" },
        { id: "a1x", parentId: "a1" },
        { id: "elsewhere" },
        { id: "unrelated", parentId: "elsewhere" },
      ]);
      const result = await getDescendantTenants(h.db, "root");
      expect(new Set(result)).toEqual(
        new Set(["root", "a", "b", "a1", "a2", "b1", "a1x"]),
      );
      expect(result).not.toContain("unrelated");
      expect(result).not.toContain("elsewhere");
    });

    test("returns only the subtree rooted at a non-root tenant", async () => {
      await seedTenants(h.db, [
        { id: "root" },
        { id: "a", parentId: "root" },
        { id: "b", parentId: "root" },
        { id: "a1", parentId: "a" },
      ]);
      expect(new Set(await getDescendantTenants(h.db, "a"))).toEqual(
        new Set(["a", "a1"]),
      );
    });

    test("returns a deep linear chain in full without a depth cap", async () => {
      const chain: { id: string; parentId: string | null }[] = [
        { id: "n0", parentId: null },
      ];
      for (let i = 1; i <= 50; i++) {
        chain.push({ id: `n${i}`, parentId: `n${i - 1}` });
      }
      await seedTenants(h.db, chain);
      const result = await getDescendantTenants(h.db, "n0");
      expect(result).toHaveLength(51);
      expect(new Set(result)).toEqual(new Set(chain.map((t) => t.id)));
    });

    test("terminates and dedups under a cyclic hierarchy", async () => {
      // A two-node cycle (a -> b -> a) cannot exist under the immediate,
      // non-deferrable self-FK: neither row can be inserted first. A
      // single-node self-cycle is constructible (insert with a null
      // parent, then point the row at itself) and still drives the
      // `visited` set's termination and dedup guard, which is the
      // behaviour under test.
      await seedTenants(h.db, [{ id: "a" }]);
      await h.db
        .update(tenant)
        .set({ parentId: "a" })
        .where(eq(tenant.id, "a"));
      const result = await getDescendantTenants(h.db, "a");
      expect(result).toEqual(["a"]);
    });
  });

  describe("getAncestorChain", () => {
    test("walks from the tenant up to the root", async () => {
      await seedTenants(h.db, [
        { id: "root" },
        { id: "mid", parentId: "root" },
        { id: "leaf", parentId: "mid" },
      ]);
      expect(await getAncestorChain(h.db, "leaf")).toEqual([
        "leaf",
        "mid",
        "root",
      ]);
    });

    test("returns just the tenant when it is itself a root", async () => {
      await seedTenants(h.db, [{ id: "root" }]);
      expect(await getAncestorChain(h.db, "root")).toEqual(["root"]);
    });

    test("returns just the id when the tenant row is absent", async () => {
      // currentId has no row, so findFirst yields undefined and the walk
      // terminates immediately rather than looping or throwing.
      await seedTenants(h.db, [{ id: "root" }]);
      expect(await getAncestorChain(h.db, "ghost")).toEqual(["ghost"]);
    });

    test("caps traversal at MAX_DEPTH", async () => {
      // 30-link chain: tnt_0 (leaf) -> tnt_1 -> ... -> tnt_29 (root). The
      // walk caps at MAX_DEPTH=20 hops, so it yields tnt_0..tnt_20 (21
      // ids) and never reaches the deeper ancestors.
      const chain: { id: string; parentId: string | null }[] = [];
      for (let i = 0; i < 30; i++) {
        chain.push({
          id: `tnt_${i}`,
          parentId: i === 29 ? null : `tnt_${i + 1}`,
        });
      }
      await seedTenants(h.db, chain);
      const result = await getAncestorChain(h.db, "tnt_0");
      expect(result).toHaveLength(21);
      expect(result[20]).toBe("tnt_20");
      expect(result).not.toContain("tnt_21");
    });
  });
});
