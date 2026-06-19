import { describe, expect, test } from "bun:test";

import type { DB } from "./client";
import { getDescendantTenants } from "./tenant-hierarchy";

type TenantRow = { id: string; parentId: string | null };

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

/**
 * Pulls the bound parameter values out of an `inArray(parentId, frontier)`
 * expression. `getDescendantTenants` issues exactly that shape: the values
 * appear as a chunk that is an array of param objects, each carrying a
 * `.value`. Collecting every such value across the expression yields the
 * frontier the query filters on.
 */
function extractInArrayValues(predicate: unknown): string[] {
  const values: string[] = [];
  function walk(node: unknown): void {
    if (Array.isArray(node)) {
      for (const child of node) walk(child);
      return;
    }
    if (!isObject(node)) return;
    if ("value" in node && typeof node["value"] === "string") {
      values.push(node["value"]);
    }
    if (Array.isArray(node["queryChunks"])) walk(node["queryChunks"]);
  }
  walk(predicate);
  return values;
}

function makeMockDB(tenants: TenantRow[]): DB["db"] {
  const mock = {
    query: {
      tenant: {
        findMany(opts: { where?: unknown }): Promise<{ id: string }[]> {
          const parents = new Set(extractInArrayValues(opts.where));
          return Promise.resolve(
            tenants
              .filter((t) => t.parentId !== null && parents.has(t.parentId))
              .map((t) => ({ id: t.id })),
          );
        },
      },
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

describe("getDescendantTenants", () => {
  test("returns just the tenant when it has no children", async () => {
    const db = makeMockDB([{ id: "root", parentId: null }]);
    expect(await getDescendantTenants(db, "root")).toEqual(["root"]);
  });

  test("collects every tenant across multiple levels, breadth-first", async () => {
    // root -> {a, b}; a -> {a1, a2}; b -> {b1}; a1 -> {a1x}
    const db = makeMockDB([
      { id: "root", parentId: null },
      { id: "a", parentId: "root" },
      { id: "b", parentId: "root" },
      { id: "a1", parentId: "a" },
      { id: "a2", parentId: "a" },
      { id: "b1", parentId: "b" },
      { id: "a1x", parentId: "a1" },
      { id: "unrelated", parentId: "elsewhere" },
    ]);
    const result = await getDescendantTenants(db, "root");
    expect(new Set(result)).toEqual(
      new Set(["root", "a", "b", "a1", "a2", "b1", "a1x"]),
    );
    expect(result).not.toContain("unrelated");
  });

  test("returns only the subtree rooted at a non-root tenant", async () => {
    const db = makeMockDB([
      { id: "root", parentId: null },
      { id: "a", parentId: "root" },
      { id: "b", parentId: "root" },
      { id: "a1", parentId: "a" },
    ]);
    expect(new Set(await getDescendantTenants(db, "a"))).toEqual(
      new Set(["a", "a1"]),
    );
  });

  test("returns a deep linear chain in full without a depth cap", async () => {
    const chain: TenantRow[] = [{ id: "n0", parentId: null }];
    for (let i = 1; i <= 50; i++) {
      chain.push({ id: `n${i}`, parentId: `n${i - 1}` });
    }
    const db = makeMockDB(chain);
    const result = await getDescendantTenants(db, "n0");
    expect(result).toHaveLength(51);
    expect(new Set(result)).toEqual(new Set(chain.map((t) => t.id)));
  });

  test("terminates and dedups under a cyclic hierarchy", async () => {
    // a -> b -> a is structurally possible (the parent FK does not forbid
    // cycles); the visited set must drain the frontier and not duplicate.
    const db = makeMockDB([
      { id: "a", parentId: "b" },
      { id: "b", parentId: "a" },
    ]);
    const result = await getDescendantTenants(db, "a");
    expect(result).toEqual(["a", "b"]);
  });
});
