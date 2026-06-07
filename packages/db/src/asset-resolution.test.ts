import { describe, test, expect } from "bun:test";

import type { DB } from "./client";
import {
  resolveAssetByName,
  resolveAssetById,
  listAssetsForTenant,
  type AssetRow,
} from "./asset-resolution";

type TenantRow = {
  id: string;
  parentId: string | null;
};

type DBState = {
  tenants: TenantRow[];
  assets: AssetRow[];
};

const SQL_TO_JS: Record<string, string> = {
  id: "id",
  tenant_id: "tenantId",
  parent_id: "parentId",
  kind: "kind",
  name: "name",
};

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function getString(v: unknown, key: string): string | undefined {
  if (!isObject(v)) return undefined;
  const candidate = v[key];
  return typeof candidate === "string" ? candidate : undefined;
}

function getArray(v: unknown, key: string): unknown[] | undefined {
  if (!isObject(v)) return undefined;
  const candidate = v[key];
  return Array.isArray(candidate) ? candidate : undefined;
}

/**
 * Walks a drizzle `where` expression and extracts the `(column = value)`
 * bindings it imposes. Recognises `eq()` (which appears as a column
 * followed by ` = ` and a value chunk) and `and()` (which wraps nested
 * predicates in additional `queryChunks`). Sufficient for the queries
 * the asset walker actually issues; not a general-purpose drizzle
 * interpreter.
 */
function extractEqualities(
  predicate: unknown,
  into: Record<string, unknown>,
): void {
  const chunks = getArray(predicate, "queryChunks");
  if (chunks === undefined) return;
  for (let i = 0; i < chunks.length; i++) {
    const c = chunks[i];
    const colName = getString(c, "name");
    if (colName !== undefined) {
      const sepValue = getArray(chunks[i + 1], "value");
      if (sepValue !== undefined && sepValue[0] === " = ") {
        const valChunk = chunks[i + 2];
        if (isObject(valChunk) && "value" in valChunk) {
          const jsName = SQL_TO_JS[colName];
          if (jsName === undefined) {
            throw new Error(`unmapped SQL column in test mock: ${colName}`);
          }
          into[jsName] = valChunk["value"];
        }
      }
    } else if (getArray(c, "queryChunks") !== undefined) {
      extractEqualities(c, into);
    }
  }
}

function matches(row: object, filter: Record<string, unknown>): boolean {
  const entries = Object.entries(row);
  const rowMap = new Map<string, unknown>(entries);
  for (const [k, v] of Object.entries(filter)) {
    if (rowMap.get(k) !== v) return false;
  }
  return true;
}

function makeMockDB(state: DBState): DB["db"] {
  function tenantFindFirst(opts: {
    where?: unknown;
  }): Promise<TenantRow | undefined> {
    const filter: Record<string, unknown> = {};
    extractEqualities(opts.where, filter);
    return Promise.resolve(state.tenants.find((t) => matches(t, filter)));
  }
  function assetFindFirst(opts: {
    where?: unknown;
  }): Promise<AssetRow | undefined> {
    const filter: Record<string, unknown> = {};
    extractEqualities(opts.where, filter);
    return Promise.resolve(state.assets.find((a) => matches(a, filter)));
  }
  function assetFindMany(opts: { where?: unknown }): Promise<AssetRow[]> {
    const filter: Record<string, unknown> = {};
    extractEqualities(opts.where, filter);
    return Promise.resolve(state.assets.filter((a) => matches(a, filter)));
  }
  const mock = {
    query: {
      tenant: { findFirst: tenantFindFirst },
      asset: { findFirst: assetFindFirst, findMany: assetFindMany },
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

function makeAsset(
  overrides: Partial<AssetRow> & {
    id: string;
    tenantId: string;
    kind: string;
    name: string;
  },
): AssetRow {
  return {
    displayName: null,
    creatorPrincipalId: null,
    createdAt: new Date("2025-01-01"),
    updatedAt: new Date("2025-01-01"),
    ...overrides,
  };
}

describe("resolveAssetByName", () => {
  test("returns null when the chain has no matching asset", async () => {
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [],
    });
    const got = await resolveAssetByName(db, "tnt_leaf", "skill", "greet");
    expect(got).toBeNull();
  });

  test("returns the asset declared on the input tenant", async () => {
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [
        makeAsset({
          id: "ast_1",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const got = await resolveAssetByName(db, "tnt_leaf", "skill", "greet");
    expect(got?.id).toBe("ast_1");
  });

  test("returns the ancestor asset when the input tenant has none", async () => {
    const db = makeMockDB({
      tenants: [
        { id: "tnt_leaf", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_root",
          tenantId: "tnt_root",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const got = await resolveAssetByName(db, "tnt_leaf", "skill", "greet");
    expect(got?.id).toBe("ast_root");
  });

  test("child shadows ancestor when both declare the same (kind, name)", async () => {
    const db = makeMockDB({
      tenants: [
        { id: "tnt_leaf", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_root",
          tenantId: "tnt_root",
          kind: "skill",
          name: "greet",
        }),
        makeAsset({
          id: "ast_leaf",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const got = await resolveAssetByName(db, "tnt_leaf", "skill", "greet");
    expect(got?.id).toBe("ast_leaf");
  });

  test("filters by kind so distinct kinds with the same name do not collide", async () => {
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [
        makeAsset({
          id: "ast_skill",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "shared",
        }),
        makeAsset({
          id: "ast_pkg",
          tenantId: "tnt_leaf",
          kind: "package-registry",
          name: "shared",
        }),
      ],
    });
    const skill = await resolveAssetByName(db, "tnt_leaf", "skill", "shared");
    const pkg = await resolveAssetByName(
      db,
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
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [
        makeAsset({
          id: "ast_1",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const got = await resolveAssetById(db, "tnt_leaf", "ast_1");
    expect(got?.id).toBe("ast_1");
  });

  test("returns the asset when it belongs to an ancestor tenant", async () => {
    const db = makeMockDB({
      tenants: [
        { id: "tnt_leaf", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_root",
          tenantId: "tnt_root",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const got = await resolveAssetById(db, "tnt_leaf", "ast_root");
    expect(got?.id).toBe("ast_root");
  });

  test("returns null when the asset belongs to a sibling tenant", async () => {
    const db = makeMockDB({
      tenants: [
        { id: "tnt_a", parentId: "tnt_root" },
        { id: "tnt_b", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_b",
          tenantId: "tnt_b",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const got = await resolveAssetById(db, "tnt_a", "ast_b");
    expect(got).toBeNull();
  });

  test("returns null when the asset does not exist", async () => {
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [],
    });
    const got = await resolveAssetById(db, "tnt_leaf", "ast_missing");
    expect(got).toBeNull();
  });
});

describe("listAssetsForTenant", () => {
  test("returns an empty list when the chain has no assets", async () => {
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [],
    });
    const got = await listAssetsForTenant(db, "tnt_leaf");
    expect(got).toEqual([]);
  });

  test("tags direct assets with origin.direct = true", async () => {
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [
        makeAsset({
          id: "ast_1",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const [only] = await listAssetsForTenant(db, "tnt_leaf");
    expect(only?.id).toBe("ast_1");
    expect(only?.origin).toEqual({ tenantId: "tnt_leaf", direct: true });
  });

  test("tags inherited assets with the ancestor tenant id and direct = false", async () => {
    const db = makeMockDB({
      tenants: [
        { id: "tnt_leaf", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_root",
          tenantId: "tnt_root",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const [only] = await listAssetsForTenant(db, "tnt_leaf");
    expect(only?.id).toBe("ast_root");
    expect(only?.origin).toEqual({ tenantId: "tnt_root", direct: false });
  });

  test("child overrides parent which overrides grandparent for matching (kind, name)", async () => {
    const db = makeMockDB({
      tenants: [
        { id: "tnt_leaf", parentId: "tnt_mid" },
        { id: "tnt_mid", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_root",
          tenantId: "tnt_root",
          kind: "skill",
          name: "greet",
        }),
        makeAsset({
          id: "ast_mid",
          tenantId: "tnt_mid",
          kind: "skill",
          name: "greet",
        }),
        makeAsset({
          id: "ast_leaf",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const list = await listAssetsForTenant(db, "tnt_leaf");
    expect(list).toHaveLength(1);
    expect(list[0]?.id).toBe("ast_leaf");
    expect(list[0]?.origin).toEqual({ tenantId: "tnt_leaf", direct: true });

    const fromMid = await listAssetsForTenant(db, "tnt_mid");
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
    const db = makeMockDB({
      tenants: [
        { id: "tnt_leaf", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_pkg_outer",
          tenantId: "tnt_root",
          kind: "package-registry",
          name: "workspace-builtins",
        }),
        makeAsset({
          id: "ast_pkg_inner",
          tenantId: "tnt_leaf",
          kind: "package-registry",
          name: "workspace-builtins",
        }),
      ],
    });
    const list = await listAssetsForTenant(db, "tnt_leaf", "package-registry");
    expect(list).toHaveLength(1);
    expect(list[0]?.id).toBe("ast_pkg_inner");
    expect(list[0]?.origin).toEqual({ tenantId: "tnt_leaf", direct: true });
  });

  test("filters by kind when supplied", async () => {
    const db = makeMockDB({
      tenants: [{ id: "tnt_leaf", parentId: null }],
      assets: [
        makeAsset({
          id: "ast_skill",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "greet",
        }),
        makeAsset({
          id: "ast_pkg",
          tenantId: "tnt_leaf",
          kind: "package-registry",
          name: "registry",
        }),
      ],
    });
    const onlyPkg = await listAssetsForTenant(
      db,
      "tnt_leaf",
      "package-registry",
    );
    expect(onlyPkg).toHaveLength(1);
    expect(onlyPkg[0]?.id).toBe("ast_pkg");
  });

  test("merges distinct (kind, name) pairs across the chain", async () => {
    const db = makeMockDB({
      tenants: [
        { id: "tnt_leaf", parentId: "tnt_root" },
        { id: "tnt_root", parentId: null },
      ],
      assets: [
        makeAsset({
          id: "ast_root",
          tenantId: "tnt_root",
          kind: "skill",
          name: "greet",
        }),
        makeAsset({
          id: "ast_leaf",
          tenantId: "tnt_leaf",
          kind: "skill",
          name: "search",
        }),
      ],
    });
    const list = await listAssetsForTenant(db, "tnt_leaf");
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
    // Build a 30-link chain: tnt_0 -> tnt_1 -> ... -> tnt_29. The
    // getAncestorChain helper caps traversal at MAX_DEPTH=20, so the
    // walker must terminate without examining the deepest ancestors;
    // we exercise this by parking an asset at tnt_25 (out of reach)
    // and verifying resolveAssetByName returns null rather than
    // looping or throwing.
    const tenants: TenantRow[] = [];
    for (let i = 0; i < 30; i++) {
      tenants.push({
        id: `tnt_${i}`,
        parentId: i === 29 ? null : `tnt_${i + 1}`,
      });
    }
    const db = makeMockDB({
      tenants,
      assets: [
        makeAsset({
          id: "ast_far",
          tenantId: "tnt_25",
          kind: "skill",
          name: "greet",
        }),
      ],
    });
    const got = await resolveAssetByName(db, "tnt_0", "skill", "greet");
    expect(got).toBeNull();
  });
});
