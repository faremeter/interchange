import { describe, expect, test } from "bun:test";

import type { DB } from "./client";
import {
  listVisibleModels,
  listVisibleOfferings,
  listVisibleProviders,
  type ModelOfferingRow,
  type ModelProviderRow,
  type ModelRow,
} from "./catalog-resolution";

type TenantRow = { id: string; parentId: string | null };

type DBState = {
  tenants: TenantRow[];
  models: ModelRow[];
  providers: ModelProviderRow[];
  offerings: ModelOfferingRow[];
};

// The catalog resolvers only ever filter on `tenant.id` and `*.tenant_id`,
// so the mock needs to understand those bindings and nothing more.
const SQL_TO_JS: Record<string, string> = {
  id: "id",
  tenant_id: "tenantId",
  parent_id: "parentId",
};

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function getArray(v: unknown, key: string): unknown[] | undefined {
  if (!isObject(v)) return undefined;
  const candidate = v[key];
  return Array.isArray(candidate) ? candidate : undefined;
}

function getString(v: unknown, key: string): string | undefined {
  if (!isObject(v)) return undefined;
  const candidate = v[key];
  return typeof candidate === "string" ? candidate : undefined;
}

/**
 * Extracts the `(column = value)` bindings a drizzle `where` expression
 * imposes. Recognises `eq()` and `and()`; sufficient for the single-column
 * equalities the catalog resolvers issue.
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
  const rowMap = new Map<string, unknown>(Object.entries(row));
  for (const [k, v] of Object.entries(filter)) {
    if (rowMap.get(k) !== v) return false;
  }
  return true;
}

function makeMockDB(state: DBState): DB["db"] {
  function finder<T extends object>(rows: T[]) {
    return {
      findFirst(opts: { where?: unknown }): Promise<T | undefined> {
        const filter: Record<string, unknown> = {};
        extractEqualities(opts.where, filter);
        return Promise.resolve(rows.find((r) => matches(r, filter)));
      },
      findMany(opts: { where?: unknown }): Promise<T[]> {
        const filter: Record<string, unknown> = {};
        extractEqualities(opts.where, filter);
        return Promise.resolve(rows.filter((r) => matches(r, filter)));
      },
    };
  }
  const mock = {
    query: {
      tenant: finder(state.tenants),
      model: finder(state.models),
      modelProvider: finder(state.providers),
      modelOffering: finder(state.offerings),
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

function makeModel(
  overrides: Partial<ModelRow> & {
    id: string;
    tenantId: string;
    canonicalName: string;
  },
): ModelRow {
  return {
    displayName: null,
    description: null,
    disabled: false,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    ...overrides,
  };
}

function makeProvider(
  overrides: Partial<ModelProviderRow> & {
    id: string;
    tenantId: string;
    name: string;
  },
): ModelProviderRow {
  return {
    plugin: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: "cred_x",
    walletId: null,
    disabled: false,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    ...overrides,
  };
}

function makeOffering(
  overrides: Partial<ModelOfferingRow> & {
    id: string;
    tenantId: string;
    modelId: string;
    providerId: string;
  },
): ModelOfferingRow {
  return {
    priority: 0,
    deploymentTags: [],
    capabilities: [],
    disabled: false,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    ...overrides,
  };
}

const CHAIN: TenantRow[] = [
  { id: "tnt_child", parentId: "tnt_root" },
  { id: "tnt_root", parentId: null },
];

describe("listVisibleModels", () => {
  test("inherits an ancestor model the child does not redefine", async () => {
    const db = makeMockDB({
      tenants: CHAIN,
      models: [
        makeModel({ id: "mdl_r", tenantId: "tnt_root", canonicalName: "opus" }),
      ],
      providers: [],
      offerings: [],
    });
    const visible = await listVisibleModels(db, "tnt_child");
    expect(visible.map((m) => m.row.id)).toEqual(["mdl_r"]);
    expect(visible[0]?.origin).toEqual({ tenantId: "tnt_root", direct: false });
  });

  test("a child row shadows the ancestor with the same canonicalName", async () => {
    const db = makeMockDB({
      tenants: CHAIN,
      models: [
        makeModel({ id: "mdl_r", tenantId: "tnt_root", canonicalName: "opus" }),
        makeModel({
          id: "mdl_c",
          tenantId: "tnt_child",
          canonicalName: "opus",
        }),
      ],
      providers: [],
      offerings: [],
    });
    const visible = await listVisibleModels(db, "tnt_child");
    expect(visible.map((m) => m.row.id)).toEqual(["mdl_c"]);
    expect(visible[0]?.origin.direct).toBe(true);
  });

  test("a disabled child row suppresses the inherited model", async () => {
    const db = makeMockDB({
      tenants: CHAIN,
      models: [
        makeModel({ id: "mdl_r", tenantId: "tnt_root", canonicalName: "opus" }),
        makeModel({
          id: "mdl_c",
          tenantId: "tnt_child",
          canonicalName: "opus",
          disabled: true,
        }),
      ],
      providers: [],
      offerings: [],
    });
    const visible = await listVisibleModels(db, "tnt_child");
    expect(visible).toEqual([]);
  });
});

describe("listVisibleProviders", () => {
  test("a disabled child provider suppresses the inherited provider", async () => {
    const db = makeMockDB({
      tenants: CHAIN,
      models: [],
      providers: [
        makeProvider({ id: "mpv_r", tenantId: "tnt_root", name: "anthropic" }),
        makeProvider({
          id: "mpv_c",
          tenantId: "tnt_child",
          name: "anthropic",
          disabled: true,
        }),
      ],
      offerings: [],
    });
    const visible = await listVisibleProviders(db, "tnt_child");
    expect(visible).toEqual([]);
  });
});

describe("listVisibleOfferings", () => {
  function seed(): DBState {
    return {
      tenants: CHAIN,
      models: [
        makeModel({ id: "mdl_r", tenantId: "tnt_root", canonicalName: "opus" }),
      ],
      providers: [
        makeProvider({ id: "mpv_r", tenantId: "tnt_root", name: "anthropic" }),
      ],
      offerings: [
        makeOffering({
          id: "mof_r",
          tenantId: "tnt_root",
          modelId: "mdl_r",
          providerId: "mpv_r",
        }),
      ],
    };
  }

  test("inherits an offering by (canonicalName, providerName)", async () => {
    const visible = await listVisibleOfferings(makeMockDB(seed()), "tnt_child");
    expect(visible.map((o) => o.offering.id)).toEqual(["mof_r"]);
  });

  test("a child provider shadow routes the inherited offering through the child's provider config", async () => {
    const state = seed();
    state.providers.push(
      makeProvider({
        id: "mpv_c",
        tenantId: "tnt_child",
        name: "anthropic",
        baseURL: "https://proxy.child.example",
        credentialId: "cred_child",
      }),
    );
    const visible = await listVisibleOfferings(makeMockDB(state), "tnt_child");
    expect(visible).toHaveLength(1);
    // The offering is inherited from root, but its provider config is the
    // child's shadow — shadowed configuration applies to inherited offerings.
    expect(visible[0]?.provider.id).toBe("mpv_c");
    expect(visible[0]?.provider.baseURL).toBe("https://proxy.child.example");
  });

  test("disabling the provider cascades to its inherited offerings", async () => {
    const state = seed();
    state.providers.push(
      makeProvider({
        id: "mpv_c",
        tenantId: "tnt_child",
        name: "anthropic",
        disabled: true,
      }),
    );
    const visible = await listVisibleOfferings(makeMockDB(state), "tnt_child");
    expect(visible).toEqual([]);
  });

  test("disabling the model cascades to its inherited offerings", async () => {
    const state = seed();
    state.models.push(
      makeModel({
        id: "mdl_c",
        tenantId: "tnt_child",
        canonicalName: "opus",
        disabled: true,
      }),
    );
    const visible = await listVisibleOfferings(makeMockDB(state), "tnt_child");
    expect(visible).toEqual([]);
  });

  test("a disabled child offering suppresses the inherited one", async () => {
    const state = seed();
    // The child localizes the model and provider (same names) so its
    // disable-offering row references its own rows, then disables the pair.
    state.models.push(
      makeModel({ id: "mdl_c", tenantId: "tnt_child", canonicalName: "opus" }),
    );
    state.providers.push(
      makeProvider({ id: "mpv_c", tenantId: "tnt_child", name: "anthropic" }),
    );
    state.offerings.push(
      makeOffering({
        id: "mof_c",
        tenantId: "tnt_child",
        modelId: "mdl_c",
        providerId: "mpv_c",
        disabled: true,
      }),
    );
    const visible = await listVisibleOfferings(makeMockDB(state), "tnt_child");
    expect(visible).toEqual([]);
  });

  test("a child model shadow routes the inherited offering through the child's model row", async () => {
    const state = seed();
    state.models.push(
      makeModel({
        id: "mdl_c",
        tenantId: "tnt_child",
        canonicalName: "opus",
        displayName: "Opus (child)",
      }),
    );
    const visible = await listVisibleOfferings(makeMockDB(state), "tnt_child");
    expect(visible).toHaveLength(1);
    expect(visible[0]?.model.id).toBe("mdl_c");
    expect(visible[0]?.model.displayName).toBe("Opus (child)");
  });

  test("drops an offering whose referent is absent from every tenant", async () => {
    const state = seed();
    state.offerings.push(
      makeOffering({
        id: "mof_dangling",
        tenantId: "tnt_root",
        modelId: "mdl_missing",
        providerId: "mpv_r",
      }),
    );
    const visible = await listVisibleOfferings(makeMockDB(state), "tnt_child");
    expect(visible.map((o) => o.offering.id)).toEqual(["mof_r"]);
  });
});

describe("listVisibleOfferings across a three-level chain", () => {
  const THREE: TenantRow[] = [
    { id: "tnt_leaf", parentId: "tnt_mid" },
    { id: "tnt_mid", parentId: "tnt_root" },
    { id: "tnt_root", parentId: null },
  ];

  test("a mid-tenant provider shadow applies to a root offering for a leaf", async () => {
    const db = makeMockDB({
      tenants: THREE,
      models: [
        makeModel({ id: "mdl_r", tenantId: "tnt_root", canonicalName: "opus" }),
      ],
      providers: [
        makeProvider({ id: "mpv_r", tenantId: "tnt_root", name: "anthropic" }),
        makeProvider({
          id: "mpv_m",
          tenantId: "tnt_mid",
          name: "anthropic",
          baseURL: "https://proxy.mid.example",
        }),
      ],
      offerings: [
        makeOffering({
          id: "mof_r",
          tenantId: "tnt_root",
          modelId: "mdl_r",
          providerId: "mpv_r",
        }),
      ],
    });
    const visible = await listVisibleOfferings(db, "tnt_leaf");
    expect(visible).toHaveLength(1);
    expect(visible[0]?.provider.id).toBe("mpv_m");
    expect(visible[0]?.origin).toEqual({ tenantId: "tnt_root", direct: false });
  });
});

describe("offeringKey collision safety", () => {
  test("names that would collide under a space separator stay distinct", async () => {
    // ("a", "b c") and ("a b", "c") both render as "a b c" under a space
    // separator; the NUL separator keeps them distinct.
    const db = makeMockDB({
      tenants: [{ id: "tnt_root", parentId: null }],
      models: [
        makeModel({ id: "mdl_1", tenantId: "tnt_root", canonicalName: "a" }),
        makeModel({ id: "mdl_2", tenantId: "tnt_root", canonicalName: "a b" }),
      ],
      providers: [
        makeProvider({ id: "mpv_1", tenantId: "tnt_root", name: "b c" }),
        makeProvider({ id: "mpv_2", tenantId: "tnt_root", name: "c" }),
      ],
      offerings: [
        makeOffering({
          id: "mof_1",
          tenantId: "tnt_root",
          modelId: "mdl_1",
          providerId: "mpv_1",
        }),
        makeOffering({
          id: "mof_2",
          tenantId: "tnt_root",
          modelId: "mdl_2",
          providerId: "mpv_2",
        }),
      ],
    });
    const visible = await listVisibleOfferings(db, "tnt_root");
    expect(visible.map((o) => o.offering.id).sort()).toEqual([
      "mof_1",
      "mof_2",
    ]);
  });
});
