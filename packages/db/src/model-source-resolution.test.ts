import { describe, expect, test } from "bun:test";

import type { ModelRequirement } from "@intx/types";

import type {
  ModelOfferingRow,
  ModelProviderRow,
  ModelRow,
} from "./catalog-resolution";
import type { credential } from "./schema/credentials";
import type { DB } from "./client";
import { resolveModelSources } from "./model-source-resolution";

type CredentialRow = typeof credential.$inferSelect;
type TenantRow = { id: string; parentId: string | null };

type DBState = {
  tenants: TenantRow[];
  models: ModelRow[];
  providers: ModelProviderRow[];
  offerings: ModelOfferingRow[];
  credentials: CredentialRow[];
};

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
      credential: finder(state.credentials),
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return mock as unknown as DB["db"];
}

function makeModel(
  o: Partial<ModelRow> & { id: string; canonicalName: string },
): ModelRow {
  return {
    tenantId: "tnt_root",
    displayName: null,
    description: null,
    disabled: false,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    ...o,
  };
}

function makeProvider(
  o: Partial<ModelProviderRow> & { id: string; name: string },
): ModelProviderRow {
  return {
    tenantId: "tnt_root",
    plugin: "anthropic",
    baseURL: "https://api.anthropic.com",
    credentialId: "cred_x",
    walletId: null,
    disabled: false,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    ...o,
  };
}

function makeOffering(
  o: Partial<ModelOfferingRow> & {
    id: string;
    modelId: string;
    providerId: string;
  },
): ModelOfferingRow {
  return {
    tenantId: "tnt_root",
    priority: 0,
    deploymentTags: [],
    capabilities: [],
    disabled: false,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    ...o,
  };
}

function makeCredential(
  o: Partial<CredentialRow> & { id: string; secret: string },
): CredentialRow {
  return {
    tenantId: "tnt_root",
    principalId: null,
    providerId: "prv_x",
    oauthClientId: null,
    name: "cred",
    type: "api_key",
    description: null,
    refreshSecret: null,
    scopes: null,
    expiresAt: null,
    status: "active",
    metadata: null,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    ...o,
  };
}

const ROOT_ONLY: TenantRow[] = [{ id: "tnt_root", parentId: null }];

/** A single credential-backed offering for model "opus" via "anthropic". */
function baseState(overrides?: {
  offering?: Partial<ModelOfferingRow>;
  provider?: Partial<ModelProviderRow>;
}): DBState {
  return {
    tenants: ROOT_ONLY,
    models: [makeModel({ id: "mdl_opus", canonicalName: "opus" })],
    providers: [
      makeProvider({
        id: "mpv_anthropic",
        name: "anthropic",
        credentialId: "cred_a",
        ...overrides?.provider,
      }),
    ],
    offerings: [
      makeOffering({
        id: "mof_a",
        modelId: "mdl_opus",
        providerId: "mpv_anthropic",
        ...overrides?.offering,
      }),
    ],
    credentials: [makeCredential({ id: "cred_a", secret: "sk-anthropic" })],
  };
}

const REQ_OPUS: ModelRequirement[] = [{ model: "opus" }];

describe("resolveModelSources", () => {
  test("returns no_requirements for an empty requirement list", async () => {
    const result = await resolveModelSources(
      makeMockDB(baseState()),
      "tnt_root",
      [],
    );
    expect(result).toEqual({ ok: false, reason: "no_requirements" });
  });

  test("builds a credential-backed source from the catalog", async () => {
    const result = await resolveModelSources(
      makeMockDB(baseState()),
      "tnt_root",
      REQ_OPUS,
    );
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.sources).toEqual([
      {
        id: "mof_a",
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "sk-anthropic",
        model: "opus",
        capabilities: [],
      },
    ]);
  });

  test("orders sources by ascending priority", async () => {
    const state = baseState();
    state.providers.push(
      makeProvider({
        id: "mpv_relay",
        name: "relay",
        credentialId: "cred_r",
      }),
    );
    state.offerings.push(
      makeOffering({
        id: "mof_relay",
        modelId: "mdl_opus",
        providerId: "mpv_relay",
        priority: 5,
      }),
    );
    state.credentials.push(
      makeCredential({ id: "cred_r", secret: "sk-relay" }),
    );

    const result = await resolveModelSources(
      makeMockDB(state),
      "tnt_root",
      REQ_OPUS,
    );
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.sources.map((s) => s.id)).toEqual(["mof_a", "mof_relay"]);
  });

  test("filters offerings by required capability", async () => {
    const state = baseState({ offering: { capabilities: ["vision"] } });
    const withCap = await resolveModelSources(makeMockDB(state), "tnt_root", [
      { model: "opus", capabilities: ["vision"] },
    ]);
    expect(withCap.ok).toBe(true);

    const missingCap = await resolveModelSources(
      makeMockDB(baseState()),
      "tnt_root",
      [{ model: "opus", capabilities: ["vision"] }],
    );
    expect(missingCap).toMatchObject({
      ok: false,
      reason: "model_unavailable",
    });
  });

  test("hard-pin restricts to the named providers in order", async () => {
    const state = baseState();
    state.providers.push(
      makeProvider({ id: "mpv_relay", name: "relay", credentialId: "cred_r" }),
    );
    state.offerings.push(
      makeOffering({
        id: "mof_relay",
        modelId: "mdl_opus",
        providerId: "mpv_relay",
      }),
    );
    state.credentials.push(
      makeCredential({ id: "cred_r", secret: "sk-relay" }),
    );

    const result = await resolveModelSources(makeMockDB(state), "tnt_root", [
      { model: "opus", providers: { mode: "pin", order: ["relay"] } },
    ]);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.sources.map((s) => s.id)).toEqual(["mof_relay"]);
  });

  test("soft-prefer fronts the named provider and keeps the rest", async () => {
    const state = baseState({ offering: { priority: 1 } });
    state.providers.push(
      makeProvider({ id: "mpv_relay", name: "relay", credentialId: "cred_r" }),
    );
    state.offerings.push(
      makeOffering({
        id: "mof_relay",
        modelId: "mdl_opus",
        providerId: "mpv_relay",
        priority: 0,
      }),
    );
    state.credentials.push(
      makeCredential({ id: "cred_r", secret: "sk-relay" }),
    );

    // relay has the better catalog priority, but the creator prefers anthropic.
    const result = await resolveModelSources(makeMockDB(state), "tnt_root", [
      { model: "opus", providers: { mode: "prefer", order: ["anthropic"] } },
    ]);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.sources.map((s) => s.id)).toEqual(["mof_a", "mof_relay"]);
  });

  test("a wallet-backed provider is skipped, leaving the model unavailable when it is the only one", async () => {
    const state = baseState({
      provider: { credentialId: null, walletId: "wal_1" },
    });
    const result = await resolveModelSources(
      makeMockDB(state),
      "tnt_root",
      REQ_OPUS,
    );
    expect(result).toMatchObject({
      ok: false,
      reason: "model_unavailable",
      model: "opus",
      skips: [{ reason: "wallet_backed", provider: "anthropic" }],
    });
  });

  test("an unresolvable credential skips the offering", async () => {
    const state = baseState();
    state.credentials = []; // the referenced credential does not exist
    const result = await resolveModelSources(
      makeMockDB(state),
      "tnt_root",
      REQ_OPUS,
    );
    expect(result).toMatchObject({
      ok: false,
      reason: "model_unavailable",
      skips: [{ reason: "credential_unresolved", provider: "anthropic" }],
    });
  });

  test("refuses a credential on a tenant outside the ancestor chain", async () => {
    const state = baseState();
    // The credential exists, but on a sibling tenant that is not in the
    // resolving tenant's ancestor chain. resolveCredentialById must refuse
    // it (INTR-203: the chain is the scoping authority), so its secret is
    // never emitted.
    state.credentials = [
      makeCredential({
        id: "cred_a",
        secret: "sk-sibling",
        tenantId: "tnt_sibling",
      }),
    ];
    const result = await resolveModelSources(
      makeMockDB(state),
      "tnt_root",
      REQ_OPUS,
    );
    expect(result).toMatchObject({
      ok: false,
      reason: "model_unavailable",
      skips: [{ reason: "credential_unresolved", provider: "anthropic" }],
    });
    expect(JSON.stringify(result)).not.toContain("sk-sibling");
  });

  test("invoker preference reorders after the creator preference", async () => {
    const state = baseState();
    state.providers.push(
      makeProvider({ id: "mpv_relay", name: "relay", credentialId: "cred_r" }),
    );
    state.offerings.push(
      makeOffering({
        id: "mof_relay",
        modelId: "mdl_opus",
        providerId: "mpv_relay",
      }),
    );
    state.credentials.push(
      makeCredential({ id: "cred_r", secret: "sk-relay" }),
    );

    const result = await resolveModelSources(
      makeMockDB(state),
      "tnt_root",
      [{ model: "opus", providers: { mode: "prefer", order: ["anthropic"] } }],
      { invokerPreferences: { opus: { mode: "pin", order: ["relay"] } } },
    );
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    // The invoker pins relay, overriding the creator's anthropic preference.
    expect(result.sources.map((s) => s.id)).toEqual(["mof_relay"]);
  });
});
