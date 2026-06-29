// Fixture seeding helpers for the real-database resolution tests.
//
// These insert rows through the real drizzle client, so they honor the
// schema's NOT NULL / FK / unique / CHECK constraints — the things the
// old query-introspection mocks silently bypassed. Helpers fill
// required columns the resolvers never read with deterministic values
// derived from the row id, so a test only specifies the fields its
// assertions actually depend on.

import type { DB } from "@intx/db";
import {
  asset,
  credential,
  oauthClient,
  principal,
  provider,
  tenant,
} from "@intx/db/schema";

type Db = DB["db"];

export type SeedTenant = {
  id: string;
  parentId?: string | null;
};

/**
 * Insert a set of tenants honoring the immediate self-referential
 * `parent_id` FK: a row is inserted only once its parent already
 * exists, so callers can pass a tree in any order. `slug` and `domain`
 * are derived from the id to satisfy their NOT NULL + UNIQUE
 * constraints.
 */
export async function seedTenants(
  db: Db,
  tenants: SeedTenant[],
): Promise<void> {
  let remaining = [...tenants];
  const inserted = new Set<string>();
  while (remaining.length > 0) {
    const ready = remaining.filter(
      (t) =>
        t.parentId === undefined ||
        t.parentId === null ||
        inserted.has(t.parentId),
    );
    if (ready.length === 0) {
      throw new Error(
        `seedTenants: unresolvable parent references among ${remaining
          .map((t) => t.id)
          .join(", ")}`,
      );
    }
    for (const t of ready) {
      await db.insert(tenant).values({
        id: t.id,
        name: t.id,
        slug: t.id,
        domain: `${t.id}.example.test`,
        parentId: t.parentId ?? null,
      });
      inserted.add(t.id);
    }
    remaining = remaining.filter((t) => !inserted.has(t.id));
  }
}

export type SeedAsset = {
  id: string;
  tenantId: string;
  kind: string;
  name: string;
  displayName?: string | null;
};

export async function seedAsset(db: Db, a: SeedAsset): Promise<void> {
  await db.insert(asset).values({
    id: a.id,
    tenantId: a.tenantId,
    kind: a.kind,
    name: a.name,
    displayName: a.displayName ?? null,
    creatorPrincipalId: null,
  });
}

export type SeedPrincipal = {
  id: string;
  tenantId: string;
  kind?: "user" | "agent";
  refId?: string;
  status?: "active" | "suspended" | "invited" | "deactivated";
};

export async function seedPrincipal(db: Db, p: SeedPrincipal): Promise<void> {
  await db.insert(principal).values({
    id: p.id,
    tenantId: p.tenantId,
    kind: p.kind ?? "user",
    refId: p.refId ?? p.id,
    status: p.status ?? "active",
  });
}

export type SeedProvider = {
  id: string;
  tenantId: string;
  name: string;
  plugin?: string;
};

export async function seedProvider(db: Db, p: SeedProvider): Promise<void> {
  await db.insert(provider).values({
    id: p.id,
    tenantId: p.tenantId,
    name: p.name,
    plugin: p.plugin ?? "test-plugin",
  });
}

export type SeedOAuthClient = {
  id: string;
  tenantId: string;
  providerId: string;
  name?: string;
  clientId?: string;
  clientSecret?: string;
};

export async function seedOAuthClient(
  db: Db,
  c: SeedOAuthClient,
): Promise<void> {
  await db.insert(oauthClient).values({
    id: c.id,
    tenantId: c.tenantId,
    providerId: c.providerId,
    name: c.name ?? c.id,
    clientId: c.clientId ?? `${c.id}-client`,
    clientSecret: c.clientSecret ?? `${c.id}-secret`,
  });
}

export type SeedCredential = {
  id: string;
  tenantId: string;
  providerId: string;
  name: string;
  type?: "api_key" | "oauth_token" | "certificate" | "other";
  secret?: string;
  status?: "active" | "expired" | "revoked" | "error";
  principalId?: string | null;
  scopes?: string[] | null;
  oauthClientId?: string | null;
};

export async function seedCredential(db: Db, c: SeedCredential): Promise<void> {
  await db.insert(credential).values({
    id: c.id,
    tenantId: c.tenantId,
    providerId: c.providerId,
    name: c.name,
    type: c.type ?? "api_key",
    secret: c.secret ?? `${c.id}-secret`,
    status: c.status ?? "active",
    principalId: c.principalId ?? null,
    scopes: c.scopes ?? null,
    oauthClientId: c.oauthClientId ?? null,
  });
}
