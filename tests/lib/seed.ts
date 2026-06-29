// Fixture seeding helpers for the real-database resolution tests.
//
// These insert rows through the real drizzle client, so they honor the
// schema's NOT NULL / FK / unique / CHECK constraints — the things the
// old query-introspection mocks silently bypassed. Helpers fill
// required columns the resolvers never read with deterministic values
// derived from the row id, so a test only specifies the fields its
// assertions actually depend on.

import type { DB } from "@intx/db";
import { asset, tenant } from "@intx/db/schema";

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
