import { eq, and } from "drizzle-orm";

import type { DB } from "./client";
import { asset } from "./schema/assets";
import { getAncestorChain } from "./tenant-hierarchy";

export type AssetRow = typeof asset.$inferSelect;

/**
 * An asset row paired with the tenant that supplied it. `direct` is
 * true when the asset was declared on the input tenant itself, false
 * when it was inherited from an ancestor.
 */
export type AssetWithOrigin = AssetRow & {
  origin: { tenantId: string; direct: boolean };
};

/**
 * Resolves an asset by (kind, name), walking up the tenant hierarchy.
 * Returns the first match (child shadows parent).
 */
export async function resolveAssetByName(
  db: DB["db"],
  tenantId: string,
  kind: string,
  name: string,
) {
  const chain = await getAncestorChain(db, tenantId);

  for (const tid of chain) {
    const row = await db.query.asset.findFirst({
      where: and(
        eq(asset.tenantId, tid),
        eq(asset.kind, kind),
        eq(asset.name, name),
      ),
    });
    if (row) return row;
  }

  return null;
}

/**
 * Resolves an asset by ID, validating that it belongs to the given
 * tenant or one of its ancestors. Sibling-tenant assets resolve to
 * null so callers cannot probe for cross-tenant existence.
 */
export async function resolveAssetById(
  db: DB["db"],
  tenantId: string,
  assetId: string,
) {
  const row = await db.query.asset.findFirst({
    where: eq(asset.id, assetId),
  });

  if (!row) return null;

  const chain = await getAncestorChain(db, tenantId);
  if (!chain.includes(row.tenantId)) return null;

  return row;
}

const KIND_NAME_SEPARATOR = "\u0000";

/**
 * Lists assets visible to the tenant, including those inherited from
 * ancestors. When two ancestors expose the same `(kind, name)` pair,
 * the descendant shadows the ancestor: the chain is walked leaf-to-root
 * and the first row to claim a `(kind, name)` key wins.
 *
 * When `kind` is supplied the result is filtered to that kind.
 */
export async function listAssetsForTenant(
  db: DB["db"],
  tenantId: string,
  kind?: string,
): Promise<AssetWithOrigin[]> {
  const chain = await getAncestorChain(db, tenantId);
  const byKey = new Map<string, AssetWithOrigin>();

  for (const tid of chain) {
    const conditions = [eq(asset.tenantId, tid)];
    if (kind !== undefined) {
      conditions.push(eq(asset.kind, kind));
    }
    const rows = await db.query.asset.findMany({
      where: and(...conditions),
    });

    for (const row of rows) {
      const key = `${row.kind}${KIND_NAME_SEPARATOR}${row.name}`;
      if (byKey.has(key)) continue;
      byKey.set(key, {
        ...row,
        origin: { tenantId: tid, direct: tid === tenantId },
      });
    }
  }

  return Array.from(byKey.values());
}
