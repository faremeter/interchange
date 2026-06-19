import { eq, inArray } from "drizzle-orm";

import type { DB } from "./client";
import { tenant } from "./schema/tenants";

const MAX_DEPTH = 20;

/**
 * Walks the tenant parentId chain from the given tenant up to the root.
 * Returns an ordered array of tenant IDs: [tenantId, parentId, grandparentId, ...rootId].
 *
 * Protects against cycles with a depth limit.
 */
export async function getAncestorChain(
  db: DB["db"],
  tenantId: string,
): Promise<string[]> {
  const chain: string[] = [tenantId];
  let currentId = tenantId;

  for (let depth = 0; depth < MAX_DEPTH; depth++) {
    const row = await db.query.tenant.findFirst({
      where: eq(tenant.id, currentId),
      columns: { parentId: true },
    });

    if (!row?.parentId) break;

    chain.push(row.parentId);
    currentId = row.parentId;
  }

  return chain;
}

/**
 * Returns the tenant and every distinct tenant in its subtree (children,
 * their children, and so on), discovered breadth-first. Used to find every
 * tenant whose resolved catalog a change at `tenantId` affects — descendants
 * inherit the ancestor's catalog, so a catalog edit must reach their running
 * agents too.
 *
 * Each level queries children by `parentId` membership rather than walking
 * one tenant at a time. A `visited` set both deduplicates the result and
 * guarantees termination: a tenant enters the frontier at most once, so even
 * a malformed cyclic hierarchy drains the frontier instead of looping. No
 * depth cap is imposed, so an arbitrarily deep tree is returned in full.
 */
export async function getDescendantTenants(
  db: DB["db"],
  tenantId: string,
): Promise<string[]> {
  const visited = new Set<string>([tenantId]);
  let frontier: string[] = [tenantId];

  while (frontier.length > 0) {
    const children = await db.query.tenant.findMany({
      where: inArray(tenant.parentId, frontier),
      columns: { id: true },
    });
    frontier = [];
    for (const child of children) {
      if (visited.has(child.id)) continue;
      visited.add(child.id);
      frontier.push(child.id);
    }
  }

  return [...visited];
}
