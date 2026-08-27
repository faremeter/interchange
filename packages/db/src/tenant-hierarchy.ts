import { type } from "arktype";
import { eq, inArray } from "drizzle-orm";

import { TenantConfig, type TenantSidecarCapabilityPolicy } from "@intx/types";

import type { DB } from "./client";
import { tenant } from "./schema/tenants";

/**
 * Walks the tenant parentId chain from the given tenant up to the root.
 * Returns an ordered array of tenant IDs: [tenantId, parentId, grandparentId, ...rootId].
 *
 * Throws if the hierarchy contains a cycle instead of returning a partial
 * chain that could omit inherited policy or configuration.
 */
export async function getAncestorChain(
  db: DB["db"],
  tenantId: string,
): Promise<string[]> {
  const chain: string[] = [];
  const visited = new Set<string>();
  let currentId = tenantId;

  while (true) {
    if (visited.has(currentId)) {
      throw new Error(
        `Tenant hierarchy contains a cycle at tenant ${currentId}`,
      );
    }
    visited.add(currentId);
    chain.push(currentId);

    const row = await db.query.tenant.findFirst({
      where: eq(tenant.id, currentId),
      columns: { parentId: true },
    });

    if (!row?.parentId) break;
    currentId = row.parentId;
  }

  return chain;
}

export async function resolveTenantSidecarCapabilityPolicies(
  db: DB["db"],
  tenantId: string,
): Promise<readonly TenantSidecarCapabilityPolicy[]> {
  const chain = await getAncestorChain(db, tenantId);
  const rows = await db.query.tenant.findMany({
    where: inArray(tenant.id, chain),
    columns: { id: true, config: true },
  });
  const rowsById = new Map(rows.map((row) => [row.id, row]));
  const policies: TenantSidecarCapabilityPolicy[] = [];
  for (const ancestorId of chain) {
    const row = rowsById.get(ancestorId);
    if (row === undefined) continue;
    if (row.config === null) continue;
    const config = TenantConfig(row.config);
    if (config instanceof type.errors) {
      throw new Error(
        `Tenant ${row.id} has invalid configuration: ${config.summary}`,
      );
    }
    const rules = config.sidecarPlacement?.capabilities;
    if (rules !== undefined && rules.length > 0) {
      policies.push({ tenantId: row.id, rules });
    }
  }
  return policies;
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
