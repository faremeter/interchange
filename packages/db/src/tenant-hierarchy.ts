import { eq } from "drizzle-orm";

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
