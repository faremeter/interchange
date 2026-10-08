import { eq, and, or, isNull, gt, inArray, type SQL } from "drizzle-orm";

import type { GrantRule, GrantStore } from "@intx/types/authz";

import type { DB } from "./client";
import { grant } from "./schema/grants";
import { principalRole } from "./schema/roles";
import { parseGrantRow } from "./parse-row";
import { getAncestorChain } from "./tenant-hierarchy";

function toGrantRule(row: typeof grant.$inferSelect): GrantRule {
  const parsed = parseGrantRow(row);
  return {
    id: parsed.id,
    resource: parsed.resource,
    action: parsed.action,
    effect: parsed.effect,
    origin: parsed.origin,
    conditions: parsed.conditions,
    expiresAt: parsed.expiresAt,
    roleId: parsed.roleId,
    principalId: parsed.principalId,
  };
}

/**
 * Collect a principal's live (non-expired) grants: those owned directly plus
 * those granted to any role it holds, restricted to the tenants matched by
 * `tenantScope`.
 */
async function collectGrantsScoped(
  db: DB["db"],
  principalId: string,
  tenantScope: SQL,
): Promise<GrantRule[]> {
  const roleAssignments = await db.query.principalRole.findMany({
    where: eq(principalRole.principalId, principalId),
  });
  const roleIds = roleAssignments.map((a) => a.roleId);

  const now = new Date();

  const ownership = [eq(grant.principalId, principalId)];
  if (roleIds.length > 0) {
    ownership.push(inArray(grant.roleId, roleIds));
  }

  const rows = await db.query.grant.findMany({
    where: and(
      tenantScope,
      or(...ownership),
      or(isNull(grant.expiresAt), gt(grant.expiresAt, now)),
    ),
  });

  return rows.map(toGrantRule);
}

export function createGrantStore(db: DB["db"]): GrantStore {
  return {
    async collectGrants(principalId, tenantId) {
      return collectGrantsScoped(db, principalId, eq(grant.tenantId, tenantId));
    },

    // Union the principal's grants across the tenant ancestor chain, mirroring
    // the ancestor-chain resolution credential lookup already performs, so a
    // `credential:{id}` / `use` grant stamped with an ancestor tenant still
    // authorizes an inherited credential. Kept distinct from `collectGrants`
    // so only the source-resolution credential-use check widens to the chain.
    async collectGrantsInChain(principalId, tenantId) {
      const chain = await getAncestorChain(db, tenantId);
      return collectGrantsScoped(
        db,
        principalId,
        inArray(grant.tenantId, chain),
      );
    },
  };
}
