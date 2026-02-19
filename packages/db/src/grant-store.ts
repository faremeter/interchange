import { eq, and, or, isNull, gt, inArray } from "drizzle-orm";

import type { GrantRule, GrantStore } from "@interchange/types/authz";

import type { DB } from "./client";
import { grant } from "./schema/grants";
import { principalRole } from "./schema/roles";

function toGrantRule(row: typeof grant.$inferSelect): GrantRule {
  return {
    id: row.id,
    resource: row.resource,
    action: row.action,
    effect: row.effect as GrantRule["effect"],
    source: row.source as GrantRule["source"],
    conditions: row.conditions as Record<string, unknown> | null,
    expiresAt: row.expiresAt,
    roleId: row.roleId,
    principalId: row.principalId,
  };
}

export function createGrantStore(db: DB["db"]): GrantStore {
  return {
    async collectGrants(principalId, tenantId) {
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
          eq(grant.tenantId, tenantId),
          or(...ownership),
          or(isNull(grant.expiresAt), gt(grant.expiresAt, now)),
        ),
      });

      return rows.map(toGrantRule);
    },
  };
}
