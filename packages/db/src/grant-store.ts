import { eq, and, or, isNull, gt, inArray } from "drizzle-orm";

import type { GrantRule, GrantStore } from "@intx/types/authz";

import type { DB } from "./client";
import { grant } from "./schema/grants";
import { principalRole } from "./schema/roles";
import { parseGrantRow } from "./parse-row";

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
