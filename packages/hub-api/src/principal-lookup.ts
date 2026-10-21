import { and, eq } from "drizzle-orm";

import { principal, tenant } from "@intx/db/schema";
import type { DB } from "@intx/db";

import type { PrincipalRow, TenantRow } from "./context";

export type FindUserPrincipalArgs = {
  tenantId: string;
  userId: string;
};

// Returns the row whatever its status; each caller owns its decision on a
// principal that is not active.
export async function findUserPrincipal(
  db: DB["db"],
  { tenantId, userId }: FindUserPrincipalArgs,
): Promise<PrincipalRow | undefined> {
  return await db.query.principal.findFirst({
    where: and(
      eq(principal.tenantId, tenantId),
      eq(principal.kind, "user"),
      eq(principal.refId, userId),
    ),
  });
}

/**
 * The identity a request authenticated as: a user, whose principal is found
 * by membership in the tenant, or a principal named directly by a token.
 */
export type TenantPrincipalSelector =
  | { userId: string }
  | { principalId: string };

export type ResolveTenantPrincipalArgs = {
  tenantId: string;
  select: TenantPrincipalSelector;
};

export type ResolveTenantPrincipalResult =
  | { kind: "ok"; tenant: TenantRow; principal: PrincipalRow }
  | { kind: "tenant_not_found" }
  | { kind: "principal_not_found"; tenant: TenantRow }
  | { kind: "principal_inactive"; tenant: TenantRow; principal: PrincipalRow };

/**
 * Load the tenant and the caller's principal in it, and require the principal
 * to be active. Each authentication middleware maps the result to its own
 * response, so the lookup and the active rule live in one place.
 */
export async function resolveTenantPrincipal(
  db: DB["db"],
  { tenantId, select }: ResolveTenantPrincipalArgs,
): Promise<ResolveTenantPrincipalResult> {
  const tenantRow = await db.query.tenant.findFirst({
    where: eq(tenant.id, tenantId),
  });
  if (!tenantRow) return { kind: "tenant_not_found" };

  const principalRow =
    "userId" in select
      ? await findUserPrincipal(db, { tenantId, userId: select.userId })
      : await db.query.principal.findFirst({
          where: and(
            eq(principal.id, select.principalId),
            eq(principal.tenantId, tenantId),
          ),
        });
  if (!principalRow) return { kind: "principal_not_found", tenant: tenantRow };

  if (principalRow.status !== "active") {
    return {
      kind: "principal_inactive",
      tenant: tenantRow,
      principal: principalRow,
    };
  }

  return { kind: "ok", tenant: tenantRow, principal: principalRow };
}
