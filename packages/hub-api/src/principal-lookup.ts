import { and, eq } from "drizzle-orm";

import { principal, tenant } from "@intx/db/schema";
import type { DB } from "@intx/db";

import type { PrincipalRow, TenantRow } from "./context";

/**
 * The tenant path segment that names the tenant the request's credential
 * belongs to, in place of that tenant's id. Tenant ids are generated as
 * `tnt_<hex>`, so no tenant has this id.
 */
export const SELF_TENANT_SEGMENT = "self";

/**
 * The tenant id that a tenant path segment names. `self` names the tenant
 * the credential belongs to, or no tenant for a credential that belongs to
 * none, such as a session, whose user can be a member of many tenants. Any
 * other segment is a tenant id.
 */
export function resolveTenantSegment(
  segment: string,
  credentialTenantId: string | null,
): string | null {
  return segment === SELF_TENANT_SEGMENT ? credentialTenantId : segment;
}

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
  /** `null` names no tenant, as `resolveTenantSegment` returns it. */
  tenantId: string | null;
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
  if (tenantId === null) return { kind: "tenant_not_found" };

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
