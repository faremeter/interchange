import type { Context, MiddlewareHandler, Next } from "hono";

import type { DB } from "@intx/db";
import { getLogger } from "@intx/log";

import { unauthorizedResponse, type AppEnv, type TenantEnv } from "../context";
import { errorResponse } from "../error-response";
import { resolveTenantPrincipal } from "../principal-lookup";

const log = getLogger(["hub", "middleware", "tenant"]);

export async function requireAuth(c: Context<AppEnv>, next: Next) {
  const user = c.get("user");
  if (!user) {
    return unauthorizedResponse(c);
  }
  await next();
}

export type CreateResolveTenantDeps = {
  db: DB["db"];
};

export function createResolveTenant({
  db,
}: CreateResolveTenantDeps): MiddlewareHandler<TenantEnv> {
  return async (c, next) => {
    if (c.get("principal") && c.get("tenant")) return await next();

    const user = c.get("user");
    if (!user) {
      return unauthorizedResponse(c);
    }

    const tenantId = c.req.param("tenantId");
    if (!tenantId) {
      return errorResponse(c, "bad_request", "Missing tenantId");
    }

    const resolved = await resolveTenantPrincipal(db, {
      tenantId,
      select: { userId: user.id },
    });

    switch (resolved.kind) {
      case "tenant_not_found":
        return errorResponse(c, "not_found", "Tenant not found");
      case "principal_not_found":
        return errorResponse(c, "forbidden", "Not a member of this tenant");
      case "principal_inactive":
        log.info(
          "Principal {principalId} has status {status}, denying access",
          {
            principalId: resolved.principal.id,
            status: resolved.principal.status,
          },
        );
        return errorResponse(
          c,
          "forbidden",
          "Your membership in this tenant is not active",
        );
      case "ok":
        break;
    }

    c.set("tenant", resolved.tenant);
    c.set("principal", resolved.principal);

    await next();
  };
}
