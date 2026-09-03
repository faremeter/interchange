import { eq, and } from "drizzle-orm";
import type { Context, MiddlewareHandler, Next } from "hono";

import { tenant, principal } from "@intx/db/schema";
import type { DB } from "@intx/db";
import { getLogger } from "@intx/log";

import { unauthorizedResponse, type AppEnv, type TenantEnv } from "../context";
import { errorResponse } from "../error-response";

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

    const tenantRow = await db.query.tenant.findFirst({
      where: eq(tenant.id, tenantId),
    });

    if (!tenantRow) {
      return errorResponse(c, "not_found", "Tenant not found");
    }

    const principalRow = await db.query.principal.findFirst({
      where: and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "user"),
        eq(principal.refId, user.id),
      ),
    });

    if (!principalRow) {
      return errorResponse(c, "forbidden", "Not a member of this tenant");
    }

    if (principalRow.status !== "active") {
      log.info("Principal {principalId} has status {status}, denying access", {
        principalId: principalRow.id,
        status: principalRow.status,
      });
      return errorResponse(
        c,
        "forbidden",
        "Your membership in this tenant is not active",
      );
    }

    c.set("tenant", tenantRow);
    c.set("principal", principalRow);

    await next();
  };
}
