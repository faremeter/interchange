import { eq, and } from "drizzle-orm";
import type { Context, MiddlewareHandler, Next } from "hono";

import { tenant, principal } from "@intx/db/schema";
import type { DB } from "@intx/db";
import { getLogger } from "@intx/log";

import type { AppEnv, TenantEnv } from "../context";

const log = getLogger(["hub", "middleware", "tenant"]);

export async function requireAuth(c: Context<AppEnv>, next: Next) {
  const user = c.get("user");
  if (!user) {
    return c.json(
      { error: { code: "unauthorized", message: "Authentication required" } },
      401,
    );
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
    const user = c.get("user");
    if (!user) {
      return c.json(
        { error: { code: "unauthorized", message: "Authentication required" } },
        401,
      );
    }

    const tenantId = c.req.param("tenantId");
    if (!tenantId) {
      return c.json(
        { error: { code: "bad_request", message: "Missing tenantId" } },
        400,
      );
    }

    const tenantRow = await db.query.tenant.findFirst({
      where: eq(tenant.id, tenantId),
    });

    if (!tenantRow) {
      return c.json(
        { error: { code: "not_found", message: "Tenant not found" } },
        404,
      );
    }

    const principalRow = await db.query.principal.findFirst({
      where: and(
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "user"),
        eq(principal.refId, user.id),
      ),
    });

    if (!principalRow) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Not a member of this tenant",
          },
        },
        403,
      );
    }

    if (principalRow.status !== "active") {
      log.info("Principal {principalId} has status {status}, denying access", {
        principalId: principalRow.id,
        status: principalRow.status,
      });
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "Your membership in this tenant is not active",
          },
        },
        403,
      );
    }

    c.set("tenant", tenantRow);
    c.set("principal", principalRow);

    await next();
  };
}
