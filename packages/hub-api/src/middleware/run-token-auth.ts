import { createMiddleware } from "hono/factory";
import type { MiddlewareHandler } from "hono";

import { sha256 } from "@intx/crypto";
import type { DB, RunTokenStore } from "@intx/db";
import { RUN_TOKEN_PREFIX } from "@intx/hub-common";
import { getLogger } from "@intx/log";

import type { TenantEnv } from "../context";
import { errorResponse } from "../error-response";
import {
  resolveTenantPrincipal,
  resolveTenantSegment,
} from "../principal-lookup";

const log = getLogger(["hub", "run-token"]);

const BEARER_SCHEME = "bearer ";

export type CreateRunTokenAuthDeps = {
  db: DB["db"];
  runTokenStore: RunTokenStore;
};

/**
 * Authenticate a workflow run token on the tenant routes. A request without
 * an `Authorization: Bearer itx_run_...` header passes through untouched, so
 * the session flow and the git bearer routes are unchanged. A run token
 * resolves to its run principal in the tenant the URL names, where `self`
 * names the run principal's own tenant, and the request carries
 * the token's consumer so a grant scoped to a consumer applies only to it.
 * The route's own grant check then decides what the run may do there.
 *
 * On success the middleware sets `tenant`, `principal` and `consumer`, which
 * makes `resolveTenant` skip the session lookup, and clears `user` and
 * `session`.
 */
export function createRunTokenAuth({
  db,
  runTokenStore,
}: CreateRunTokenAuthDeps): MiddlewareHandler<TenantEnv> {
  return createMiddleware<TenantEnv>(async (c, next) => {
    const secret = readRunToken(c.req.header("authorization"));
    if (secret === null) return await next();

    const token = await runTokenStore.findByHash(await sha256(secret));
    if (token === undefined) {
      log.info("run-token auth: unknown token");
      return errorResponse(c, "unauthorized", "Authentication required");
    }

    const tenantSegment = c.req.param("tenantId");
    if (!tenantSegment) {
      return errorResponse(c, "bad_request", "Missing tenantId");
    }

    const resolved = await resolveTenantPrincipal(db, {
      tenantId: resolveTenantSegment(tenantSegment, token.tenantId),
      select: { principalId: token.principalId },
    });

    if (
      resolved.kind === "tenant_not_found" ||
      resolved.kind === "principal_not_found"
    ) {
      log.info("run-token auth: token not valid for tenant {tokenId}", {
        tokenId: token.id,
      });
      return errorResponse(
        c,
        "forbidden",
        "Token is not valid for this tenant",
      );
    }

    if (resolved.kind === "principal_inactive") {
      log.info("run-token auth: run principal not active {tokenId}", {
        tokenId: token.id,
        principalId: resolved.principal.id,
        status: resolved.principal.status,
      });
      return errorResponse(c, "unauthorized", "Authentication required");
    }

    const { tenant: tenantRow, principal: principalRow } = resolved;

    if (principalRow.kind !== "workflow") {
      log.error(
        "run-token auth: token {tokenId} names principal {principalId} of kind {kind}",
        {
          tokenId: token.id,
          principalId: principalRow.id,
          kind: principalRow.kind,
        },
      );
      return errorResponse(c, "unauthorized", "Authentication required");
    }

    log.info(
      "run-token auth: success {tokenId} principal={principalId} consumer={consumer} tenant={tenantId}",
      {
        tokenId: token.id,
        principalId: principalRow.id,
        consumer: token.consumer,
        tenantId: tenantRow.id,
      },
    );

    // The run token authenticated this request. A session cookie sent with it
    // names someone else, so no handler may read it as the caller.
    c.set("user", null);
    c.set("session", null);
    c.set("tenant", tenantRow);
    c.set("principal", principalRow);
    c.set("consumer", token.consumer);

    await next();
  });
}

function readRunToken(header: string | undefined): string | null {
  if (header === undefined) return null;
  const trimmed = header.trim();
  if (trimmed.slice(0, BEARER_SCHEME.length).toLowerCase() !== BEARER_SCHEME) {
    return null;
  }
  const secret = trimmed.slice(BEARER_SCHEME.length).trim();
  return secret.startsWith(RUN_TOKEN_PREFIX) ? secret : null;
}
