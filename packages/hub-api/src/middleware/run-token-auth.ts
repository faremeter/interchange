import { createMiddleware } from "hono/factory";
import type { MiddlewareHandler } from "hono";

import { sha256 } from "@intx/crypto";
import type { DB, RunTokenStore } from "@intx/db";
import { RUN_TOKEN_PREFIX } from "@intx/hub-common";
import { getLogger } from "@intx/log";

import type { TenantEnv } from "../context";
import { errorResponse } from "../error-response";
import { resolveTenantPrincipal } from "../principal-lookup";

const log = getLogger(["hub", "run-token"]);

const BEARER_SCHEME = "bearer ";

/** A route a workflow run token may reach: a method and a Hono path pattern. */
export type RunTokenRoute = {
  method: "GET" | "POST" | "DELETE";
  path: string;
};

/**
 * The only tenant routes a workflow run token may reach. Every other route
 * refuses a run token, whatever grants the run holds, so a run can never
 * resolve approvals, change grants, roles, credentials, OAuth clients, tokens
 * or federation trusts, or manage the tenant. The routes listed still apply
 * their own grant checks. Patterns use the router's form, and a test holds
 * each one to a registered route so a renamed route cannot leave a stale
 * entry.
 */
export const RUN_TOKEN_ROUTES: readonly RunTokenRoute[] = [
  { method: "GET", path: "/api/tenants/:tenantId/workflows/runs" },
  {
    method: "GET",
    path: "/api/tenants/:tenantId/workflows/runs/blobs/:blobId",
  },
  { method: "GET", path: "/api/tenants/:tenantId/workflows/runs/:runId" },
  { method: "DELETE", path: "/api/tenants/:tenantId/workflows/runs/:runId" },
  {
    method: "GET",
    path: "/api/tenants/:tenantId/workflows/runs/:runId/health",
  },
  {
    method: "GET",
    path: "/api/tenants/:tenantId/workflows/runs/:runId/lifecycle",
  },
  {
    method: "GET",
    path: "/api/tenants/:tenantId/workflows/runs/:runId/events",
  },
  {
    method: "GET",
    path: "/api/tenants/:tenantId/workflows/runs/:runId/turns",
  },
  { method: "GET", path: "/api/tenants/:tenantId/workflows/runs/:runId/mail" },
  { method: "GET", path: "/api/tenants/:tenantId/workflows/deployments" },
  { method: "POST", path: "/api/tenants/:tenantId/workflows/deployments" },
  { method: "POST", path: "/api/tenants/:tenantId/workflows/:runId/signals" },
  { method: "GET", path: "/api/tenants/:tenantId/workflows/:runId/runs" },
  {
    method: "GET",
    path: "/api/tenants/:tenantId/workflows/:runId/runs/:eventRunId/events",
  },
  { method: "GET", path: "/api/tenants/:tenantId/workflows/definitions" },
  {
    method: "GET",
    path: "/api/tenants/:tenantId/workflows/definitions/:definitionId/versions",
  },
  { method: "GET", path: "/api/tenants/:tenantId/assets" },
  { method: "GET", path: "/api/tenants/:tenantId/assets/:assetId" },
  { method: "GET", path: "/api/tenants/:tenantId/assets/:assetId/tarballs" },
];

/**
 * Whether a request path matches a route pattern. A `:name` segment matches
 * exactly one non-empty segment, and a trailing slash is a segment. The caller
 * passes the path the router routes on: decoded, except that an encoded slash
 * stays inside its segment.
 */
export function matchesRunTokenRoute(pattern: string, path: string): boolean {
  const want = pattern.split("/");
  const have = path.split("/");
  if (want.length !== have.length) return false;
  return want.every((segment, i) => {
    const actual = have[i];
    if (actual === undefined) return false;
    if (segment.startsWith(":")) return actual.length > 0;
    return segment === actual;
  });
}

export function isRunTokenRoute(method: string, path: string): boolean {
  return RUN_TOKEN_ROUTES.some(
    (route) =>
      route.method === method && matchesRunTokenRoute(route.path, path),
  );
}

export type CreateRunTokenAuthDeps = {
  db: DB["db"];
  runTokenStore: RunTokenStore;
};

/**
 * Authenticate a workflow run token on the tenant routes. A request without
 * an `Authorization: Bearer itx_run_...` header passes through untouched, so
 * the session flow and the git bearer routes are unchanged. A run token
 * resolves to its run principal in the URL's tenant, and the request carries
 * the token's consumer so a grant scoped to a consumer applies only to it.
 *
 * On success the middleware sets `tenant`, `principal` and `consumer`, which
 * makes `resolveTenant` skip the session lookup.
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

    const tenantId = c.req.param("tenantId");
    if (!tenantId) {
      return errorResponse(c, "bad_request", "Missing tenantId");
    }

    const resolved = await resolveTenantPrincipal(db, {
      tenantId,
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

    if (!isRunTokenRoute(c.req.method, c.req.path)) {
      log.info("run-token auth: route refused {tokenId} {method} {path}", {
        tokenId: token.id,
        method: c.req.method,
        path: c.req.path,
      });
      return errorResponse(
        c,
        "forbidden",
        "A workflow run token cannot use this route",
      );
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
