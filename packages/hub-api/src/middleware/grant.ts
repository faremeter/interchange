import { createMiddleware } from "hono/factory";
import type { Context, MiddlewareHandler } from "hono";

import { authorize } from "@intx/authz";
import { getLogger } from "@intx/log";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";

import type { TenantEnv } from "../context";
import { errorResponse } from "../error-response";

const log = getLogger(["hub", "middleware", "grant"]);

type ResourceFn = (c: {
  param: (name: string) => string | undefined;
}) => string;

/**
 * Closure-bound grant-check middleware factory returned by
 * `createRequireGrant`. Returns a Hono middleware that authorizes the
 * current principal against the given resource and action. The function
 * form of `resource` is intended to be built with `idResource(...)`.
 */
export type RequireGrant = (
  resource: string | ResourceFn,
  action: string,
) => MiddlewareHandler<TenantEnv>;

export type CreateRequireGrantDeps = {
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
};

export type AuthorizeGrantCheck = {
  principalId: string;
  tenantId: string;
  resource: string;
  action: string;
};

/**
 * Bound grant check for a caller that names the principal and tenant itself,
 * such as a route whose tenant comes from the request body rather than the
 * path. Logs a denial and returns whether the grant allows the action.
 */
export type AuthorizeGrant = (check: AuthorizeGrantCheck) => Promise<boolean>;

export function createAuthorizeGrant({
  grantStore,
  conditionRegistry,
}: CreateRequireGrantDeps): AuthorizeGrant {
  return async function authorizeGrant({
    principalId,
    tenantId,
    resource,
    action,
  }) {
    const result = await authorize(
      grantStore,
      principalId,
      tenantId,
      resource,
      action,
      conditionRegistry,
    );
    if (result.effect === "allow") return true;

    log.info(
      "Authorization denied for {principalId}: {resource} {action} -> {effect}",
      {
        principalId,
        tenantId,
        resource,
        action,
        effect: result.effect ?? "no_match",
        resolvedBy: result.resolvedBy?.id ?? null,
      },
    );
    return false;
  };
}

/**
 * Builds a `requireGrant` middleware factory bound to the application's
 * grant store and condition registry. Usage:
 *
 *   const requireGrant = createRequireGrant({ grantStore, conditionRegistry });
 *   app.get("/", requireGrant("workflow-run:*", "read"), handler);
 */
export function createRequireGrant(deps: CreateRequireGrantDeps): RequireGrant {
  const authorizeGrant = createAuthorizeGrant(deps);
  return function requireGrant(resource, action) {
    return createMiddleware<TenantEnv>(async (c, next) => {
      const resolvedResource =
        typeof resource === "function"
          ? resource({ param: (name) => c.req.param(name) })
          : resource;

      const allowed = await authorizeGrant({
        principalId: c.get("principal").id,
        tenantId: c.get("tenant").id,
        resource: resolvedResource,
        action,
      });
      if (allowed) {
        await next();
        return;
      }

      return errorResponse(
        c,
        "forbidden",
        "You do not have permission to perform this action",
      );
    });
  };
}

/**
 * Helper that builds a resource string from a URL parameter.
 *
 * Usage:
 *   requireGrant(idResource("workflow-run", "runId"), "manage")
 *   // resolves to "workflow-run:run_abc123" from the URL
 */
export function idResource(
  resourceType: string,
  paramName: string,
): ResourceFn {
  return (c) => {
    const id = c.param(paramName);
    return id ? `${resourceType}:${id}` : `${resourceType}:*`;
  };
}

/**
 * In-handler grant check for an asset named in a request body. `idResource`
 * only reads URL params and `requireGrant` runs before the body validator, so
 * a body-sourced `asset:<id>` cannot be gated in middleware. Returns the same
 * 403 envelope as `requireGrant` when the principal lacks `read`, or `null`
 * when the caller may proceed.
 */
export async function denyUnlessAssetGrant(args: {
  c: Context<TenantEnv>;
  authorizeGrant: AuthorizeGrant;
  assetId: string;
}): Promise<Response | null> {
  const allowed = await args.authorizeGrant({
    principalId: args.c.get("principal").id,
    tenantId: args.c.get("tenant").id,
    resource: `asset:${args.assetId}`,
    action: "read",
  });
  if (allowed) return null;
  return errorResponse(
    args.c,
    "forbidden",
    "You do not have permission to perform this action",
  );
}
