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

/**
 * Builds a `requireGrant` middleware factory bound to the application's
 * grant store and condition registry. Usage:
 *
 *   const requireGrant = createRequireGrant({ grantStore, conditionRegistry });
 *   app.get("/", requireGrant("workflow-run:*", "read"), handler);
 */
export function createRequireGrant({
  grantStore,
  conditionRegistry,
}: CreateRequireGrantDeps): RequireGrant {
  return function requireGrant(resource, action) {
    return createMiddleware<TenantEnv>(async (c, next) => {
      const principal = c.get("principal");
      const tenant = c.get("tenant");

      const resolvedResource =
        typeof resource === "function"
          ? resource({ param: (name) => c.req.param(name) })
          : resource;

      const result = await authorize(
        grantStore,
        principal.id,
        tenant.id,
        resolvedResource,
        action,
        conditionRegistry,
      );

      if (result.effect === "allow") {
        await next();
        return;
      }

      log.info(
        "Authorization denied for {principalId}: {resource} {action} -> {effect}",
        {
          principalId: principal.id,
          resource: resolvedResource,
          action,
          effect: result.effect ?? "no_match",
          resolvedBy: result.resolvedBy?.id ?? null,
        },
      );

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
 * a body-sourced `asset:<id>` cannot be gated in middleware. Returns the
 * same 403 envelope as `requireGrant` when the principal lacks `read`.
 */
export async function requireAssetGrant(args: {
  c: Context<TenantEnv>;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  assetId: string;
}): Promise<Response | null> {
  const principal = args.c.get("principal");
  const tenant = args.c.get("tenant");
  const resource = `asset:${args.assetId}`;
  const result = await authorize(
    args.grantStore,
    principal.id,
    tenant.id,
    resource,
    "read",
    args.conditionRegistry,
  );
  if (result.effect === "allow") {
    return null;
  }
  log.info(
    "Authorization denied for {principalId}: {resource} {action} -> {effect}",
    {
      principalId: principal.id,
      resource,
      action: "read",
      effect: result.effect ?? "no_match",
      resolvedBy: result.resolvedBy?.id ?? null,
    },
  );
  return errorResponse(
    args.c,
    "forbidden",
    "You do not have permission to perform this action",
  );
}
