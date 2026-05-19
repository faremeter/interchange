import { createMiddleware } from "hono/factory";
import type { MiddlewareHandler } from "hono";

import { authorize } from "@interchange/authz";
import { getLogger } from "@interchange/log";
import type { ConditionRegistry, GrantStore } from "@interchange/types/authz";

import type { TenantEnv } from "../context";

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
 *   app.get("/", requireGrant("agent:*", "read"), handler);
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

      return c.json(
        {
          error: {
            code: "forbidden",
            message: "You do not have permission to perform this action",
          },
        },
        403,
      );
    });
  };
}

/**
 * Helper that builds a resource string from a URL parameter.
 *
 * Usage:
 *   requireGrant(idResource("agent", "agentId"), "manage")
 *   // resolves to "agent:agt_abc123" from the URL
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
