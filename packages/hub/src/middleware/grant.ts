import { createMiddleware } from "hono/factory";

import { authorize } from "@interchange/authz";
import { getLogger } from "@interchange/log";

import type { TenantEnv } from "../context";

const log = getLogger(["hub", "middleware", "grant"]);

type ResourceFn = (c: {
  param: (name: string) => string | undefined;
}) => string;

/**
 * Middleware factory that checks authorization grants before
 * allowing a request to proceed.
 *
 * The resource can be a static string or a function that extracts
 * the resource identifier from request parameters.
 *
 * Usage:
 *   app.get("/", requireGrant("agent:*", "read"), handler)
 *   app.delete("/:agentId", requireGrant(idResource("agent", "agentId"), "manage"), handler)
 */
export function requireGrant(resource: string | ResourceFn, action: string) {
  return createMiddleware<TenantEnv>(async (c, next) => {
    const grantStore = c.get("grantStore");
    const conditionRegistry = c.get("conditionRegistry");
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
