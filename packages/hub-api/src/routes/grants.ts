import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, validator } from "hono-openapi";

import {
  authorize,
  MAIL_SEND_ACTION,
  mailSendResourceProblem,
  matchPattern,
  validateGrantConditions,
} from "@intx/authz";
import { isMailSendNamespaceResource } from "@intx/types/mail-send";
import { grant, principal, workflowRun } from "@intx/db/schema";
import { parseGrantRow } from "@intx/db";
import type { DB, DBExecutor, GrantStoreFactory } from "@intx/db";
import type { SidecarRouter } from "@intx/hub-sessions";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import {
  CreateGrant,
  ErrorResponse,
  UpdateGrant,
  GrantResponse,
  EvaluateRequest,
  EvaluateResult,
  paginatedSchema,
} from "@intx/types";

import type { TenantEnv } from "../context";
import { errorResponse } from "../error-response";
import { first, ts } from "../format";
import { generateId } from "@intx/hub-common";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import { resolveWorkflowPrincipalLabels } from "./workflow-principal-name";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";
import { jsonResponse } from "../openapi";
import {
  changeDelegatorGrants,
  findRolePrincipalIds,
} from "../run-delegated-grants";

type ResolvedNames = {
  roleNames: Map<string, string>;
  principalNames: Map<string, string>;
};

function formatGrant(row: typeof grant.$inferSelect, names?: ResolvedNames) {
  const parsed = parseGrantRow(row);
  return {
    id: parsed.id,
    tenantId: parsed.tenantId,
    roleId: parsed.roleId ?? null,
    roleName: (parsed.roleId && names?.roleNames.get(parsed.roleId)) ?? null,
    principalId: parsed.principalId ?? null,
    principalName:
      (parsed.principalId && names?.principalNames.get(parsed.principalId)) ??
      null,
    resource: parsed.resource,
    action: parsed.action,
    effect: parsed.effect,
    conditions: parsed.conditions,
    origin: parsed.origin,
    expiresAt: parsed.expiresAt ? ts(parsed.expiresAt) : null,
    createdAt: ts(parsed.createdAt),
    updatedAt: ts(parsed.updatedAt),
  };
}

async function resolveGrantNames(
  db: DB["db"],
  grants: (typeof grant.$inferSelect)[],
): Promise<ResolvedNames> {
  const roleIds = [
    ...new Set(
      grants.map((g) => g.roleId).filter((id): id is string => id !== null),
    ),
  ];
  const principalIds = [
    ...new Set(
      grants
        .map((g) => g.principalId)
        .filter((id): id is string => id !== null),
    ),
  ];

  const roleNames = new Map<string, string>();
  if (roleIds.length > 0) {
    const roles = await db.query.role.findMany({
      where: (r, { inArray }) => inArray(r.id, roleIds),
    });
    for (const r of roles) {
      roleNames.set(r.id, r.name);
    }
  }

  const principalNames = new Map<string, string>();
  if (principalIds.length > 0) {
    const principals = await db.query.principal.findMany({
      where: (p, { inArray }) => inArray(p.id, principalIds),
    });

    const userRefIds = principals
      .filter((p) => p.kind === "user")
      .map((p) => p.refId);
    const workflowRefIds = principals
      .filter((p) => p.kind === "workflow")
      .map((p) => p.refId);

    const refToName = new Map<string, string>();

    if (userRefIds.length > 0) {
      const users = await db.query.user.findMany({
        where: (u, { inArray }) => inArray(u.id, userRefIds),
      });
      for (const u of users) {
        refToName.set(u.id, u.name);
      }
    }

    if (workflowRefIds.length > 0) {
      const wfNames = await resolveWorkflowPrincipalLabels(db, workflowRefIds);
      for (const [refId, name] of wfNames) {
        refToName.set(refId, name);
      }
    }

    for (const p of principals) {
      const name = refToName.get(p.refId);
      if (name) principalNames.set(p.id, name);
    }
  }

  return { roleNames, principalNames };
}

// One evaluator's problem can itself join several field errors, and problem
// text can quote author values, so the list is JSON-encoded to keep each
// problem's bounds unambiguous.
function conditionsMessage(problems: readonly string[]): string {
  return `invalid conditions: ${JSON.stringify(problems)}`;
}

const CONDITIONS_400_DESCRIPTION =
  "Validation error. A body that fails its schema gets the body validator's error envelope. A body whose `conditions` names an unregistered condition, holds a malformed condition body, or, on a workflow run's principal, holds a condition the run cannot evaluate gets an `ErrorResponse` whose message lists each problem as a JSON array.";

// A grant on a workflow run's principal ships to the run, which evaluates it
// with narrower registries than the hub's. A run principal is the workflow
// principal whose `refId` names a run in the tenant; it exists from deploy,
// before its first trigger links it to the run.
async function isRunPrincipal(
  db: DB["db"],
  tenantId: string,
  principalId: string,
): Promise<boolean> {
  const [run] = await db
    .select({ id: workflowRun.id })
    .from(principal)
    .innerJoin(
      workflowRun,
      and(
        eq(workflowRun.id, principal.refId),
        eq(workflowRun.tenantId, principal.tenantId),
      ),
    )
    .where(
      and(
        eq(principal.id, principalId),
        eq(principal.tenantId, tenantId),
        eq(principal.kind, "workflow"),
      ),
    )
    .limit(1);
  return run !== undefined;
}

export type CreateGrantRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
  grantStoreFor: GrantStoreFactory;
  sidecarRouter: Pick<SidecarRouter, "sendRunGrants">;
  // The registry a stored grant's conditions are evaluated against. Create
  // and update refuse conditions it cannot evaluate.
  conditionRegistry: ConditionRegistry;
};

// The principals whose effective grants a grant row is part of: its own
// principal, or every member of its role.
async function grantHolderIds(
  tx: DBExecutor,
  tenantId: string,
  row: { principalId: string | null; roleId: string | null },
): Promise<string[]> {
  if (row.principalId !== null) return [row.principalId];
  if (row.roleId !== null)
    return findRolePrincipalIds(tx, tenantId, row.roleId);
  return [];
}

export function createGrantRoutes({
  db,
  requireGrant,
  grantStoreFor,
  sidecarRouter,
  conditionRegistry,
}: CreateGrantRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();
  const delegatorDeps = { db, grantStoreFor, sidecarRouter };

  app.get(
    "/",
    requireGrant("grant:*", "read"),
    describeRoute({
      tags: ["Grants"],
      summary: "List grants in the tenant",
      description:
        "Lists all grants. Filterable by principalId, roleId, resource pattern, and effect.",
      parameters: [
        { name: "principalId", in: "query", schema: { type: "string" } },
        { name: "roleId", in: "query", schema: { type: "string" } },
        { name: "resource", in: "query", schema: { type: "string" } },
        {
          name: "effect",
          in: "query",
          schema: { type: "string", enum: ["allow", "deny", "ask"] },
        },
        ...pageParameters,
      ],
      responses: {
        200: jsonResponse("List of grants", paginatedSchema(GrantResponse)),
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");

      const principalId = c.req.query("principalId");
      const roleId = c.req.query("roleId");
      const resource = c.req.query("resource");
      const effect = c.req.query("effect");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(grant.tenantId, tenantCtx.id)];
      if (principalId) conditions.push(eq(grant.principalId, principalId));
      if (roleId) conditions.push(eq(grant.roleId, roleId));
      if (resource) conditions.push(eq(grant.resource, resource));
      if (effect === "allow" || effect === "deny" || effect === "ask") {
        conditions.push(eq(grant.effect, effect));
      }
      if (cursor) {
        conditions.push(cursorCondition(grant.createdAt, grant.id, cursor));
      }

      const rows = await db.query.grant.findMany({
        where: and(...conditions),
        orderBy: pageOrder(grant.createdAt, grant.id),
        limit,
      });

      const names = await resolveGrantNames(db, rows);
      return c.json(
        paginatedResponse(
          rows.map((g) => formatGrant(g, names)),
          rows,
          limit,
        ),
      );
    },
  );

  app.post(
    "/",
    requireGrant("grant:*", "create"),
    describeRoute({
      tags: ["Grants"],
      summary: "Create a grant",
      description:
        "Creates a grant targeting either a role or a principal directly. Exactly one of roleId or principalId must be provided.",
      responses: {
        201: jsonResponse("Grant created", GrantResponse),
        400: jsonResponse(
          `${CONDITIONS_400_DESCRIPTION} A \`mail.send\` resource that can match no recipient coordinate, a domain pattern that is not in lower case, a \`mail.send\` grant whose action cannot match \`send\`, or a pattern outside the \`mail.send\` namespace that names part of a send resource, such as \`mail.send*:domain:<id>\`, gets an \`ErrorResponse\` that names the problem.`,
          ErrorResponse,
        ),
      },
    }),
    validator("json", CreateGrant),
    async (c) => {
      const tenantCtx = c.get("tenant");
      // The body validator (CreateGrant) already enforces exactly one target
      // -- roleId or principalId, not both and not neither -- so a malformed
      // request is a 400 before this handler runs.
      const body = c.req.valid("json");

      // A `mail.send` row that can match no coordinate, or whose action cannot
      // match `send`, would never decide a send, so a deny of that shape would
      // block nothing. The update route cannot change a resource or an
      // action, so only creation checks them.
      const resourceProblem = mailSendResourceProblem(body.resource);
      if (resourceProblem !== null) {
        return errorResponse(c, "bad_request", resourceProblem);
      }
      if (
        isMailSendNamespaceResource(body.resource) &&
        !matchPattern(body.action, MAIL_SEND_ACTION)
      ) {
        return errorResponse(
          c,
          "bad_request",
          `a ${JSON.stringify(body.resource)} grant decides only sends, so its action must match "${MAIL_SEND_ACTION}"`,
        );
      }

      const problems = validateGrantConditions(
        {
          resource: body.resource,
          action: body.action,
          conditions: body.conditions ?? null,
          onRunPrincipal:
            body.principalId != null &&
            (await isRunPrincipal(db, tenantCtx.id, body.principalId)),
        },
        conditionRegistry,
      );
      if (problems.length > 0) {
        return errorResponse(c, "bad_request", conditionsMessage(problems));
      }

      const now = new Date();
      const row = await changeDelegatorGrants(
        delegatorDeps,
        tenantCtx.id,
        async (tx) => {
          const inserted = first(
            await tx
              .insert(grant)
              .values({
                id: generateId("grant"),
                tenantId: tenantCtx.id,
                roleId: body.roleId ?? null,
                principalId: body.principalId ?? null,
                resource: body.resource,
                action: body.action,
                effect: body.effect,
                conditions: body.conditions ?? null,
                origin: body.origin,
                expiresAt: body.expiresAt ? new Date(body.expiresAt) : null,
                createdAt: now,
                updatedAt: now,
              })
              .returning(),
          );
          return {
            value: inserted,
            principalIds: await grantHolderIds(tx, tenantCtx.id, inserted),
          };
        },
      );

      return c.json(formatGrant(row), 201);
    },
  );

  app.get(
    "/:grantId",
    requireGrant(idResource("grant", "grantId"), "read"),
    describeRoute({
      tags: ["Grants"],
      summary: "Get grant details",
      responses: {
        200: jsonResponse("Grant details", GrantResponse),
        404: jsonResponse("Grant not found", ErrorResponse),
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const grantId = c.req.param("grantId");

      const row = await db.query.grant.findFirst({
        where: and(eq(grant.id, grantId), eq(grant.tenantId, tenantCtx.id)),
      });

      if (!row) {
        return errorResponse(c, "not_found", "Grant not found");
      }

      return c.json(formatGrant(row));
    },
  );

  app.patch(
    "/:grantId",
    requireGrant(idResource("grant", "grantId"), "manage"),
    describeRoute({
      tags: ["Grants"],
      summary: "Update a grant",
      description: "Update effect, conditions, or expiry on an existing grant.",
      responses: {
        200: jsonResponse("Grant updated", GrantResponse),
        400: jsonResponse(CONDITIONS_400_DESCRIPTION, ErrorResponse),
        404: jsonResponse("Grant not found", ErrorResponse),
      },
    }),
    validator("json", UpdateGrant),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const grantId = c.req.param("grantId");
      const body = c.req.valid("json");

      // An omitted `conditions` leaves the stored value unchecked and
      // unchanged; only a body that sets it is checked.
      if (body.conditions !== undefined) {
        // The update cannot change a grant's resource, action, or target, so
        // the stored row supplies them.
        const existing = await db.query.grant.findFirst({
          where: and(eq(grant.id, grantId), eq(grant.tenantId, tenantCtx.id)),
        });
        if (!existing) {
          return errorResponse(c, "not_found", "Grant not found");
        }
        const problems = validateGrantConditions(
          {
            resource: existing.resource,
            action: existing.action,
            conditions: body.conditions,
            onRunPrincipal:
              existing.principalId !== null &&
              (await isRunPrincipal(db, tenantCtx.id, existing.principalId)),
          },
          conditionRegistry,
        );
        if (problems.length > 0) {
          return errorResponse(c, "bad_request", conditionsMessage(problems));
        }
      }

      const updates: Record<string, unknown> = { updatedAt: new Date() };
      if (body.effect !== undefined) updates["effect"] = body.effect;
      if (body.conditions !== undefined)
        updates["conditions"] = body.conditions;
      if (body.expiresAt !== undefined) {
        updates["expiresAt"] = body.expiresAt ? new Date(body.expiresAt) : null;
      }

      const updated = await changeDelegatorGrants(
        delegatorDeps,
        tenantCtx.id,
        async (tx) => {
          const [row] = await tx
            .update(grant)
            .set(updates)
            .where(and(eq(grant.id, grantId), eq(grant.tenantId, tenantCtx.id)))
            .returning();
          return {
            value: row,
            principalIds:
              row === undefined
                ? []
                : await grantHolderIds(tx, tenantCtx.id, row),
          };
        },
      );

      if (!updated) {
        return errorResponse(c, "not_found", "Grant not found");
      }

      return c.json(formatGrant(updated));
    },
  );

  app.delete(
    "/:grantId",
    requireGrant(idResource("grant", "grantId"), "manage"),
    describeRoute({
      tags: ["Grants"],
      summary: "Revoke a grant",
      responses: {
        204: {
          description: "Grant revoked",
        },
        404: jsonResponse("Grant not found", ErrorResponse),
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const grantId = c.req.param("grantId");

      const deleted = await changeDelegatorGrants(
        delegatorDeps,
        tenantCtx.id,
        async (tx) => {
          const [row] = await tx
            .delete(grant)
            .where(and(eq(grant.id, grantId), eq(grant.tenantId, tenantCtx.id)))
            .returning();
          return {
            value: row,
            principalIds:
              row === undefined
                ? []
                : await grantHolderIds(tx, tenantCtx.id, row),
          };
        },
      );

      if (deleted === undefined) {
        return errorResponse(c, "not_found", "Grant not found");
      }

      return c.body(null, 204);
    },
  );

  return app;
}

// Evaluate endpoint is mounted under principals
export type CreateEvaluateRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
};

export function createEvaluateRoutes({
  db,
  requireGrant,
  grantStore,
  conditionRegistry,
}: CreateEvaluateRoutesDeps): Hono<TenantEnv> {
  const evaluateApp = new Hono<TenantEnv>();

  evaluateApp.post(
    "/",
    requireGrant("grant:*", "read"),
    describeRoute({
      tags: ["Grants"],
      summary: "Evaluate grants for a principal",
      description:
        "Evaluates what would happen if a principal attempted an operation. Returns the resolved effect and all matching grants. Useful for debugging authorization. Pass `consumer` to evaluate as a workflow run token's consumer, so grants scoped to that consumer match. Requires read on grants (`grant:*` / `read`).",
      responses: {
        200: jsonResponse("Evaluation result", EvaluateResult),
        403: jsonResponse("Insufficient grants", ErrorResponse),
        404: jsonResponse("Principal not found", ErrorResponse),
      },
    }),
    validator("json", EvaluateRequest),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const principalId = c.req.param("principalId") ?? "";
      const body = c.req.valid("json");
      const principalRow = await db.query.principal.findFirst({
        where: and(
          eq(principal.id, principalId),
          eq(principal.tenantId, tenantCtx.id),
        ),
      });

      if (!principalRow) {
        return errorResponse(c, "not_found", "Principal not found");
      }

      const result = await authorize(
        grantStore,
        principalId,
        tenantCtx.id,
        body.resource,
        body.action,
        {
          conditions: conditionRegistry,
          ...(body.consumer !== undefined ? { consumer: body.consumer } : {}),
        },
      );

      return c.json({
        effect: result.effect ?? "deny",
        matchingGrants: result.matchingGrants.map((g) => ({
          id: g.id,
          resource: g.resource,
          action: g.action,
          effect: g.effect,
          origin: g.origin,
        })),
      });
    },
  );

  return evaluateApp;
}
