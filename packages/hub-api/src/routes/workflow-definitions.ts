import { eq, and, inArray } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, validator } from "hono-openapi";

import { authorizeAction } from "@intx/authz";
import {
  asset,
  workflowDefinition,
  workflowDefinitionVersion,
} from "@intx/db/schema";
import {
  parseWorkflowDefinitionRow,
  parseWorkflowDefinitionVersionRow,
  createWorkflowDefinitionStore,
  TenantConfigInvalidError,
  validateLifecyclePolicyEdit,
} from "@intx/db";
import type { DB } from "@intx/db";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import {
  WorkflowDefinitionVersion,
  ErrorResponse,
  WorkflowDefinitionResponse,
  WorkflowRollbackRequest,
  UpdateWorkflowDefinitionLifecycle,
  paginatedSchema,
} from "@intx/types";

import type { TenantEnv } from "../context";
import { errorResponse, tenantConfigErrorResponse } from "../error-response";
import { ts } from "../format";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";
import { jsonResponse } from "../openapi";

export type CreateWorkflowDefinitionRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
};

export function createWorkflowDefinitionRoutes({
  db,
  requireGrant,
  grantStore,
  conditionRegistry,
}: CreateWorkflowDefinitionRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();
  const definitionStore = createWorkflowDefinitionStore(db);

  app.get(
    "/",
    requireGrant("workflow-definition:*", "read"),
    describeRoute({
      tags: ["Workflow Definitions"],
      summary: "List workflow definitions",
      description:
        "Lists the workflow definitions for the tenant, most recent first.",
      parameters: [...pageParameters],
      responses: {
        200: jsonResponse(
          "List of workflow definitions",
          paginatedSchema(WorkflowDefinitionResponse),
        ),
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(workflowDefinition.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(
            workflowDefinition.createdAt,
            workflowDefinition.id,
            cursor,
          ),
        );
      }

      const rows = await db.query.workflowDefinition.findMany({
        where: and(...conditions),
        orderBy: pageOrder(workflowDefinition.createdAt, workflowDefinition.id),
        limit,
      });

      const items = rows.map((row) => {
        const def = parseWorkflowDefinitionRow(row);
        return {
          id: def.id,
          tenantId: def.tenantId,
          name: def.name,
          description: def.description ?? null,
          currentVersion: def.currentVersion,
          lifecycle: def.lifecyclePolicy,
          status: def.status,
          createdAt: ts(def.createdAt),
          updatedAt: ts(def.updatedAt),
        };
      });

      return c.json(paginatedResponse(items, rows, limit));
    },
  );

  app.get(
    "/:definitionId/versions",
    requireGrant(idResource("workflow-definition", "definitionId"), "read"),
    describeRoute({
      tags: ["Workflow Definitions"],
      summary: "List definition versions",
      description: "Lists all versions of a workflow definition with status.",
      parameters: [...pageParameters],
      responses: {
        200: jsonResponse(
          "List of versions",
          paginatedSchema(WorkflowDefinitionVersion),
        ),
        404: jsonResponse("Definition not found", ErrorResponse),
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const definitionId = c.req.param("definitionId");

      // Scope to the URL tenant before reading the version history. The
      // grant check authorizes the resource id but not its tenant, so
      // without this a principal could read another tenant's definition
      // versions by id. A miss -- wrong tenant or no such definition -- is
      // a 404, matching the rollback sibling.
      const definition = await db.query.workflowDefinition.findFirst({
        where: and(
          eq(workflowDefinition.id, definitionId),
          eq(workflowDefinition.tenantId, tenantCtx.id),
        ),
      });
      if (definition === undefined) {
        return errorResponse(c, "not_found", "Definition not found");
      }

      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [
        eq(workflowDefinitionVersion.definitionId, definitionId),
      ];
      if (cursor) {
        conditions.push(
          cursorCondition(
            workflowDefinitionVersion.createdAt,
            workflowDefinitionVersion.id,
            cursor,
          ),
        );
      }

      const rows = await db.query.workflowDefinitionVersion.findMany({
        where: and(...conditions),
        orderBy: pageOrder(
          workflowDefinitionVersion.createdAt,
          workflowDefinitionVersion.id,
        ),
        limit,
      });

      const items = rows.map((v) => {
        const parsed = parseWorkflowDefinitionVersionRow(v);
        return {
          version: parsed.version,
          status: parsed.status,
          createdAt: ts(parsed.createdAt),
        };
      });

      return c.json(paginatedResponse(items, rows, limit));
    },
  );

  app.patch(
    "/:definitionId/lifecycle",
    requireGrant(idResource("workflow-definition", "definitionId"), "manage"),
    describeRoute({
      tags: ["Workflow Definitions"],
      summary: "Update workflow lifecycle policy",
      description:
        "Replaces the installed workflow's lifecycle overrides for future deployments. The overrides cover every revision of the definition's workflow asset, including revisions deployed later, so the caller needs manage on each existing revision. Omitted fields inherit tenant limits.",
      responses: {
        200: jsonResponse("Policy updated", WorkflowDefinitionResponse),
        400: jsonResponse(
          "Invalid policy or inherited limit exceeded",
          ErrorResponse,
        ),
        403: jsonResponse(
          "Caller cannot manage every revision of the workflow",
          ErrorResponse,
        ),
        404: jsonResponse("Definition not found", ErrorResponse),
        409: jsonResponse(
          "The workflow's revisions changed during the update, or a stored tenant config it inherits is invalid",
          ErrorResponse,
        ),
      },
    }),
    validator("json", UpdateWorkflowDefinitionLifecycle),
    async (c) => {
      const tenantId = c.get("tenant").id;
      const definitionId = c.req.param("definitionId");
      const lifecycle = c.req.valid("json").lifecycle;
      const definition = await db.query.workflowDefinition.findFirst({
        where: and(
          eq(workflowDefinition.id, definitionId),
          eq(workflowDefinition.tenantId, tenantId),
        ),
      });
      if (definition === undefined)
        return errorResponse(c, "not_found", "Definition not found");
      // Each revision of a workflow asset is its own definition, so the
      // override is written to all of them, and the caller must be able to
      // manage each one.
      const assetId = definition.assetId;
      const revisionFilter = and(
        eq(workflowDefinition.tenantId, tenantId),
        assetId === null
          ? eq(workflowDefinition.id, definitionId)
          : eq(workflowDefinition.assetId, assetId),
      );
      const revisionIds = (
        await db
          .select({ id: workflowDefinition.id })
          .from(workflowDefinition)
          .where(revisionFilter)
      ).map((row) => row.id);
      if (!revisionIds.includes(definitionId))
        return errorResponse(c, "not_found", "Definition not found");
      const principalId = c.get("principal").id;
      const grants = await grantStore.collectGrants(principalId, tenantId);
      for (const revisionId of revisionIds) {
        const decision = await authorizeAction(
          grants,
          `workflow-definition:${revisionId}`,
          "manage",
          { registry: conditionRegistry, principalId, tenantId },
        );
        if (!decision.ok)
          return errorResponse(
            c,
            "forbidden",
            "You do not have permission to manage every revision of this workflow",
          );
      }
      let resolved;
      try {
        resolved = await validateLifecyclePolicyEdit(db, tenantId, lifecycle);
      } catch (err) {
        if (!(err instanceof TenantConfigInvalidError)) throw err;
        return tenantConfigErrorResponse(c, err);
      }
      if (!resolved.ok)
        return errorResponse(
          c,
          "bad_request",
          `${resolved.field} exceeds inherited limit`,
        );
      // A revision created later copies the override under the asset lock
      // (see ensureWorkflowDefinitionForAsset). One created since the check
      // above is not authorized, so the update is refused rather than
      // leaving it on the old override.
      const updated = await db.transaction(async (tx) => {
        if (assetId !== null)
          await tx
            .select({ id: asset.id })
            .from(asset)
            .where(eq(asset.id, assetId))
            .for("update");
        const current = await tx
          .select({ id: workflowDefinition.id })
          .from(workflowDefinition)
          .where(revisionFilter);
        if (
          current.length !== revisionIds.length ||
          current.some((row) => !revisionIds.includes(row.id))
        )
          return "revisions_changed" as const;
        const rows = await tx
          .update(workflowDefinition)
          .set({ lifecyclePolicy: lifecycle, updatedAt: new Date() })
          .where(inArray(workflowDefinition.id, revisionIds))
          .returning();
        return rows.find((row) => row.id === definitionId);
      });
      if (updated === "revisions_changed")
        return errorResponse(
          c,
          "conflict",
          "The workflow's revisions changed during the update; retry",
        );
      if (updated === undefined)
        return errorResponse(c, "not_found", "Definition not found");
      return c.json({
        id: updated.id,
        tenantId: updated.tenantId,
        name: updated.name,
        description: updated.description,
        currentVersion: updated.currentVersion,
        status: parseWorkflowDefinitionRow(updated).status,
        lifecycle,
        createdAt: ts(updated.createdAt),
        updatedAt: ts(updated.updatedAt),
      });
    },
  );

  app.post(
    "/:definitionId/rollback",
    requireGrant(idResource("workflow-definition", "definitionId"), "manage"),
    describeRoute({
      tags: ["Workflow Definitions"],
      summary: "Roll back to a previous version",
      description:
        "Activates the specified version and stops the current one; repoints currentVersion.",
      responses: {
        200: jsonResponse("Rollback applied", WorkflowDefinitionResponse),
        400: jsonResponse("Invalid version", ErrorResponse),
        404: jsonResponse("Definition not found", ErrorResponse),
      },
    }),
    validator("json", WorkflowRollbackRequest),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const definitionId = c.req.param("definitionId");
      const body = c.req.valid("json");

      const result = await definitionStore.rollback(
        tenantCtx.id,
        definitionId,
        body.version,
      );

      if (!result.ok) {
        if (result.reason === "definition_not_found") {
          return errorResponse(c, "not_found", "Definition not found");
        }
        return errorResponse(c, "bad_request", "Target version not found");
      }

      const def = result.definition;
      return c.json({
        id: def.id,
        tenantId: def.tenantId,
        name: def.name,
        description: def.description ?? null,
        currentVersion: def.currentVersion,
        lifecycle: def.lifecyclePolicy,
        status: def.status,
        createdAt: ts(def.createdAt),
        updatedAt: ts(def.updatedAt),
      });
    },
  );

  return app;
}
