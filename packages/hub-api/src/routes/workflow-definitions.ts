import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { workflowDefinition, workflowDefinitionVersion } from "@intx/db/schema";
import {
  parseWorkflowDefinitionRow,
  parseWorkflowDefinitionVersionRow,
  createWorkflowDefinitionStore,
} from "@intx/db";
import type { DB } from "@intx/db";
import {
  WorkflowDefinitionVersion,
  WorkflowDefinitionResponse,
  WorkflowRollbackRequest,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";

import type { TenantEnv } from "../context";
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

export type CreateWorkflowDefinitionRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createWorkflowDefinitionRoutes({
  db,
  requireGrant,
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
        200: {
          description: "List of workflow definitions",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(WorkflowDefinitionResponse)),
            },
          },
        },
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
        200: {
          description: "List of versions",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(WorkflowDefinitionVersion)),
            },
          },
        },
      },
    }),
    async (c) => {
      const definitionId = c.req.param("definitionId");
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

  app.post(
    "/:definitionId/rollback",
    requireGrant(idResource("workflow-definition", "definitionId"), "manage"),
    describeRoute({
      tags: ["Workflow Definitions"],
      summary: "Roll back to a previous version",
      description:
        "Activates the specified version and stops the current one; repoints currentVersion.",
      responses: {
        200: {
          description: "Rollback applied",
          content: {
            "application/json": {
              schema: resolver(WorkflowDefinitionResponse),
            },
          },
        },
        400: {
          description: "Invalid version",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Definition not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
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
          return c.json(
            { error: { code: "not_found", message: "Definition not found" } },
            404,
          );
        }
        return c.json(
          {
            error: { code: "bad_request", message: "Target version not found" },
          },
          400,
        );
      }

      const def = result.definition;
      return c.json({
        id: def.id,
        tenantId: def.tenantId,
        name: def.name,
        description: def.description ?? null,
        currentVersion: def.currentVersion,
        status: def.status,
        createdAt: ts(def.createdAt),
        updatedAt: ts(def.updatedAt),
      });
    },
  );

  return app;
}
