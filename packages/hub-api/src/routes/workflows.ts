import { and, desc, eq } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import { asset, workflowDeployment } from "@intx/db/schema";
import type { DB } from "@intx/db";
import { ErrorResponse } from "@intx/types";
import { InferenceSource } from "@intx/types/runtime";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  workflowDefinitionEnvelopeSchema,
  WORKFLOW_JSON_PATH,
  type AssetService,
  type SessionService,
  type SidecarRouter,
  type WorkflowDefinition,
} from "@intx/hub-sessions";
import { generateId } from "@intx/hub-common";

import type { TenantEnv } from "../context";
import { idResource, type RequireGrant } from "../middleware/grant";
import { ts } from "../format";

// Request body for the general workflow deploy. The workflow definition
// is hydrated from `assetId`'s `workflow.json`; the caller supplies the
// inference sources the per-step agents launch against (full credential
// resolution is the agent-instance path's concern, not this one).
const DeployWorkflow = type({
  assetId: "string",
  sources: InferenceSource.array(),
  defaultSource: "string",
});

// Request body for signal delivery. `signalId` is caller-supplied and
// stable: the workflow-run state machine dedups on `observedSignalIds`,
// so a server-generated id would defeat idempotent retries. Reject an
// empty id at the boundary rather than letting a blank value reach the
// supervisor.
const DeliverSignal = type({
  runId: "string > 0",
  signalName: "string > 0",
  signalId: "string > 0",
  payload: "unknown",
});

const WorkflowDeploymentResponse = type({
  id: "string",
  tenantId: "string",
  definitionAssetId: "string",
  status: "string",
  createdAt: "string",
});

function formatDeployment(row: typeof workflowDeployment.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    definitionAssetId: row.definitionAssetId,
    status: row.status,
    createdAt: ts(row.createdAt),
  };
}

export type CreateWorkflowRoutesDeps = {
  db: DB["db"];
  sessionService: SessionService;
  sidecarRouter: SidecarRouter;
  assetService: AssetService;
  requireGrant: RequireGrant;
};

export function createWorkflowRoutes({
  db,
  sessionService,
  sidecarRouter,
  assetService,
  requireGrant,
}: CreateWorkflowRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.post(
    "/instances",
    requireGrant("workflow:*", "create"),
    describeRoute({
      tags: ["Workflows"],
      summary: "Deploy a workflow",
      description:
        "Hydrates a workflow definition from its workflow asset's workflow.json and deploys it through the general multi-step workflow deploy path. Returns the deployment record.",
      responses: {
        201: {
          description: "Workflow deployed",
          content: {
            "application/json": {
              schema: resolver(WorkflowDeploymentResponse),
            },
          },
        },
        404: {
          description: "Workflow asset not found",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        409: {
          description: "Workflow definition could not be hydrated",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        502: {
          description: "Sidecar unavailable",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
      },
    }),
    validator("json", DeployWorkflow),
    async (c) => {
      const tenant = c.get("tenant");
      const body = c.req.valid("json");

      const assetRow = await db.query.asset.findFirst({
        where: and(
          eq(asset.id, body.assetId),
          eq(asset.tenantId, tenant.id),
          eq(asset.kind, "workflow"),
        ),
      });
      if (!assetRow) {
        return c.json(
          {
            error: { code: "not_found", message: "Workflow asset not found" },
          },
          404,
        );
      }

      let definition: WorkflowDefinition;
      try {
        definition = await hydrateDefinition(assetService, assetRow.id);
      } catch (err) {
        return c.json(
          {
            error: {
              code: "invalid_workflow",
              message:
                err instanceof Error
                  ? err.message
                  : "Failed to hydrate workflow definition",
            },
          },
          409,
        );
      }

      const [firstSource] = body.sources;
      if (firstSource === undefined) {
        return c.json(
          {
            error: {
              code: "invalid_workflow",
              message: "Workflow deploy requires at least one inference source",
            },
          },
          409,
        );
      }

      const deploymentId = generateId("deployment");
      const sessionId = generateId("session");
      const config: HarnessConfig = {
        sessionId,
        agentId: `ins_${deploymentId}`,
        tenantId: tenant.id,
        principalId: c.get("principal").id,
        agentAddress: `ins_${deploymentId}@${tenant.domain}`,
        systemPrompt: "",
        tools: [],
        grants: [],
        sources: body.sources,
        defaultSource: body.defaultSource,
      };

      try {
        const result = await sessionService.deployWorkflowDefinition({
          tenantId: tenant.id,
          deploymentId,
          deploymentDomain: tenant.domain,
          definition,
          definitionAssetId: assetRow.id,
          config,
          deployContent: { systemPrompt: "" },
        });

        const row = await db.query.workflowDeployment.findFirst({
          where: eq(workflowDeployment.id, result.deploymentId),
        });
        if (!row) {
          throw new Error(
            `workflow_deployment row ${result.deploymentId} missing after deploy`,
          );
        }
        return c.json(formatDeployment(row), 201);
      } catch (err) {
        return c.json(
          {
            error: {
              code: "sidecar_unavailable",
              message:
                err instanceof Error
                  ? err.message
                  : "Failed to deploy workflow",
            },
          },
          502,
        );
      }
    },
  );

  app.get(
    "/instances",
    requireGrant("workflow:*", "read"),
    describeRoute({
      tags: ["Workflows"],
      summary: "List workflow deployments",
      description:
        "Lists the workflow deployments for the tenant, most recent first.",
      responses: {
        200: {
          description: "List of workflow deployments",
          content: {
            "application/json": {
              schema: resolver(WorkflowDeploymentResponse.array()),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenant = c.get("tenant");
      const rows = await db
        .select()
        .from(workflowDeployment)
        .where(eq(workflowDeployment.tenantId, tenant.id))
        .orderBy(desc(workflowDeployment.createdAt));
      return c.json(rows.map(formatDeployment));
    },
  );

  app.post(
    "/:deploymentId/signals",
    requireGrant(idResource("workflow-run", "deploymentId"), "manage"),
    describeRoute({
      tags: ["Workflows"],
      summary: "Deliver a signal to a workflow run",
      description:
        "Delivers a caller-supplied, stable signal to the named run of a workflow deployment. The signalId must be supplied by the caller; the run state machine dedups on it.",
      responses: {
        202: {
          description: "Signal accepted for delivery",
        },
        404: {
          description: "Workflow deployment not found",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        502: {
          description: "Sidecar unavailable",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
      },
    }),
    validator("json", DeliverSignal),
    async (c) => {
      const tenant = c.get("tenant");
      const deploymentId = c.req.param("deploymentId");
      const body = c.req.valid("json");

      const row = await db.query.workflowDeployment.findFirst({
        where: and(
          eq(workflowDeployment.id, deploymentId),
          eq(workflowDeployment.tenantId, tenant.id),
        ),
      });
      if (!row) {
        return c.json(
          {
            error: {
              code: "not_found",
              message: "Workflow deployment not found",
            },
          },
          404,
        );
      }

      try {
        sidecarRouter.sendSignalDeliver({
          agentAddress: `ins_${deploymentId}@${tenant.domain}`,
          runId: body.runId,
          signalName: body.signalName,
          signalId: body.signalId,
          payload: body.payload,
        });
      } catch (err) {
        return c.json(
          {
            error: {
              code: "sidecar_unavailable",
              message:
                err instanceof Error
                  ? err.message
                  : "Failed to deliver signal to sidecar",
            },
          },
          502,
        );
      }

      return c.body(null, 202);
    },
  );

  return app;
}

/**
 * Read and hydrate the workflow definition from a workflow asset's
 * `workflow.json`. Validates the structural envelope at this boundary,
 * mirroring the workflow-host child's `loadWorkflowDefinition`: the
 * per-primitive narrows live in the runtime layer that consumes the
 * definition, so the envelope check plus the documented narrow is the
 * canonical hydration shape.
 */
async function hydrateDefinition(
  assetService: AssetService,
  assetId: string,
): Promise<WorkflowDefinition> {
  const raw = await assetService.readAssetBlob({
    assetId,
    path: WORKFLOW_JSON_PATH,
  });
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    throw new Error(
      `workflow asset ${assetId} ${WORKFLOW_JSON_PATH} is not valid JSON`,
      { cause },
    );
  }
  const validated = workflowDefinitionEnvelopeSchema(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow asset ${assetId} ${WORKFLOW_JSON_PATH} failed envelope validation: ${validated.summary}`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope schema enforces structural shape; per-primitive narrows live in the runtime layer that consumes the definition, matching loadWorkflowDefinition in @intx/workflow-host
  return validated as unknown as WorkflowDefinition;
}
