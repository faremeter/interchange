import { and, desc, eq, isNotNull } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import {
  asset,
  isLiveWorkflowRunStatus,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import { WorkflowRunDispatchPayloadConflictError, type DB } from "@intx/db";
import type { GrantStore } from "@intx/types/authz";
import {
  correlationIdFromSignalName,
  deriveWorkflowRunId,
  ErrorResponse,
  isSidecarAllocationDispatchable,
  SendMessage,
  type SidecarAllocationStatus,
} from "@intx/types";
import { InferenceSource } from "@intx/types/runtime";
import type { HarnessConfig } from "@intx/types/runtime";
import { WorkflowDefinitionSource } from "@intx/types/workflow-sources";
import {
  createWorkflowRunReader,
  ExclusiveWorkflowPlacementError,
  resolveWorkflowSidecarPlacement,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
  type WorkflowAllocationService,
  type WorkflowDispatchService,
} from "@intx/hub-sessions";
import { generateId } from "@intx/hub-common";
import {
  deriveRunAddress,
  deriveRunAgentId,
  WorkflowDefinitionInvalidError,
} from "@intx/workflow-deploy";

import type { TenantEnv } from "../context";
import { idResource, type RequireGrant } from "../middleware/grant";
import {
  lockDispatchableAllocation,
  lockWorkflowRunState,
} from "../run-grant-materialization";
import { ts } from "../format";
import { WorkflowRunEventsResponse, formatRunEvent } from "./run-events-view";
import {
  readDurableWorkflowRunLifecycle,
  workflowRunRepoId,
  WORKFLOW_RUN_REF,
} from "../workflow-run-lifecycle";
import {
  createWorkflowRunTrigger,
  MAX_MAIL_BODY_BYTES,
  WorkflowRunTriggerResponse,
} from "../workflow-run-trigger";

// Request body for the general workflow deploy. The definition is CODE-SOURCED:
// `source` names where its bytes come from and `entry` the `interchange.workflow`
// module the sidecar evaluates; the hub installs + probes + gates + freezes it
// and deploys by source-ref. The caller supplies the inference chain the
// per-step agents launch against. `pin` selects the definition package for the
// `registry` and asset-`tarball` variants (asset-`source` selects by
// `packageName`). The `source` union is validated at this boundary.
const DeployWorkflow = type({
  source: WorkflowDefinitionSource,
  entry: "string > 0",
  sources: InferenceSource.array(),
  defaultSource: "string",
  "pin?": "string > 0",
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
  "payload?": "unknown",
});

const WorkflowDeploymentResponse = type({
  id: "string",
  tenantId: "string",
  definitionAssetId: "string",
  status: "string",
  createdAt: "string",
});

const WorkflowRunListResponse = type({
  runIds: "string[]",
});

// A deployment's API shape, assembled from its anchor run and the run's
// definition. The old projection reported a constant "deployed" for every row
// -- no code path ever wrote another status -- and the anchor run's own status
// (running -> terminal) is a different, run-level concept, so the deployment
// status is synthesized as that same constant. `definitionAssetId` comes from
// the run's definition; a null asset is a corrupt definition the deployment
// contract cannot represent, so it surfaces loudly rather than emitting null
// into a string field.
function formatDeployment(
  row: {
    id: string;
    tenantId: string;
    definitionAssetId: string | null;
    createdAt: Date;
    allocationStatus?: SidecarAllocationStatus | null;
    allocationNextAttemptAt?: Date | null;
  },
  statusOverride?: string,
) {
  if (row.definitionAssetId === null) {
    throw new Error(
      `deployment ${row.id}: anchor run's definition has no asset`,
    );
  }
  return {
    id: row.id,
    tenantId: row.tenantId,
    definitionAssetId: row.definitionAssetId,
    status: statusOverride ?? formatAllocationStatus(row),
    createdAt: ts(row.createdAt),
  };
}

function formatAllocationStatus(row: {
  allocationStatus?: SidecarAllocationStatus | null;
  allocationNextAttemptAt?: Date | null;
}): string {
  switch (row.allocationStatus) {
    case undefined:
    case null:
      return "deployed";
    case "pending":
    case "provisioning":
      return "pending";
    case "allocated":
      return row.allocationNextAttemptAt == null ? "deployed" : "pending";
    case "replacing":
      return "recovering";
    case "releasing":
    case "released":
    case "failed":
      return row.allocationStatus;
    default: {
      const unreachable: never = row.allocationStatus;
      return unreachable;
    }
  }
}

// A deployment exists iff its anchor run does -- the workflow_run whose id is
// the deployment id, carrying its routing identity. `anchorRunId` is
// non-null on a deployment-anchored run, distinguishing it from a folded-agent
// run (which never shares an anchor run anyway).
async function deploymentAnchorRunExists(
  db: DB["db"],
  anchorRunId: string,
  tenantId: string,
): Promise<boolean> {
  const [row] = await db
    .select({ id: workflowRun.id })
    .from(workflowRun)
    .where(
      and(
        eq(workflowRun.id, anchorRunId),
        eq(workflowRun.tenantId, tenantId),
        isNotNull(workflowRun.anchorRunId),
      ),
    )
    .limit(1);
  return row !== undefined;
}

export type CreateWorkflowRoutesDeps = {
  db: DB["db"];
  sessionService: SessionService;
  workflowAllocationService?: WorkflowAllocationService;
  workflowDispatchService?: WorkflowDispatchService;
  sidecarRouter: SidecarRouter;
  repoStore: RepoStore;
  grantStore: GrantStore;
  requireGrant: RequireGrant;
};

export function createWorkflowRoutes({
  db,
  sessionService,
  workflowAllocationService,
  workflowDispatchService,
  sidecarRouter,
  repoStore,
  grantStore,
  requireGrant,
}: CreateWorkflowRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();
  const runReader = createWorkflowRunReader(repoStore);
  const triggerWorkflowRun = createWorkflowRunTrigger({
    db,
    grantStore,
    sidecarRouter,
    ...(workflowDispatchService !== undefined
      ? { workflowDispatchService }
      : {}),
    repoStore,
  });

  async function readRunLifecycle(
    anchorRunId: string,
    tenantDomain: string,
    runId: string,
  ) {
    const deploymentAddress = deriveRunAddress({
      runId: anchorRunId,
      domain: tenantDomain,
    });
    return readDurableWorkflowRunLifecycle(repoStore, deploymentAddress, runId);
  }

  app.post(
    "/deployments",
    requireGrant("workflow:*", "create"),
    describeRoute({
      tags: ["Workflows"],
      summary: "Deploy a workflow",
      description:
        "Installs, probes, gates, and freezes a code-sourced workflow definition from its `source`/`entry`, then deploys it by source-ref. Returns the deployment record.",
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
          description:
            "Workflow definition invalid, exclusive placement unavailable on this Hub, or exclusive prepare rejected the source chain",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        500: {
          description:
            "Deployment projection row missing after deploy, or exclusive prepare failed unexpectedly",
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

      // The deployment anchors its frozen `workflow_definition` to a
      // `workflow`-kind asset. An asset-sourced deploy projects the definition
      // over the very asset it sources from; a registry-sourced deploy has no
      // backing asset for the definition, so this route (which anchors every
      // deployment to a workflow asset) does not support it yet.
      if (body.source.kind !== "asset") {
        return c.json(
          {
            error: {
              code: "unsupported_source",
              message:
                "Registry-sourced workflow deploys are not yet supported on this route",
            },
          },
          400,
        );
      }
      const definitionAssetId = body.source.assetId;

      const assetRow = await db.query.asset.findFirst({
        where: and(
          eq(asset.id, definitionAssetId),
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

      // Placement is now a pure tenant-config concern, decided BEFORE any
      // definition is installed: a code-sourced deploy never hydrates a live
      // definition to read declared placement off.
      let placement;
      try {
        placement = await resolveWorkflowSidecarPlacement(db, tenant.id);
      } catch (err) {
        return c.json(
          {
            error: {
              code: "placement_resolution_failed",
              message:
                err instanceof Error
                  ? err.message
                  : "Failed to resolve workflow sidecar placement",
            },
          },
          500,
        );
      }

      const anchorRunId = generateId("workflowRun");
      const sessionId = generateId("session");
      const agentAddress = deriveRunAddress({
        runId: anchorRunId,
        domain: tenant.domain,
      });

      let deployedId: string;
      let deploymentStatus = "deployed";
      if (placement?.sharing === "exclusive") {
        // Exclusive placement freezes the code-sourced approval on shared
        // capacity NOW and defers the deploy to a dedicated allocation. The
        // route only records the intent and returns a pending deployment;
        // `deployReadyAllocation` deploys the frozen bundle once the sidecar is
        // provisioned. (Exclusive is dormant in-tree -- no provisioner is
        // registered -- so `prepareExclusiveDeployment` fails closed unless a
        // tenant config requests it AND an operator build wires a provisioner.)
        if (workflowAllocationService === undefined) {
          return c.json(
            {
              error: {
                code: "exclusive_placement_unavailable",
                message:
                  "Exclusive workflow placement is not configured on this Hub",
              },
            },
            409,
          );
        }
        try {
          const prepared =
            await workflowAllocationService.prepareExclusiveDeployment({
              tenantId: tenant.id,
              anchorRunId,
              deploymentDomain: tenant.domain,
              source: body.source,
              entry: body.entry,
              ...(body.pin !== undefined ? { pin: body.pin } : {}),
              definitionAssetId: assetRow.id,
              placement,
              sessionId,
              sourceAuthorityPrincipalId: c.get("principal").id,
              sourceOfferingIds: body.sources.map((source) => source.id),
              defaultSourceOfferingId: body.defaultSource,
              deployContent: { systemPrompt: "" },
            });
          deployedId = prepared.anchorRunId;
          deploymentStatus = prepared.status;
        } catch (err) {
          // The shared-capacity probe/gate ran inside prepare: an unapproved
          // definition is a client/definition error, not an infra failure.
          if (err instanceof WorkflowDefinitionInvalidError) {
            return c.json(
              { error: { code: "invalid_workflow", message: err.message } },
              409,
            );
          }
          if (err instanceof ExclusiveWorkflowPlacementError) {
            return c.json(
              { error: { code: err.code, message: err.message } },
              409,
            );
          }
          return c.json(
            {
              error: {
                code: "exclusive_deployment_failed",
                message:
                  err instanceof Error
                    ? err.message
                    : "Failed to prepare exclusive workflow deployment",
              },
            },
            500,
          );
        }
      } else {
        const config: HarnessConfig = {
          sessionId,
          agentId: deriveRunAgentId({ runId: anchorRunId }),
          tenantId: tenant.id,
          principalId: c.get("principal").id,
          agentAddress,
          systemPrompt: "",
          tools: [],
          grants: [],
          sources: body.sources,
          defaultSource: body.defaultSource,
        };
        try {
          const result = await sessionService.deployWorkflowFromSource({
            tenantId: tenant.id,
            anchorRunId,
            deploymentDomain: tenant.domain,
            agentAddress,
            source: body.source,
            entry: body.entry,
            ...(body.pin !== undefined ? { pin: body.pin } : {}),
            definitionAssetId: assetRow.id,
            config,
          });
          deployedId = result.anchorRunId;
        } catch (err) {
          // An install/gate rejection or an unapproved/mis-ordered source chain
          // is a client/definition error, not a sidecar-reachability failure.
          if (err instanceof WorkflowDefinitionInvalidError) {
            return c.json(
              { error: { code: "invalid_workflow", message: err.message } },
              409,
            );
          }
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
      }

      // The sidecar deploy succeeded; reading back the anchor run is an
      // internal persistence concern. The id is server-minted for this
      // request, so a missing row is an invariant violation, not a
      // sidecar-reachability failure, and must surface as a 500 rather than be
      // mislabeled 502. Keyed on the run id alone -- no tenant filter -- since
      // the id was just minted for this tenant's deploy.
      const [row] = await db
        .select({
          id: workflowRun.id,
          tenantId: workflowRun.tenantId,
          definitionAssetId: workflowDefinition.assetId,
          createdAt: workflowRun.createdAt,
        })
        .from(workflowRun)
        .innerJoin(
          workflowDefinition,
          eq(workflowRun.definitionId, workflowDefinition.id),
        )
        .where(eq(workflowRun.id, deployedId))
        .limit(1);
      if (!row) {
        return c.json(
          {
            error: {
              code: "anchor_run_missing",
              message: `anchor workflow_run ${deployedId} missing after deploy`,
            },
          },
          500,
        );
      }
      return c.json(formatDeployment(row, deploymentStatus), 201);
    },
  );

  app.get(
    "/deployments",
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
      // List the deployments as their anchor runs -- the workflow_run whose id
      // equals its own deployment_id. There is deliberately NO run-status
      // filter: allocation lifecycle is separate from run execution status.
      // Shared deployments have no allocation row and retain the legacy
      // "deployed" projection; exclusive deployments derive their public
      // lifecycle from the allocation joined below. The `id = deployment_id`
      // predicate (with the explicit non-null, matching
      // deploymentAnchorRunExists) is the anchor-run identity; child and
      // folded runs never satisfy it.
      const rows = await db
        .select({
          id: workflowRun.id,
          tenantId: workflowRun.tenantId,
          definitionAssetId: workflowDefinition.assetId,
          createdAt: workflowRun.createdAt,
          allocationStatus: sidecarAllocation.status,
          allocationNextAttemptAt: sidecarAllocation.nextAttemptAt,
        })
        .from(workflowRun)
        .innerJoin(
          workflowDefinition,
          eq(workflowRun.definitionId, workflowDefinition.id),
        )
        .leftJoin(
          sidecarAllocation,
          eq(sidecarAllocation.anchorRunId, workflowRun.id),
        )
        .where(
          and(
            eq(workflowRun.tenantId, tenant.id),
            eq(workflowRun.id, workflowRun.anchorRunId),
            isNotNull(workflowRun.anchorRunId),
          ),
        )
        .orderBy(desc(workflowRun.createdAt));
      return c.json(rows.map((row) => formatDeployment(row)));
    },
  );

  app.post(
    "/:runId/signals",
    requireGrant(idResource("workflow-run", "runId"), "manage"),
    describeRoute({
      tags: ["Workflows"],
      summary: "Deliver a signal to a workflow run",
      description:
        "Delivers a caller-supplied, stable signal to the named run of a workflow deployment. The signalId must be supplied by the caller; the run state machine dedups on it.",
      responses: {
        202: {
          description: "Signal accepted for delivery",
        },
        400: {
          description:
            "Reserved signal name or a runId that is not the deployment's addressable run",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        404: {
          description: "Workflow deployment not found",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        409: {
          description:
            "Workflow run has not started, is terminal, its deployment allocation is no longer active, or the signalId conflicts with a previously accepted payload",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        502: {
          description: "Sidecar unavailable",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        503: {
          description: "Durable workflow dispatch unavailable",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
      },
    }),
    validator("json", DeliverSignal),
    async (c) => {
      const tenant = c.get("tenant");
      const anchorRunId = c.req.param("runId");
      const body = c.req.valid("json");
      const agentAddress = deriveRunAddress({
        runId: anchorRunId,
        domain: tenant.domain,
      });
      const runId = deriveWorkflowRunId(agentAddress);
      const signalRun = alias(workflowRun, "signal_top_level_run");

      const [deployment] = await db
        .select({
          allocationId: sidecarAllocation.id,
          allocationStatus: sidecarAllocation.status,
          anchorStatus: workflowRun.status,
          runStatus: signalRun.status,
        })
        .from(workflowRun)
        .leftJoin(
          sidecarAllocation,
          eq(sidecarAllocation.anchorRunId, workflowRun.id),
        )
        .leftJoin(
          signalRun,
          and(
            eq(signalRun.id, runId),
            eq(signalRun.anchorRunId, workflowRun.id),
          ),
        )
        .where(
          and(
            eq(workflowRun.id, anchorRunId),
            eq(workflowRun.tenantId, tenant.id),
            isNotNull(workflowRun.anchorRunId),
          ),
        )
        .limit(1);
      if (deployment === undefined) {
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

      // A reserved control-plane channel name (`signalName(correlationId)`) is
      // the hub's own approval/input plane. Delivering on it through this route
      // would let a caller answer a pending approval directly -- bypassing the
      // approval co-write and its authorization -- so reject it. Author signals
      // to a workflow use free-form names.
      if (correlationIdFromSignalName(body.signalName) !== undefined) {
        return c.json(
          {
            error: {
              code: "reserved_signal_name",
              message:
                "signalName names a reserved control-plane channel; deliver author-named signals only",
            },
          },
          400,
        );
      }

      // Only the deployment's single addressable run may be signaled. A
      // synthetic body-child run id (or any other id) is not addressable: a
      // signal for a section body is delivered to the parent deployment run and
      // relayed down by the runtime, never addressed to the child directly.
      if (body.runId !== runId) {
        return c.json(
          {
            error: {
              code: "unaddressable_run",
              message: "runId is not the addressable run of this deployment",
            },
          },
          400,
        );
      }

      const durableLifecycle = await readRunLifecycle(
        anchorRunId,
        tenant.domain,
        runId,
      );

      if (
        !isLiveWorkflowRunStatus(deployment.anchorStatus) ||
        deployment.runStatus === null ||
        !isLiveWorkflowRunStatus(deployment.runStatus) ||
        durableLifecycle !== "live"
      ) {
        return c.json(
          {
            error: {
              code: "workflow_run_not_running",
              message:
                durableLifecycle !== "live"
                  ? durableLifecycle === "terminal"
                    ? "Workflow run is terminal"
                    : "Workflow run has not started"
                  : !isLiveWorkflowRunStatus(deployment.anchorStatus)
                    ? `Workflow deployment is ${deployment.anchorStatus}`
                    : deployment.runStatus === null
                      ? "Workflow run has not started"
                      : `Workflow run is ${deployment.runStatus}`,
            },
          },
          409,
        );
      }

      if (
        deployment.allocationStatus !== null &&
        !isSidecarAllocationDispatchable(deployment.allocationStatus)
      ) {
        return c.json(
          {
            error: {
              code: "deployment_unreachable",
              message: `Workflow deployment allocation is ${deployment.allocationStatus}`,
            },
          },
          409,
        );
      }

      const allocationId = deployment.allocationId;
      if (allocationId !== null) {
        if (workflowDispatchService === undefined) {
          return c.json(
            {
              error: {
                code: "workflow_dispatch_unavailable",
                message:
                  "Durable workflow dispatch is unavailable for this exclusive deployment",
              },
            },
            503,
          );
        }
        try {
          const enqueued = await db.transaction(async (tx) => {
            if (
              !(await lockDispatchableAllocation(tx, allocationId, anchorRunId))
            ) {
              return "allocation-unavailable" as const;
            }
            // Pack receipt advances Git while holding this allocation lock. Read
            // again after acquiring it so the preflight result cannot go stale
            // while this transaction waits behind a terminal pack.
            if (
              (await readRunLifecycle(anchorRunId, tenant.domain, runId)) !==
              "live"
            ) {
              return "run-not-running" as const;
            }
            if (
              (await lockWorkflowRunState(tx, anchorRunId, anchorRunId)) !==
              "running"
            ) {
              return "run-not-running" as const;
            }
            await workflowDispatchService.enqueueSignal(
              {
                id: `dispatch:${anchorRunId}:${body.signalId}`,
                anchorRunId,
                signal: {
                  agentAddress,
                  runId: body.runId,
                  signalName: body.signalName,
                  signalId: body.signalId,
                  payload: body.payload ?? null,
                },
              },
              tx,
            );
            return "enqueued" as const;
          });
          if (enqueued !== "enqueued") {
            return c.json(
              {
                error: {
                  code:
                    enqueued === "run-not-running"
                      ? "workflow_run_not_running"
                      : "deployment_unreachable",
                  message:
                    enqueued === "run-not-running"
                      ? "Workflow run is no longer running"
                      : "Workflow deployment allocation is no longer active",
                },
              },
              409,
            );
          }
          // enqueueSignal may wake before the transaction commits.
          workflowDispatchService.wake();
          return c.body(null, 202);
        } catch (error) {
          if (error instanceof WorkflowRunDispatchPayloadConflictError) {
            return c.json(
              {
                error: {
                  code: "signal_id_conflict",
                  message:
                    "signalId has already been used with a different signal payload",
                },
              },
              409,
            );
          }
          throw error;
        }
      }

      try {
        sidecarRouter.sendSignalDeliver({
          agentAddress,
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

  app.post(
    "/:runId/mail",
    requireGrant(idResource("workflow-run", "runId"), "manage"),
    describeRoute({
      tags: ["Workflows"],
      summary: "Trigger a workflow run",
      description:
        "Delivers a fresh signed conversation message to the deployment's stable top-level run. The first accepted message fires that run; while it remains live, later messages may resume its onTrigger input. A terminal deployment run cannot be fired again. The returned messageId identifies this trigger occurrence.",
      responses: {
        202: {
          description: "Trigger accepted for delivery",
          content: {
            "application/json": {
              schema: resolver(WorkflowRunTriggerResponse),
            },
          },
        },
        400: {
          description:
            "Attachment validation error. Each variant carries a structured code (oversize_attachment, disallowed_mime_type, malformed_base64, oversize_total) with the offending index and limits. A malformed request body that fails SendMessage validation returns the generic error shape instead.",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        404: {
          description: "Workflow deployment not found",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        409: {
          description:
            "Deployment address is not routable, its allocation is no longer active, or its top-level run is terminal",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        413: {
          description: "Request body exceeds the maximum allowed size",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        503: {
          description: "Durable workflow dispatch unavailable",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
      },
    }),
    bodyLimit({
      maxSize: MAX_MAIL_BODY_BYTES,
      onError: (c) =>
        c.json(
          {
            error: {
              code: "payload_too_large",
              message: "Request body exceeds the maximum allowed size",
            },
          },
          413,
        ),
    }),
    validator("json", SendMessage),
    async (c) => {
      const result = await triggerWorkflowRun({
        tenant: c.get("tenant"),
        principal: c.get("principal"),
        userName: c.get("user")?.name ?? null,
        anchorRunId: c.req.param("runId"),
        message: c.req.valid("json"),
      });
      return c.json(result.body, result.status);
    },
  );

  app.get(
    "/:runId/runs",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Workflows"],
      summary: "List workflow runs",
      description:
        "Lists the run ids present in the deployment's workflow-run event log. Returns an empty list when no run has committed events yet.",
      responses: {
        200: {
          description: "List of run ids",
          content: {
            "application/json": {
              schema: resolver(WorkflowRunListResponse),
            },
          },
        },
        404: {
          description: "Workflow deployment not found",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
      },
    }),
    async (c) => {
      const tenant = c.get("tenant");
      const anchorRunId = c.req.param("runId");

      if (!(await deploymentAnchorRunExists(db, anchorRunId, tenant.id))) {
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

      const runIds = await runReader.listRunIds(
        workflowRunRepoId(anchorRunId, tenant.domain),
        WORKFLOW_RUN_REF,
      );
      return c.json({ runIds });
    },
  );

  app.get(
    "/:runId/runs/:eventRunId/events",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Workflows"],
      summary: "Read a workflow run's event log",
      description:
        "Returns the seq-ordered event projection (RunStarted, StepStarted, StepCompleted, SignalAwaited, RunCompleted, etc.) for a single run. The full event log is returned in ascending seq order; an unknown run returns an empty list.",
      responses: {
        200: {
          description: "Seq-ordered run events",
          content: {
            "application/json": {
              schema: resolver(WorkflowRunEventsResponse),
            },
          },
        },
        404: {
          description: "Workflow deployment not found",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
      },
    }),
    async (c) => {
      const tenant = c.get("tenant");
      const anchorRunId = c.req.param("runId");
      const runId = c.req.param("eventRunId");

      if (!(await deploymentAnchorRunExists(db, anchorRunId, tenant.id))) {
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

      // The inner :eventRunId is not independently tenant-checked, and does
      // not need to be: the events repo is addressed by the tenant-verified
      // run id and the caller's own tenant.domain, so an event run id only
      // selects within this tenant's deployment repo -- it cannot reach
      // another tenant's runs.
      const events = await runReader.readRunEvents(
        workflowRunRepoId(anchorRunId, tenant.domain),
        WORKFLOW_RUN_REF,
        runId,
      );
      return c.json({ runId, events: events.map(formatRunEvent) });
    },
  );

  return app;
}
