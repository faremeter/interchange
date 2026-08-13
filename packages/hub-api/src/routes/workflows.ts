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
import {
  WorkflowRunDispatchPayloadConflictError,
  type DB,
  type DBExecutor,
} from "@intx/db";
import type { GrantStore } from "@intx/types/authz";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  base64Encode,
  correlationIdFromSignalName,
  deriveWorkflowRunId,
  ErrorResponse,
  isSidecarAllocationDispatchable,
  SendMessage,
  type SidecarAllocationStatus,
} from "@intx/types";
import { InferenceSource } from "@intx/types/runtime";
import type { HarnessConfig } from "@intx/types/runtime";
import type { RunGrantsFrame } from "@intx/types/sidecar";
import { ToolPackagePinArray } from "@intx/types/tool-packages";
import {
  createWorkflowRunReader,
  ExclusiveWorkflowPlacementError,
  resolveWorkflowSidecarPlacement,
  type AssetService,
  type RepoId,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
  type WorkflowDefinition,
  type WorkflowRunEvent,
  type WorkflowAllocationService,
  type WorkflowDispatchService,
} from "@intx/hub-sessions";
import { deriveRunPrincipalId, generateId } from "@intx/hub-common";
import {
  deriveRunAddress,
  deriveRunAgentId,
  WorkflowDefinitionInvalidError,
} from "@intx/workflow-deploy";

import type { TenantEnv } from "../context";
import { idResource, type RequireGrant } from "../middleware/grant";
import { validateAttachments } from "../attachment-validation";
import {
  collectCreatorGrants,
  commitRunGrants,
  hydrateDefinition,
  loadCommittedRunGrants,
  lockWorkflowRunState,
  parseGrantRequirements,
  stageRunGrants,
} from "../run-grant-materialization";
import { ts } from "../format";
import type { MaterializedGrantRow } from "../grant-materialization";
import {
  readDurableWorkflowRunLifecycle,
  workflowRunRepoIdForAddress,
  WORKFLOW_RUN_REF,
} from "../workflow-run-lifecycle";

// DoS guard on the trigger route body. Sized identically to the agent
// mail route: above the legitimate ceiling (the 30 MB per-message
// attachment cap is ~40 MB once base64-encoded, plus JSON and text
// overhead) so over-business-cap requests are rejected by the handler
// with a structured error, while genuine garbage is rejected here
// before the JSON parser allocates a giant string.
const MAX_MAIL_BODY_BYTES = 44 * 1024 * 1024;

// The sidecar's deploy router keys the workflow-run repo by
// `deriveWorkflowRunRepoId(deploymentAddress)`, where the deployment
// address is `deriveRunAddress({ runId, domain })`
// and `deploymentDomain` is the tenant's domain (see
// `deployWorkflowDefinition` in `@intx/hub-sessions`, which passes
// `deploymentDomain: tenant.domain`). The read side must reconstruct the
// identical address and apply the same sanitization, or it opens a
// different on-disk repo than the one events committed to.
function workflowRunRepoId(deploymentId: string, tenantDomain: string): RepoId {
  const deploymentAddress = deriveRunAddress({
    runId: deploymentId,
    domain: tenantDomain,
  });
  return workflowRunRepoIdForAddress(deploymentAddress);
}

async function lockDispatchableAllocation(
  tx: DBExecutor,
  allocationId: string,
  anchorRunId: string,
): Promise<boolean> {
  const [allocation] = await tx
    .select({ status: sidecarAllocation.status })
    .from(sidecarAllocation)
    .where(
      and(
        eq(sidecarAllocation.id, allocationId),
        eq(sidecarAllocation.anchorRunId, anchorRunId),
      ),
    )
    .limit(1)
    .for("update");
  return (
    allocation !== undefined &&
    isSidecarAllocationDispatchable(allocation.status)
  );
}

// Request body for the general workflow deploy. The workflow definition
// is hydrated from `assetId`'s `workflow.json`; the caller supplies the
// inference sources the per-step agents launch against (full credential
// resolution is the agent-instance path's concern, not this one).
const DeployWorkflow = type({
  assetId: "string",
  sources: InferenceSource.array(),
  defaultSource: "string",
  "toolPackages?": ToolPackagePinArray,
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

// Response for the run-trigger route. The trigger fires a mail at the
// deployment address; the run id is minted by the supervisor on the
// sidecar side and is not known synchronously here, so the caller
// correlates the downstream RunStarted via the returned messageId.
const WorkflowRunTriggerResponse = type({
  deploymentId: "string",
  address: "string",
  messageId: "string",
});

const WorkflowRunListResponse = type({
  runIds: "string[]",
});

// A single committed workflow-run event. `type` is the discriminator;
// `body` carries the full per-type payload verbatim (the workflow-run
// kind handler validates the shape at push time).
const WorkflowRunEventResponse = type({
  seq: "number",
  type: "string",
  body: "Record<string, unknown>",
});

const WorkflowRunEventsResponse = type({
  runId: "string",
  events: WorkflowRunEventResponse.array(),
});

function formatRunEvent(event: WorkflowRunEvent) {
  return { seq: event.seq, type: event.type, body: event.body };
}

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
// the deployment id, carrying its routing identity. The `deploymentId` is
// non-null on a deployment-anchored run, distinguishing it from a folded-agent
// run (which never shares a deployment id anyway).
async function deploymentAnchorRunExists(
  db: DB["db"],
  deploymentId: string,
  tenantId: string,
): Promise<boolean> {
  const [row] = await db
    .select({ id: workflowRun.id })
    .from(workflowRun)
    .where(
      and(
        eq(workflowRun.id, deploymentId),
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
  assetService: AssetService;
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
  assetService,
  repoStore,
  grantStore,
  requireGrant,
}: CreateWorkflowRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();
  const runReader = createWorkflowRunReader(repoStore);

  async function readRunLifecycle(
    deploymentId: string,
    tenantDomain: string,
    runId: string,
  ) {
    const deploymentAddress = deriveRunAddress({
      runId: deploymentId,
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
        500: {
          description: "Deployment projection row missing after deploy",
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

      // A single-step deploy pins its full ordered inference chain and the
      // reactor activates the head, so the default source must be the chain
      // head. Reject a contradictory ordering here at the edge with a
      // caller-facing message rather than letting it fall through to the
      // orchestrator's internal invariant guard, which speaks in reactor terms.
      // Multi-step deploys select a source per step, so their deploy-wide
      // ordering is unconstrained and this check does not apply.
      if (
        definition.stepOrder.length === 1 &&
        firstSource.id !== body.defaultSource
      ) {
        return c.json(
          {
            error: {
              code: "invalid_workflow",
              message:
                "defaultSource must be the first entry in sources: a single-step deploy runs the default at the head of its pinned chain",
            },
          },
          409,
        );
      }

      let placement;
      try {
        placement = await resolveWorkflowSidecarPlacement(
          db,
          tenant.id,
          definition,
        );
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

      const deploymentId = generateId("workflowRun");
      const sessionId = generateId("session");
      const config: HarnessConfig = {
        sessionId,
        agentId: deriveRunAgentId({ runId: deploymentId }),
        tenantId: tenant.id,
        principalId: c.get("principal").id,
        agentAddress: deriveRunAddress({
          runId: deploymentId,
          domain: tenant.domain,
        }),
        systemPrompt: "",
        tools: [],
        grants: [],
        sources: body.sources,
        defaultSource: body.defaultSource,
      };

      let deployedId: string;
      let deploymentStatus = "deployed";
      if (placement?.sharing === "exclusive") {
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
              deploymentId,
              deploymentDomain: tenant.domain,
              definition,
              definitionAssetId: assetRow.id,
              placement,
              sessionId,
              sourceAuthorityPrincipalId: c.get("principal").id,
              sourceOfferingIds: body.sources.map((source) => source.id),
              defaultSourceOfferingId: body.defaultSource,
              deployContent: { systemPrompt: "" },
              ...(body.toolPackages !== undefined
                ? { toolPackagePins: body.toolPackages }
                : {}),
            });
          deployedId = prepared.deploymentId;
          deploymentStatus = prepared.status;
        } catch (err) {
          if (err instanceof ExclusiveWorkflowPlacementError) {
            return c.json(
              {
                error: {
                  code: err.code,
                  message: err.message,
                },
              },
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
        try {
          const result = await sessionService.deployWorkflowDefinition({
            tenantId: tenant.id,
            deploymentId,
            deploymentDomain: tenant.domain,
            definition,
            definitionAssetId: assetRow.id,
            config,
            deployContent: { systemPrompt: "" },
            ...(body.toolPackages !== undefined
              ? { toolPackagePins: body.toolPackages }
              : {}),
          });
          deployedId = result.deploymentId;
        } catch (err) {
          // A single-step deploy whose source chain is invalid (head is not the
          // default source, or a chain source the operator never approved) is a
          // client/definition error, not a sidecar-reachability failure.
          if (err instanceof WorkflowDefinitionInvalidError) {
            return c.json(
              {
                error: {
                  code: "invalid_workflow",
                  message: err.message,
                },
              },
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
      const deploymentId = c.req.param("deploymentId");
      const body = c.req.valid("json");
      const agentAddress = deriveRunAddress({
        runId: deploymentId,
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
            eq(workflowRun.id, deploymentId),
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
        deploymentId,
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
              !(await lockDispatchableAllocation(
                tx,
                allocationId,
                deploymentId,
              ))
            ) {
              return "allocation-unavailable" as const;
            }
            // Pack receipt advances Git while holding this allocation lock. Read
            // again after acquiring it so the preflight result cannot go stale
            // while this transaction waits behind a terminal pack.
            if (
              (await readRunLifecycle(deploymentId, tenant.domain, runId)) !==
              "live"
            ) {
              return "run-not-running" as const;
            }
            if (
              (await lockWorkflowRunState(tx, deploymentId, deploymentId)) !==
                "running" ||
              (await lockWorkflowRunState(tx, deploymentId, runId)) !==
                "running"
            ) {
              return "run-not-running" as const;
            }
            await workflowDispatchService.enqueueSignal(
              {
                id: `dispatch:${deploymentId}:${body.signalId}`,
                anchorRunId: deploymentId,
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
    "/:deploymentId/mail",
    requireGrant(idResource("workflow-run", "deploymentId"), "manage"),
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
      const tenant = c.get("tenant");
      const principal = c.get("principal");
      const deploymentId = c.req.param("deploymentId");
      const body = c.req.valid("json");
      const address = deriveRunAddress({
        runId: deploymentId,
        domain: tenant.domain,
      });
      const runId = deriveWorkflowRunId(address);
      const topLevelRun = alias(workflowRun, "mail_top_level_run");

      // Decode and validate attachments at the boundary, emitting
      // ordered, per-index structured errors, exactly as the agent mail
      // route does.
      const attachmentResult = validateAttachments(body.attachments ?? []);
      if (!attachmentResult.ok) {
        return c.json({ error: attachmentResult.error }, 400);
      }
      const messageAttachments = attachmentResult.attachments;

      // Resolve the deployment's anchor run and, through its definition, the
      // workflow asset the trigger's grants derive from. The inner join to the
      // definition yields the asset id and the definition id in one read, off
      // the run rather than the deployment projection.
      const [anchor] = await db
        .select({
          definitionId: workflowRun.definitionId,
          definitionAssetId: workflowDefinition.assetId,
          allocationId: sidecarAllocation.id,
          allocationStatus: sidecarAllocation.status,
          anchorStatus: workflowRun.status,
          runStatus: topLevelRun.status,
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
        .leftJoin(
          topLevelRun,
          and(
            eq(topLevelRun.id, runId),
            eq(topLevelRun.anchorRunId, workflowRun.id),
          ),
        )
        .where(
          and(
            eq(workflowRun.id, deploymentId),
            eq(workflowRun.tenantId, tenant.id),
            isNotNull(workflowRun.anchorRunId),
          ),
        )
        .limit(1);
      if (anchor === undefined) {
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
      const durableLifecycle = await readRunLifecycle(
        deploymentId,
        tenant.domain,
        runId,
      );
      // A "deployed" anchor is live: mail-triggering it IS its first trigger.
      // The durable check here only rejects "terminal" (an absent durable log is
      // the valid pre-start state), so the status axis must accept "deployed" or
      // the first mail trigger of a freshly-deployed run would 409 as terminal.
      if (
        !isLiveWorkflowRunStatus(anchor.anchorStatus) ||
        (anchor.runStatus !== null &&
          !isLiveWorkflowRunStatus(anchor.runStatus)) ||
        durableLifecycle === "terminal"
      ) {
        return c.json(
          {
            error: {
              code: "workflow_run_terminal",
              message: `Workflow run ${runId} is terminal and cannot receive more mail`,
            },
          },
          409,
        );
      }
      if (
        anchor.allocationStatus !== null &&
        !isSidecarAllocationDispatchable(anchor.allocationStatus)
      ) {
        return c.json(
          {
            error: {
              code: "deployment_unreachable",
              message: `Workflow deployment allocation is ${anchor.allocationStatus}`,
            },
          },
          409,
        );
      }
      const definitionAssetId = anchor.definitionAssetId;
      if (definitionAssetId === null) {
        // A native workflow definition names its asset; a null here is a
        // corrupt definition the trigger cannot hydrate from.
        return c.json(
          {
            error: {
              code: "invalid_workflow",
              message: "Workflow definition has no asset",
            },
          },
          409,
        );
      }

      const messageId = `<${generateId("sessionMail")}@${tenant.domain}>`;
      const fromAddr = `${principal.refId}@${tenant.domain}`;
      const user = c.get("user");
      const from = user?.name ? `"${user.name}" <${fromAddr}>` : fromAddr;

      const now = new Date();
      let runPrincipalId: string;
      let stagedGrantRows: MaterializedGrantRow[];
      let stepGrants: RunGrantsFrame["stepGrants"];
      const committedRunGrants = await loadCommittedRunGrants(
        db,
        tenant.id,
        runId,
      );
      if (committedRunGrants !== null) {
        // This is another trigger occurrence for the one live top-level run.
        // Reuse its immutable authorization snapshot; recomputing from the new
        // caller would let one run change authority between sections.
        runPrincipalId = committedRunGrants.runPrincipalId;
        stagedGrantRows = [];
        stepGrants = committedRunGrants.stepGrants;
      } else {
        let definition: WorkflowDefinition;
        try {
          definition = await hydrateDefinition(assetService, definitionAssetId);
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
              error: {
                code: "invalid_workflow",
                message: `Workflow asset ${definitionAssetId} not found`,
              },
            },
            409,
          );
        }

        runPrincipalId = await deriveRunPrincipalId(tenant.id, runId);
        const parsedRequirements = parseGrantRequirements(definition);
        if (!parsedRequirements.ok) {
          return c.json(
            {
              error: {
                code: "invalid_workflow",
                message: parsedRequirements.message,
              },
            },
            409,
          );
        }
        const declaredGrantRequirements = parsedRequirements.requirements;
        const invokerGrants = await grantStore.collectGrants(
          principal.id,
          tenant.id,
        );
        const creatorGrants = await collectCreatorGrants(
          grantStore,
          tenant.id,
          assetRow.creatorPrincipalId,
          declaredGrantRequirements,
        );
        const staged = await stageRunGrants({
          definition,
          tenantId: tenant.id,
          runPrincipalId,
          now,
          invokerGrants,
          creatorGrants,
          grantRequirements: declaredGrantRequirements,
        });
        if (!staged.ok) {
          const { status, code, message } = staged.rejection;
          return c.json({ error: { code, message } }, status);
        }
        stagedGrantRows = staged.grantRows;
        stepGrants = staged.stepGrants;
      }

      // A trigger occurrence is threading-less at the mail boundary, so no
      // inReplyTo or references are stamped. The supervisor decides whether
      // this first-fires the absent top-level log or resumes a live onTrigger
      // input. This is the same fresh-signed-message
      // shape the deploy-flow fixture's mail trigger and the production
      // session-service mail path assemble. The route does not route
      // through sessionService.sendUserMessage because that path stamps
      // interchangeSessionId/agentId headers that scope the message to an
      // agent session; a workflow run trigger has no such session.
      const keyPair = await generateKeyPair();
      const crypto = createEd25519Crypto(keyPair);
      const headers: MessageHeaders = {
        from,
        to: [address],
        cc: undefined,
        date: new Date(),
        messageId,
        subject: undefined,
        inReplyTo: undefined,
        references: undefined,
        mimeVersion: "1.0",
        interchangeType: "conversation.message",
        interchangeCorrelationId: undefined,
        interchangeTenantId: tenant.id,
        interchangeAgentId: undefined,
        interchangeSessionId: undefined,
        interchangeOfferingId: undefined,
        interchangeSchemaVersion: undefined,
        traceparent: undefined,
        tracestate: undefined,
      };
      const signedContent = assembleSignedContent({
        kind: "conversation",
        text: body.content,
        ...(messageAttachments.length > 0
          ? { attachments: messageAttachments }
          : {}),
      });
      const signature = await createDetachedSignatureFromProvider(
        signedContent,
        crypto,
      );
      const rawMessage = assembleMessage(headers, signedContent, signature);
      const base64 = base64Encode(rawMessage);

      const allocationId = anchor.allocationId;
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
        const committed = await db.transaction(async (tx) => {
          if (
            !(await lockDispatchableAllocation(tx, allocationId, deploymentId))
          ) {
            return "allocation-unavailable" as const;
          }
          // The allocation lock serializes this read with authoritative pack
          // advancement. An absent run is still valid for its first mail.
          if (
            (await readRunLifecycle(deploymentId, tenant.domain, runId)) ===
            "terminal"
          ) {
            return "run-terminal" as const;
          }
          if (
            (await lockWorkflowRunState(tx, deploymentId, deploymentId)) !==
              "running" ||
            (await lockWorkflowRunState(tx, deploymentId, runId)) === "terminal"
          ) {
            return "run-terminal" as const;
          }
          const canonicalStepGrants = await commitRunGrants(
            {
              db,
              tenantId: tenant.id,
              deploymentId,
              definitionId: anchor.definitionId,
              runId,
              runPrincipalId,
              now,
              grantRows: stagedGrantRows,
            },
            tx,
          );
          await workflowDispatchService.enqueue(
            {
              id: `dispatch:${deploymentId}:${messageId}`,
              anchorRunId: deploymentId,
              messageId,
              rawMessage,
              stepGrants: canonicalStepGrants,
              now,
            },
            tx,
          );
          return "committed" as const;
        });
        if (committed !== "committed") {
          return c.json(
            {
              error: {
                code:
                  committed === "run-terminal"
                    ? "workflow_run_terminal"
                    : "deployment_unreachable",
                message:
                  committed === "run-terminal"
                    ? `Workflow run ${runId} is terminal and cannot receive more mail`
                    : "Workflow deployment allocation is no longer active",
              },
            },
            409,
          );
        }
        // enqueue may wake before the transaction commits.
        workflowDispatchService.wake();
        return c.json({ deploymentId, address, messageId }, 202);
      }

      const reserved = await db.transaction(async (tx) => {
        if (
          (await lockWorkflowRunState(tx, deploymentId, deploymentId)) !==
            "running" ||
          (await lockWorkflowRunState(tx, deploymentId, runId)) === "terminal"
        ) {
          return null;
        }
        return commitRunGrants(
          {
            db,
            tenantId: tenant.id,
            deploymentId,
            definitionId: anchor.definitionId,
            runId,
            runPrincipalId,
            now,
            grantRows: stagedGrantRows,
          },
          tx,
        );
      });
      if (reserved === null) {
        return c.json(
          {
            error: {
              code: "workflow_run_terminal",
              message: `Workflow run ${runId} is terminal and cannot receive more mail`,
            },
          },
          409,
        );
      }
      stepGrants = reserved;

      // Send the run's grants BEFORE the trigger mail. Both frames route
      // through the same per-address channel, so same-websocket FIFO
      // ordering guarantees the grants land at the sidecar before the mail
      // that dispatches the run -- no ack round-trip is needed. Reserving the
      // grants first makes concurrent deliveries converge on this exact
      // snapshot. If routing fails, the grants-only run remains eligible for
      // its first fire because Git has no RunStarted event yet.
      const grantsDelivered = sidecarRouter.sendRunGrants(
        address,
        runId,
        stepGrants,
      );
      if (!grantsDelivered) {
        return c.json(
          {
            error: {
              code: "deployment_unreachable",
              message: `Deployment address ${address} is not routable`,
            },
          },
          409,
        );
      }

      const delivered = sidecarRouter.routeMail(address, base64, messageId);
      if (!delivered) {
        return c.json(
          {
            error: {
              code: "deployment_unreachable",
              message: `Deployment address ${address} is not routable`,
            },
          },
          409,
        );
      }

      return c.json({ deploymentId, address, messageId }, 202);
    },
  );

  app.get(
    "/:deploymentId/runs",
    requireGrant(idResource("workflow-run", "deploymentId"), "read"),
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
      const deploymentId = c.req.param("deploymentId");

      if (!(await deploymentAnchorRunExists(db, deploymentId, tenant.id))) {
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
        workflowRunRepoId(deploymentId, tenant.domain),
        WORKFLOW_RUN_REF,
      );
      return c.json({ runIds });
    },
  );

  app.get(
    "/:deploymentId/runs/:runId/events",
    requireGrant(idResource("workflow-run", "deploymentId"), "read"),
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
      const deploymentId = c.req.param("deploymentId");
      const runId = c.req.param("runId");

      if (!(await deploymentAnchorRunExists(db, deploymentId, tenant.id))) {
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

      // The inner :runId is not independently tenant-checked, and does not
      // need to be: the events repo is addressed by the tenant-verified
      // deploymentId and the caller's own tenant.domain, so a runId only
      // selects within this tenant's deployment repo -- it cannot reach
      // another tenant's runs.
      const events = await runReader.readRunEvents(
        workflowRunRepoId(deploymentId, tenant.domain),
        WORKFLOW_RUN_REF,
        runId,
      );
      return c.json({ runId, events: events.map(formatRunEvent) });
    },
  );

  return app;
}
