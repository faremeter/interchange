import { and, desc, eq } from "drizzle-orm";
import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import {
  asset,
  grant as grantTable,
  principal as principalTable,
  workflowDeployment,
} from "@intx/db/schema";
import type { DB } from "@intx/db";
import { createWorkflowRunStore } from "@intx/db";
import type { GrantStore } from "@intx/types/authz";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import { base64Encode, ErrorResponse, SendMessage } from "@intx/types";
import { InferenceSource } from "@intx/types/runtime";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  createWorkflowRunReader,
  workflowDefinitionEnvelopeSchema,
  WORKFLOW_JSON_PATH,
  type AssetService,
  type RepoId,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
  type WorkflowDefinition,
  type WorkflowRunEvent,
} from "@intx/hub-sessions";
import { generateId } from "@intx/hub-common";
import {
  deriveDeploymentAddress,
  deriveWorkflowRunRepoId,
  walkCapabilities,
  type CapabilityWalkResult,
} from "@intx/workflow-deploy";
import { createDefaultDirectorRegistry } from "@intx/agent";
import { GrantRequirement, type GrantEffect } from "@intx/types";
import type { RunGrantsFrame } from "@intx/types/sidecar";

import type { TenantEnv } from "../context";
import { idResource, type RequireGrant } from "../middleware/grant";
import { validateAttachments } from "../attachment-validation";
import {
  resolveGrantMaterialization,
  type MaterializedGrantRow,
} from "../grant-materialization";
import { ts } from "../format";

const GrantRequirements = GrantRequirement.array();

// DoS guard on the trigger route body. Sized identically to the agent
// mail route: above the legitimate ceiling (the 30 MB per-message
// attachment cap is ~40 MB once base64-encoded, plus JSON and text
// overhead) so over-business-cap requests are rejected by the handler
// with a structured error, while genuine garbage is rejected here
// before the JSON parser allocates a giant string.
const MAX_MAIL_BODY_BYTES = 44 * 1024 * 1024;

// Workflow-run events commit on the substrate's default branch; the
// supervisor wires the workflow-process child against this ref.
const WORKFLOW_RUN_REF = "refs/heads/main";

// The sidecar's deploy router keys the workflow-run repo by
// `deriveWorkflowRunRepoId(deploymentAddress)`, where the deployment
// address is `deriveDeploymentAddress({ deploymentId, deploymentDomain })`
// and `deploymentDomain` is the tenant's domain (see
// `deployWorkflowDefinition` in `@intx/hub-sessions`, which passes
// `deploymentDomain: tenant.domain`). The read side must reconstruct the
// identical address and apply the same sanitization, or it opens a
// different on-disk repo than the one events committed to.
function workflowRunRepoId(deploymentId: string, tenantDomain: string): RepoId {
  const deploymentAddress = deriveDeploymentAddress({
    deploymentId,
    deploymentDomain: tenantDomain,
  });
  return {
    kind: "workflow-run",
    id: deriveWorkflowRunRepoId(deploymentAddress),
  };
}

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
  repoStore: RepoStore;
  grantStore: GrantStore;
  requireGrant: RequireGrant;
};

export function createWorkflowRoutes({
  db,
  sessionService,
  sidecarRouter,
  assetService,
  repoStore,
  grantStore,
  requireGrant,
}: CreateWorkflowRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();
  const runReader = createWorkflowRunReader(repoStore);
  const workflowRunStore = createWorkflowRunStore(db);

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

      let result: Awaited<
        ReturnType<SessionService["deployWorkflowDefinition"]>
      >;
      try {
        result = await sessionService.deployWorkflowDefinition({
          tenantId: tenant.id,
          deploymentId,
          deploymentDomain: tenant.domain,
          definition,
          definitionAssetId: assetRow.id,
          config,
          deployContent: { systemPrompt: "" },
        });
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

      // The sidecar deploy succeeded; reading back the projection row is
      // an internal persistence concern. A missing row here is an
      // invariant violation, not a sidecar-reachability failure, so it
      // must surface as a 500 rather than be mislabeled 502.
      const row = await db.query.workflowDeployment.findFirst({
        where: eq(workflowDeployment.id, result.deploymentId),
      });
      if (!row) {
        return c.json(
          {
            error: {
              code: "deployment_projection_missing",
              message: `workflow_deployment row ${result.deploymentId} missing after deploy`,
            },
          },
          500,
        );
      }
      return c.json(formatDeployment(row), 201);
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

  app.post(
    "/:deploymentId/mail",
    requireGrant(idResource("workflow-run", "deploymentId"), "manage"),
    describeRoute({
      tags: ["Workflows"],
      summary: "Trigger a workflow run",
      description:
        "Assembles a fresh signed conversation message and delivers it to the deployment's inbound mail address, starting a new workflow run. The run id is minted by the supervisor and is not returned synchronously; correlate the resulting RunStarted via the returned messageId.",
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
          description: "Deployment address is not routable",
          content: { "application/json": { schema: resolver(ErrorResponse) } },
        },
        413: {
          description: "Request body exceeds the maximum allowed size",
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

      // Decode and validate attachments at the boundary, emitting
      // ordered, per-index structured errors, exactly as the agent mail
      // route does.
      const attachmentResult = validateAttachments(body.attachments ?? []);
      if (!attachmentResult.ok) {
        return c.json({ error: attachmentResult.error }, 400);
      }
      const messageAttachments = attachmentResult.attachments;

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

      const address = `ins_${deploymentId}@${tenant.domain}`;
      const messageId = `<${generateId("sessionMail")}@${tenant.domain}>`;
      const fromAddr = `${principal.refId}@${tenant.domain}`;
      const user = c.get("user");
      const from = user?.name ? `"${user.name}" <${fromAddr}>` : fromAddr;

      // The runId the supervisor mints for this trigger is the mail's
      // Message-ID header verbatim (see `deriveMessageId` in
      // `@intx/workflow-host`), so it equals `messageId` byte-identically.
      // The run principal and its grants key off it.
      const runId = messageId;

      // Derive the run's authorization grants from the deployment's workflow
      // definition and stage them on a fresh run principal. Nothing is
      // written to the database until the trigger is accepted for delivery:
      // an unroutable deployment (409 below) must not leave an orphaned
      // principal or grant rows behind.
      let definition: WorkflowDefinition;
      try {
        definition = await hydrateDefinition(
          assetService,
          row.definitionAssetId,
        );
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

      // Load the workflow asset row for its `creatorPrincipalId`: the CREATOR
      // whose authority creator-sourced grant requirements resolve against.
      // The deployment row alone does not carry the creator identity.
      const assetRow = await db.query.asset.findFirst({
        where: and(
          eq(asset.id, row.definitionAssetId),
          eq(asset.tenantId, tenant.id),
          eq(asset.kind, "workflow"),
        ),
      });
      if (!assetRow) {
        return c.json(
          {
            error: {
              code: "invalid_workflow",
              message: `Workflow asset ${row.definitionAssetId} not found`,
            },
          },
          409,
        );
      }

      const runPrincipalId = generateId("principal");
      const now = new Date();

      // The runtime-enforced grant surface is the run's definition-pure grant
      // content. The walk yields `tool:<name>` grants (one per tool a step's
      // agent declares, carrying the tool's static effect) and `effect:<cap>`
      // grants (one per capability an action step's `effect.requires`
      // declares). Both are authorized fail-closed at runtime -- the agent
      // harness gates `tool:` use and the action EffectContext gates
      // `effect:` use, each throwing on a non-allow decision. They materialize
      // DIRECTLY as creator-origin `grant` rows (`action: invoke`) -- unlike
      // declared grantRequirements, they are not gated on creator authority.
      // The walk's capability:/director:/inference.source:/mail.* strings are
      // deploy-time approval concerns, not runtime authority, and are excluded.
      const directorRegistry = createDefaultDirectorRegistry();
      const walk = walkCapabilities(definition, directorRegistry);
      const runtimeGrantRows = deriveRunRuntimeGrantRows(
        walk,
        tenant.id,
        runPrincipalId,
        now,
      );

      // Declared grantRequirements resolve through the shared creator/invoker
      // delegation path. Invoker-sourced requirements resolve against this
      // trigger's caller and creator-sourced requirements against the
      // workflow asset's creator, so two runs triggered by principals with
      // different authority materialize different grants.
      const declaredGrantRequirements = GrantRequirements(
        definition.grantRequirements ?? [],
      );
      if (declaredGrantRequirements instanceof type.errors) {
        return c.json(
          {
            error: {
              code: "invalid_workflow",
              message: `Invalid grant requirements: ${declaredGrantRequirements.summary}`,
            },
          },
          409,
        );
      }
      const invokerGrants = await grantStore.collectGrants(
        principal.id,
        tenant.id,
      );

      // Creator-sourced requirements resolve against the workflow ASSET's
      // creator (`asset.creatorPrincipalId`), mirroring the agent-instance
      // path's creator resolution. Only collect the creator's grants when a
      // creator-sourced requirement actually exists and the asset records a
      // creator. `creatorPrincipalId` is nullable (the FK is `set null` on
      // principal deletion); when a creator-sourced requirement exists but
      // the creator is null, we intentionally leave `creatorGrants` empty and
      // let `resolveGrantMaterialization` fail closed with its 403
      // `insufficient_grants` rather than inventing a fallback principal.
      const hasCreatorReqs = declaredGrantRequirements.some(
        (r) => r.source === "creator",
      );
      const creatorPrincipalId = assetRow.creatorPrincipalId;
      const creatorGrants =
        hasCreatorReqs && creatorPrincipalId !== null
          ? await grantStore.collectGrants(creatorPrincipalId, tenant.id)
          : [];

      const materialization = await resolveGrantMaterialization({
        tenantId: tenant.id,
        targetPrincipalId: runPrincipalId,
        grantRequirements: declaredGrantRequirements,
        adHocInvokerGrants: [],
        invokerGrants,
        creatorGrants,
        now,
      });
      if (!materialization.ok) {
        const { status, code, message } = materialization.rejection;
        return c.json({ error: { code, message } }, status);
      }
      // The walk's `tool:`/`effect:` grants and the resolved declared
      // requirements share the run principal's grant namespace with no unique
      // constraint on (resource, action). An overlap on the same
      // (resource, action) is resolved by effect precedence at authz time
      // (`deny > ask > allow`, strongest wins), NOT by union of the rows here.
      const stagedGrantRows = [
        ...runtimeGrantRows,
        ...materialization.grantRows,
      ];

      // A run trigger is threading-less by construction: it starts a new
      // run rather than continuing a conversation, so no inReplyTo or
      // references are stamped. This is the same fresh-signed-message
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

      // The run's grants ride the wire in the same validated encoding the
      // agent.deploy frame's `config.grants` uses. Project the staged rows
      // directly rather than reading them back through `collectGrants`: the
      // DB write is deferred until delivery is accepted (below), so the frame
      // carries exactly the rows that will be committed.
      const stepGrants = stagedGrantRows.map((g) => runGrantToWire(g));

      // Send the run's grants BEFORE the trigger mail. Both frames route
      // through the same per-address channel, so same-websocket FIFO
      // ordering guarantees the grants land at the sidecar before the mail
      // that dispatches the run -- no ack round-trip is needed. A `false`
      // here means the deployment is unroutable; abandon the run without
      // committing any authz state (no orphaned principal or grant rows).
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

      const delivered = sidecarRouter.routeMail(address, base64);
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

      // Commit the run principal and its grants after delivery is accepted,
      // so the 409 path above never leaves orphaned authz state behind. The
      // principal is a bare `workflow`-kind row keyed on the runId, mirroring
      // the per-instance principal the agent.deploy path mints.
      //
      // The run row has two co-writers keyed on the same runId: this path and
      // the lazy anchor in signal-correlation registration, which fires if the
      // run parks on an approval before this commits and inserts the row with
      // a null principal. `anchorWithPrincipal` is therefore conflict-tolerant
      // and, on a prior co-write insert, reconciles by attaching this run's
      // principal, so terminal deactivation still finds one.
      await db.transaction(async (tx) => {
        await tx.insert(principalTable).values({
          id: runPrincipalId,
          tenantId: tenant.id,
          kind: "workflow",
          refId: runId,
          status: "active",
          createdAt: now,
          updatedAt: now,
        });
        await workflowRunStore.anchorWithPrincipal(
          {
            id: runId,
            deploymentId,
            tenantId: tenant.id,
            principalId: runPrincipalId,
            status: "running",
          },
          tx,
        );
        for (const g of stagedGrantRows) {
          await tx.insert(grantTable).values(g);
        }
      });

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

// Grant-string prefixes the capability walk emits that carry RUNTIME
// authority, each authorized fail-closed at run time:
//   - `tool:<name>`  -- gated by the agent harness before a tool call.
//   - `effect:<cap>` -- gated by the action EffectContext before an effect
//                       (`authorize("effect:<cap>", "invoke")`, throws unless
//                       the decision is allow).
// The walk's other strings (capability:/director:/inference.source:/mail.*)
// are deploy-time approval concerns, not runtime authority, so they do not
// materialize as run grant rows.
//
// The `tool:<name>` rows carry BARE tool names: the walk reads inline
// `agent.toolFactories`, which have no bundle context. A workflow child gates
// each tool call on `tool:<call.name>`, and every runnable step tool is a
// pinned package the loader namespaces to `<bundleId>:<name>`, so the child
// queries `tool:<bundleId>:<name>`. These bare rows therefore never address a
// pinned tool's runtime gate; they are inert against a pinned call. A pinned
// tool's authority (including its `ask` mark) is supplied instead by the
// sidecar tool-mark floor (`deriveToolMarkFloorGrants`), derived from the
// loaded factory's already-namespaced definitions. The `effect:<cap>` rows are
// different: an action's EffectContext authorizes the bare `effect:<cap>` on
// both sides, so those rows ARE name-matched and operative at run time.
const TOOL_GRANT_PREFIX = "tool:";
const EFFECT_GRANT_PREFIX = "effect:";

/**
 * Project the capability walk into the run's runtime grant rows -- the
 * `tool:<name>` and `effect:<cap>` grants the runtime enforces fail-closed.
 * Every distinct grant string across all steps becomes one creator-origin
 * `grant` row with `action: invoke`. The run's runtime authority is
 * definition-pure: identical across every run of the deployment, so the walk
 * output alone determines it.
 *
 * Tool grants carry the effect the tool's static declaration requested (`ask`
 * for approval-gated tools, `allow` otherwise) via the walk's `grantEffects`
 * map. A tool in more than one step is emitted once; when two steps disagree
 * on its effect, `ask` wins over `allow` so an approval-gated declaration is
 * never silently downgraded.
 *
 * Effect grants are always `allow` -- the `effect.requires` set names the
 * capability floor an action needs, with no per-effect ask/allow distinction,
 * so they are NOT routed through the `grantEffects` map (which covers tool
 * grants only). An `effect:<cap>` in more than one step is emitted once.
 */
export function deriveRunRuntimeGrantRows(
  walk: CapabilityWalkResult,
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow[] {
  const effectByResource = new Map<string, GrantEffect>();
  for (const declarations of walk.perStep.values()) {
    for (const grant of declarations.grants) {
      if (grant.startsWith(TOOL_GRANT_PREFIX)) {
        // Every `tool:` grant the walk emits carries a `grantEffects`
        // entry (the tool-mark floor: `ask` for an approval-gated tool,
        // `allow` otherwise). A missing entry means the walk's `grants`
        // and `grantEffects` maps have diverged -- a defaulted `allow`
        // here would silently DOWNGRADE an `ask` tool below its floor,
        // defeating the approval gate. Fail loudly instead.
        const effect = declarations.grantEffects.get(grant);
        if (effect === undefined) {
          throw new Error(
            `deriveRunRuntimeGrantRows: tool grant ${JSON.stringify(grant)} has no grantEffects entry; the capability walk must emit an effect for every tool grant`,
          );
        }
        const existing = effectByResource.get(grant);
        if (existing === "ask" || effect === "ask") {
          effectByResource.set(grant, "ask");
        } else if (existing === undefined) {
          effectByResource.set(grant, effect);
        }
      } else if (grant.startsWith(EFFECT_GRANT_PREFIX)) {
        // Effect grants are always allow; a repeat across steps is idempotent.
        if (!effectByResource.has(grant)) {
          effectByResource.set(grant, "allow");
        }
      }
    }
  }

  const rows: MaterializedGrantRow[] = [];
  for (const [resource, effect] of effectByResource) {
    rows.push({
      id: generateId("grant"),
      tenantId,
      principalId: runPrincipalId,
      resource,
      action: "invoke",
      effect,
      conditions: null,
      origin: "creator",
      expiresAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  return rows;
}

/**
 * Project a materialized run grant row into the `run.grants` wire shape --
 * the same `WireGrantRule` encoding the `agent.deploy` frame's
 * `config.grants` ships. A run grant is always principal-scoped and never
 * role-scoped, so `roleId` is null and `principalId` is the run principal.
 */
function runGrantToWire(
  row: MaterializedGrantRow,
): RunGrantsFrame["stepGrants"][number] {
  return {
    id: row.id,
    resource: row.resource,
    action: row.action,
    effect: row.effect,
    origin: row.origin,
    conditions: row.conditions,
    expiresAt: row.expiresAt,
    roleId: null,
    principalId: row.principalId,
  };
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
