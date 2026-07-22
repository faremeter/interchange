import { and, desc, eq } from "drizzle-orm";
import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import { asset, workflowDeployment } from "@intx/db/schema";
import type { DB } from "@intx/db";
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
  type AssetService,
  type RepoId,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
  type WorkflowDefinition,
  type WorkflowRunEvent,
} from "@intx/hub-sessions";
import { deriveRunPrincipalId, generateId } from "@intx/hub-common";
import {
  deriveDeploymentAddress,
  deriveWorkflowRunRepoId,
} from "@intx/workflow-deploy";

import type { TenantEnv } from "../context";
import { idResource, type RequireGrant } from "../middleware/grant";
import { validateAttachments } from "../attachment-validation";
import {
  collectCreatorGrants,
  commitRunGrants,
  hydrateDefinition,
  parseGrantRequirements,
  stageRunGrants,
} from "../run-grant-materialization";
import { ts } from "../format";

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

      // Derive the run principal id from `(tenantId, runId)` so a
      // redelivery of the same trigger mints the same principal id, keeping
      // the mint idempotent on the runId shared with the mail-triggered
      // path.
      const runPrincipalId = await deriveRunPrincipalId(tenant.id, runId);
      const now = new Date();

      // Declared grantRequirements resolve through the shared creator/invoker
      // delegation path. Invoker-sourced requirements resolve against this
      // trigger's caller and creator-sourced requirements against the
      // workflow asset's creator, so two runs triggered by principals with
      // different authority materialize different grants.
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
      // Creator-sourced requirements resolve against the workflow ASSET's
      // creator (`asset.creatorPrincipalId`). When a creator-sourced
      // requirement exists but the creator is null (the FK is `set null` on
      // principal deletion), the grants stay empty and the staging below
      // fails closed with its 403 rather than inventing a fallback principal.
      const creatorGrants = await collectCreatorGrants(
        grantStore,
        tenant.id,
        assetRow.creatorPrincipalId,
        declaredGrantRequirements,
      );

      // Stage the run's grants: the walk's `tool:`/`effect:` runtime grants
      // plus the resolved declared requirements. An overlap on the same
      // (resource, action) is resolved by effect precedence at authz time,
      // NOT by union of the rows here.
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
      const stagedGrantRows = staged.grantRows;

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
      // agent.deploy frame's `config.grants` uses -- the staging above already
      // projected them, so the frame carries exactly the rows the commit below
      // writes.
      const stepGrants = staged.stepGrants;

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
      // the per-instance principal the agent.deploy path mints. The commit is
      // idempotent on the runId (unique for an external trigger), sharing the
      // same staging and commit the mail-triggered run path uses.
      await commitRunGrants({
        db,
        tenantId: tenant.id,
        deploymentId,
        runId,
        runPrincipalId,
        now,
        grantRows: stagedGrantRows,
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
