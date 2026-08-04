import { eq, and, inArray, asc, isNull, isNotNull } from "drizzle-orm";
import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { describeRoute, resolver, validator } from "hono-openapi";
import { streamSSE } from "hono/streaming";
import { type } from "arktype";

import {
  agentRole,
  agentSession,
  grant as grantTable,
  inferenceTurn,
  offering,
  principal as principalTable,
  principalRole,
  sessionMail,
  turnPart,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import {
  AmbiguousCredentialError,
  parseWorkflowDefinitionRow,
  resolveCredentialRequirement,
} from "@intx/db";
import type { DB } from "@intx/db";
import { authorize, toolConsumer } from "@intx/authz";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import { parseMailToEmail, extractPartByPath } from "@intx/mime";

import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import {
  CreateWorkflowRun,
  WorkflowRunResponse,
  WorkflowRunHealth,
  OfferingDetail,
  GrantRequirement,
  SendMessage,
  MailResponse,
  AttachmentErrorResponse,
  InferenceTurnResponse,
  ErrorResponse,
  formatAgentAddress,
  paginatedSchema,
} from "@intx/types";
import type { CredentialCipher, ProviderPreference } from "@intx/types";
import type { CryptoProvider } from "@intx/types/runtime";
import {
  findRoutableById,
  resolveRunIdForSession,
  resolveRunSessionId,
  runRowToRoutableRecord,
  SessionLaunchError,
  type AssetService,
  type EventCollectorRegistry,
  type RoutableRecord,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import { extractFoldedBody } from "@intx/workflow-deploy";
import { hydrateDefinition } from "../run-grant-materialization";
import { resolveDefinitionSources } from "../run-source-resolution";
import { formatOffering } from "./offerings";
import {
  formatInstanceView,
  instanceStatusOf,
  mapRunStatusToInstanceStatus,
} from "./instance-view";
import { validateAttachments } from "../attachment-validation";
import {
  resolveGrantMaterialization,
  makeGrantRow,
} from "../grant-materialization";

import type { TenantEnv } from "../context";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import { generateId } from "@intx/hub-common";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

const GrantRequirements = GrantRequirement.array();

// DoS guard on the mail route body. Sized above the legitimate ceiling
// (the 30 MB per-message attachment cap is ~40 MB once base64-encoded,
// plus JSON and text overhead) so over-business-cap requests are rejected
// by the handler with a structured error, while genuine garbage is
// rejected here before the JSON parser allocates a giant string.
const MAX_MAIL_BODY_BYTES = 44 * 1024 * 1024;

type InstanceStatusFilter =
  | "deployed"
  | "running"
  | "updating"
  | "error"
  | "stopped";

function isInstanceStatusFilter(
  status: string | undefined,
): status is InstanceStatusFilter {
  return (
    status === "deployed" ||
    status === "running" ||
    status === "updating" ||
    status === "error" ||
    status === "stopped"
  );
}

type WorkflowRunStatus = (typeof workflowRun.$inferSelect)["status"];

// The `workflow_run` statuses that present as a given instance-status filter,
// derived as the inverse of `mapRunStatusToInstanceStatus` so the two cannot
// drift: every run status is bucketed under the instance status it maps onto.
// No run status maps onto `deployed` or `updating`, so those filters select no
// runs (empty array) and the caller skips the run query entirely.
const RUN_STATUSES_BY_INSTANCE_STATUS: Record<
  InstanceStatusFilter,
  WorkflowRunStatus[]
> = (() => {
  const byStatus: Record<InstanceStatusFilter, WorkflowRunStatus[]> = {
    deployed: [],
    running: [],
    updating: [],
    error: [],
    stopped: [],
  };
  for (const runStatus of workflowRun.status.enumValues) {
    byStatus[mapRunStatusToInstanceStatus(runStatus)].push(runStatus);
  }
  return byStatus;
})();

// The row shape the folded-run list sub-query projects: the run columns the
// instance view needs, plus the definition name. The sub-query already gates on
// an instance-kind definition, so the kind is not carried on the row.
type FoldedRunListRow = {
  run: typeof workflowRun.$inferSelect;
  definitionName: string;
};

// Shape a folded run into the fold-normalized record the instance view expects,
// through the same builder `findRoutableById` uses. The sub-query filters
// `address` non-null, so a null here is a broken invariant and surfaces loudly.
function foldedRunToRecord(row: FoldedRunListRow): RoutableRecord {
  if (row.run.address === null) {
    throw new Error(`folded run ${row.run.id} listed with a null address`);
  }
  return runRowToRoutableRecord(row.run, row.run.address);
}

export type CreateInstanceRoutesDeps = {
  db: DB["db"];
  sessionService: SessionService;
  sidecarRouter: SidecarRouter;
  eventCollectors: EventCollectorRegistry;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  requireGrant: RequireGrant;
  assetService: AssetService | null;
  credentialCipher: CredentialCipher;
};

export function createInstanceRoutes({
  db,
  sessionService,
  sidecarRouter,
  eventCollectors,
  grantStore,
  conditionRegistry,
  requireGrant,
  assetService,
  credentialCipher,
}: CreateInstanceRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.post(
    "/",
    requireGrant("workflow-run:*", "create"),
    describeRoute({
      tags: ["Instances"],
      summary: "Deploy an agent instance",
      description:
        "Creates a new running instance of the specified agent definition. Resolves the definition's model requirements against the tenant catalog into an ordered inference-source list, materializes grants on a new agent principal, provisions the agent on a sidecar, and starts it. The invoker can provide invokerGrants to delegate additional capabilities, and modelPreferences to reorder or restrict the resolved providers for the session.",
      responses: {
        201: {
          description: "Instance deployed",
          content: {
            "application/json": { schema: resolver(WorkflowRunResponse) },
          },
        },
        404: {
          description: "Agent definition not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Agent not launchable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        502: {
          description: "Sidecar unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateWorkflowRun),
    async (c) => {
      const tenant = c.get("tenant");
      const principal = c.get("principal");
      const body = c.req.valid("json");

      // The caller names the definition directly by its id. It is
      // authoritative for launchability and model resolution.
      const definitionRow = await db.query.workflowDefinition.findFirst({
        where: and(
          eq(workflowDefinition.id, body.definitionId),
          eq(workflowDefinition.tenantId, tenant.id),
        ),
      });
      if (definitionRow === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Definition not found" } },
          404,
        );
      }
      const definition = parseWorkflowDefinitionRow(definitionRow);

      if (definition.status !== "deployed") {
        return c.json(
          {
            error: {
              code: "conflict",
              message: `Definition is not in a launchable state (status: ${definition.status})`,
            },
          },
          409,
        );
      }

      // Source the launch body from the definition's materialized asset (the
      // frozen `workflow.json`), not the origin agent row. The materialization
      // cliff runs before any launch, so a null asset id is a broken invariant,
      // not a normal state -- reject loudly rather than launch a body-less
      // instance.
      if (definition.assetId === null) {
        return c.json(
          {
            error: {
              code: "conflict",
              message: "This definition has not been materialized",
            },
          },
          409,
        );
      }
      if (assetService === null) {
        return c.json(
          {
            error: {
              code: "unavailable",
              message:
                "Asset service is not configured; cannot hydrate the body",
            },
          },
          503,
        );
      }
      const foldedBody = extractFoldedBody(
        await hydrateDefinition(assetService, definition.assetId),
      );
      if (!foldedBody.systemPrompt) {
        return c.json(
          {
            error: {
              code: "not_launchable",
              message:
                "Agent cannot be launched without a system prompt configured",
            },
          },
          409,
        );
      }

      const instanceId = generateId("instance");
      const agentAddress = formatAgentAddress(instanceId, tenant.domain);

      // --- Inference source resolution (catalog) ---

      // The definition carries the creator the fold copied off its agent. It is
      // a nullable column but always set for a folded definition; a null here is
      // corruption, so fail loud rather than resolve grants against no creator.
      const creatorPrincipalId = definition.creatorPrincipalId;
      if (creatorPrincipalId === null) {
        return c.json(
          {
            error: {
              code: "conflict",
              message: "This definition has no creator principal",
            },
          },
          409,
        );
      }

      // The invoker's launch-time preference reorders or restricts the
      // tenant-visible providers; it cannot introduce one the catalog lacks.
      const invokerPreferences: Record<string, ProviderPreference> = {};
      for (const preference of body.modelPreferences ?? []) {
        invokerPreferences[preference.model] = preference.providers;
      }

      const resolution = await resolveDefinitionSources({
        db,
        tenantId: tenant.id,
        modelRequirements: definition.modelRequirements,
        fallbackModel: foldedBody.model,
        invokerPreferences,
        credentialCipher,
      });
      if (!resolution.ok) {
        return c.json(
          { error: { code: "not_launchable", message: resolution.message } },
          409,
        );
      }
      const sources = resolution.sources;
      const defaultSource = resolution.defaultSource;

      // --- Grant requirement resolution (creator/invoker delegation) ---

      const instancePrincipalId = generateId("principal");

      const now = new Date();

      const parsedGrantReqs = GrantRequirements(foldedBody.grantRequirements);
      if (parsedGrantReqs instanceof type.errors) {
        return c.json(
          {
            error: {
              code: "not_launchable",
              message: `Invalid grant requirements: ${parsedGrantReqs.summary}`,
            },
          },
          409,
        );
      }

      // Collect invoker's grants once — used for both creator and invoker resolution.
      const invokerGrants = await grantStore.collectGrants(
        principal.id,
        tenant.id,
      );

      // Collect creator's grants once for all creator-sourced requirements.
      const hasCreatorReqs = parsedGrantReqs.some(
        (r) => r.source === "creator",
      );
      const creatorGrants = hasCreatorReqs
        ? await grantStore.collectGrants(creatorPrincipalId, tenant.id)
        : [];

      const materialization = await resolveGrantMaterialization({
        tenantId: tenant.id,
        targetPrincipalId: instancePrincipalId,
        grantRequirements: parsedGrantReqs,
        adHocInvokerGrants: body.invokerGrants ?? [],
        invokerGrants,
        creatorGrants,
        now,
      });
      if (!materialization.ok) {
        const { status, code, message } = materialization.rejection;
        return c.json({ error: { code, message } }, status);
      }
      const grantRows = materialization.grantRows;

      // --- Credential binding resolution ---
      //
      // Each credential binding names a tool-package handle bound to a concrete
      // provider. Resolve it to a tenant-owned credential (the `tenant` locator
      // filters `principalId IS NULL`), whose use is authorized by ownership --
      // proven by this walk-up resolution, not re-checked -- and stamp a
      // consumer-scoped `credential:{id}` / `use` grant on the instance
      // principal, scoped to the tool package by the `{ tool }` condition.
      // origin `system`: the authority is tenant ownership, not a delegated
      // personal grant. The runtime gate that CHECKS the condition is C8, so
      // this commit only stamps the grant; no production `workflow.json` carries
      // a binding yet, so this path runs only under test fixtures today.
      for (const binding of foldedBody.credentialBindings) {
        let credentialId: string;
        try {
          const resolved = await resolveCredentialRequirement(
            db,
            tenant.id,
            {
              providerName: binding.provider,
              source: binding.locator,
              ...(binding.name !== undefined ? { name: binding.name } : {}),
            },
            creatorPrincipalId,
            principal.id,
          );
          if (resolved === null) {
            return c.json(
              {
                error: {
                  code: "not_launchable",
                  message: `No credential resolves the binding for provider ${binding.provider} (package ${binding.package}, handle ${binding.handle})`,
                },
              },
              409,
            );
          }
          credentialId = resolved.id;
        } catch (e) {
          if (!(e instanceof AmbiguousCredentialError)) {
            throw e;
          }
          return c.json(
            {
              error: {
                code: "not_launchable",
                message: `Ambiguous credential for the binding on provider ${binding.provider} (package ${binding.package}, handle ${binding.handle}): ${e.message}`,
              },
            },
            409,
          );
        }
        grantRows.push(
          makeGrantRow({
            tenantId: tenant.id,
            principalId: instancePrincipalId,
            resource: `credential:${credentialId}`,
            action: "use",
            effect: "allow",
            conditions: { tool: toolConsumer(binding.package) },
            origin: "system",
            expiresAt: null,
            now,
          }),
        );
      }

      // An instance-kind `workflow_definition` launches as a `workflow_run`
      // rather than an `agent_instance`: the run IS the launched instance. The
      // plain `ins_<hex>` address, per-launch principal, grants, and deploy are
      // identical to a legacy instance launch; only the persisted row (and how
      // its session is keyed) differ.

      // --- Resolve role assignments for the instance principal ---

      // Role assignments hang off the folded definition -- agent_role.agent_id
      // holds definition ids.
      const agentRoleRows = await db.query.agentRole.findMany({
        where: eq(agentRole.agentId, definition.id),
      });
      const agentRoleIds = agentRoleRows.map((a) => a.roleId);
      const agentRoleAssignments =
        agentRoleIds.length > 0
          ? (
              await db.query.role.findMany({
                where: (r, { inArray, and: a }) =>
                  a(inArray(r.id, agentRoleIds), eq(r.tenantId, tenant.id)),
                columns: { id: true },
              })
            ).map((r) => ({ roleId: r.id }))
          : [];

      // --- Write all DB rows in a transaction ---

      const sessionId = generateId("session");

      await db.transaction(async (tx) => {
        // Create the endpoint's principal. A folded run is a workflow run, so
        // its principal is `workflow`-kind, converging on the native run's
        // principal shape. The refId is the instance id.
        await tx.insert(principalTable).values({
          id: instancePrincipalId,
          tenantId: tenant.id,
          kind: "workflow",
          refId: instanceId,
          status: "active",
          createdAt: now,
          updatedAt: now,
        });

        // Assign the agent definition's roles to the instance principal so
        // that grants flow through the existing RBAC path (collectGrants).
        for (const { roleId } of agentRoleAssignments) {
          await tx.insert(principalRole).values({
            principalId: instancePrincipalId,
            roleId,
            createdAt: now,
          });
        }

        // Materialize grants on the instance principal
        for (const g of grantRows) {
          await tx.insert(grantTable).values(g);
        }

        // The agentSession row is keyed to the folded definition (agent_id) and
        // to the run's principal (`workflow_run.principalId`, which is
        // `instancePrincipalId`), so the run <-> session bridge resolves the
        // session by that shared principal.
        await tx.insert(agentSession).values({
          id: sessionId,
          tenantId: tenant.id,
          agentId: definition.id,
          principalId: instancePrincipalId,
          status: "active",
          createdAt: now,
          updatedAt: now,
        });

        // The folded run IS the launched instance: it owns the plain routing
        // address and carries the runtime bindings an agent_instance would.
        // `deploymentId` is null (a folded run has no deployment); the public
        // key lands later at deploy-ack.
        await tx.insert(workflowRun).values({
          id: instanceId,
          definitionId: definition.id,
          deploymentId: null,
          tenantId: tenant.id,
          principalId: instancePrincipalId,
          address: agentAddress,
          status: "running",
          modelPreferences: body.modelPreferences ?? null,
          createdAt: now,
        });

        // Seed a creator-level read grant on the per-instance
        // agent-state repo so the definition creator can read runtime
        // state out of the box. The definition seed point covers the
        // deploy-artifact repo; this covers the runtime repo.
        await tx.insert(grantTable).values({
          id: generateId("grant"),
          tenantId: tenant.id,
          principalId: creatorPrincipalId,
          resource: `agent-state:${instanceId}`,
          action: "read",
          effect: "allow",
          origin: "creator",
          createdAt: now,
          updatedAt: now,
        });
      });

      // Collect the materialized grants for the deploy frame
      const grants = await grantStore.collectGrants(
        instancePrincipalId,
        tenant.id,
      );

      // Open the inference-turn collector for the launched endpoint. The
      // collector records turns under this id, which is the instance id or the
      // folded run id from the shared id space -- inference_turn.instanceId
      // carries no foreign key, so either is storable.
      eventCollectors.create(agentAddress, tenant.id, sessionId, instanceId);

      try {
        // Deploy the instance as a single-step workflow at the head: it runs
        // as a supervised workflow-process child. The deploy keys by the
        // instance id -- the surviving id the address encodes and every
        // deploy-ref reader resolves by (the child resolves its skills and
        // pinned tool packages by mailbox address, not this id). The returned
        // head public key is surfaced separately via the sidecar's
        // `agent.deploy.ack`, so the route discards it here.
        await sessionService.deployInstanceAtHead({
          agentAddress,
          agentId: instanceId,
          instanceId,
          config: {
            sessionId,
            agentId: instanceId,
            tenantId: tenant.id,
            principalId: instancePrincipalId,
            agentAddress,
            systemPrompt: foldedBody.systemPrompt,
            tools: [],
            grants,
            sources,
            defaultSource,
          },
          toolPackagePins: foldedBody.toolPackagePins,
          deployContent: {
            systemPrompt: foldedBody.systemPrompt,
          },
        });
      } catch (err) {
        eventCollectors.abandon(agentAddress);

        const failedAt = new Date();

        await db
          .update(agentSession)
          .set({ status: "ended", endedAt: failedAt, updatedAt: failedAt })
          .where(eq(agentSession.id, sessionId));

        const leaked = err instanceof SessionLaunchError && err.leakedAgent;

        // A leaked deploy left a running child. Mark the run failed but leave
        // it routable (endedAt null), so the leaked child stays reachable to
        // inspect or clean up. Otherwise roll the run back entirely.
        if (leaked) {
          await db
            .update(workflowRun)
            .set({ status: "failed" })
            .where(eq(workflowRun.id, instanceId));
        } else {
          await db.delete(workflowRun).where(eq(workflowRun.id, instanceId));
        }

        // Deactivate the instance principal created during this launch
        await db
          .update(principalTable)
          .set({ status: "deactivated", updatedAt: failedAt })
          .where(eq(principalTable.id, instancePrincipalId));

        return c.json(
          {
            error: {
              code: "sidecar_unavailable",
              message:
                err instanceof Error
                  ? err.message
                  : "Failed to dispatch agent to sidecar",
            },
          },
          502,
        );
      }

      // The folded run was inserted `running`, so there is no deployed ->
      // running flip. Shape it through the shared instance view so it reads
      // identically to a later GET: the run has no `updatedAt` (it mirrors
      // `createdAt`), and its public key is null until deploy-ack lands it.
      const runRecord: RoutableRecord = {
        id: instanceId,
        tenantId: tenant.id,
        address: agentAddress,
        publicKey: null,
        status: "running",
        createdAt: now,
        updatedAt: now,
        endedAt: null,
        definitionId: definition.id,
        principalId: instancePrincipalId,
        kernelId: null,
        sidecarId: null,
      };
      return c.json(formatInstanceView(runRecord, definition.name), 201);
    },
  );

  app.get(
    "/",
    requireGrant("workflow-run:*", "read"),
    describeRoute({
      tags: ["Instances"],
      summary: "List agent instances",
      description:
        "Lists agent instances in the tenant. Filterable by definitionId and status.",
      parameters: [
        { name: "definitionId", in: "query", schema: { type: "string" } },
        {
          name: "status",
          in: "query",
          schema: {
            type: "string",
            enum: ["deployed", "running", "updating", "error", "stopped"],
          },
        },
        ...pageParameters,
      ],
      responses: {
        200: {
          description: "List of instances",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(WorkflowRunResponse)),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const definitionId = c.req.query("definitionId");
      const status = c.req.query("status");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      // A run presents as an instance when it owns a plain routing address and
      // carries no deployment id; the two predicates enforce that, dropping a
      // deployment-anchored run (which carries its deployment id) and an
      // address-less child run. When a status filter selects no run statuses
      // (`deployed`/`updating`), skip the query entirely.
      const statusFilter = isInstanceStatusFilter(status) ? status : undefined;

      const runStatuses =
        statusFilter === undefined
          ? undefined
          : RUN_STATUSES_BY_INSTANCE_STATUS[statusFilter];
      let runRows: FoldedRunListRow[] = [];
      if (runStatuses === undefined || runStatuses.length > 0) {
        const runConditions = [
          eq(workflowRun.tenantId, tenantCtx.id),
          isNotNull(workflowRun.address),
          isNull(workflowRun.deploymentId),
        ];
        if (definitionId !== undefined) {
          runConditions.push(eq(workflowRun.definitionId, definitionId));
        }
        if (runStatuses !== undefined) {
          runConditions.push(inArray(workflowRun.status, runStatuses));
        }
        if (cursor) {
          runConditions.push(
            cursorCondition(workflowRun.createdAt, workflowRun.id, cursor),
          );
        }

        runRows = await db
          .select({
            run: workflowRun,
            definitionName: workflowDefinition.name,
          })
          .from(workflowRun)
          .innerJoin(
            workflowDefinition,
            eq(workflowRun.definitionId, workflowDefinition.id),
          )
          .where(and(...runConditions))
          .orderBy(...pageOrder(workflowRun.createdAt, workflowRun.id))
          .limit(limit);
      }

      return c.json(
        paginatedResponse(
          runRows.map((r) =>
            formatInstanceView(foldedRunToRecord(r), r.definitionName),
          ),
          runRows.map((r) => ({ createdAt: r.run.createdAt, id: r.run.id })),
          limit,
        ),
      );
    },
  );

  app.get(
    "/blobs/:blobId",
    describeRoute({
      tags: ["Instances"],
      summary: "Fetch a blob by ID",
      description:
        "Returns raw bytes for a MIME part. Blob IDs are issued by the mail parsing layer.",
      responses: {
        200: {
          description: "Blob bytes",
          content: { "application/octet-stream": {} },
        },
        400: {
          description: "Invalid blob ID",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        403: {
          description: "Forbidden",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Blob not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const blobId = c.req.param("blobId");

      // Blob IDs have the format: blob_<mailId>_<partPath>
      // where partPath is an IMAP-style section specifier (digits and dots only).
      // mailId may itself contain underscores, so we match the suffix.
      const blobMatch = /^blob_(.+?)_(\d[\d.]*)$/.exec(blobId);
      if (!blobMatch) {
        return c.json(
          { error: { code: "bad_request", message: "Invalid blob ID format" } },
          400,
        );
      }

      const mailId = blobMatch[1];
      const partPath = blobMatch[2];

      if (!mailId || !partPath) {
        return c.json(
          { error: { code: "bad_request", message: "Invalid blob ID format" } },
          400,
        );
      }

      const tenant = c.get("tenant");

      const mailRow = await db.query.sessionMail.findFirst({
        where: and(
          eq(sessionMail.id, mailId),
          eq(sessionMail.tenantId, tenant.id),
        ),
      });

      if (!mailRow) {
        return c.json(
          { error: { code: "not_found", message: "Blob not found" } },
          404,
        );
      }

      // The authorization subject is the mail's owning routable. A folded run's
      // mail carries a null instanceId and keys on its session, so recover the
      // run id from the session; a legacy row's non-null instanceId is used
      // directly. Route the run resolution through workflow_run so the subject
      // is proven to name a real run of this tenant -- a session held by any
      // non-run principal fails closed to 404 rather than authorizing a
      // fabricated subject.
      const resolvedInstanceId =
        mailRow.instanceId ??
        (await resolveRunIdForSession(db, mailRow.sessionId, tenant.id));
      if (!resolvedInstanceId) {
        return c.json(
          { error: { code: "not_found", message: "Blob not found" } },
          404,
        );
      }

      const principal = c.get("principal");

      const authResult = await authorize(
        grantStore,
        principal.id,
        tenant.id,
        `workflow-run:${resolvedInstanceId}`,
        "read",
        conditionRegistry,
      );

      if (authResult.effect !== "allow") {
        return c.json(
          {
            error: {
              code: "forbidden",
              message: "You do not have permission to perform this action",
            },
          },
          403,
        );
      }

      let partBytes: Uint8Array;
      try {
        partBytes = extractPartByPath(mailRow.raw, partPath);
      } catch {
        return c.json(
          { error: { code: "not_found", message: "Blob not found" } },
          404,
        );
      }

      return c.body(
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Uint8Array.buffer.slice always returns ArrayBuffer
        partBytes.buffer.slice(
          partBytes.byteOffset,
          partBytes.byteOffset + partBytes.byteLength,
        ) as ArrayBuffer,
        200,
        {
          "Content-Type": "application/octet-stream",
        },
      );
    },
  );

  app.get(
    "/:runId",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Instances"],
      summary: "Get instance detail",
      description:
        "Returns instance runtime state including status, public key, and sidecar assignment.",
      responses: {
        200: {
          description: "Instance detail",
          content: {
            "application/json": { schema: resolver(WorkflowRunResponse) },
          },
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const instanceId = c.req.param("runId");

      // Resolve across the fold: a legacy agent instance or a folded run. The
      // origin agent supplies the display name for either kind.
      const record = await findRoutableById(db, instanceId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      // The display name lives on the definition the record belongs to,
      // scoped to the tenant.
      const definitionRow = await db.query.workflowDefinition.findFirst({
        where: and(
          eq(workflowDefinition.id, record.definitionId),
          eq(workflowDefinition.tenantId, tenantCtx.id),
        ),
      });
      if (definitionRow === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      // Enrich with runtime status from the event collector if available.
      const runtimeStatus = eventCollectors.getStatus(record.address);

      return c.json(
        formatInstanceView(record, definitionRow.name, runtimeStatus?.status),
      );
    },
  );

  app.get(
    "/:runId/health",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Instances"],
      summary: "Get instance health",
      description:
        "Returns liveness and readiness for a running instance. Liveness reflects whether the instance's sidecar connection is active. Readiness reflects whether the instance has an active event collector and can process work.",
      responses: {
        200: {
          description: "Health status",
          content: {
            "application/json": { schema: resolver(WorkflowRunHealth) },
          },
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        410: {
          description: "Instance stopped",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const instanceId = c.req.param("runId");

      const record = await findRoutableById(db, instanceId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      if (instanceStatusOf(record) === "stopped") {
        return c.json(
          { error: { code: "gone", message: "Instance has stopped" } },
          410,
        );
      }

      const routableAddresses = sidecarRouter.getRoutableAddresses();
      const liveness = routableAddresses.includes(record.address)
        ? "ok"
        : "unhealthy";

      const status = eventCollectors.getStatus(record.address);
      const readiness = status !== undefined ? "ok" : "not_ready";

      return c.json({ liveness, readiness, lastCheckedAt: null });
    },
  );

  app.get(
    "/:runId/offerings",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Instances"],
      summary: "List instance offerings",
      description:
        "Returns the offerings associated with the instance's agent definition. These represent the capabilities the instance can provide.",
      responses: {
        200: {
          description: "List of offerings",
          content: {
            "application/json": {
              schema: resolver(OfferingDetail.array()),
            },
          },
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const instanceId = c.req.param("runId");

      const record = await findRoutableById(db, instanceId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      // Offerings are keyed on the definition (their `agentId` column holds a
      // definition id). Resolve the record's definition, then filter offerings
      // on its id and display its name.
      const definitionRow = await db.query.workflowDefinition.findFirst({
        where: eq(workflowDefinition.id, record.definitionId),
      });
      if (definitionRow === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      const offerings = await db.query.offering.findMany({
        where: and(
          eq(offering.agentId, definitionRow.id),
          eq(offering.tenantId, tenantCtx.id),
        ),
      });

      return c.json(
        offerings.map((o) => formatOffering(o, definitionRow.name)),
      );
    },
  );

  app.delete(
    "/:runId",
    requireGrant(idResource("workflow-run", "runId"), "manage"),
    describeRoute({
      tags: ["Instances"],
      summary: "Stop an instance",
      description:
        "Stops the running instance and undeploys the agent from the sidecar.",
      responses: {
        204: {
          description: "Instance stopped",
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Instance already stopped",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        502: {
          description: "Sidecar unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const instanceId = c.req.param("runId");

      // A folded agent runs as a workflow_run under the same id; stop it there.
      // findRoutableById resolves exactly the folded-instance runs, returning
      // undefined for a missing row, a null address, or a workflow-derived
      // (deployment-anchor) address -- a run it does not resolve is not this
      // route's to stop and reads as absent, never driven into an undeploy.
      const record = await findRoutableById(db, instanceId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      if (record.endedAt !== null) {
        return c.json(
          {
            error: {
              code: "conflict",
              message: "Instance is already stopped",
            },
          },
          409,
        );
      }
      try {
        await sessionService.endSession(record.address, "instance_stopped");
      } catch (err) {
        return c.json(
          {
            error: {
              code: "sidecar_unavailable",
              message:
                err instanceof Error
                  ? err.message
                  : "Failed to reach sidecar for instance teardown",
            },
          },
          502,
        );
      }

      const endedAt = new Date();
      // Settle the run and its principal/session atomically. The terminal
      // flip guards on `endedAt` rather than reusing the workflow-run store's
      // markTerminal `status = 'running'` guard on purpose: a launch that
      // leaked a folded run leaves it `failed` with a null `endedAt` so it
      // stays routable, and this stop must still settle that non-terminal row
      // — a `status = 'running'` guard would skip it.
      await db.transaction(async (tx) => {
        await tx
          .update(workflowRun)
          .set({ status: "cancelled", endedAt })
          .where(
            and(eq(workflowRun.id, instanceId), isNull(workflowRun.endedAt)),
          );

        if (record.principalId !== null) {
          // Deactivate the run's principal (refId guard scopes it to this
          // run), then end its transitional session, which is keyed by that
          // principal.
          await tx
            .update(principalTable)
            .set({ status: "deactivated", updatedAt: endedAt })
            .where(
              and(
                eq(principalTable.id, record.principalId),
                eq(principalTable.refId, instanceId),
              ),
            );
          await tx
            .update(agentSession)
            .set({ status: "ended", endedAt, updatedAt: endedAt })
            .where(
              and(
                eq(agentSession.principalId, record.principalId),
                isNull(agentSession.endedAt),
              ),
            );
        }
      });

      eventCollectors.abandon(record.address);
      instanceKeyCache.delete(instanceId);
      sidecarRouter.dispatchAgentEvent(record.address, {
        type: "session.ended",
      });

      return c.body(null, 204);
    },
  );

  app.get(
    "/:runId/events",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Instances"],
      summary: "SSE event stream",
      description:
        "Server-Sent Events stream for agent events. Use POST .../messages for client-to-server messaging.",
      responses: {
        200: {
          description: "SSE event stream",
          content: {
            "text/event-stream": {},
          },
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        410: {
          description: "Instance stopped",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const instanceId = c.req.param("runId");

      const record = await findRoutableById(db, instanceId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      if (instanceStatusOf(record) === "stopped") {
        return c.json(
          { error: { code: "gone", message: "Instance has stopped" } },
          410,
        );
      }

      const address = record.address;

      return streamSSE(c, async (stream) => {
        const noop = () => undefined;

        // Emit the replay before subscribing to live events so that a
        // delta arriving between subscribe() and the replay write cannot
        // beat the catch-up text onto the stream.
        const status = eventCollectors.getStatus(address);
        if (status?.status === "busy") {
          await stream.writeSSE({
            event: "agent.event",
            data: JSON.stringify({
              type: "inference.start",
              seq: 0,
              data: { model: "unknown" },
            }),
          });
        }
        const accumulatedText = eventCollectors.getAccumulatedText(address);
        if (accumulatedText !== undefined && accumulatedText !== "") {
          const turnId = eventCollectors.getLastTurnId(address);
          await stream.writeSSE({
            event: "agent.event",
            data: JSON.stringify({
              type: "inference.text.replay",
              data: { turnId, text: accumulatedText },
            }),
          });
        }

        const unsubscribe = sidecarRouter.subscribeAgent(address, (event) => {
          stream
            .writeSSE({
              event: "agent.event",
              data: JSON.stringify(event),
            })
            .catch(noop);
        });

        const keepalive = setInterval(() => {
          stream.write(": keepalive\n\n").catch(noop);
        }, 30_000);

        stream.onAbort(() => {
          clearInterval(keepalive);
          unsubscribe();
        });

        // Keep the stream open until the client disconnects.
        await new Promise<void>(noop);
      });
    },
  );

  // Crypto providers for signing outbound messages, keyed by instance ID.
  // Evicted when an instance is stopped. The cache is per-factory call,
  // not per-process; two createInstanceRoutes() calls in the same process
  // do not share signing keys, which is intentional — each router owns
  // its own crypto state and lifecycle.
  const instanceKeyCache = new Map<string, Promise<CryptoProvider>>();

  function getInstanceCryptoProvider(
    instanceId: string,
  ): Promise<CryptoProvider> {
    let pending = instanceKeyCache.get(instanceId);
    if (pending !== undefined) return pending;
    pending = generateKeyPair().then((kp) => createEd25519Crypto(kp));
    instanceKeyCache.set(instanceId, pending);
    return pending;
  }

  app.post(
    "/:runId/mail",
    requireGrant(idResource("workflow-run", "runId"), "write"),
    describeRoute({
      tags: ["Instances"],
      summary: "Send mail to the agent",
      description:
        "Persists the user message as a mail record and dispatches it to the running agent. Returns JMAP Email-shaped response.",
      responses: {
        201: {
          description: "Mail sent",
          content: {
            "application/json": { schema: resolver(MailResponse) },
          },
        },
        400: {
          description:
            "Attachment validation error. Each variant carries a structured code (oversize_attachment, disallowed_mime_type, malformed_base64, oversize_total) with the offending index and limits. A malformed request body that fails SendMessage validation returns the generic error shape instead.",
          content: {
            "application/json": { schema: resolver(AttachmentErrorResponse) },
          },
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Instance not running",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        413: {
          description: "Request body exceeds the maximum allowed size",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        502: {
          description: "Sidecar unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
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
      const instanceId = c.req.param("runId");
      const body = c.req.valid("json");

      // Decode and validate attachments at the boundary, emitting ordered,
      // per-index structured errors. The effective policy defaults to the
      // system-level allowlist and limits; a future per-agent/per-workflow
      // lookup substitutes a narrowed policy here.
      const attachmentResult = validateAttachments(body.attachments ?? []);
      if (!attachmentResult.ok) {
        return c.json({ error: attachmentResult.error }, 400);
      }
      const messageAttachments = attachmentResult.attachments;

      const record = await findRoutableById(db, instanceId, tenant.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      const mappedStatus = instanceStatusOf(record);
      if (mappedStatus !== "running") {
        return c.json(
          {
            error: {
              code: "conflict",
              message: `Instance is not running (status: ${mappedStatus})`,
            },
          },
          409,
        );
      }

      // A send needs the run's live session. The running gate above guarantees
      // one exists; resolve it here (live-only) rather than have the id lookup
      // fetch a session for every caller when only this route reads it. The
      // GET-history read below resolves the same way, with ended sessions.
      const sessionId = await resolveRunSessionId(db, record.principalId);
      if (sessionId === null) {
        return c.json(
          {
            error: {
              code: "conflict",
              message: "Instance has no active session",
            },
          },
          409,
        );
      }

      const mailId = generateId("sessionMail");
      const now = new Date();

      const user = c.get("user");
      const fromAddr = `${principal.refId}@${tenant.domain}`;
      const from = user?.name ? `"${user.name}" <${fromAddr}>` : fromAddr;
      const mimeMessageId = `<${mailId}@${tenant.domain}>`;

      // Fetch recent delivered inbound mail for the MIME References chain. A
      // run's mail carries a null instanceId, so it is keyed on the session.
      // GET history keys the same way.
      const mailScope = eq(sessionMail.sessionId, sessionId);
      const priorMail = await db
        .select({ id: sessionMail.id })
        .from(sessionMail)
        .where(
          and(
            mailScope,
            eq(sessionMail.direction, "inbound"),
            eq(sessionMail.status, "delivered"),
          ),
        )
        .orderBy(asc(sessionMail.createdAt), asc(sessionMail.id))
        .limit(100);

      const priorIds = priorMail.map((m) => `<${m.id}@${tenant.domain}>`);
      const lastIdFromSession = priorIds[priorIds.length - 1];

      // Threading-header policy:
      //   1. Session history (the user's prior mail to this instance)
      //      wins whenever it exists. inReplyTo points at the user's
      //      most recent message, references lists the chain.
      //   2. With no session history, fall back to the agent's active
      //      connector thread. The connector is one durable shared
      //      thread per agent — anyone with a session joins whatever
      //      thread is active. Stamp inReplyTo and references from the
      //      cached state so the harness routes the message as
      //      `continue` and adds the user to the participant set.
      //   3. With no session history and no active connector, send
      //      threading-less mail. The harness routes it as `start`,
      //      establishing this user as the first participant on a new
      //      thread.
      let inReplyTo: string | undefined;
      let references: string[] | undefined;
      if (lastIdFromSession !== undefined) {
        inReplyTo = lastIdFromSession;
        references = priorIds;
      } else {
        const connectorState = sidecarRouter.getConnectorState(record.address);
        if (connectorState !== null) {
          inReplyTo = connectorState.lastMessageId;
          references = [connectorState.threadRoot];
        }
      }

      const cryptoProvider = await getInstanceCryptoProvider(instanceId);

      // A run's mail is not an instance's, so it records a null instanceId and
      // anchors on the session (mirroring the sidecar mail-persist path).
      const mailInstanceId = null;

      let rawMIME: Uint8Array;
      try {
        rawMIME = await sessionService.sendUserMessage({
          agentAddress: record.address,
          from,
          messageId: mimeMessageId,
          date: now,
          content: body.content,
          ...(messageAttachments.length > 0
            ? { attachments: messageAttachments }
            : {}),
          ...(inReplyTo !== undefined ? { inReplyTo } : {}),
          ...(references !== undefined && references.length > 0
            ? { references }
            : {}),
          sessionId,
          tenantId: tenant.id,
          cryptoProvider,
        });
      } catch (err) {
        return c.json(
          {
            error: {
              code: "sidecar_unavailable",
              message:
                err instanceof Error
                  ? err.message
                  : "Failed to deliver message to sidecar",
            },
          },
          502,
        );
      }

      const mailCreatedAt = new Date();

      await db.insert(sessionMail).values({
        id: mailId,
        sessionId,
        instanceId: mailInstanceId,
        tenantId: tenant.id,
        direction: "inbound",
        status: "delivered",
        raw: rawMIME,
        createdAt: mailCreatedAt,
      });

      const parsed = parseMailToEmail(rawMIME, mailId);
      sidecarRouter.dispatchAgentEvent(record.address, {
        type: "mail.delivered",
        data: {
          ...parsed,
          id: mailId,
          direction: "inbound" as const,
          receivedAt: mailCreatedAt.toISOString(),
        },
      });

      return c.json(
        {
          id: mailId,
          sessionId,
          instanceId: mailInstanceId,
          direction: "inbound" as const,
          status: "delivered" as const,
          receivedAt: mailCreatedAt.toISOString(),
          ...parsed,
        },
        201,
      );
    },
  );

  app.get(
    "/:runId/mail",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Instances"],
      summary: "List mail for an instance",
      description:
        "Returns parsed JMAP Email objects in reverse chronological order. Cursor-paginated.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of mail",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(MailResponse)),
            },
          },
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenant = c.get("tenant");
      const instanceId = c.req.param("runId");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const record = await findRoutableById(db, instanceId, tenant.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      // History must serve a terminated run. A run's mail carries a null
      // instanceId; its principal is 1:1 with its single session, so resolve
      // that session (ended included) and key on it.
      const runSessionId = await resolveRunSessionId(db, record.principalId, {
        includeEnded: true,
      });
      if (runSessionId === null) {
        return c.json(paginatedResponse([], [], limit));
      }
      // Bind the read to the caller's tenant directly, not only through the
      // session the run's principal owns: the sessionId scope borrows the
      // principal-session 1:1 invariant, so an explicit tenantId filter keeps
      // the mail tenant-safe even if that invariant is ever violated.
      const conditions = [
        eq(sessionMail.tenantId, tenant.id),
        eq(sessionMail.sessionId, runSessionId),
      ];
      if (cursor) {
        conditions.push(
          cursorCondition(sessionMail.createdAt, sessionMail.id, cursor),
        );
      }

      const rows = await db
        .select()
        .from(sessionMail)
        .where(and(...conditions))
        .orderBy(...pageOrder(sessionMail.createdAt, sessionMail.id))
        .limit(limit);

      const items = rows.map((m) => {
        const parsed = parseMailToEmail(m.raw, m.id);
        return {
          id: m.id,
          sessionId: m.sessionId,
          instanceId: m.instanceId ?? null,
          direction: m.direction,
          status: m.status,
          receivedAt: m.createdAt.toISOString(),
          ...parsed,
        };
      });

      return c.json(paginatedResponse(items, rows, limit));
    },
  );

  app.get(
    "/:runId/turns",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Instances"],
      summary: "List inference turns for an instance",
      description:
        "Returns inference turns with their parts in reverse chronological order. Cursor-paginated.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of inference turns",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(InferenceTurnResponse)),
            },
          },
        },
        404: {
          description: "Instance not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenant = c.get("tenant");
      const instanceId = c.req.param("runId");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const record = await findRoutableById(db, instanceId, tenant.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Instance not found" } },
          404,
        );
      }

      // A turn records its producing endpoint's id: an instance id or, since the
      // turn table's foreign key to the instance was dropped, a folded run id.
      // Both equal the path id, so the filter is the same for either kind.
      const conditions = [eq(inferenceTurn.instanceId, record.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(inferenceTurn.startedAt, inferenceTurn.id, cursor),
        );
      }

      const turns = await db
        .select()
        .from(inferenceTurn)
        .where(and(...conditions))
        .orderBy(...pageOrder(inferenceTurn.startedAt, inferenceTurn.id))
        .limit(limit);

      const turnIds = turns.map((t) => t.id);

      const parts =
        turnIds.length > 0
          ? await db
              .select()
              .from(turnPart)
              .where(inArray(turnPart.turnId, turnIds))
              .orderBy(asc(turnPart.ordinal))
          : [];

      const partsByTurn = new Map<string, typeof parts>();
      for (const part of parts) {
        let list = partsByTurn.get(part.turnId);
        if (list === undefined) {
          list = [];
          partsByTurn.set(part.turnId, list);
        }
        list.push(part);
      }

      const items = turns.map((t) => ({
        id: t.id,
        sessionId: t.sessionId,
        instanceId: t.instanceId,
        model: t.model,
        status: t.status,
        startedAt: t.startedAt.toISOString(),
        endedAt: t.endedAt ? t.endedAt.toISOString() : null,
        parts: (partsByTurn.get(t.id) ?? []).map((p) => ({
          id: p.id,
          type: p.type,
          content: p.content ?? null,
          metadata: p.metadata ?? null,
          ordinal: p.ordinal,
        })),
      }));

      return c.json(
        paginatedResponse(
          items,
          turns.map((t) => ({ createdAt: t.startedAt, id: t.id })),
          limit,
        ),
      );
    },
  );

  return app;
}
