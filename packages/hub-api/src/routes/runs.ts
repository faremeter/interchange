import { eq, and, inArray, isNotNull } from "drizzle-orm";
import { Hono, type Context } from "hono";
import { bodyLimit } from "hono/body-limit";
import { describeRoute, resolver, validator } from "hono-openapi";

import {
  offering,
  sessionMail,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type { DB, ApprovalStore } from "@intx/db";
import { authorize } from "@intx/authz";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import { extractPartByPath } from "@intx/mime";

import {
  WorkflowRunResponse,
  WorkflowRunHealth,
  RunAuthorizationResponse,
  RunApprovalsResponse,
  OfferingDetail,
  SendMessage,
  MailResponse,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";
import {
  createWorkflowRunReader,
  findRoutableById,
  resolveRunIdForSession,
  runRowToRoutableRecord,
  type EventCollectorRegistry,
  type RepoStore,
  type RoutableRecord,
  type SessionService,
  type SidecarRouter,
  type WorkflowDispatchService,
} from "@intx/hub-sessions";
import { formatOffering } from "./offerings";
import { formatApproval } from "./approvals";
import {
  formatRunView,
  viewStatusOf,
  mapRunStatusToViewStatus,
} from "./run-view";
import { WorkflowRunEventsResponse, formatRunEvent } from "./run-events-view";

import type { TenantEnv } from "../context";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import { loadCommittedRunGrants } from "../run-grant-materialization";
import { workflowRunRepoId, WORKFLOW_RUN_REF } from "../workflow-run-lifecycle";
import {
  createWorkflowRunTrigger,
  MAX_MAIL_BODY_BYTES,
  WorkflowRunTriggerResponse,
} from "../workflow-run-trigger";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

// Stop and mail history are not yet wired onto a workflow (anchor) run. They
// were built for the retired folded-launch surface and need genuinely new
// backing (INTR-454): stop is a forced early-cancel with no author yet, and a
// run has no durable mail archive. Each such route returns this not-implemented
// signal (kept mounted, not 404, so the admin UI gets a clean answer) until its
// mapping lands. Mail SEND is wired: it routes through the run's workflow-native
// Trigger path.
function workflowRunOperationUnsupported(
  c: Context<TenantEnv>,
  operation: string,
) {
  return c.json(
    {
      error: {
        code: "not_implemented",
        message: `${operation} is not yet supported for workflow runs`,
      },
    },
    501,
  );
}

type RunStatusFilter =
  | "deployed"
  | "running"
  | "updating"
  | "error"
  | "stopped";

function isRunStatusFilter(
  status: string | undefined,
): status is RunStatusFilter {
  return (
    status === "deployed" ||
    status === "running" ||
    status === "updating" ||
    status === "error" ||
    status === "stopped"
  );
}

type WorkflowRunStatus = (typeof workflowRun.$inferSelect)["status"];

// The `workflow_run` statuses that present as a given run-view status filter,
// derived as the inverse of `mapRunStatusToViewStatus` so the two cannot drift:
// every run status is bucketed under the view status it maps onto. The
// `deployed` run status maps onto the `deployed` filter, and the run-list query
// below now selects top-level anchor runs, so that filter returns the deployed
// anchors. No run status maps onto `updating`, so that filter selects no runs
// (empty array) and the caller skips the run query entirely.
const RUN_STATUSES_BY_VIEW_STATUS: Record<
  RunStatusFilter,
  WorkflowRunStatus[]
> = (() => {
  const byStatus: Record<RunStatusFilter, WorkflowRunStatus[]> = {
    deployed: [],
    running: [],
    updating: [],
    error: [],
    stopped: [],
  };
  for (const runStatus of workflowRun.status.enumValues) {
    byStatus[mapRunStatusToViewStatus(runStatus)].push(runStatus);
  }
  return byStatus;
})();

// The row shape the run-list query projects: the run columns the run view
// needs, plus the definition name.
type RunListRow = {
  run: typeof workflowRun.$inferSelect;
  definitionName: string;
};

// Shape a listed run into the routing record the run view expects, through the
// same builder `findRoutableById` uses. The query filters `address` non-null,
// so a null here is a broken invariant and surfaces loudly.
function runToRecord(row: RunListRow): RoutableRecord {
  if (row.run.address === null) {
    throw new Error(`run ${row.run.id} listed with a null address`);
  }
  return runRowToRoutableRecord(row.run, row.run.address);
}

export type CreateRunRoutesDeps = {
  db: DB["db"];
  sessionService: SessionService;
  sidecarRouter: SidecarRouter;
  eventCollectors: EventCollectorRegistry;
  // The workflow-run substrate that backs the durable run-event log the
  // turns/events routes read and the run-event state the mail-send trigger
  // reads. It is null when the hub runs without the deploy surface; the
  // substrate-backed routes then answer 503 rather than fabricating state,
  // since createRunRoutes mounts unconditionally.
  repoStore: RepoStore | null;
  // The durable dispatch queue an exclusive deployment's trigger enqueues onto.
  // Absent when the hub runs without durable dispatch; the trigger then 503s an
  // exclusive send, exactly as the deployment Trigger route does.
  workflowDispatchService?: WorkflowDispatchService;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  requireGrant: RequireGrant;
  // The run approvals-list route reads the run's approval decisions here.
  approvalStore: ApprovalStore;
};

export function createRunRoutes({
  db,
  sidecarRouter,
  eventCollectors,
  repoStore,
  workflowDispatchService,
  grantStore,
  conditionRegistry,
  requireGrant,
  approvalStore,
}: CreateRunRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  // The run-event log reader is available only when the workflow-run substrate
  // is; a null reader makes the turns/events routes answer 503, never an empty
  // log masquerading as real state.
  const runReader =
    repoStore !== null ? createWorkflowRunReader(repoStore) : null;

  // The mail-send trigger fires the run through its workflow-native Trigger
  // path. It needs the run-event substrate (terminal-state read), so a null
  // repoStore leaves the trigger null and the mail-send route answers 503.
  const triggerWorkflowRun =
    repoStore !== null
      ? createWorkflowRunTrigger({
          db,
          grantStore,
          sidecarRouter,
          ...(workflowDispatchService !== undefined
            ? { workflowDispatchService }
            : {}),
          repoStore,
        })
      : null;

  // The turns and events routes project the same top-level run: its committed,
  // git-backed event log, read whole and seq-ordered. `turns` presents that log
  // as the run's step and lifecycle events (the log carries only those); the
  // `events` stream is the same projection the admin UI polls, deduplicating on
  // seq. Both resolve the run first (a 404 tenant-scopes the read) and 503 when
  // the substrate is absent.
  async function serveRunEvents(c: Context<TenantEnv>, runId: string) {
    const tenantCtx = c.get("tenant");

    const record = await findRoutableById(db, runId, tenantCtx.id);
    if (record === undefined) {
      return c.json(
        { error: { code: "not_found", message: "Run not found" } },
        404,
      );
    }

    if (runReader === null) {
      return c.json(
        {
          error: {
            code: "unavailable",
            message: "The run event log is not available",
          },
        },
        503,
      );
    }

    // The top-level run's own events live under `runs/<runId>/` in its
    // deployment's event repo: the run id is the local part of the deployment
    // address the supervisor keys the log by, so this reads exactly the run
    // named by `:runId` and excludes its section/body child runs. A run that
    // has not been triggered yet has no `runs/<runId>/` and reads as an empty
    // timeline -- honestly empty, distinct from the 503 absent-substrate case.
    const events = await runReader.readRunEvents(
      workflowRunRepoId(runId, tenantCtx.domain),
      WORKFLOW_RUN_REF,
      runId,
    );
    return c.json({ runId, events: events.map(formatRunEvent) });
  }

  app.get(
    "/",
    requireGrant("workflow-run:*", "read"),
    describeRoute({
      tags: ["Runs"],
      summary: "List workflow runs",
      description:
        "Lists the tenant's top-level workflow runs. Filterable by definitionId and status.",
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
          description: "List of runs",
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

      // A run is listed when it is a top-level run: it owns a routing address
      // and self-anchors (`anchorRunId === id`). This is the SQL form of the
      // shared `isTopLevelRun` predicate the detail resolver classifies on, so
      // the list and the resolver cannot drift. It drops an address-less child
      // park row, and now includes the deployed/live anchors. When a status
      // filter selects no run statuses (`updating`), skip the query entirely.
      const statusFilter = isRunStatusFilter(status) ? status : undefined;

      const runStatuses =
        statusFilter === undefined
          ? undefined
          : RUN_STATUSES_BY_VIEW_STATUS[statusFilter];
      let runRows: RunListRow[] = [];
      if (runStatuses === undefined || runStatuses.length > 0) {
        const runConditions = [
          eq(workflowRun.tenantId, tenantCtx.id),
          isNotNull(workflowRun.address),
          eq(workflowRun.anchorRunId, workflowRun.id),
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
          runRows.map((r) => formatRunView(runToRecord(r), r.definitionName)),
          runRows.map((r) => ({ createdAt: r.run.createdAt, id: r.run.id })),
          limit,
        ),
      );
    },
  );

  app.get(
    "/blobs/:blobId",
    describeRoute({
      tags: ["Runs"],
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
      // mail carries a null runId and keys on its session, so recover the
      // run id from the session; a legacy row's non-null runId is used
      // directly. Route the run resolution through workflow_run so the subject
      // is proven to name a real run of this tenant -- a session held by any
      // non-run principal fails closed to 404 rather than authorizing a
      // fabricated subject.
      const resolvedRunId =
        mailRow.runId ??
        (await resolveRunIdForSession(db, mailRow.sessionId, tenant.id));
      if (!resolvedRunId) {
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
        `workflow-run:${resolvedRunId}`,
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
      tags: ["Runs"],
      summary: "Get run detail",
      description:
        "Returns workflow run state including status, public key, and sidecar assignment.",
      responses: {
        200: {
          description: "Run detail",
          content: {
            "application/json": { schema: resolver(WorkflowRunResponse) },
          },
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const runId = c.req.param("runId");

      // Resolve the top-level workflow run. The run's definition supplies the
      // display name.
      const record = await findRoutableById(db, runId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Run not found" } },
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
          { error: { code: "not_found", message: "Run not found" } },
          404,
        );
      }

      // Enrich with runtime status from the event collector if available.
      const runtimeStatus = eventCollectors.getStatus(record.address);

      return c.json(
        formatRunView(record, definitionRow.name, runtimeStatus?.status),
      );
    },
  );

  app.get(
    "/:runId/authorization",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Runs"],
      summary: "Get run authorization",
      description:
        "Returns the run's effective authorization floor: its committed grants and their resolved effects. A standing 'always' approval mutates the tool's committed grant in place at resolve time (approve-always sets allow, reject-always sets deny), so a standing-resolved tool reads that effect directly here. This is the floor the runtime enforces, so the view mirrors what the run can do. Complete for the source-ref deploy lineage (the shipping pipeline); a pinned-tool deploy's sidecar-injected ask floor is not reflected here.",
      responses: {
        200: {
          description: "Run authorization",
          content: {
            "application/json": {
              schema: resolver(RunAuthorizationResponse),
            },
          },
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const runId = c.req.param("runId");

      const record = await findRoutableById(db, runId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Run not found" } },
          404,
        );
      }

      // The run's committed per-run grants ARE its effective floor: a standing
      // ("always") resolution mutates them in place (ask -> allow on
      // approve-always, ask -> deny on reject-always), so what the child
      // enforces and what this returns are the same rows -- the view cannot
      // drift from enforcement. A run with no committed grants (deployed but
      // never triggered) has an empty floor.
      const committed = await loadCommittedRunGrants(db, tenantCtx.id, runId);
      if (committed === null) {
        return c.json({ runId, grants: [] });
      }
      return c.json({
        runId,
        grants: committed.stepGrants.map((g) => ({
          resource: g.resource,
          action: g.action,
          effect: g.effect,
        })),
      });
    },
  );

  app.get(
    "/:runId/approvals",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Runs"],
      summary: "List run approvals",
      description:
        "Returns the run's approval decisions, newest first, across every status. The tools an operator turned into standing approvals are the entries with scope 'always' and status 'approved'.",
      responses: {
        200: {
          description: "Run approvals",
          content: {
            "application/json": { schema: resolver(RunApprovalsResponse) },
          },
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const runId = c.req.param("runId");

      const record = await findRoutableById(db, runId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Run not found" } },
          404,
        );
      }

      const approvals = await approvalStore.listByRunId(tenantCtx.id, runId);
      return c.json({ runId, approvals: approvals.map(formatApproval) });
    },
  );

  app.get(
    "/:runId/health",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Runs"],
      summary: "Get run health",
      description:
        "Returns liveness and readiness for a live run. Liveness reflects whether the run's sidecar connection is active. Readiness reflects whether the run has an active event collector and can process work.",
      responses: {
        200: {
          description: "Health status",
          content: {
            "application/json": { schema: resolver(WorkflowRunHealth) },
          },
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        410: {
          description: "Run stopped",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const runId = c.req.param("runId");

      const record = await findRoutableById(db, runId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Run not found" } },
          404,
        );
      }

      if (viewStatusOf(record) === "stopped") {
        return c.json(
          { error: { code: "gone", message: "Run has stopped" } },
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
      tags: ["Runs"],
      summary: "List run offerings",
      description:
        "Returns the offerings associated with the run's workflow definition. These represent the capabilities the run can provide.",
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
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const runId = c.req.param("runId");

      const record = await findRoutableById(db, runId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Run not found" } },
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
          { error: { code: "not_found", message: "Run not found" } },
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
      tags: ["Runs"],
      summary: "Stop a run",
      description:
        "Stops a live workflow run and releases its sidecar allocation.",
      responses: {
        204: {
          description: "Run stopped",
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Run already stopped",
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
      // Stopping a run is a forced early-cancel that a workflow (anchor) run
      // has no backing for yet -- it needs a hub-side CancelRequested author
      // and a lease-free allocation-release seam. The stop is gated to
      // not-implemented until that mapping lands (tracked as INTR-454). A
      // `markTerminal`-only stop would lie -- it would desync the child and its
      // allocation -- so no partial stop is shipped.
      return workflowRunOperationUnsupported(c, "Stopping a run");
    },
  );

  app.get(
    "/:runId/events",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Runs"],
      summary: "Read a run's event log",
      description:
        "Returns the run's committed, seq-ordered event log (RunStarted, StepStarted, StepCompleted, SignalAwaited, RunCompleted, etc.). The full log is returned on every call; a client polling for live updates deduplicates on seq. A run that has not been triggered yet returns an empty list.",
      responses: {
        200: {
          description: "Seq-ordered run events",
          content: {
            "application/json": { schema: resolver(WorkflowRunEventsResponse) },
          },
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        503: {
          description: "Run event log unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => serveRunEvents(c, c.req.param("runId")),
  );

  app.post(
    "/:runId/mail",
    requireGrant(idResource("workflow-run", "runId"), "manage"),
    describeRoute({
      tags: ["Runs"],
      summary: "Send mail to a run",
      description:
        "Delivers a fresh signed conversation message to the run, firing it through the run's workflow-native Trigger path. The first accepted message fires the run; while it remains live, later messages may resume its onTrigger input. A terminal run cannot be fired again. The returned messageId identifies this trigger occurrence.",
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
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description:
            "Run address is not routable, its allocation is no longer active, or the run is terminal",
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
        503: {
          description: "Run trigger substrate unavailable",
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
      const tenantCtx = c.get("tenant");
      const runId = c.req.param("runId");

      // Resolve the top-level run first so an unknown id tenant-scopes to a 404,
      // exactly as the turns/events routes do, before the trigger runs.
      const record = await findRoutableById(db, runId, tenantCtx.id);
      if (record === undefined) {
        return c.json(
          { error: { code: "not_found", message: "Run not found" } },
          404,
        );
      }

      // Mail send fires the run through its workflow-native Trigger path. That
      // path needs the workflow asset and the run-event substrate; when the hub
      // runs without the deploy surface the trigger is null, so answer 503
      // rather than silently dropping the send.
      if (triggerWorkflowRun === null) {
        return c.json(
          {
            error: {
              code: "unavailable",
              message: "The run trigger substrate is not available",
            },
          },
          503,
        );
      }

      const result = await triggerWorkflowRun({
        tenant: tenantCtx,
        principal: c.get("principal"),
        userName: c.get("user")?.name ?? null,
        anchorRunId: runId,
        message: c.req.valid("json"),
      });
      return c.json(result.body, result.status);
    },
  );

  app.get(
    "/:runId/mail",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Runs"],
      summary: "List mail for a run",
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
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      // A run's mail history was keyed on a launched instance's session; a
      // workflow (anchor) run has no durable mail archive yet, so there is
      // genuinely new backing to build. Gated to not-implemented until a
      // workflow-run mail archive lands (tracked as INTR-454).
      return workflowRunOperationUnsupported(c, "Run mail history");
    },
  );

  app.get(
    "/:runId/turns",
    requireGrant(idResource("workflow-run", "runId"), "read"),
    describeRoute({
      tags: ["Runs"],
      summary: "List a run's turns",
      description:
        "Returns the run's step and lifecycle events in seq order -- the same committed event log the events route serves, presented as the run's turns. A run that has not been triggered yet returns an empty list.",
      responses: {
        200: {
          description: "Seq-ordered run events",
          content: {
            "application/json": { schema: resolver(WorkflowRunEventsResponse) },
          },
        },
        404: {
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        503: {
          description: "Run event log unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => serveRunEvents(c, c.req.param("runId")),
  );

  return app;
}
