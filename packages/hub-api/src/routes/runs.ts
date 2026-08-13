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
import type { DB } from "@intx/db";
import { authorize } from "@intx/authz";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import { extractPartByPath } from "@intx/mime";

import {
  WorkflowRunResponse,
  WorkflowRunHealth,
  OfferingDetail,
  SendMessage,
  MailResponse,
  AttachmentErrorResponse,
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
} from "@intx/hub-sessions";
import { formatOffering } from "./offerings";
import {
  formatRunView,
  viewStatusOf,
  mapRunStatusToViewStatus,
} from "./run-view";
import { WorkflowRunEventsResponse, formatRunEvent } from "./run-events-view";

import type { TenantEnv } from "../context";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import { workflowRunRepoId, WORKFLOW_RUN_REF } from "../workflow-run-lifecycle";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

// Stop, mail send, and mail history are not yet wired onto a workflow (anchor)
// run. They were built for the retired folded-launch surface: stop and mail
// history need genuinely new backing (INTR-454), and mail send routes through
// the run's workflow-native Trigger path (a follow-on slice). Each such route
// returns this not-implemented signal (kept mounted, not 404, so the admin UI
// gets a clean answer) until its mapping lands.
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

// DoS guard on the mail route body. Sized above the legitimate ceiling
// (the 30 MB per-message attachment cap is ~40 MB once base64-encoded,
// plus JSON and text overhead) so over-business-cap requests are rejected
// by the handler with a structured error, while genuine garbage is
// rejected here before the JSON parser allocates a giant string.
const MAX_MAIL_BODY_BYTES = 44 * 1024 * 1024;

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
  // turns/events routes read. Null when the hub runs without the deploy
  // surface (the app.ts XOR keeps assetService and repoStore moving as a
  // unit); those two routes then answer 503 rather than a fabricated empty
  // log, since createRunRoutes mounts unconditionally.
  repoStore: RepoStore | null;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  requireGrant: RequireGrant;
};

export function createRunRoutes({
  db,
  sidecarRouter,
  eventCollectors,
  repoStore,
  grantStore,
  conditionRegistry,
  requireGrant,
}: CreateRunRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  // The run-event log reader is available only when the workflow-run substrate
  // is; a null reader makes the turns/events routes answer 503, never an empty
  // log masquerading as real state.
  const runReader =
    repoStore !== null ? createWorkflowRunReader(repoStore) : null;

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
    requireGrant(idResource("workflow-run", "runId"), "write"),
    describeRoute({
      tags: ["Runs"],
      summary: "Send mail to a run",
      description:
        "Persists the user message as a mail record and dispatches it to the run. Returns JMAP Email-shaped response.",
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
          description: "Run not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Run not live",
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
      // Sending mail to a run is not yet wired onto this surface. It must route
      // through the run's existing workflow-native Trigger path (which enqueues
      // a workflow_run_dispatch), not the launched-agent session this route was
      // built for. Gated to not-implemented until that mapping lands.
      return workflowRunOperationUnsupported(c, "Sending mail to a run");
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
