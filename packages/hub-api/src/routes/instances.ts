import { eq, and, inArray, isNull, isNotNull } from "drizzle-orm";
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
  InferenceTurnResponse,
  ErrorResponse,
  paginatedSchema,
} from "@intx/types";
import {
  findRoutableById,
  resolveRunIdForSession,
  runRowToRoutableRecord,
  type EventCollectorRegistry,
  type RoutableRecord,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import { formatOffering } from "./offerings";
import {
  formatInstanceView,
  instanceStatusOf,
  mapRunStatusToInstanceStatus,
} from "./instance-view";

import type { TenantEnv } from "../context";
import { idResource } from "../middleware/grant";
import type { RequireGrant } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

// The interactive run operations -- stop, mail send + history, turns, and the
// event stream -- have no workflow-run backing yet: they were built for the
// retired folded-launch surface, and a workflow (anchor) run carries none of
// the per-run session, collector, or terminal machinery they read. Each such
// route returns this not-implemented signal (kept mounted, not 404, so the
// admin UI gets a clean answer) until the mapping lands.
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
};

export function createInstanceRoutes({
  db,
  sidecarRouter,
  eventCollectors,
  grantStore,
  conditionRegistry,
  requireGrant,
}: CreateInstanceRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

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
          isNull(workflowRun.anchorRunId),
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
      const runId = c.req.param("runId");

      // Resolve across the fold: a legacy agent instance or a folded run. The
      // origin agent supplies the display name for either kind.
      const record = await findRoutableById(db, runId, tenantCtx.id);
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
      const runId = c.req.param("runId");

      const record = await findRoutableById(db, runId, tenantCtx.id);
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
      const runId = c.req.param("runId");

      const record = await findRoutableById(db, runId, tenantCtx.id);
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
      // Stopping a run is a forced early-cancel that a workflow (anchor) run
      // has no backing for yet -- it needs a hub-side CancelRequested author
      // and a lease-free allocation-release seam. Since the folded-launch
      // surface was retired, every run this route could resolve is a workflow
      // run, so the stop is gated to not-implemented until that mapping lands
      // (tracked as INTR-454). A `markTerminal`-only stop would lie -- it would
      // desync the child and its allocation -- so no partial stop is shipped.
      return workflowRunOperationUnsupported(c, "Stopping a run");
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
      // The live SSE event stream reads a per-run event collector that only a
      // launched instance ever had; a workflow (anchor) run has none. Since the
      // folded-launch surface was retired, every run this route could resolve
      // is a workflow run, so the stream is gated to not-implemented until it
      // is remapped onto the run's git-backed event timeline (a repurpose
      // slice). A malformed status-only stub would misrepresent a live run.
      return workflowRunOperationUnsupported(c, "The run event stream");
    },
  );

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
      // Sending mail needs a launched instance's live session, which a workflow
      // (anchor) run does not have. Since the folded-launch surface was retired,
      // every run this route could resolve is a workflow run, so the send is gated
      // to not-implemented until it is remapped onto the run's Trigger route (a
      // repurpose slice).
      return workflowRunOperationUnsupported(c, "Sending mail to a run");
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
      // A run's mail history was keyed on a launched instance's session; a workflow
      // (anchor) run has no durable mail archive yet. Since the folded-launch
      // surface was retired, every run this route could resolve is a workflow run,
      // so the history is gated to not-implemented until a workflow-run mail archive
      // lands (tracked as INTR-454).
      return workflowRunOperationUnsupported(c, "Run mail history");
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
      // A run's inference turns were recorded per launched instance; a workflow
      // (anchor) run has no such per-run turn stream yet. Since the folded-launch
      // surface was retired, every run this route could resolve is a workflow run,
      // so the listing is gated to not-implemented until it is remapped onto the
      // run's git-backed step events (a repurpose slice).
      return workflowRunOperationUnsupported(
        c,
        "Listing a run's inference turns",
      );
    },
  );

  return app;
}
