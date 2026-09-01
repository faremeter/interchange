import { and, eq } from "drizzle-orm";
import { Hono } from "hono";
import type { Context } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { authorize } from "@intx/authz";
import type { DB, ApprovalStore, SignalCorrelationStore } from "@intx/db";
import { approval, sidecarAllocation } from "@intx/db/schema";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import { generateId } from "@intx/hub-common";
import { parseApprovalRow } from "@intx/db";
import type {
  SidecarRouter,
  WorkflowDispatchService,
  WorkflowRunLifecycle,
} from "@intx/hub-sessions";
import {
  ApprovalResponse,
  ApprovalDecision,
  ApproveAction,
  RejectAction,
  ErrorResponse,
  isSidecarAllocationDispatchable,
  paginatedSchema,
  signalName,
} from "@intx/types";

import { getLogger } from "@intx/log";

import type { TenantEnv } from "../context";
import { ts } from "../format";
import {
  approvalToolName,
  loadCommittedRunGrants,
  lockWorkflowRunState,
  setRunToolGrantEffect,
} from "../run-grant-materialization";
import {
  cursorCondition,
  pageOrder,
  pageParameters,
  paginatedResponse,
  parsePageParams,
} from "../pagination";

type ParsedApproval = ReturnType<typeof parseApprovalRow>;

const log = getLogger(["hub", "approvals"]);

/**
 * Push a run's grants to the sidecar after a standing (`scope: "always"`)
 * resolution has mutated them in the resolve transaction. Re-reads the now
 * durably-mutated committed grants and rewrites the run's `grants.json` (the
 * `run.grants` handler also refreshes a live child), so the running child sees
 * the change in-flight and any respawn re-reads it.
 *
 * Best-effort relative to the resume: this runs after the resolve committed and
 * the parked run must still be resumed, so a failure is logged loudly but never
 * propagated -- a throw would skip the caller's signal delivery and hang the
 * run. It self-heals regardless: the mutation is already durable in the
 * committed grants, and the next dispatch's per-run re-establish reads the same
 * committed grants, so a missed push only delays the effect, never loses it.
 */
async function propagateRunGrantsToSidecar(
  deps: CreateApprovalRoutesDeps,
  approval: ParsedApproval,
  tenantId: string,
): Promise<void> {
  try {
    const committed = await loadCommittedRunGrants(
      deps.db,
      tenantId,
      approval.runId,
    );
    // A run with no committed per-run grants has nothing to push.
    if (committed === null) return;
    const delivered = deps.sidecarRouter.sendRunGrants(
      approval.agentAddress,
      approval.runId,
      committed.stepGrants,
    );
    if (!delivered) {
      log.warn`standing grant for run ${approval.runId} not pushed: deployment ${approval.agentAddress} is not routable; the next dispatch re-establishes it`;
    }
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    log.error`failed to push standing grant for run ${approval.runId}; the parked run still resumes and the next dispatch re-establishes it: ${message}`;
  }
}

export function formatApproval(row: ParsedApproval) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    anchorRunId: row.anchorRunId,
    runId: row.runId,
    agentAddress: row.agentAddress,
    correlationId: row.correlationId,
    toolDefinition: row.toolDefinition,
    toolArguments: row.toolArguments,
    scope: row.scope,
    status: row.status,
    timeoutAt: row.timeoutAt ? ts(row.timeoutAt) : null,
    resolvedAt: row.resolvedAt ? ts(row.resolvedAt) : null,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

export type CreateApprovalRoutesDeps = {
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  workflowDispatchService?: Pick<
    WorkflowDispatchService,
    "enqueueSignal" | "wake"
  >;
  readRunLifecycles?: ReadRunLifecycles;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  approvalStore: ApprovalStore;
  signalCorrelationStore: SignalCorrelationStore;
};

export type ReadRunLifecycles = (
  agentAddress: string,
  topLevelRunId: string,
  targetRunId: string,
) => Promise<{
  topLevel: WorkflowRunLifecycle;
  target: WorkflowRunLifecycle;
}>;

export type ResolveApprovalRequest = {
  approvalId: string;
  tenantId: string;
  principalId: string;
  status: "approved" | "rejected";
  scope?: "once" | "always";
  decisionPayload: ApprovalDecision;
};

export type ResolveApprovalOutcome =
  | { kind: "resolved"; approval: ParsedApproval }
  | { kind: "not_found" }
  | { kind: "forbidden" }
  | { kind: "already_resolved" }
  | { kind: "deployment_unavailable" }
  | { kind: "run_not_running" }
  | { kind: "dispatch_unavailable" };

type PendingFailureKind = Extract<
  ResolveApprovalOutcome["kind"],
  "deployment_unavailable" | "run_not_running" | "dispatch_unavailable"
>;

/**
 * Close the approval round-trip: authorize the approver, claim the correlation
 * so a redelivered decision cannot resolve twice, flip the approval to its
 * terminal status, and hand the decision to the parked run.
 *
 * Cross-tenant existence is masked as `not_found` rather than `forbidden`: a
 * caller in one tenant must not learn that an approval id exists in another.
 *
 * The claim and the resolve happen inside a single transaction so a duplicate
 * delivery cannot observe a claimed-but-unresolved intermediate state. For a
 * provisioned deployment, that transaction also locks the allocation and
 * enqueues the stable signal id and payload. Delivery then follows the
 * allocation's durable, generation-fenced dispatch path and remains replayable
 * until workflow Git records the signal as received. Deployments without an
 * allocation retain their direct sidecar delivery behavior.
 */
export async function resolveApproval(
  deps: CreateApprovalRoutesDeps,
  args: ResolveApprovalRequest,
): Promise<ResolveApprovalOutcome> {
  const {
    db,
    sidecarRouter,
    workflowDispatchService,
    readRunLifecycles,
    grantStore,
    conditionRegistry,
    approvalStore,
    signalCorrelationStore,
  } = deps;

  const approval = await approvalStore.findById(args.approvalId);
  if (approval === null || approval.tenantId !== args.tenantId) {
    return { kind: "not_found" };
  }

  const authz = await authorize(
    grantStore,
    args.principalId,
    args.tenantId,
    `approval:${approval.anchorRunId}`,
    "resolve",
    conditionRegistry,
  );
  if (authz.effect !== "allow") {
    return { kind: "forbidden" };
  }

  // A standing resolution mutates the run's committed grant for the approved
  // tool. Resolve the tool name up front (before the claim) so a malformed
  // snapshot fails the resolve cleanly rather than mid-transaction. `allow` for
  // approve-always, `deny` for reject-always.
  const standingToolName =
    args.scope === "always" ? approvalToolName(approval.toolDefinition) : null;
  const standingEffect = args.status === "approved" ? "allow" : "deny";

  const resolvedAt = new Date();
  const signalId = generateId("signal");
  const claimed = await db.transaction(async (tx) => {
    const failIfStillPending = async (kind: PendingFailureKind) => {
      const currentApproval = await approvalStore.findById(args.approvalId, tx);
      if (
        currentApproval === null ||
        currentApproval.tenantId !== args.tenantId ||
        currentApproval.anchorRunId !== approval.anchorRunId
      ) {
        return { kind: "not_found" } as const;
      }
      if (currentApproval.status !== "pending") {
        return { kind: "already_resolved" } as const;
      }
      return { kind };
    };

    const [allocation] = await tx
      .select({
        id: sidecarAllocation.id,
        status: sidecarAllocation.status,
      })
      .from(sidecarAllocation)
      .where(eq(sidecarAllocation.anchorRunId, approval.anchorRunId))
      .limit(1)
      .for("update");
    const allocationUnavailable =
      allocation !== undefined &&
      !isSidecarAllocationDispatchable(allocation.status);
    const dispatchUnavailable =
      allocation !== undefined &&
      (workflowDispatchService === undefined ||
        readRunLifecycles === undefined);
    if (allocationUnavailable || dispatchUnavailable) {
      if (allocationUnavailable) {
        return failIfStillPending("deployment_unavailable");
      }
      return failIfStillPending("dispatch_unavailable");
    }
    const provisionedDispatchService =
      allocation === undefined ? undefined : workflowDispatchService;

    if (provisionedDispatchService !== undefined) {
      if (readRunLifecycles === undefined) {
        throw new Error(
          "provisioned approval dispatch is missing its durable lifecycle reader",
        );
      }
      // Pack receipt advances the committed Git ref while holding this same
      // allocation lock. Re-read lifecycle evidence after acquiring the fence
      // so a terminal pack cannot race between this check and the claim.
      const lifecycles = await readRunLifecycles(
        approval.agentAddress,
        approval.anchorRunId,
        approval.runId,
      );
      if (lifecycles.topLevel !== "live" || lifecycles.target !== "live") {
        return failIfStillPending("run_not_running");
      }

      const anchorState = await lockWorkflowRunState(
        tx,
        approval.anchorRunId,
        approval.anchorRunId,
      );
      const targetState =
        approval.runId === approval.anchorRunId
          ? anchorState
          : await lockWorkflowRunState(
              tx,
              approval.anchorRunId,
              approval.runId,
            );
      if (anchorState !== "running" || targetState !== "running") {
        return failIfStillPending("run_not_running");
      }
    }

    const claim = await signalCorrelationStore.claimTerminal(
      approval.correlationId,
      resolvedAt,
      signalId,
      tx,
    );
    if (claim === null) {
      return { kind: "already_resolved" } as const;
    }

    const resolved = await approvalStore.resolve(
      approval.correlationId,
      {
        status: args.status,
        ...(args.scope ? { scope: args.scope } : {}),
        resolvedAt,
      },
      tx,
    );
    if (resolved === null) {
      // The correlation was claimable but the approval did not flip: the two
      // are co-written and must move together, so a divergence is a broken
      // invariant, not a benign race. Throw rather than swallow it.
      throw new Error(
        `approval ${approval.id} correlation ${approval.correlationId} claimed but not resolved`,
      );
    }

    // A standing resolution durably mutates the run's committed grant for this
    // tool IN this transaction -- `allow` on approve-always, `deny` on
    // reject-always -- so a rolled-back resolve reverts the grant change too.
    // The run keeps it: enforcement, the authorization view, and the
    // per-dispatch re-establish all read the committed grant.
    if (standingToolName !== null) {
      await setRunToolGrantEffect(
        tx,
        args.tenantId,
        approval.runId,
        standingToolName,
        standingEffect,
      );
    }

    if (provisionedDispatchService !== undefined) {
      await provisionedDispatchService.enqueueSignal(
        {
          id: `dispatch:${approval.anchorRunId}:${signalId}`,
          anchorRunId: approval.anchorRunId,
          signal: {
            agentAddress: claim.agentAddress,
            runId: claim.runId,
            signalName: signalName(approval.correlationId),
            signalId,
            payload: args.decisionPayload,
          },
          now: resolvedAt,
        },
        tx,
      );
    }
    return {
      kind: "resolved",
      claim,
      resolved,
      provisionedDispatchService,
    } as const;
  });

  if (claimed.kind !== "resolved") {
    return claimed;
  }

  // Standing resolution: the committed grant was mutated in the transaction
  // above. Push the now-durable grants to the sidecar so the running child sees
  // the change in-flight. Post-commit, so a rolled-back resolve pushes nothing.
  // It runs before the wake/signal delivery so the floor lands promptly, but
  // correctness does not depend on that ordering: `deliverSignal` refreshes
  // grants on the child's control FIFO immediately ahead of the resume signal,
  // and the next dispatch re-establishes them regardless.
  if (args.scope === "always") {
    await propagateRunGrantsToSidecar(deps, approval, args.tenantId);
  }

  if (claimed.provisionedDispatchService !== undefined) {
    // enqueueSignal may wake before its surrounding transaction commits. Wake
    // once more after commit so the row cannot wait for the periodic sweep.
    claimed.provisionedDispatchService.wake();
    return { kind: "resolved", approval: claimed.resolved };
  }

  sidecarRouter.sendSignalDeliver({
    agentAddress: claimed.claim.agentAddress,
    runId: claimed.claim.runId,
    signalName: signalName(approval.correlationId),
    signalId,
    payload: args.decisionPayload,
  });

  return { kind: "resolved", approval: claimed.resolved };
}

export function createApprovalRoutes(
  deps: CreateApprovalRoutesDeps,
): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    describeRoute({
      tags: ["Approvals"],
      summary: "List pending approvals in the tenant",
      description:
        "Returns pending approval requests within this tenant, newest first.",
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of pending approvals",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(ApprovalResponse)),
            },
          },
        },
        403: {
          description: "Caller lacks the tenant-wide approval grant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenant = c.get("tenant");
      const principal = c.get("principal");

      // Listing spans the whole tenant, so it needs a tenant-wide grant
      // (`approval:*`), not a per-deployment one: a per-deployment approver
      // holds only `approval:<their deployment>` and reads individual
      // approvals by id via the detail route. The action is `resolve`, the
      // same capability the approve/reject routes gate on -- reading an
      // approval is part of being able to resolve it.
      const authz = await authorize(
        deps.grantStore,
        principal.id,
        tenant.id,
        "approval:*",
        "resolve",
        deps.conditionRegistry,
      );
      if (authz.effect !== "allow") {
        return c.json(
          {
            error: {
              code: "forbidden",
              message: "You do not have permission to list approvals",
            },
          },
          403,
        );
      }

      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [
        eq(approval.tenantId, tenant.id),
        eq(approval.status, "pending"),
      ];
      if (cursor) {
        conditions.push(
          cursorCondition(approval.createdAt, approval.id, cursor),
        );
      }

      const rows = await deps.db.query.approval.findMany({
        where: and(...conditions),
        orderBy: pageOrder(approval.createdAt, approval.id),
        limit,
      });

      const parsed = rows.map(parseApprovalRow);
      return c.json(
        paginatedResponse(parsed.map(formatApproval), parsed, limit),
      );
    },
  );

  app.get(
    "/:approvalId",
    describeRoute({
      tags: ["Approvals"],
      summary: "Get approval details",
      description:
        "Returns the approver-facing tool snapshot, status, and originating deployment for one approval.",
      responses: {
        200: {
          description: "Approval details",
          content: {
            "application/json": { schema: resolver(ApprovalResponse) },
          },
        },
        403: {
          description: "Caller lacks the approval grant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Approval not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenant = c.get("tenant");
      const principal = c.get("principal");
      const approvalId = c.req.param("approvalId");

      const row = await deps.approvalStore.findById(approvalId);
      // Mask cross-tenant existence as 404 -- a caller in one tenant must not
      // learn that an approval id exists in another. Checked before authz so
      // the 403 below never leaks a foreign id.
      if (row === null || row.tenantId !== tenant.id) {
        return c.json(
          { error: { code: "not_found", message: "Approval not found" } },
          404,
        );
      }

      // Same grant the approve/reject routes require: whoever can resolve this
      // approval can read it. No status filter -- a resolved approval is still
      // fetchable so the UI can show its terminal state.
      const authz = await authorize(
        deps.grantStore,
        principal.id,
        tenant.id,
        `approval:${row.anchorRunId}`,
        "resolve",
        deps.conditionRegistry,
      );
      if (authz.effect !== "allow") {
        return c.json(
          {
            error: {
              code: "forbidden",
              message: "You do not have permission to read this approval",
            },
          },
          403,
        );
      }

      return c.json(formatApproval(row));
    },
  );

  app.post(
    "/:approvalId/approve",
    describeRoute({
      tags: ["Approvals"],
      summary: "Approve an action",
      description:
        "Approves the pending action. Scope 'once' authorizes only this suspended call. Scope 'always' additionally records a standing approval, so the same tool is not asked again for the rest of this run.",
      responses: {
        200: {
          description: "Action approved",
          content: {
            "application/json": { schema: resolver(ApprovalResponse) },
          },
        },
        404: {
          description: "Approval not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        403: {
          description: "Approver lacks the approval resolve grant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description:
            "Approval already resolved (takes precedence on retries), workflow run no longer running, or workflow deployment unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        503: {
          description: "Durable workflow dispatch unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", ApproveAction),
    async (c) => {
      const tenant = c.get("tenant");
      const principal = c.get("principal");
      const approvalId = c.req.param("approvalId");
      const body = c.req.valid("json");

      const result = await resolveApproval(deps, {
        approvalId,
        tenantId: tenant.id,
        principalId: principal.id,
        status: "approved",
        scope: body.scope,
        decisionPayload: { outcome: "approved" },
      });

      return respond(c, result);
    },
  );

  app.post(
    "/:approvalId/reject",
    describeRoute({
      tags: ["Approvals"],
      summary: "Reject an action",
      description:
        "Rejects the pending action. An optional message provides feedback to the agent. Scope 'once' (the default) rejects only this call; scope 'always' additionally records a standing rejection, setting the tool to a standing deny so it is blocked without asking again for the rest of this run.",
      responses: {
        200: {
          description: "Action rejected",
          content: {
            "application/json": { schema: resolver(ApprovalResponse) },
          },
        },
        404: {
          description: "Approval not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        403: {
          description: "Approver lacks the approval resolve grant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description:
            "Approval already resolved (takes precedence on retries), workflow run no longer running, or workflow deployment unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        503: {
          description: "Durable workflow dispatch unavailable",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", RejectAction),
    async (c) => {
      const tenant = c.get("tenant");
      const principal = c.get("principal");
      const approvalId = c.req.param("approvalId");
      const body = c.req.valid("json");

      const result = await resolveApproval(deps, {
        approvalId,
        tenantId: tenant.id,
        principalId: principal.id,
        status: "rejected",
        ...(body.scope !== undefined ? { scope: body.scope } : {}),
        decisionPayload: {
          outcome: "rejected",
          ...(body.message !== undefined ? { message: body.message } : {}),
        },
      });

      return respond(c, result);
    },
  );

  return app;
}

function respond(c: Context<TenantEnv>, result: ResolveApprovalOutcome) {
  switch (result.kind) {
    case "resolved":
      return c.json(formatApproval(result.approval), 200);
    case "not_found":
      return c.json(
        { error: { code: "not_found", message: "Approval not found" } },
        404,
      );
    case "forbidden":
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "You do not have permission to resolve this approval",
          },
        },
        403,
      );
    case "already_resolved":
      return c.json(
        {
          error: {
            code: "already_resolved",
            message: "Approval has already been resolved",
          },
        },
        409,
      );
    case "deployment_unavailable":
      return c.json(
        {
          error: {
            code: "deployment_unreachable",
            message: "Workflow deployment allocation is no longer active",
          },
        },
        409,
      );
    case "run_not_running":
      return c.json(
        {
          error: {
            code: "workflow_run_not_running",
            message: "Workflow run is no longer running",
          },
        },
        409,
      );
    case "dispatch_unavailable":
      return c.json(
        {
          error: {
            code: "workflow_dispatch_unavailable",
            message:
              "Durable workflow dispatch is unavailable for this provisioned deployment",
          },
        },
        503,
      );
  }
}
