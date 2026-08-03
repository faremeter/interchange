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

import type { TenantEnv } from "../context";
import { ts } from "../format";
import {
  cursorCondition,
  pageOrder,
  pageParameters,
  paginatedResponse,
  parsePageParams,
} from "../pagination";

type ParsedApproval = ReturnType<typeof parseApprovalRow>;

function formatApproval(row: ParsedApproval) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    deploymentId: row.deploymentId,
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
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  approvalStore: ApprovalStore;
  signalCorrelationStore: SignalCorrelationStore;
};

type ResolveApprovalArgs = {
  approvalId: string;
  tenantId: string;
  principalId: string;
  status: "approved" | "rejected";
  scope?: "once";
  decisionPayload: ApprovalDecision;
};

type ResolveApprovalResult =
  | { kind: "resolved"; approval: ParsedApproval }
  | { kind: "not_found" }
  | { kind: "forbidden" }
  | { kind: "already_resolved" }
  | { kind: "deployment_unavailable" }
  | { kind: "dispatch_unavailable" };

/**
 * Close the approval round-trip: authorize the approver, claim the correlation
 * so a redelivered decision cannot resolve twice, flip the approval to its
 * terminal status, and hand the decision to the parked run.
 *
 * Cross-tenant existence is masked as `not_found` rather than `forbidden`: a
 * caller in one tenant must not learn that an approval id exists in another.
 *
 * The claim and the resolve happen inside a single transaction so a duplicate
 * delivery cannot observe a claimed-but-unresolved intermediate state. For an
 * exclusive deployment, that transaction also locks the allocation and
 * enqueues the stable signal id and payload. Delivery then follows the
 * allocation's durable, generation-fenced dispatch path and remains replayable
 * until workflow Git records the signal as received. Shared deployments retain
 * their direct sidecar delivery behavior.
 */
async function resolveApproval(
  deps: CreateApprovalRoutesDeps,
  args: ResolveApprovalArgs,
): Promise<ResolveApprovalResult> {
  const {
    db,
    sidecarRouter,
    workflowDispatchService,
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
    `approval:${approval.deploymentId}`,
    "resolve",
    conditionRegistry,
  );
  if (authz.effect !== "allow") {
    return { kind: "forbidden" };
  }

  const resolvedAt = new Date();
  const signalId = generateId("signal");
  const claimed = await db.transaction(async (tx) => {
    const [allocation] = await tx
      .select({
        id: sidecarAllocation.id,
        status: sidecarAllocation.status,
      })
      .from(sidecarAllocation)
      .where(eq(sidecarAllocation.anchorRunId, approval.deploymentId))
      .limit(1)
      .for("update");
    const allocationUnavailable =
      allocation !== undefined &&
      !isSidecarAllocationDispatchable(allocation.status);
    const dispatchUnavailable =
      allocation !== undefined && workflowDispatchService === undefined;
    if (allocationUnavailable || dispatchUnavailable) {
      const currentApproval = await approvalStore.findById(args.approvalId, tx);
      if (
        currentApproval === null ||
        currentApproval.tenantId !== args.tenantId ||
        currentApproval.deploymentId !== approval.deploymentId
      ) {
        return { kind: "not_found" } as const;
      }
      if (currentApproval.status !== "pending") {
        return { kind: "already_resolved" } as const;
      }
      if (allocationUnavailable) {
        return { kind: "deployment_unavailable" } as const;
      }
      return { kind: "dispatch_unavailable" } as const;
    }
    const exclusiveDispatchService =
      allocation === undefined ? undefined : workflowDispatchService;

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

    if (exclusiveDispatchService !== undefined) {
      await exclusiveDispatchService.enqueueSignal(
        {
          id: `dispatch:${approval.deploymentId}:${signalId}`,
          anchorRunId: approval.deploymentId,
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
      exclusiveDispatchService,
    } as const;
  });

  if (claimed.kind !== "resolved") {
    return claimed;
  }

  if (claimed.exclusiveDispatchService !== undefined) {
    // enqueueSignal may wake before its surrounding transaction commits. Wake
    // once more after commit so the row cannot wait for the periodic sweep.
    claimed.exclusiveDispatchService.wake();
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
        `approval:${row.deploymentId}`,
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
        "Approves the pending action. With scope 'once', the approval is one-time. Scope 'always' is not yet supported: a standing grant requires the tool identity, which the suspend path does not yet capture.",
      responses: {
        200: {
          description: "Action approved",
          content: {
            "application/json": { schema: resolver(ApprovalResponse) },
          },
        },
        400: {
          description: "Unsupported scope",
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
        403: {
          description: "Approver lacks the approval resolve grant",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description:
            "Approval already resolved (takes precedence on retries) or workflow deployment unavailable",
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

      if (body.scope === "always") {
        return c.json(
          {
            error: {
              code: "unsupported_scope",
              message:
                "scope 'always' is not yet supported: a standing grant requires the tool identity, which the suspend path does not yet capture",
            },
          },
          400,
        );
      }

      const result = await resolveApproval(deps, {
        approvalId,
        tenantId: tenant.id,
        principalId: principal.id,
        status: "approved",
        scope: "once",
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
        "Rejects the pending action. An optional message provides feedback to the agent.",
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
            "Approval already resolved (takes precedence on retries) or workflow deployment unavailable",
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

function respond(c: Context<TenantEnv>, result: ResolveApprovalResult) {
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
    case "dispatch_unavailable":
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
}
