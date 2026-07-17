import { Hono } from "hono";
import type { Context } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { authorize } from "@intx/authz";
import type { DB, ApprovalStore, SignalCorrelationStore } from "@intx/db";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import { generateId } from "@intx/hub-common";
import { parseApprovalRow } from "@intx/db";
import type { SidecarRouter } from "@intx/hub-sessions";
import {
  ApprovalResponse,
  ApprovalDecision,
  ApproveAction,
  RejectAction,
  ErrorResponse,
  signalName,
} from "@intx/types";

import type { TenantEnv } from "../context";
import { ts } from "../format";

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
  | { kind: "already_resolved" };

/**
 * Close the approval round-trip: authorize the approver, claim the correlation
 * so a redelivered decision cannot resolve twice, flip the approval to its
 * terminal status, and hand the decision to the parked run through the sidecar.
 *
 * Cross-tenant existence is masked as `not_found` rather than `forbidden`: a
 * caller in one tenant must not learn that an approval id exists in another.
 *
 * The claim and the resolve happen inside a single transaction so a duplicate
 * delivery cannot observe a claimed-but-unresolved intermediate state. The
 * signal delivery is deliberately outside the transaction: the row transition
 * is the durable record, and delivery is a best-effort push. When delivery
 * throws after the commit, the approval is resolved but the run was not
 * notified; that surfaces as a 5xx so the failure is visible. Redelivery of an
 * already-recorded decision on a failed push is deferred; it is keyed on the
 * persisted signalId.
 */
async function resolveApproval(
  deps: CreateApprovalRoutesDeps,
  args: ResolveApprovalArgs,
): Promise<ResolveApprovalResult> {
  const {
    db,
    sidecarRouter,
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
    const claim = await signalCorrelationStore.claimTerminal(
      approval.correlationId,
      resolvedAt,
      signalId,
      tx,
    );
    if (claim === null) {
      return null;
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
    return { claim, resolved };
  });

  if (claimed === null) {
    return { kind: "already_resolved" };
  }

  // Redelivery of an already-recorded decision on a failed push is deferred,
  // keyed on the persisted signalId; here a delivery failure is surfaced to
  // the caller, never swallowed.
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
        "Returns pending approval requests for the authenticated user within this tenant.",
      responses: {
        200: {
          description: "List of approvals",
          content: {
            "application/json": {
              schema: resolver(ApprovalResponse.array()),
            },
          },
        },
      },
    }),
    (c) =>
      c.json(
        { error: { code: "not_implemented", message: "Not implemented" } },
        501,
      ),
  );

  app.get(
    "/:approvalId",
    describeRoute({
      tags: ["Approvals"],
      summary: "Get approval details",
      description:
        "Returns the proposed action, context, originating agent, and session.",
      responses: {
        200: {
          description: "Approval details",
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
      },
    }),
    (c) =>
      c.json(
        { error: { code: "not_implemented", message: "Not implemented" } },
        501,
      ),
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
          description: "Approval already resolved",
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
          description: "Approval already resolved",
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
  }
}
