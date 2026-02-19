import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import {
  ApprovalResponse,
  ApproveAction,
  RejectAction,
  ErrorResponse,
} from "@interchange/types";

import type { AppEnv } from "../context";

const app = new Hono<AppEnv>();

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
      "Approves the pending action. With scope 'once', the approval is one-time. With scope 'always', a persistent grant is created so the agent won't need to ask again.",
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
    },
  }),
  validator("json", ApproveAction),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
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
    },
  }),
  validator("json", RejectAction),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { app as approvalRoutes };
