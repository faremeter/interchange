// A "rich" tool that opens an approval gate. The handler returns a
// ToolResult whose `pendingMarker` carries a correlation ID; the
// reactor registers a gate keyed by that ID and waits for a matching
// inbound message to arrive before the operation is treated as
// completed. The model sees the tool's `content` (so it can tell the
// user "I have requested approval") but the gate is invisible to the
// model — it lives on the reactor's pending-operations table.

import { tool, type AgentTool } from "@interchange/agent";
import type { ToolResult } from "@interchange/types/runtime";

/**
 * Build the request_approval tool. `correlationIdFor(callId)` lets
 * the caller pin the correlation ID for tests; production callers
 * leave it undefined and the helper picks a UUID.
 */
export function createApprovalTool(opts?: {
  correlationIdFor?: (callId: string) => string;
  expectedFrom?: string;
}): AgentTool {
  const expectedFrom = opts?.expectedFrom ?? "approver@local";
  return tool({
    definition: {
      name: "request_approval",
      description:
        "Submit a sensitive action for human approval. Returns immediately with a correlation ID; the action only proceeds once a matching approval message is delivered.",
      inputSchema: {
        type: "object",
        properties: {
          action: { type: "string" },
        },
        required: ["action"],
      },
    },
    handler: async (call): Promise<ToolResult> => {
      const action =
        typeof call.arguments["action"] === "string"
          ? call.arguments["action"]
          : "(unspecified)";
      const correlationId =
        opts?.correlationIdFor?.(call.id) ??
        `approval-${call.id}-${crypto.randomUUID()}`;
      return {
        callId: call.id,
        content: [
          "Approval request submitted.",
          `Action: ${action}`,
          `Correlation: ${correlationId}`,
          "",
          "Wait for the human approver to deliver a matching inbound",
          `message (interchangeCorrelationId=${correlationId}) before`,
          "treating the action as completed.",
        ].join("\n"),
        detail: {
          action,
          correlationId,
        },
        pendingMarker: {
          status: "pending",
          correlationId,
          expectedFrom,
        },
      };
    },
  });
}
