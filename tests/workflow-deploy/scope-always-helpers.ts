// Helpers shared by the spawn-real scope-always approval tests: a two-call
// mock inference server, the approve-route session and event-collector stand-ins,
// and the real hub's signal correlation co-write.

import { and, eq, inArray } from "drizzle-orm";

import {
  createApprovalStore,
  createSignalCorrelationStore,
  createWorkflowRunStore,
} from "@intx/db";
import { workflowRun } from "@intx/db/schema";
import type { GetSession } from "@intx/hub-api";
import { generateId } from "@intx/hub-common";
import type { EventCollectorRegistry } from "@intx/hub-sessions";
import { signalName } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";
import type { TestDb } from "@intx/test-harness/db-harness";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

// --- two-call inference mock -----------------------------------------------
//
// A minimal Anthropic-style SSE server that issues the ask-marked tool call
// once per entry in `calls` in a single run before replying. It counts the
// `tool_result` blocks already in history: while fewer than that many results
// are present it re-issues a `tool_use` (with a distinct id per call); once all
// are present it replies with `${replyPrefix}<last result>`. The shared
// harness mock only drives one call per run, so this small server -- not a
// harness change -- is what lets the run make the SECOND call that proves the
// run's committed grant was mutated. The SSE framing mirrors the harness mock's.

type MockBlock = {
  type?: string;
  text?: string;
  content?: string | MockBlock[];
};
type MockMessage = { role?: string; content?: string | MockBlock[] };
type MockRequest = { tools?: { name: string }[]; messages?: MockMessage[] };

function sse(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

function textTurnText(text: string): string[] {
  return [
    sse("message_start", {
      type: "message_start",
      message: {
        id: "msg_mock",
        type: "message",
        role: "assistant",
        content: [],
        model: "mock-model",
        stop_reason: null,
        usage: { input_tokens: 10, output_tokens: 0 },
      },
    }),
    sse("content_block_start", {
      type: "content_block_start",
      index: 0,
      content_block: { type: "text", text: "" },
    }),
    sse("content_block_delta", {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text },
    }),
    sse("content_block_stop", { type: "content_block_stop", index: 0 }),
    sse("message_delta", {
      type: "message_delta",
      delta: { stop_reason: "end_turn" },
      usage: { output_tokens: 20 },
    }),
    sse("message_stop", { type: "message_stop" }),
  ];
}

function toolUseTurn(
  toolName: string,
  input: Record<string, unknown>,
  toolUseId: string,
): string[] {
  return [
    sse("message_start", {
      type: "message_start",
      message: {
        id: "msg_mock_tooluse",
        type: "message",
        role: "assistant",
        content: [],
        model: "mock-model",
        stop_reason: null,
        usage: { input_tokens: 10, output_tokens: 0 },
      },
    }),
    sse("content_block_start", {
      type: "content_block_start",
      index: 0,
      content_block: {
        type: "tool_use",
        id: toolUseId,
        name: toolName,
        input: {},
      },
    }),
    sse("content_block_delta", {
      type: "content_block_delta",
      index: 0,
      delta: {
        type: "input_json_delta",
        partial_json: JSON.stringify(input),
      },
    }),
    sse("content_block_stop", { type: "content_block_stop", index: 0 }),
    sse("message_delta", {
      type: "message_delta",
      delta: { stop_reason: "tool_use" },
      usage: { output_tokens: 20 },
    }),
    sse("message_stop", { type: "message_stop" }),
  ];
}

function flattenBlockText(content: string | MockBlock[] | undefined): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .filter((b) => b.type === "text" && b.text !== undefined)
      .map((b) => b.text ?? "")
      .join("");
  }
  return "";
}

function countToolResults(req: MockRequest): number {
  let count = 0;
  for (const message of req.messages ?? []) {
    if (!Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (block.type === "tool_result") count += 1;
    }
  }
  return count;
}

function lastToolResultText(req: MockRequest): string {
  let last = "";
  for (const message of req.messages ?? []) {
    if (!Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (block.type === "tool_result") last = flattenBlockText(block.content);
    }
  }
  return last;
}

export function startTwoCallMockInference(opts: {
  toolName: string;
  calls: readonly Record<string, unknown>[];
  replyPrefix: string;
  toolUseIdPrefix: string;
}): {
  server: ReturnType<typeof Bun.serve>;
  requests: MockRequest[];
} {
  const requests: MockRequest[] = [];
  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this is a test mock server that only receives requests from the sidecar under test; the Anthropic request shape is known
      const body = (await req.json()) as MockRequest;
      requests.push(body);

      const seenResults = countToolResults(body);
      let events: string[];
      if (seenResults < opts.calls.length) {
        const input = opts.calls[seenResults];
        if (input === undefined) throw new Error("unreachable");
        events = toolUseTurn(
          opts.toolName,
          input,
          `${opts.toolUseIdPrefix}${seenResults}`,
        );
      } else {
        events = textTurnText(`${opts.replyPrefix}${lastToolResultText(body)}`);
      }

      const stream = new ReadableStream({
        start(controller) {
          for (const event of events) {
            controller.enqueue(new TextEncoder().encode(event));
          }
          controller.close();
        },
      });
      return new Response(stream, {
        headers: { "content-type": "text/event-stream" },
      });
    },
  });
  return { server, requests };
}

// --- approve-route scaffolding ---------------------------------------------

export function createMockGetSession(
  userId: string,
  label: string,
): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "operator@example.com",
      emailVerified: true,
      name: "Operator",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: `session_${label}`,
      userId,
      token: `tok_${label}`,
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

export function createMockEventCollectors(
  label: string,
): EventCollectorRegistry {
  const notImpl = (name: string): never => {
    throw new Error(`${label} mock: ${name} not implemented`);
  };
  return {
    create: () => notImpl("create"),
    dispatch: () => notImpl("dispatch"),
    abandon: () => notImpl("abandon"),
    has: () => false,
    getStatus: () => undefined,
    getAccumulatedText: () => undefined,
    getCurrentTurnId: () => undefined,
    getLastTurnId: () => undefined,
  };
}

/**
 * The real hub co-write, mirroring `createHubSessionLookups`'s
 * `registerSignalCorrelation`: resolve tenancy from the deployment's anchor run
 * the address names, cross-check the frame's `anchorRunId` against the slug the
 * address derives, and co-write the `signal_correlation` + `approval` rows in one
 * transaction. Wired into the fixture hub's sidecar router so the parked run's
 * `signal.correlation.register` frame lands durable rows the approve route reads.
 */
export function createRegisterSignalCorrelation(db: TestDb["db"]) {
  const signalCorrelationStore = createSignalCorrelationStore(db);
  const approvalStore = createApprovalStore(db);
  const workflowRunStore = createWorkflowRunStore(db);
  return async ({
    correlationId,
    runId,
    anchorRunId,
    agentAddress,
    kind,
    approvalSnapshot,
  }: {
    correlationId: string;
    runId: string;
    anchorRunId: string;
    agentAddress: string;
    kind: "approval";
    approvalSnapshot: ApprovalSnapshot;
  }): Promise<void> => {
    const anchor = await db
      .select({
        id: workflowRun.id,
        tenantId: workflowRun.tenantId,
        definitionId: workflowRun.definitionId,
      })
      .from(workflowRun)
      .where(
        and(
          eq(workflowRun.address, agentAddress),
          inArray(workflowRun.status, ["deployed", "running"]),
        ),
      )
      .limit(1)
      .then((rows) => rows[0]);
    if (anchor === undefined) {
      throw new Error(
        `No live workflow run for address "${agentAddress}"; cannot register signal correlation ${correlationId}`,
      );
    }
    const addressSlug = deriveWorkflowRunRepoId(agentAddress);
    if (addressSlug !== anchorRunId) {
      throw new Error(
        `Anchor run id mismatch registering signal correlation ${correlationId}: frame claims "${anchorRunId}" but address "${agentAddress}" derives the workflow-run repo slug "${addressSlug}"`,
      );
    }
    const tenantId = anchor.tenantId;
    const anchorDbId = anchor.id;
    await db.transaction(async (tx) => {
      // The real trigger route already anchored the run row, so this is a no-op;
      // it mirrors the production co-write's lazy anchor for parity.
      await workflowRunStore.createIfAbsent(
        {
          id: runId,
          anchorRunId: anchorDbId,
          tenantId,
          definitionId: anchor.definitionId,
          principalId: null,
          status: "running",
        },
        tx,
      );
      await signalCorrelationStore.registerIfAbsent(
        {
          correlationId,
          tenantId,
          anchorRunId: anchorDbId,
          agentAddress,
          runId,
          signalName: signalName(correlationId),
          kind,
        },
        tx,
      );
      await approvalStore.createIfAbsent(
        {
          id: generateId("approval"),
          tenantId,
          anchorRunId: anchorDbId,
          runId,
          agentAddress,
          correlationId,
          status: "pending",
          toolDefinition: {
            name: approvalSnapshot.name,
            description: approvalSnapshot.description,
            inputSchema: approvalSnapshot.inputSchema,
          },
          toolArguments: approvalSnapshot.arguments,
          scope: null,
          timeoutAt: null,
        },
        tx,
      );
    });
  };
}
