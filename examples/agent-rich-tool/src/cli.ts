// agent-rich-tool: demonstrate the `pendingMarker` ToolResult field
// end-to-end. A tool opens an approval gate by returning a marker
// with a correlation ID; the reactor registers the gate and the
// conversation continues. A second actor — here, the same process,
// but in production typically a separate approver service — later
// delivers an inbound message whose `interchangeCorrelationId`
// header matches the gate. The reactor correlates the message,
// removes the pending operation, and emits a `message.correlated`
// event so any watcher knows the approval landed.
//
// The example uses `createInboundMessage` from @interchange/mime to
// synthesise the approval message because that is the same builder
// any production approver service would use. The agent's surface
// for this flow is two methods: `deliver(message)` to ingest the
// approval, and `stream()` to observe the correlation event.

import { type } from "arktype";

import {
  openExampleAgent,
  resolveAgentProvider,
  resolveStdio,
  type SingleProviderMainOptions,
} from "@interchange/example-agent-common";
import type { ReactorEmittedEvent } from "@interchange/inference";
import { createInboundMessage } from "@interchange/mime";
import type {
  ContentBlock,
  ConversationTurn,
} from "@interchange/types/runtime";

import { createApprovalTool } from "./approval-tool";

const EXAMPLE_NAME = "agent-rich-tool";

const ApprovalDetail = type({
  correlationId: "string",
  "action?": "string",
  "+": "delete",
});
type ApprovalDetail = typeof ApprovalDetail.infer;

export type MainOptions = SingleProviderMainOptions & {
  /** Override the correlation ID picked by the tool (tests use this). */
  correlationIdFor?: (callId: string) => string;
  /**
   * Bound on how long the CLI waits for the synthesised approval
   * message to correlate before giving up and exiting non-zero.
   * Defaults to 30 seconds so a hung correlation does not hang the
   * caller indefinitely.
   */
  correlationTimeoutMs?: number;
};

const DEFAULT_CORRELATION_TIMEOUT_MS = 30_000;

function findToolDetail(
  turns: ConversationTurn[],
  toolName: string,
): ApprovalDetail | undefined {
  for (const turn of turns) {
    for (const block of turn.content) {
      if (block.type !== "tool_result") continue;
      const validated = ApprovalDetail(block.detail);
      if (validated instanceof type.errors) continue;
      if (findToolCallName(turns, block.callId) !== toolName) continue;
      return validated;
    }
  }
  return undefined;
}

function findToolCallName(
  turns: ConversationTurn[],
  callId: string,
): string | undefined {
  for (const turn of turns) {
    for (const block of turn.content) {
      const candidate = blockAsToolCall(block);
      if (candidate !== undefined && candidate.id === callId) {
        return candidate.name;
      }
    }
  }
  return undefined;
}

function blockAsToolCall(
  block: ContentBlock,
): { id: string; name: string } | undefined {
  if (block.type !== "tool_call") return undefined;
  return { id: block.id, name: block.name };
}

export async function main(
  argv: string[],
  env: NodeJS.ProcessEnv,
  opts: MainOptions = {},
): Promise<number> {
  const { stdout, stderr } = resolveStdio(opts);

  const prompt = argv.join(" ").trim();
  if (prompt === "") {
    stderr(
      'usage: bun run start "<a request that should trigger an approval gate>"\n',
    );
    return 1;
  }

  const resolved = resolveAgentProvider(opts, env, EXAMPLE_NAME, stderr);
  if (resolved === null) return 1;

  const approvalTool = createApprovalTool(
    opts.correlationIdFor !== undefined
      ? { correlationIdFor: opts.correlationIdFor }
      : {},
  );

  const agent = await openExampleAgent(opts, {
    exampleName: EXAMPLE_NAME,
    systemPrompt:
      "You are a careful assistant. Use the request_approval tool for any sensitive action. Reply concisely.",
    tools: [approvalTool],
    providers: [resolved.provider],
    defaultModel: resolved.model,
  });

  const timeoutMs = opts.correlationTimeoutMs ?? DEFAULT_CORRELATION_TIMEOUT_MS;

  try {
    // Subscribe to the stream BEFORE sending so the
    // `message.correlated` event we wait for later cannot fire and
    // be dropped between subscription and delivery.
    const correlated = waitForCorrelation(agent.stream(), timeoutMs);

    const { reply } = await agent.send(prompt);
    stdout(`assistant: ${reply}\n\n`);

    // The tool stamped the correlation ID into the tool_result's
    // `detail` field; pull it back out so we can construct a
    // matching approval message.
    const detail = findToolDetail(await agent.history(), "request_approval");
    if (detail === undefined) {
      stderr("expected request_approval to have produced a tool_result\n");
      return 2;
    }
    const correlationId = detail.correlationId;
    const action = detail.action ?? "(unknown)";

    stdout(`pending operation registered:\n`);
    stdout(`  correlationId: ${correlationId}\n`);
    stdout(`  action:        ${action}\n\n`);

    // Synthesise the approval message and hand it to the agent.
    // `interchangeCorrelationId` is what the reactor's tryCorrelate
    // routes on; the message body is opaque from the gate's
    // perspective.
    const approval = createInboundMessage({
      from: "approver@local",
      to: "agent@local",
      content: `Approval granted for: ${action}`,
      correlationId,
      interchangeType: "conversation.message",
    });
    agent.deliver(approval);

    const outcome = await correlated;
    if (outcome === "timeout") {
      stderr(
        `did not observe message.correlated within ${String(timeoutMs)}ms\n`,
      );
      return 2;
    }
    if (outcome === undefined) {
      stderr("reactor closed before message.correlated fired\n");
      return 2;
    }
    stdout(`correlation resolved:\n`);
    stdout(`  event:         ${outcome.type}\n`);
    stdout(`  correlationId: ${correlationId}\n`);
    return 0;
  } finally {
    await agent.close();
  }
}

/**
 * Watch a reactor event stream for a `message.correlated` event,
 * returning the event when it fires, `undefined` if the reactor
 * shuts down first, or `"timeout"` if `timeoutMs` elapses before
 * either. The timeout uses wall-clock `setTimeout`; tests that
 * cannot tolerate a real-time wait should pass a small
 * `correlationTimeoutMs` and rely on the success path firing first.
 */
async function waitForCorrelation(
  events: AsyncIterable<ReactorEmittedEvent>,
  timeoutMs: number,
): Promise<ReactorEmittedEvent | undefined | "timeout"> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => resolve("timeout"), timeoutMs);
  });
  // The IIFE keeps running in the background when `timeout` wins
  // the race; `.catch` swallows any rejection from the stream
  // consumer's `close()` path so it does not surface as an unhandled
  // promise rejection after the timeout has already been reported.
  // The example deliberately does not depend on the precise
  // termination shape of `agent.stream()` — graceful end or thrown
  // error both produce `undefined` here.
  const found = (async (): Promise<ReactorEmittedEvent | undefined> => {
    for await (const event of events) {
      if (event.type === "message.correlated") return event;
      if (event.type === "reactor.done") return undefined;
    }
    return undefined;
  })().catch((): undefined => undefined);
  try {
    return await Promise.race([found, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

if (import.meta.main) {
  const code = await main(process.argv.slice(2), process.env);
  if (code !== 0) process.exit(code);
}
