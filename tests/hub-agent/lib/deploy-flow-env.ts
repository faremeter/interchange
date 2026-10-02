// Integration-test fixture for the hub-agent deploy-flow surface.
//
// Spins up a real hub WebSocket server, a mock inference HTTP server, and
// a real sidecar subprocess wired to the hub. The fixture owns the full
// lifecycle: tempdir allocation, hub initialization, mock-inference boot,
// sidecar process spawn, stderr drain, and teardown of every resource.
//
// Tests use the fixture as:
//
//   let env: DeployFlowEnv;
//
//   beforeAll(async () => {
//     env = await startDeployFlowEnv();
//   });
//
//   afterAll(async () => {
//     await env.teardown();
//   });
//
// The fixture exposes the hub handle, the inference request capture, the
// sidecar process handle, and a `sidecarDiagnostics()` callback that
// surfaces sidecar stderr and hub state-pack receive failures. A wait helper
// given a `timeoutMs` renders it when that bound lapses; `teardown()` renders
// it when a wait is still in flight, which is what a wedged test leaves
// behind, and then stops that wait so it does not poll on into the env
// teardown is dismantling (see the in-flight wait registry below).
//
// Shared constants
// ----------------
// AGENT_ADDRESS, AGENT_ID, SESSION_ID, SIDECAR_ID, TOKEN are exported so
// tests that exercise the same agent across multiple lifecycle steps can
// reuse them without re-declaring.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import * as tar from "tar";
import git from "isomorphic-git";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import type { Subprocess } from "bun";

import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import {
  committedReadsToSourceTree,
  createAgentRepoStore,
  createSessionService,
  createSidecarRouter,
  createWorkflowRunReader,
  deployCodeSourcedWorkflow,
  dequeueToProcessing,
  enqueueInbox,
  installAndApproveWorkflowDefinition,
  DEFAULT_ASSET_REF,
  parseAgentId,
  type AgentRepoStore,
  type InstallAndApproveResult,
  type RepoId,
  type SidecarCredentialIdentity,
  type SidecarLookups,
  type SessionService,
  type WorkflowRunEvent,
  type WorkflowRunHubPrincipal,
  type WsHandle,
} from "@intx/hub-sessions";
import {
  base64Encode,
  deriveWorkflowRunId,
  hasCode,
  hexEncode,
} from "@intx/types";
import type { CredentialCipher } from "@intx/types";
import type { WireGrantRule } from "@intx/types/grant-wire";
import {
  createEd25519Crypto,
  createNoopCredentialCipher,
  generateKeyPair,
} from "@intx/crypto";
import {
  buildInertProjectionStepSources,
  deriveRunAddress,
} from "@intx/workflow-deploy";
import { decodeToolName } from "@intx/inference";
import type {
  HarnessConfig,
  InferenceSource,
  MessageAttachment,
} from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import type { ApprovalSet } from "@intx/workflow-deploy";
import type { WorkflowDefinition } from "@intx/workflow";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  resolveFrameSenderKey,
  resolveSenderKey,
  type DBExecutor,
  type PrincipalKeyStore,
} from "@intx/db";
import { credential, provider } from "@intx/db/schema";
import { stopServerBounded } from "@intx/test-harness/bun-server";
import type { TestDb } from "@intx/test-harness/db-harness";

import { bundleWorkflowEntry } from "./bundle-workflow-entry";

export const AGENT_ADDRESS = "run_test-agent@integration.interchange";
export const AGENT_ID = "run_test-agent";
export const SESSION_ID = "ses_integration-1";
export const SIDECAR_ID = "sc-integration-1";
export const TOKEN = "test-token";

// The production hub-link reconnect backoff (see `DEFAULT_RECONNECT_DELAY_MS`
// in `@intx/hub-agent`'s hub-link). The fixture's default sidecar env replaces
// it with a short test delay (see `startSidecarSubprocess`), so a test whose
// recovery must run through the real delayed-reconnect cycle rather than the
// shortened one pins it back via
// `sidecarEnv: { SIDECAR_RECONNECT_DELAY_MS: PRODUCTION_RECONNECT_DELAY_MS }`.
export const PRODUCTION_RECONNECT_DELAY_MS = "3000";

// The fixture's default reconnect backoff for spawned sidecars: short enough
// that the reconnect-survival suite does not burn 3s of wall clock per dropped
// link, long enough that a drop still lands as a genuine disconnect before the
// reconnect cycle starts. Tests that pin the production delay override it via
// `sidecarEnv` (see `PRODUCTION_RECONNECT_DELAY_MS`).
const TEST_RECONNECT_DELAY_MS = "250";

// Grace period for each stage of the teardown's sidecar reap (SIGTERM, then
// SIGKILL). Bounds the wait so a sidecar that is slow to reap under contention
// cannot wedge the afterAll hook.
const SIDECAR_REAP_GRACE_MS = 10_000;
// Grace period for the sidecar's *descendants* after the reap SIGKILLs them
// during `terminateSidecarSubprocess`. SIGKILL cannot be caught or ignored, so
// a descendant still alive this long after is stuck in an uninterruptible
// kernel state; failing loudly surfaces it instead of leaking a ~300MB
// process (the full @intx module graph) into the next test file.
const CHILD_REAP_GRACE_MS = 2_000;
// A second sidecar identity, for tests that need two sidecars on one hub
// (e.g. proving a cross-sidecar/federated mail deliver reaches the
// receiver). The default fixture only spawns the first; a caller spawns the
// second via `startSidecarSubprocess` with these in `extraEnv`.
export const SECOND_SIDECAR_ID = "sc-integration-2";
export const SECOND_TOKEN = "test-token-2";
export const PRIMARY_ALLOCATION_TARGET = {
  allocationId: "allocation-integration-1",
  generation: 1,
} as const;

// ---- In-flight harness waits ----
//
// Every wait helper below, and every `retrying` block, registers on entry and
// deregisters in a `finally`. `startDeployFlowEnv`'s `teardown()` dumps the
// sidecar output when anything is still registered, then stops what it found.
//
// An outstanding wait at teardown IS a wedge, not a proxy for one. When the
// runner's per-test budget lapses, bun abandons the test body's promise, but
// nothing aborts the poll loop the body was suspended in: the loop keeps
// polling and never reaches its `finally`. Every other way out of a helper --
// returning, throwing, a guarded `timeoutMs` expiring -- runs the `finally`,
// so a clean finish leaves the registry empty.
//
// That abandoned loop is the reason teardown stops what it reports. Left
// running, it polls on into an env whose deployments, sidecar, servers and
// tempdirs teardown has already dismantled, and the fault it eventually hits
// is a fault of the dismantling, not of the test: a read through
// `requireDeployment` after `deployments.clear()` reports that a test forgot
// to register its deployment. Bun charges that rejection to whichever test is
// running when it settles, so the report names an innocent test or no test at
// all. `stopOutstandingWaits` ends the loop at its next iteration instead,
// with an error that says what actually happened.
//
// The registry is module-scoped because `waitFor` takes no env, so an
// abandoned record outlives the file that made it (`--no-isolate` keeps one
// module registry per worker across that worker's files). `currentWaitMark`
// fences that off: an env reports and stops only registrations at or after
// its creation.

type InFlightWait = { seq: number; label: string; envTornDown: boolean };

let nextWaitSeq = 0;
const inFlightWaits = new Set<InFlightWait>();

/**
 * A marker for "every wait registered from here on". Pass it to
 * `renderOutstandingWaitReport` to report only those, or to
 * `stopOutstandingWaits` to report and stop them.
 */
export function currentWaitMark(): number {
  return nextWaitSeq;
}

function registerWait(label: string): InFlightWait {
  const record = { seq: nextWaitSeq, label, envTornDown: false };
  nextWaitSeq += 1;
  inFlightWaits.add(record);
  return record;
}

function deregisterWait(record: InFlightWait): void {
  inFlightWaits.delete(record);
}

/**
 * Throw when the env a wait belongs to was torn down while the wait was still
 * in flight. Every poll loop below calls this at the top of each iteration,
 * ahead of the read or predicate that iteration would perform, so a stopped
 * loop neither reads dismantled fixture state nor returns a result the
 * dismantling produced -- a quiescence wait, for one, goes quiet precisely
 * because teardown killed the sidecar.
 */
function throwIfEnvTornDown(record: InFlightWait): void {
  if (!record.envTornDown) return;
  throw new Error(
    `deploy-flow env: torn down while ${record.label} was still in flight; ` +
      `the wait was stopped instead of polling on against a dismantled env`,
  );
}

// Cap on the predicate source a `waitFor` label carries. Long enough to tell
// one predicate in a test file from another, short enough that a handful of
// outstanding labels stay readable above the sidecar output they precede.
const PREDICATE_LABEL_MAX_CHARS = 140;

/**
 * Render a `waitFor` predicate for its label. `waitFor` receives a closure and
 * no other distinguishing argument, so the source text is the only thing that
 * tells one of a file's several bare `waitFor` calls from the next.
 */
function describePredicate(
  predicate: () => boolean | Promise<boolean>,
): string {
  const source = String(predicate).replace(/\s+/gu, " ").trim();
  return source.length > PREDICATE_LABEL_MAX_CHARS
    ? `${source.slice(0, PREDICATE_LABEL_MAX_CHARS)}...`
    : source;
}

/**
 * The harness waits registered at or after `mark` that have not deregistered,
 * rendered one per line, or `null` when there are none.
 */
export function renderOutstandingWaitReport(mark: number): string | null {
  const outstanding = [...inFlightWaits].filter((wait) => wait.seq >= mark);
  if (outstanding.length === 0) return null;
  const lines = outstanding.map((wait) => `  ${wait.label}`).join("\n");
  return (
    `deploy-flow env torn down with ${String(outstanding.length)} harness ` +
    `wait(s) still in flight:\n${lines}`
  );
}

/**
 * Stop every harness wait registered at or after `mark`: each one's poll loop
 * throws at its next iteration rather than polling on into an env teardown is
 * dismantling. Returns the report `renderOutstandingWaitReport` renders for
 * the same mark, taken before the waits were stopped, so the caller reports
 * the wedge it is about to end. Reporting and stopping are one call because
 * the report is the only record of what was stopped.
 *
 * A wait that returned or threw deregistered on its way out, so it is not in
 * the registry and nothing here can reach it. What is still registered when
 * an env tears down was abandoned rather than awaited, which is why stopping
 * it cannot disturb a test that passed.
 */
export function stopOutstandingWaits(mark: number): string | null {
  const report = renderOutstandingWaitReport(mark);
  for (const wait of inFlightWaits) {
    if (wait.seq >= mark) wait.envTornDown = true;
  }
  return report;
}

/**
 * Run `fn` registered under `label`, so a wedge inside it reaches the teardown
 * report the way a wedge inside `waitFor` does. For the retry loops `waitFor`
 * cannot express: one that performs an action each iteration (inject a signal,
 * then read) rather than reading a predicate, and one whose exit is an elapsed
 * window rather than a state.
 *
 * Callers reach it as `env.retrying`. It is exported so an env assembled by
 * hand wires this registration rather than a stub that registers nothing.
 *
 * `fn` is opaque to the registry, so teardown cannot reach inside it on its
 * own. Two things let it in. A loop `fn` runs through one of the helpers here
 * is stopped through that helper. A loop that polls on its own gets
 * `checkTornDown`, the same check `waitFor` runs at the top of each of its
 * iterations, bound to this registration: calling it first thing in the loop
 * body throws there instead of performing that pass's read against an env
 * teardown is dismantling.
 *
 * A loop that does neither is not interrupted, and the guarantee that remains
 * is on the way out: `fn` completing after the env was torn down throws
 * instead of returning a value read out of a dismantled env.
 */
export async function retrying<T>(
  label: string,
  fn: (checkTornDown: () => void) => Promise<T>,
): Promise<T> {
  const registration = registerWait(`retrying(${label})`);
  const checkTornDown = (): void => {
    throwIfEnvTornDown(registration);
  };
  try {
    const result = await fn(checkTornDown);
    checkTornDown();
    return result;
  } finally {
    deregisterWait(registration);
  }
}

/**
 * Poll `predicate` until it is true. Carries no deadline unless the caller
 * supplies `timeoutMs`, so a slow machine makes the wait slower and never
 * makes it fail; a hang is failed by the lane budget in the `Makefile` target.
 *
 * Pass `timeoutMs` only where expiry is the behavior under test -- a wait that
 * must prove an address never becomes routable needs the bound, because the
 * expiry is its pass condition.
 */
export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<void> {
  const { timeoutMs, diagnostics } = opts;
  const registration = registerWait(`waitFor(${describePredicate(predicate)})`);
  try {
    const start = Date.now();
    for (;;) {
      throwIfEnvTornDown(registration);
      if (await predicate()) return;
      if (timeoutMs !== undefined && Date.now() - start > timeoutMs) {
        const diag = diagnostics?.();
        const ctx = diag ? `\n${diag}` : "";
        throw new Error(`waitFor timed out after ${timeoutMs}ms${ctx}`);
      }
      await new Promise((r) => setTimeout(r, 50));
    }
  } finally {
    deregisterWait(registration);
  }
}

export type InferenceTool = {
  name: string;
  description?: string;
  input_schema?: Record<string, unknown>;
};

export type InferenceMessageBlock = {
  type?: string;
  text?: string;
  // Present on assistant `tool_use` blocks (the id the model minted) and on
  // user `tool_result` blocks (`tool_use_id`, the id being answered). The mock
  // reads these to detect whether a request already carries the tool's result.
  id?: string;
  tool_use_id?: string;
  // A `tool_result` block's payload. The Anthropic adapter serializes it as an
  // array of `{ type: "text", text }` blocks; the mock flattens their text so a
  // test can assert the resumed reply reflects the real tool output.
  content?: string | InferenceMessageBlock[];
};

export type InferenceMessage = {
  role?: string;
  content?: string | InferenceMessageBlock[];
};

export type InferenceRequest = {
  tools?: InferenceTool[];
  messages?: InferenceMessage[];
};

export type MockInference = {
  server: ReturnType<typeof Bun.serve>;
  requests: InferenceRequest[];
};

/**
 * Opt-in tool-call behavior for the mock inference server. When set, the
 * FIRST request whose `tools` array contains a tool named `toolName`
 * yields a `tool_use` turn (stop_reason `tool_use`) calling that tool
 * with `input`; every later request (the one carrying the tool_result)
 * yields the ordinary `I see these tools: ...` text turn. This drives a
 * real tool execution + tool_result round-trip through the spawned
 * child so a test can assert the tool ran in-child (e.g. by its
 * filesystem side effect).
 */
export type MockToolCall = {
  toolName: string;
  input: Record<string, unknown>;
};

/**
 * One scripted assistant turn for the mock inference server: either a plain
 * text reply or a single tool call with a fixed input. See `scriptedTurns`.
 */
export type ScriptedTurn =
  | { text: string }
  | { toolUse: { name: string; input: Record<string, unknown> } };

export type StartMockInferenceOpts = {
  toolCall?: MockToolCall;
  /**
   * When true, `toolCall` is emitted on the FIRST turn of EVERY run (any
   * request whose history carries no tool_result yet) rather than only once
   * across the mock's lifetime. A run that drives the same tool then completes
   * with a text turn once its result lands. Lets a single env exercise the
   * tool across several runs (e.g. re-running a credential tool before and
   * after a rotation) without the default one-shot latch swallowing the
   * later runs.
   */
  toolCallEachRun?: boolean;
  /**
   * When true, the assistant reply echoes the last user message's text
   * as `echo:<text>` instead of the tool-names text turn. This lets a
   * test assert the agent's `agent.send` actually received the inbound
   * mail body (the body reaches inference as the user turn, so the echo
   * reflects it). Mutually exclusive in spirit with `toolCall`, which
   * drives a different reply shape.
   */
  echoUserMessage?: boolean;
  /**
   * Persistent tool-call behavior for the approval capstone. Unlike
   * `toolCall`, which emits its `tool_use` once and then falls back to a
   * text turn, this re-issues the `tool_use` on EVERY request whose history
   * does not yet carry a `tool_result` answering the named tool, and only
   * replies once it sees that result -- the reply being `${resultPrefix}<the
   * tool_result content>` so a test can assert the resumed reply reflects the
   * real tool output.
   *
   * This distinguishes the fixed re-dispatch rail from the old broken one. On
   * the broken rail the approval decision arrives as a bare user turn (no
   * tool_result) and re-inference re-issues the call -> re-suspends on the ask
   * grant -> loops forever. On the fixed rail the approved call is
   * re-dispatched and RUNS, appending a real tool_result, so the next
   * inference sees it and the mock replies -> the run completes.
   */
  approvalToolCall?: {
    toolName: string;
    input: Record<string, unknown>;
    resultPrefix: string;
  };
  /**
   * A fixed, ordered script of assistant turns, dispatched by how many tool
   * results the request's history already carries: the Nth turn is returned
   * once N tool results are present (turn 0 on the first inference, turn 1
   * after the first tool ran, and so on). This drives a deterministic
   * multi-tool agent conversation -- e.g. `mail_send` a reply, `mail_wait` for
   * the next inbound, `mail_send` the next reply -- through the spawned child.
   * Every turn's tool input is static, so the caller scripts only inputs it
   * knows up front (the message-ids it fired). Once the script is exhausted,
   * the mock falls back to the ordinary tool-names text turn.
   *
   * Mutually exclusive with `toolCall`/`approvalToolCall`; when set it takes
   * precedence over the other reply shapes.
   */
  scriptedTurns?: ScriptedTurn[];
};

/**
 * Recover the last user message's plain text from an Anthropic-style
 * request body. The agent sends the inbound conversation content as a
 * user turn whose `content` is either a bare string or an array of
 * `{ type: "text", text }` blocks; both shapes are flattened here.
 */
function lastUserText(req: InferenceRequest): string {
  const messages = req.messages ?? [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const message = messages[i];
    if (message === undefined || message.role !== "user") continue;
    const content = message.content;
    if (typeof content === "string") return content;
    if (Array.isArray(content)) {
      return content
        .filter((block) => block.type === "text" && block.text !== undefined)
        .map((block) => block.text ?? "")
        .join("");
    }
  }
  return "";
}

/**
 * Find the text of the first `tool_result` block in the request's history, or
 * `null` if none is present. The Anthropic adapter serializes a tool result as
 * a user-turn content block `{ type: "tool_result", tool_use_id, content: [{
 * type: "text", text }] }`; the mock flattens that inner text and uses the
 * block's presence to decide it has already seen the tool run and may now reply.
 */
function firstToolResultText(req: InferenceRequest): string | null {
  for (const message of req.messages ?? []) {
    const content = message.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (block.type !== "tool_result") continue;
      const inner = block.content;
      if (typeof inner === "string") return inner;
      if (Array.isArray(inner)) {
        return inner
          .filter((b) => b.type === "text" && b.text !== undefined)
          .map((b) => b.text ?? "")
          .join("");
      }
      return "";
    }
  }
  return null;
}

/**
 * Count the `tool_result` blocks across a request's message history. The mock
 * uses this to index its scripted turns: each completed tool call appends
 * exactly one `tool_result`, so the count is the number of scripted turns
 * already consumed.
 */
function countToolResults(req: InferenceRequest): number {
  let count = 0;
  for (const message of req.messages ?? []) {
    const content = message.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (block.type === "tool_result") count += 1;
    }
  }
  return count;
}

// Mock inference server
//
// Returns a canned Anthropic-style SSE assistant response that includes the
// tool names it was given in the request. This lets tests assert that the
// harness passed the deploy-tree tools through to inference.
//
// With `opts.toolCall`, the first request that exposes the named tool
// instead returns a `tool_use` turn so the agent executes the tool and
// loops back with a tool_result, on which the server returns the text
// turn. This is how the Phase 2 posix-tool test drives a real tool run
// inside the spawned child.
export function startMockInference(
  opts: StartMockInferenceOpts = {},
): MockInference {
  const requests: InferenceRequest[] = [];
  let toolCallEmitted = false;

  const textTurn = (toolNames: string[]): string[] =>
    textTurnText(`I see these tools: ${toolNames.join(", ")}`);

  const textTurnText = (text: string): string[] => {
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
  };

  const toolUseTurn = (
    call: MockToolCall,
    toolUseId = "toolu_mock_1",
  ): string[] => [
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
        name: call.toolName,
        input: {},
      },
    }),
    sse("content_block_delta", {
      type: "content_block_delta",
      index: 0,
      delta: {
        type: "input_json_delta",
        partial_json: JSON.stringify(call.input),
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

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this is a test mock server that only receives requests from the sidecar under test; the shape is known
      const body = (await req.json()) as InferenceRequest;
      // The adapter encodes tool names for the provider wire charset. Decode
      // them back to the qualified names the rest of the mock — and the tests
      // asserting on `requests` — reason about, so this double stays in terms
      // of the logical tool identity rather than the on-wire form.
      for (const tool of body.tools ?? []) {
        tool.name = decodeToolName(tool.name);
      }
      requests.push(body);

      const toolNames = (body.tools ?? []).map((t) => t.name);
      const wantsToolCall =
        opts.toolCall !== undefined &&
        toolNames.includes(opts.toolCall.toolName) &&
        // Default: emit once across the mock's lifetime. `toolCallEachRun`:
        // emit on any request whose history has no tool_result yet, so each
        // fresh run drives the tool and then completes once its result lands.
        (opts.toolCallEachRun === true
          ? firstToolResultText(body) === null
          : !toolCallEmitted);

      let events: string[];
      const approval = opts.approvalToolCall;
      if (opts.scriptedTurns !== undefined) {
        // Dispatch the scripted turn by how many tool results the history
        // already carries. Each tool_use gets a distinct id so the child's
        // reactor correlates each result to its own call across turns.
        const step = countToolResults(body);
        const turn = opts.scriptedTurns[step];
        if (turn === undefined) {
          events = textTurn(toolNames);
        } else if ("text" in turn) {
          events = textTurnText(turn.text);
        } else {
          events = toolUseTurn(
            { toolName: turn.toolUse.name, input: turn.toolUse.input },
            `toolu_mock_${String(step)}`,
          );
        }
      } else if (
        approval !== undefined &&
        toolNames.includes(approval.toolName)
      ) {
        // Re-issue the call until the history carries its result; then reply
        // with the result content. Persistent (no latch): under the broken
        // resume rail no result ever arrives and this loops (the test times
        // out); under the fixed re-dispatch rail the approved call runs, its
        // result lands in history, and this reply completes the run.
        const result = firstToolResultText(body);
        events =
          result === null
            ? toolUseTurn({
                toolName: approval.toolName,
                input: approval.input,
              })
            : textTurnText(`${approval.resultPrefix}${result}`);
      } else if (wantsToolCall && opts.toolCall !== undefined) {
        toolCallEmitted = true;
        events = toolUseTurn(opts.toolCall);
      } else if (opts.echoUserMessage === true) {
        events = textTurnText(`echo:${lastUserText(body)}`);
      } else {
        events = textTurn(toolNames);
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

function sse(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

/**
 * Build a plain npm-package tarball (package.json + index.mjs), for serving as
 * a workflow's EXTERNAL dependency from an in-process registry. The registry
 * reads the tarball's own package.json to resolve name@version.
 */
export async function buildSyntheticNpmPackageTarball(
  registerTempDir: (dir: string) => void,
  opts: {
    packageName: string;
    version: string;
    moduleSource: string;
    interchange?: Record<string, string>;
  },
): Promise<Uint8Array> {
  const stagingDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "npm-pkg-fixture-"),
  );
  registerTempDir(stagingDir);
  const packageDir = path.join(stagingDir, "package");
  await fs.promises.mkdir(packageDir, { recursive: true });
  await fs.promises.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: opts.packageName,
      version: opts.version,
      type: "module",
      exports: "./index.mjs",
      ...(opts.interchange !== undefined
        ? { interchange: opts.interchange }
        : {}),
    }),
  );
  await fs.promises.writeFile(
    path.join(packageDir, "index.mjs"),
    opts.moduleSource,
  );
  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.promises.readFile(tarballPath);
  return new Uint8Array(bytes);
}

export type HubEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: ReturnType<typeof createSidecarRouter>;
  probeRouter: {
    sendProbe(
      args: Parameters<
        ReturnType<typeof createSidecarRouter>["sendProbeToAllocation"]
      >[1],
    ): ReturnType<
      ReturnType<typeof createSidecarRouter>["sendProbeToAllocation"]
    >;
  };
  setPrimaryAllocationIdentity(anchorRunId: string, address: string): void;
  prepareAllocationIdentity(
    anchorRunId: string,
    address: string,
    sidecarId?: string,
  ): { allocationId: string; generation: number };
  sessionService: SessionService;
  agentRepoStore: AgentRepoStore;
  agentEvents: { addr: string; sid: string; event: unknown }[];
  deployAcks: Map<string, string>;
  statePacks: { agentAddress: string; ref: string; commitSha: string }[];
  statePackReceiveFailures: { agentAddress: string; error: string }[];
  /**
   * Every delivered `mail.outbound` frame the sidecar forwarded to the hub
   * for persistence, keyed by the signing sender. A frame reaches here only
   * after the sidecar signed and delivered the send, so its presence proves
   * the sender's identity was registered on the host transport.
   *
   * `raw` is the full signed outbound MIME the sidecar delivered on the wire
   * (the `persistMail` lookup's own `raw` argument, which the wire layer
   * base64-decodes from the `mail.outbound` frame before calling the lookup).
   * Retaining it lets a test read the delivered message's actual headers --
   * `In-Reply-To`, `Message-ID`, `To`, `Cc`, `References` -- via
   * `parseHeaderSection`, the only faithful way to observe an outbound reply's
   * threading on the wire (the hub mints no durable mail row here).
   */
  outboundMail: {
    senderAddress: string;
    recipients: string[];
    raw: Uint8Array;
  }[];
  hubDataDir: string;
  /**
   * Every server-side `WsHandle` currently open against this hub. Added on
   * `onOpen`, removed on `onClose`. The sidecar holds exactly one hub link
   * at a time, so this set carries a single handle in steady state; the
   * reconnect helpers force-close every handle in it to sever the link.
   */
  liveHandles: Set<WsHandle>;
  /**
   * Monotonic count of workflow-run packs the hub has accepted from the
   * sidecar, held in a mutable box so the router callback that bumps it and
   * the settle helper that reads it share one reference. Bumped on every
   * successful `receiveWorkflowRunPack`. The settle helper watches this
   * count for a quiet window so it drops the hub link only once no
   * workflow-run pack push is mid-flight.
   */
  workflowRunPackReceipts: { count: number };
  /**
   * Opt-in, arm-once mid-pack interrupt for the workflow-run push. Off by
   * default (`armed: false`), so ordinary tests are unaffected. When a test
   * sets `armed = true`, the FIRST `refs/heads/main` workflow-run pack the
   * hub receives is applied DURABLY (the commit lands on the hub), then
   * every live hub link is dropped BEFORE the ack is returned, and `armed`
   * flips back to `false`. `handleClose` then rejects the sidecar's pending
   * pack transfer, so the sidecar's push rejects and latches "Connection
   * lost" -- the deterministic interrupted-pack failure mode, provoked
   * without a bespoke hub. The recorded fields let a test observe that the
   * interrupt fired and on which ref/commit.
   */
  interrupt: {
    armed: boolean;
    interruptedRef: string | null;
    interruptedCommitSha: string | null;
  };
};

// Hub WebSocket server (in-process) wired against a real AgentRepoStore
// and SessionService. It wires no tool-package registry surface; the DB stub
// below explains why.
export async function startHub(
  registerTempDir: (dir: string) => void,
  opts: {
    registerSignalCorrelation?: SidecarLookups["registerSignalCorrelation"];
    materializeMailTriggeredRunGrants?: SidecarLookups["materializeMailTriggeredRunGrants"];
    senderKeyResolution?: {
      db: DBExecutor;
      principalKeyStore: PrincipalKeyStore;
    };
  } = {},
): Promise<HubEnv> {
  const agentEvents: HubEnv["agentEvents"] = [];
  const deployAcks = new Map<string, string>();
  const statePacks: HubEnv["statePacks"] = [];
  const statePackReceiveFailures: HubEnv["statePackReceiveFailures"] = [];
  const outboundMail: HubEnv["outboundMail"] = [];
  // Every live server-side WsHandle, so the reconnect helpers can force-close
  // the sidecar's hub link. Populated by the upgrade callback's onOpen/onClose.
  const liveHandles = new Set<WsHandle>();
  // Mutable box: the router callback below bumps `.count`; the settle helper
  // reads it. A bare number field on the returned env would not reflect the
  // bumps, so the count lives behind a stable object reference.
  const workflowRunPackReceipts = { count: 0 };

  // Held in a local so the sender-key lookup closures below narrow away the
  // `undefined` case once and capture the concrete resolution channel.
  const senderKeyResolution = opts.senderKeyResolution;

  // Arm-once mid-pack interrupt state, off by default. A test flips
  // `armed = true` to make the FIRST refs/heads/main workflow-run pack drop
  // every live link before its ack. Shared by reference between the
  // `receiveWorkflowRunPack` lookup below and the returned env so the test
  // can arm it and observe that it fired.
  const interrupt: HubEnv["interrupt"] = {
    armed: false,
    interruptedRef: null,
    interruptedCommitSha: null,
  };

  function dropAllHandles(): void {
    for (const handle of [...liveHandles]) {
      handle.close();
    }
  }

  const hubDataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "hub-data-"),
  );
  registerTempDir(hubDataDir);

  const hubSigningKey = await generateKeyPair();
  const agentRepoStore = createAgentRepoStore({
    dataDir: hubDataDir,
    signingKey: hubSigningKey,
  });

  const primaryIdentity: SidecarCredentialIdentity = {
    kind: "allocated",
    sidecarId: SIDECAR_ID,
    allocationId: "allocation-integration-1",
    tenantId: "tenant-integration",
    anchorRunId: "run-integration-1",
    workflowRunAddress: "workflow-integration-1@example.test",
    generation: 1,
  };
  const secondaryIdentity: SidecarCredentialIdentity = {
    kind: "allocated",
    sidecarId: SECOND_SIDECAR_ID,
    allocationId: "allocation-integration-2",
    tenantId: "tenant-integration",
    anchorRunId: "run-integration-2",
    workflowRunAddress: "workflow-integration-2@example.test",
    generation: 1,
  };
  const router = createSidecarRouter({
    requestTimeoutMs: 10_000,
    hubPublicKey: hexEncode(hubSigningKey.publicKey),
    // The spawned sidecar presents TOKEN on its handshake; verify it and
    // resolve to the fixed integration sidecar id, exercising the real
    // token-authenticated handshake rather than accepting any token.
    authenticateSidecar: async ({ token }) => {
      if (token === TOKEN) return primaryIdentity;
      if (token === SECOND_TOKEN) return secondaryIdentity;
      return null;
    },
    validateSidecarIdentity: async () => true,
    lookups: {
      async receiveAgentStatePack(repoId, pack, ref, commitSha) {
        if (repoId.kind !== "agent-state") {
          throw new Error(
            `deploy-flow test mock received unsupported repo kind ${JSON.stringify(repoId.kind)}`,
          );
        }
        const agentAddress = repoId.id;
        const agentId = parseAgentId(agentAddress);
        // Mirror createHubSessionLookups' fallback branch only: catch
        // every receive failure and surface it as a structured "corrupt"
        // rejection, so a transient (e.g. the agent directory being torn
        // down concurrently with an in-flight pack write) does not
        // propagate as an unhandled rejection through the WebSocket
        // message handler. The production lookups distinguish a
        // path_violation prefix and report that as a separate reason;
        // this mock does not, because this fixture never exercises
        // tree-validator rejection.
        try {
          await agentRepoStore.receiveAgentStatePack(
            { kind: "agent-state", id: agentId },
            pack,
            ref,
            commitSha,
          );
        } catch (err) {
          // Capture the underlying error into the hub's diagnostic
          // buffer so a regression does not hide behind the catch.
          // sidecarDiagnostics surfaces this on waitFor timeouts, and
          // tests that care can inspect hub.statePackReceiveFailures
          // directly.
          const message = err instanceof Error ? err.message : String(err);
          statePackReceiveFailures.push({ agentAddress, error: message });
          return { accepted: false, reason: "corrupt" as const };
        }
        statePacks.push({ agentAddress, ref, commitSha });
        return { accepted: true };
      },
      async receiveWorkflowRunPack(repoId, pack, ref, commitSha) {
        if (repoId.kind !== "workflow-run") {
          throw new Error(
            `deploy-flow test mock received unsupported workflow-run repo kind ${JSON.stringify(repoId.kind)}`,
          );
        }
        // Arm-once mid-pack interrupt: apply the pack durably (so the hub
        // has the commit), then drop every live link BEFORE returning the
        // ack. `handleClose` rejects the sidecar's pending transfer, so the
        // sidecar's push rejects and latches "Connection lost" even though
        // the hub durably holds the commit -- the interrupted-pack failure
        // mode. Restricted to the run-events ref so the claim-check ref
        // (refs/heads/events) keeps flowing.
        if (interrupt.armed && ref === "refs/heads/main") {
          interrupt.armed = false;
          interrupt.interruptedRef = ref;
          interrupt.interruptedCommitSha = commitSha;
          await agentRepoStore.receiveWorkflowRunPack(
            repoId,
            pack,
            ref,
            commitSha,
          );
          workflowRunPackReceipts.count += 1;
          dropAllHandles();
          return { accepted: true };
        }
        try {
          await agentRepoStore.receiveWorkflowRunPack(
            repoId,
            pack,
            ref,
            commitSha,
          );
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          statePackReceiveFailures.push({
            agentAddress: repoId.id,
            error: `workflow-run pack: ${message}`,
          });
          return { accepted: false, reason: "corrupt" as const };
        }
        // Record the accepted receipt so the settle helper can watch for a
        // quiet window with no in-flight pack push before dropping the link.
        workflowRunPackReceipts.count += 1;
        return { accepted: true };
      },
      // Capture delivered outbound mail the sidecar forwards for
      // persistence. Recording the signing sender, recipients, and the raw
      // signed MIME the wire layer decoded lets a test read the delivered
      // reply's threading headers directly; no durable row is minted, so this
      // returns an empty result set.
      persistMail({ senderAddress, recipients, raw }) {
        outboundMail.push({ senderAddress, recipients, raw });
        return Promise.resolve([]);
      },
      // Co-write the signal_correlation + approval rows when a suspending
      // agent step's `signal.correlation.register` frame arrives. Only the
      // approval capstone wires this (against a real DB); every other test
      // omits it, and the wire handler drops the frame with a warning when
      // it is absent. Threaded through so the capstone can drive the real
      // hub co-write without standing up a second hub.
      ...(opts.registerSignalCorrelation !== undefined
        ? { registerSignalCorrelation: opts.registerSignalCorrelation }
        : {}),
      // Materialize a mail-triggered run's grants for a workflow-derived
      // recipient. Only the federated-mail capstone supplies it (the real
      // `createMailTriggeredRunGrantsMaterializer` backed by a test DB);
      // every other test leaves it unset, so `deliverMailToRecipient`
      // routes inbound mail without materializing a run's grants.
      ...(opts.materializeMailTriggeredRunGrants !== undefined
        ? {
            materializeMailTriggeredRunGrants:
              opts.materializeMailTriggeredRunGrants,
          }
        : {}),
      // Resolve a signed mail sender's durable public key so the materializer
      // path co-delivers it on the recipient run's grants barrier, exactly as
      // production wires it (apps/hub/src/server.ts). Without this, the
      // materializer-path mail resolves `unknown` and strict enforcement drops
      // it. The best-effort `resolveSenderKey` degrades a fault to null
      // (`resolveFrameSenderKey`); the strict sibling for reconnect
      // reconciliation preserves the throw and unwraps to the hex key. Only
      // wired when a test supplies its db + principal key store.
      ...(senderKeyResolution !== undefined
        ? {
            resolveSenderKey: (address: string) =>
              resolveFrameSenderKey(
                senderKeyResolution.db,
                senderKeyResolution.principalKeyStore,
                address,
              ),
            resolveSenderKeyStrict: async (address: string) =>
              (
                await resolveSenderKey(
                  senderKeyResolution.db,
                  senderKeyResolution.principalKeyStore,
                  address,
                )
              )?.publicKey ?? null,
          }
        : {}),
    },
  });
  router.fenceAllocation(primaryIdentity.allocationId, 1);
  router.fenceAllocation(secondaryIdentity.allocationId, 1);
  // The hub stamps the hub-approved wire hash onto every source-ref workflow
  // deploy frame before it reaches the sidecar (production does this in
  // `sendMultiStepDeployFrame`), so a frame reaching the sidecar always carries
  // one -- no harness-side stamping is needed.
  router.events.on("agent.event", ({ agentAddress, sessionId, event }) => {
    agentEvents.push({ addr: agentAddress, sid: sessionId, event });
  });
  router.events.on("agent.deploy.ack", ({ agentAddress, publicKey }) => {
    deployAcks.set(agentAddress, publicKey);
  });

  // The DB stub satisfies the narrow surface the session-service paths the
  // deploy tests exercise actually consult: a tenant lookup and the
  // session_asset insert/delete audit writes. Its other members throw on
  // access so the test fails loudly if production code drifts into a
  // dependency the stub does not cover. No package-registry asset or
  // tool-package registry is wired: deploy tests carry their tools inline in
  // the workflow source closure, so the session-service tool-package resolver
  // path is never entered.
  const fakeDb = {
    query: {
      tenant: {
        findFirst: async (_args: unknown) =>
          ({ parentId: null }) as { parentId: string | null },
      },
    },
    insert(_table: unknown) {
      return {
        values(_row: unknown) {
          return Promise.resolve();
        },
      };
    },
    delete(_table: unknown) {
      return {
        where(_predicate: unknown) {
          return Promise.resolve();
        },
      };
    },
  };

  const sessionService = createSessionService({
    sidecarRouter: router,
    sidecarAllocationRouter: router,
    agentRepoStore,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the stub satisfies the narrow surface the session-service paths the deploy tests exercise actually call (query.tenant.findFirst, insert/delete), but cannot structurally satisfy the full drizzle PgDatabase type
    db: fakeDb as unknown as NonNullable<
      Parameters<typeof createSessionService>[0]["db"]
    >,
  });

  const app = new Hono();
  app.get(
    "/ws",
    upgradeWebSocket((_c) => {
      let handle: WsHandle;
      return {
        onOpen(_evt, ws) {
          handle = {
            send(data: string) {
              ws.send(data);
            },
            close() {
              ws.close();
            },
          };
          liveHandles.add(handle);
          router.handleOpen(handle);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            router.handleMessage(handle, evt.data);
          }
        },
        onClose(_evt, _ws) {
          liveHandles.delete(handle);
          router.handleClose(handle);
        },
      };
    }),
  );

  const server = Bun.serve({
    fetch: app.fetch,
    websocket,
    port: 0,
  });

  return {
    server,
    router,
    probeRouter: {
      sendProbe: (args) =>
        router.sendProbeToAllocation(PRIMARY_ALLOCATION_TARGET, args),
    },
    setPrimaryAllocationIdentity(anchorRunId, address) {
      // The fixture's manually spawned process models one provisioner-owned
      // allocation. Update the allocation identity before probing/deploying so
      // every routed operation remains bound to the deployment under test.
      Object.assign(primaryIdentity, {
        anchorRunId,
        workflowRunAddress: address,
      });
    },
    prepareAllocationIdentity(anchorRunId, address, sidecarId) {
      // A caller that runs two sidecars must say which one it means. The
      // fallback below infers it from whether the primary is connected, which
      // is a fact about timing rather than intent: a primary that reconnects
      // before this call rebinds ITS identity to `address`, so a deployment
      // meant for the secondary lands on the primary and overwrites the
      // binding the previous deployment is still using. Both then share one
      // transport, which makes a send between them local.
      const selected =
        sidecarId === undefined
          ? router.getConnectedSidecars().includes(SIDECAR_ID)
            ? primaryIdentity
            : secondaryIdentity
          : sidecarId === SIDECAR_ID
            ? primaryIdentity
            : sidecarId === SECOND_SIDECAR_ID
              ? secondaryIdentity
              : (() => {
                  throw new Error(
                    `deploy-flow env: no allocation identity for sidecar ${sidecarId}; expected ${SIDECAR_ID} or ${SECOND_SIDECAR_ID}`,
                  );
                })();
      Object.assign(selected, {
        anchorRunId,
        workflowRunAddress: address,
      });
      return {
        allocationId: selected.allocationId,
        generation: selected.generation,
      };
    },
    sessionService,
    agentRepoStore,
    agentEvents,
    deployAcks,
    statePacks,
    statePackReceiveFailures,
    outboundMail,
    hubDataDir,
    liveHandles,
    workflowRunPackReceipts,
    interrupt,
  };
}

export type SidecarHandle = {
  proc: Subprocess;
  dataDir: string;
  /** Rolling stderr buffer; capped at 500 chunks. */
  stderr: readonly string[];
  /**
   * The env this subprocess was spawned with -- the object handed to
   * `Bun.spawn`, not a re-derivation of it. `assertPinnedSidecarEnvReached`
   * reads it to check a caller's pin against what the process actually got,
   * which is only sound because this is the spawned value.
   */
  env: Readonly<Record<string, string | undefined>>;
};

// The env the fixture hands a spawned sidecar. Separated from the spawn so a
// test can read the variables the fixture would pass without paying for a
// subprocess -- `SIDECAR_RECONNECT_DELAY_MS` is the one whose value a
// reconnect test's behavior depends on, and nothing else in this fixture
// reports which of the two delays is in effect.
//
// `extraEnv` is written last and may override any key the fixture sets.
// Callers use it to inject opt-in flags, but the override contract is
// uniform across every fixture-owned key so a future caller can also point
// the sidecar at a different hub or data directory without the fixture
// silently winning.
export function buildSidecarSubprocessEnv(opts: {
  hubPort: number;
  dataDir: string;
  extraEnv?: Record<string, string>;
}): Record<string, string | undefined> {
  return {
    PATH: process.env["PATH"],
    HOME: process.env["HOME"],
    TMPDIR: process.env["TMPDIR"],
    HUB_WS_URL: `ws://localhost:${String(opts.hubPort)}/ws`,
    SIDECAR_ID,
    SIDECAR_TOKEN: TOKEN,
    SIDECAR_DATA_DIR: opts.dataDir,
    // A fixed test key so the spawned sidecar boots with a REAL cipher and
    // every deployed e2e exercises the at-rest credential sealing, rather than
    // a noop that would let a plaintext-sealing regression pass green.
    SIDECAR_CREDENTIAL_ENCRYPTION_KEY:
      "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
    // Fast reconnect backoff for tests that do not assert the delay itself
    // (recovery semantics are independent of the backoff duration); a test
    // that pins the production 3s delay passes
    // `sidecarEnv: { SIDECAR_RECONNECT_DELAY_MS: PRODUCTION_RECONNECT_DELAY_MS }`.
    SIDECAR_RECONNECT_DELAY_MS: TEST_RECONNECT_DELAY_MS,
    ...(opts.extraEnv ?? {}),
  };
}

// Spawn a real sidecar subprocess pointed at the supplied hub. The
// caller passes the hub's port so the sidecar reaches the hub over
// `ws://localhost:<port>/ws`. The `extraEnv` argument is merged into the
// sidecar's process env after the standard variables; callers use it to
// inject opt-in flags (for example, the `SIDECAR_WORKFLOW_RUN_SHADOW`
// gate that opts the sidecar into emitting a shadow audit-event log).
export async function startSidecarSubprocess(opts: {
  hubPort: number;
  registerTempDir: (dir: string) => void;
  extraEnv?: Record<string, string>;
}): Promise<SidecarHandle> {
  const { hubPort, registerTempDir } = opts;
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "sidecar-data-"),
  );
  registerTempDir(dataDir);

  const stderr: string[] = [];

  const env = buildSidecarSubprocessEnv({
    hubPort,
    dataDir,
    ...(opts.extraEnv !== undefined ? { extraEnv: opts.extraEnv } : {}),
  });

  // --conditions=intx-src resolves @intx/* to source; the spawned sidecar
  // runs from the workspace, where the dev loop builds no dist.
  const proc = Bun.spawn(
    ["bun", "run", "--conditions=intx-src", "apps/sidecar/src/index.ts"],
    {
      cwd: path.resolve(import.meta.dir, "../../.."),
      env,
      stdout: "pipe",
      stderr: "pipe",
    },
  );

  // Drain stderr into a rolling buffer for diagnostics on timeout.
  void (async () => {
    const reader = proc.stderr.getReader();
    const decoder = new TextDecoder();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      stderr.push(decoder.decode(value));
      if (stderr.length > 500) stderr.shift();
    }
  })();
  void (async () => {
    const reader = proc.stdout.getReader();
    const decoder = new TextDecoder();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      stderr.push(decoder.decode(value));
      if (stderr.length > 500) stderr.shift();
    }
  })();

  return { proc, dataDir, stderr, env };
}

/**
 * Throw unless every variable `pinned` names carries that exact value in
 * `spawned`, the env a sidecar subprocess was actually started with.
 *
 * A caller that passes `sidecarEnv` is claiming the subprocess runs with those
 * variables. Nothing downstream re-states the claim: the fixture's own
 * defaults are the same shape as an override, so a break anywhere in the
 * `sidecarEnv` -> `extraEnv` -> spawned-env chain leaves the subprocess
 * running on a fixture default that looks deliberate. A reconnect test pinning
 * `SIDECAR_RECONNECT_DELAY_MS` to the production delay is the case that
 * motivates this -- it would silently run at the short test delay, and its
 * recovery assertions hold at either delay, so nothing would fail.
 *
 * Keyed off the caller's own variables rather than any particular one, so it
 * covers a pin of any value of any variable. A caller that passes no
 * `sidecarEnv` makes no claim and has no keys here, so there is nothing for
 * this to check and it cannot fire on that path.
 */
export function assertPinnedSidecarEnvReached(
  pinned: Record<string, string>,
  spawned: Readonly<Record<string, string | undefined>>,
): void {
  const mismatches = Object.entries(pinned)
    .filter(([key, value]) => spawned[key] !== value)
    .map(
      ([key, value]) =>
        `${key}: pinned ${value}, subprocess env has ${spawned[key] === undefined ? "no value" : spawned[key]}`,
    );
  if (mismatches.length > 0) {
    throw new Error(
      `deploy-flow env: sidecarEnv did not reach the sidecar subprocess -- ${mismatches.join("; ")}`,
    );
  }
}

function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Live PIDs in the process subtree rooted at `rootPid`, excluding the root
 * itself. Walked transitively over `ps -A -o pid=,ppid=` (the same table
 * shape `listWorkflowHostChildren` reads), so no depth is assumed and no argv
 * match is needed: every descendant of the sidecar is a process the test
 * owns (its workflow-process children and any tools they spawn).
 */
function listProcessSubtree(rootPid: number): number[] {
  const result = Bun.spawnSync(["ps", "-A", "-o", "pid=,ppid="]);
  const childrenOf = new Map<number, number[]>();
  for (const line of new TextDecoder().decode(result.stdout).split("\n")) {
    const match = /^\s*(\d+)\s+(\d+)\s*$/.exec(line);
    if (match === null || match[1] === undefined || match[2] === undefined) {
      continue;
    }
    const pid = Number.parseInt(match[1], 10);
    const ppid = Number.parseInt(match[2], 10);
    const siblings = childrenOf.get(ppid) ?? [];
    siblings.push(pid);
    childrenOf.set(ppid, siblings);
  }
  const found: number[] = [];
  const seen = new Set<number>([rootPid]);
  const queue: number[] = [rootPid];
  while (queue.length > 0) {
    const current = queue.shift();
    if (current === undefined) continue;
    for (const child of childrenOf.get(current) ?? []) {
      if (!seen.has(child)) {
        seen.add(child);
        queue.push(child);
        found.push(child);
      }
    }
  }
  return found;
}

/**
 * Terminate a sidecar subprocess (bounded SIGTERM -> SIGKILL escalation) and
 * then reap its process subtree.
 *
 * The escalation mirrors the production sidecar provisioner and the previous
 * inline teardown logic: the sidecar installs no graceful-shutdown SIGTERM
 * handler, so under contention a plain SIGTERM can leave `proc.exited`
 * unresolved; an unbounded wait there wedged the afterAll hook until the whole
 * suite was torn down. Waiting for the exit before the caller removes the
 * data directory keeps the earlier fix intact: removing `sidecar-data-*` while
 * a subprocess still holds file handles raced EBUSY/EACCES on slow hosts, so
 * errors must surface from the rm rather than be shrouded.
 *
 * The subtree reap closes the second half of the sidecar leak: killing the
 * sidecar orphans its workflow-process children. They normally notice the dead
 * host through their broken IPC channel and exit on their own, but under load
 * that has been observed to take tens of seconds, and each straggler holds
 * ~300MB of RSS plus its OS file handles into the next test file's boot,
 * inflating the pass's peak RSS. The descendants are snapshotted *before* the
 * kill: once the sidecar exits, the kernel re-parents its children to init
 * and they can no longer be attributed to it.
 *
 * Safe to call twice on the same handle (crash tests terminate the sidecar
 * mid-test, then `teardown()` terminates it again): `proc.kill()` is a no-op
 * and `proc.exited` is already resolved on a finished subprocess, and a
 * subtree walk from a dead pid finds nothing.
 */
export async function terminateSidecarSubprocess(
  handle: SidecarHandle,
): Promise<void> {
  const pid = handle.proc.pid;
  const subtree = pid !== undefined ? listProcessSubtree(pid) : [];
  handle.proc.kill();
  const exitedWithin = (ms: number) =>
    Promise.race([
      handle.proc.exited.then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), ms)),
    ]);
  if (!(await exitedWithin(SIDECAR_REAP_GRACE_MS))) {
    handle.proc.kill(9);
    if (!(await exitedWithin(SIDECAR_REAP_GRACE_MS))) {
      throw new Error(
        `sidecar pid ${String(pid)} did not exit after SIGKILL within ${String(SIDECAR_REAP_GRACE_MS)}ms`,
      );
    }
  }
  for (const descendant of subtree) {
    if (descendant === pid || !pidAlive(descendant)) continue;
    try {
      process.kill(descendant, "SIGKILL");
    } catch {
      // Exited between the liveness check and the signal.
    }
  }
  const deadline = Date.now() + CHILD_REAP_GRACE_MS;
  for (;;) {
    const lingering = subtree.filter((d) => d !== pid && pidAlive(d));
    if (lingering.length === 0) return;
    if (Date.now() >= deadline) {
      throw new Error(
        `sidecar pid ${String(pid)} descendant(s) ${lingering.join(",")} did not exit after SIGKILL within ${String(CHILD_REAP_GRACE_MS)}ms`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

/**
 * Per-deployment handle tracked by the env. Populated by
 * `deployWorkflowSourceForTest`; consulted by `readWorkflowRunEvents`,
 * `waitForWorkflowRunComplete`, `injectSignal`, and
 * `simulateProcessingCrash` so the Phase I integration tests never
 * thread the workflow-run repo identity themselves.
 */
export type DeploymentHandle = {
  anchorRunId: string;
  workflowDefinition: WorkflowDefinition;
  workflowRunRepoId: RepoId;
  workflowRunRef: string;
  mailAddress: string;
};

export type DeployFlowEnv = {
  hub: HubEnv;
  inference: MockInference;
  sidecar: SidecarHandle;
  /**
   * Sidecar stderr plus hub state-pack receive failures, rendered for a
   * failing wait. Reports every sidecar registered on the env, so a restart
   * test's replacement process is covered by every wait that already passes
   * this function -- including `waitForReconnect`, which wires it itself.
   */
  sidecarDiagnostics: () => string;
  /** Per-deployment handles populated by `deployWorkflowSourceForTest`. */
  deployments: Map<string, DeploymentHandle>;
  /**
   * Register an externally-constructed deployment handle on the env.
   * Tests call this when the deployment was driven outside
   * `deployWorkflowSourceForTest` (e.g. a pre-staged repo state) so the env's
   * helpers can resolve the handle by `anchorRunId`.
   */
  registerDeployment(handle: DeploymentHandle): void;
  /**
   * Register a sidecar the test spawned itself, so `sidecarDiagnostics`
   * reports its output too. A restart test spawns a replacement sidecar
   * against the crashed process's data dir and registers it here.
   *
   * Registration covers diagnostics only. `teardown()` terminates the primary
   * sidecar it spawned and no other, so the caller still owns the registered
   * handle's lifetime.
   */
  registerSidecar(handle: SidecarHandle): void;
  /**
   * Run a retry loop that `waitFor` cannot express under the in-flight wait
   * registry, so a wedge inside it reaches this env's teardown report. See
   * `retrying`.
   */
  retrying: typeof retrying;
  teardown: () => Promise<void>;
};

export type StartDeployFlowEnvOpts = {
  /**
   * Extra env vars written last into the sidecar subprocess env. Wins
   * over every fixture-owned key, including `HUB_WS_URL`,
   * `SIDECAR_ID`, `SIDECAR_TOKEN`, `SIDECAR_DATA_DIR`, and the
   * inherited `PATH`/`HOME`/`TMPDIR`, so callers can both inject new
   * flags and override any fixture default.
   *
   * Every variable named here is verified against the env the subprocess was
   * spawned with; a value that does not arrive throws rather than letting the
   * test run on the fixture default. See `assertPinnedSidecarEnvReached`.
   */
  sidecarEnv?: Record<string, string>;
  /**
   * Opt-in tool-call behavior for the mock inference server. When set,
   * the first request exposing the named tool returns a `tool_use`
   * turn so the spawned child's agent actually runs the tool. See
   * `MockToolCall`.
   */
  inferenceToolCall?: MockToolCall;
  /**
   * When true, `inferenceToolCall` drives the tool on every run rather than
   * once across the env's lifetime. See `StartMockInferenceOpts.
   * toolCallEachRun`; used to re-run a credential tool across a rotation.
   */
  inferenceToolCallEachRun?: boolean;
  /**
   * Persistent tool-call behavior for the approval capstone: the mock
   * re-issues the named tool until the history carries its result, then
   * replies with `${resultPrefix}<result>`. See
   * `StartMockInferenceOpts.approvalToolCall` for why this loops under the
   * broken resume rail and completes under the fixed one.
   */
  inferenceApprovalToolCall?: StartMockInferenceOpts["approvalToolCall"];
  /**
   * A fixed, ordered script of assistant turns for the mock inference server,
   * dispatched by how many tool results the history carries. Drives a
   * deterministic multi-tool agent conversation (e.g. reply, wait, reply)
   * through the spawned child. See `StartMockInferenceOpts.scriptedTurns`.
   */
  inferenceScriptedTurns?: ScriptedTurn[];
  /**
   * When true, the mock inference server echoes the last user message's
   * text as `echo:<text>` so a test can assert the inbound mail body
   * reached the agent's `agent.send` as the step input. See
   * `StartMockInferenceOpts.echoUserMessage`.
   */
  inferenceEchoUserMessage?: boolean;
  /**
   * Co-write hook for the `signal.correlation.register` frame a suspending
   * agent step emits. When set, the mock hub's sidecar router wires it as
   * the `registerSignalCorrelation` lookup, so a parked run's correlation +
   * approval rows are written through the same wire handler production runs.
   * Only the approval capstone supplies it (backed by a real DB); every
   * other test leaves it unset and the frame is dropped with a warning.
   */
  registerSignalCorrelation?: SidecarLookups["registerSignalCorrelation"];
  /**
   * Materializer for a mail-triggered run's grants, wired into the mock
   * hub's sidecar router as the `materializeMailTriggeredRunGrants` lookup.
   * When a `mail.outbound` frame names a workflow-derived recipient, the
   * router's `deliverMailToRecipient` calls this to stage the receiving
   * run's grants before forwarding the mail. Only the federated-mail
   * capstone supplies it (the real `createMailTriggeredRunGrantsMaterializer`
   * backed by a test DB); every other test leaves it unset and the
   * mail is routed without materialization.
   */
  materializeMailTriggeredRunGrants?: SidecarLookups["materializeMailTriggeredRunGrants"];
  /**
   * A db + principal key store the mock hub uses to resolve a signed mail
   * sender's durable public key, wired into the sidecar router as the
   * `resolveSenderKey` / `resolveSenderKeyStrict` lookups exactly as production
   * does (apps/hub/src/server.ts). When set, the materializer path co-delivers
   * the resolved key on the recipient run's grants barrier, so a same-hub
   * sender with a resolvable key verifies `clean` instead of the cache-miss
   * `unknown` strict enforcement drops. Only the federated-mail capstone
   * supplies it (over its real test DB); every other test leaves it unset and
   * no key is co-delivered on that path.
   */
  senderKeyResolution?: {
    db: DBExecutor;
    principalKeyStore: PrincipalKeyStore;
  };
};

// Compose the full deploy-flow env: hub server, mock inference, sidecar
// subprocess. Owns every tempdir these subsystems open and tears them
// all down in `teardown()`.
//
// Returns once the sidecar has registered with the hub.
export async function startDeployFlowEnv(
  opts: StartDeployFlowEnvOpts = {},
): Promise<DeployFlowEnv> {
  // Every harness wait registered from here on belongs to this env; see the
  // in-flight wait registry above for why the mark is needed at all.
  const waitMark = currentWaitMark();
  const tempDirs: string[] = [];
  const registerTempDir = (dir: string): void => {
    tempDirs.push(dir);
  };

  const hub = await startHub(registerTempDir, {
    ...(opts.registerSignalCorrelation !== undefined
      ? { registerSignalCorrelation: opts.registerSignalCorrelation }
      : {}),
    ...(opts.materializeMailTriggeredRunGrants !== undefined
      ? {
          materializeMailTriggeredRunGrants:
            opts.materializeMailTriggeredRunGrants,
        }
      : {}),
    ...(opts.senderKeyResolution !== undefined
      ? { senderKeyResolution: opts.senderKeyResolution }
      : {}),
  });
  const inference = startMockInference({
    ...(opts.inferenceToolCall !== undefined
      ? { toolCall: opts.inferenceToolCall }
      : {}),
    ...(opts.inferenceToolCallEachRun === true
      ? { toolCallEachRun: true }
      : {}),
    ...(opts.inferenceApprovalToolCall !== undefined
      ? { approvalToolCall: opts.inferenceApprovalToolCall }
      : {}),
    ...(opts.inferenceScriptedTurns !== undefined
      ? { scriptedTurns: opts.inferenceScriptedTurns }
      : {}),
    ...(opts.inferenceEchoUserMessage === true
      ? { echoUserMessage: true }
      : {}),
  });

  const hubPort = hub.server.port;
  if (hubPort === undefined) {
    throw new Error(
      "hub.server.port is undefined; expected a bound port from Bun.serve({ port: 0 })",
    );
  }

  const sidecar = await startSidecarSubprocess({
    hubPort,
    registerTempDir,
    ...(opts.sidecarEnv !== undefined ? { extraEnv: opts.sidecarEnv } : {}),
  });

  // This layer owns the `sidecarEnv` -> `extraEnv` -> spawned-env wiring, so
  // it is the one that can check the caller's pin against what the process
  // got. Checked after the spawn and against `sidecar.env` rather than before
  // and against a rebuilt map, so a break at either hop is caught instead of
  // being reproduced by the check.
  if (opts.sidecarEnv !== undefined) {
    assertPinnedSidecarEnvReached(opts.sidecarEnv, sidecar.env);
  }

  // The primary sidecar plus every handle a test registered later, in spawn
  // order. `sidecarDiagnostics` reports all of them, so a restart test's
  // replacement process rides every wait the primary already rode.
  const sidecars: SidecarHandle[] = [sidecar];
  const registerSidecar = (handle: SidecarHandle): void => {
    if (sidecars.includes(handle)) {
      throw new Error("deploy-flow env: sidecar handle is already registered");
    }
    sidecars.push(handle);
  };

  const sidecarDiagnostics = (): string => {
    const parts: string[] = [];
    for (const [index, handle] of sidecars.entries()) {
      if (handle.stderr.length === 0) continue;
      const label = index === 0 ? "sidecar" : `sidecar #${String(index)}`;
      parts.push(`${label} stderr:\n${handle.stderr.slice(-300).join("")}`);
    }
    const failures = hub.statePackReceiveFailures;
    if (failures.length > 0) {
      parts.push(
        `state-pack receive failures (last ${String(Math.min(failures.length, 10))}):\n` +
          failures
            .slice(-10)
            .map((f) => `  ${f.agentAddress}: ${f.error}`)
            .join("\n"),
      );
    }
    return parts.join("\n\n");
  };

  await waitFor(() => hub.router.getConnectedSidecars().length > 0, {
    diagnostics: sidecarDiagnostics,
  });

  const deployments = new Map<string, DeploymentHandle>();
  const registerDeployment = (handle: DeploymentHandle): void => {
    if (deployments.has(handle.anchorRunId)) {
      throw new Error(
        `deploy-flow env: deployment ${handle.anchorRunId} is already registered`,
      );
    }
    deployments.set(handle.anchorRunId, handle);
  };

  const teardown = async (): Promise<void> => {
    // A wait helper renders its `diagnostics` only when its own `timeoutMs`
    // lapses, and almost every caller supplies none, so a test that hangs
    // inside a deadline-free poll loop is ended by the runner's per-test budget
    // with nothing printed. Bun runs `afterAll` after that budget lapses, and
    // this hook is what `afterAll` calls, so it is the remaining place that can
    // report what the sidecar said.
    //
    // The in-flight wait registry is the gate: a wedged test leaves the helper
    // it was suspended in registered, and a test that returned or threw leaves
    // nothing registered, so a clean run prints nothing. A test that passed
    // while one of these waits was still in flight also trips it. That is not
    // a false positive: CONVENTIONS.md requires a test to own every async
    // operation it starts, and this is the gate that can see the violation.
    //
    // Reporting and stopping are the same call, so the waits the rest of this
    // hook is about to dismantle the env underneath are always the ones the
    // report named. Each stopped wait throws at its next iteration; nothing
    // here awaits that, because the promise it rejects was already abandoned.
    const outstanding = stopOutstandingWaits(waitMark);
    if (outstanding !== null) {
      const diagnostics = sidecarDiagnostics();
      process.stderr.write(
        `\n${outstanding}\n${diagnostics.length > 0 ? `${diagnostics}\n` : ""}`,
      );
    }
    // Close every tracked hub-side WebSocket handle before killing the
    // sidecar so no live link lingers. Then terminate the sidecar and reap
    // its process subtree (`terminateSidecarSubprocess`) before removing its
    // data directory: waiting for every process to exit before the rm keeps
    // the earlier fix intact -- removing `sidecar-data-*` while a subprocess
    // still holds file handles raced EBUSY/EACCES on slow hosts, so errors
    // must surface from the rm rather than be shrouded. The server stops are
    // bounded (`stopServerBounded`) because a test that dropped the hub link
    // leaves Bun with a phantom connection its `server.stop` would wait on
    // forever.
    deployments.clear();
    for (const handle of hub.liveHandles) {
      handle.close();
    }
    hub.liveHandles.clear();
    await terminateSidecarSubprocess(sidecar);
    await stopServerBounded(hub.server);
    await stopServerBounded(inference.server);
    for (const d of tempDirs.splice(0)) {
      await fs.promises.rm(d, { recursive: true, force: true });
    }
  };

  return {
    hub,
    inference,
    sidecar,
    sidecarDiagnostics,
    deployments,
    registerDeployment,
    registerSidecar,
    retrying,
    teardown,
  };
}

// =========================================================================
// Phase I helpers
// =========================================================================
//
// Helpers shared by the Phase I end-to-end tests. Pre-landing them in
// one fixture commit avoids the file-touch conflict that would result
// from five parallel test commits each extending the fixture
// independently.
//
// Each helper composes against the actual production paths in
// `@intx/workflow-deploy`, `@intx/workflow-host`, and the workflow-run
// kind handler in `@intx/hub-sessions`. None of the helpers reach into
// stubs; the `injectSignal` path commits a real `SignalReceived` blob
// via `createWorkflowHostSignalChannel`, the `simulateProcessingCrash`
// path drives the workflow-run kind handler's exported claim-check
// primitives, and so on.

const DEFAULT_DEPLOYMENT_DOMAIN = "integration.interchange";
const DEFAULT_WORKFLOW_RUN_REF = "refs/heads/main";

/**
 * Handle carrying a deployment's anchor id plus the workflow-run repo
 * identity the other helpers consult. Returned (extended) by
 * `deployWorkflowSourceForTest`.
 */
export type DeployWorkflowHandle = {
  anchorRunId: string;
  workflowRunRepoId: RepoId;
  workflowRunRef: string;
  mailAddress: string;
};

const SOURCE_FIXTURE_PACKAGE_NAME = "@wf/source-fixture";
const SOURCE_FIXTURE_PACKAGE_VERSION = "1.0.0";
const DEFAULT_WORKFLOW_ENTRY = "./workflow.mjs";

export type DeployWorkflowSourceForTestOpts = {
  /**
   * Which sidecar's allocation identity this deployment binds to. Required
   * when a test runs two sidecars and needs them on separate transports;
   * omitted, the identity is inferred from which sidecar is connected, which
   * races a reconnect.
   */
  sidecarId?: string;
  /**
   * The source entry module text to bundle. A fixture builder (e.g.
   * `singleStepAgentEntry`) produces this; the helper bundles it to a
   * self-contained `.mjs`, writes it as a `workflow`-kind source asset, and
   * deploys the definition BY SOURCE-REF.
   */
  entryModule: string;
  /**
   * The path inside the source package that exports `workflow`. Defaults to
   * `./workflow.mjs`, the path the bundle is written to.
   */
  entry?: string;
  /**
   * The `interchange.loops` module path, set on the package.json when the
   * fixture ships loop `while`/`carry` functions. Point it at the same bundled
   * entry (`./workflow.mjs`) when the entry module exports both `workflow` and
   * the loop fns. Omit for a workflow with no loop primitive.
   */
  loops?: string;
  /**
   * The `interchange.actions` module path, set when the fixture ships `action`
   * handlers. Point it at the same bundled entry when the entry module exports
   * both `workflow` and the handlers. Omit for a workflow with no action.
   */
  actions?: string;
  /**
   * Extra source files seeded alongside the bundle under the source asset
   * (e.g. an inline tool module the entry imports).
   */
  extraSourceFiles?: Record<string, string>;

  /**
   * The real test DB. REQUIRED: the install/approve freeze and the anchor
   * `workflow_run` insert both write through it.
   */
  db: TestDb["db"];
  /** The definition's OWN tenant. */
  tenantId: string;
  /** The `workflow`-kind asset the frozen definition projects over. */
  definitionAssetId: string;

  /** The deployment's anchor run id. */
  anchorRunId: string;
  /** The mail domain the run address lives under. Default `integration.interchange`. */
  deploymentDomain?: string;
  /** The deployment's mail address. Default `deriveRunAddress({ runId, domain })`. */
  agentAddress?: string;
  /** Optional `workflow-run` ref override. Default `refs/heads/main`. */
  workflowRunRef?: string;

  /**
   * The approval policy: an operator `ApprovalSet` the gate holds the probed
   * surface to, or `"approve-probed"` to approve exactly the probed surface.
   */
  approvals: ApprovalSet | "approve-probed";

  /** The deploy harness config. */
  config: HarnessConfig;
  /**
   * Per-step inference sources. Omit to compute them the way production does --
   * `buildInertProjectionStepSources` over the frozen projection and the
   * approved grants (which recurses into loop bodies) -- so a fixture that omits
   * this exercises the real source-pin path. Supply it to override.
   */
  sources?: Record<string, InferenceSource[]>;

  /** Credential delivery for a credential-consuming fixture. */
  credentialCipher?: CredentialCipher;
};

/**
 * Handle returned by `deployWorkflowSourceForTest`. Carries the same fields as
 * `DeployWorkflowHandle` (so per-test assertion helpers are unchanged) plus the
 * frozen approve result and the deploy's public key.
 */
export type DeployWorkflowSourceForTestHandle = DeployWorkflowHandle & {
  approved: InstallAndApproveResult;
  publicKey: string;
};

/**
 * Deploy a workflow BY SOURCE-REF against the env's hub, mirroring
 * `source-workflow.e2e.test.ts`: bundle the entry module, seed it as a
 * `workflow`-kind source asset, install + probe + gate + freeze it against the
 * real DB, then emit the source-ref deploy frame and write the anchor
 * `workflow_run` row. Registers the resulting handle on the env so the Phase I
 * assertion helpers resolve it by `anchorRunId`.
 *
 * The caller owns the DB lifecycle (`createTestDb`) and seeds the
 * tenant/principal/definition asset in its own `beforeAll`, matching the e2e
 * pattern; this helper does not own the DB.
 */
// A single tool-provider row per tenant that the seeded inference credentials
// reference; `credential.providerId` needs a real provider row.
const TEST_INFERENCE_PROVIDER_PREFIX = "prov-test-inference-";

/**
 * Seed a tenant-owned credential for each distinct inference-source
 * `credentialId` referenced by the deploy (top-level `sources` and the deploy
 * `config`'s pool, which the hub also pins body sources from). The unified
 * pre-register deploy resolves each `credentialId` to material through the
 * credential table; a fixture source that references an unseeded id would fail
 * the deploy closed. The mock inference server ignores the secret, so a stable
 * per-id placeholder suffices. Idempotent (`onConflictDoNothing`) so a test that
 * deploys repeatedly does not double-insert.
 */
export async function seedInferenceCredentials(
  db: TestDb["db"],
  tenantId: string,
  sources: Record<string, InferenceSource[]>,
  config: HarnessConfig,
): Promise<void> {
  const credentialIds = new Set<string>();
  for (const chain of Object.values(sources)) {
    for (const source of chain) credentialIds.add(source.credentialId);
  }
  for (const source of config.sources) credentialIds.add(source.credentialId);
  if (credentialIds.size === 0) return;

  const providerId = `${TEST_INFERENCE_PROVIDER_PREFIX}${tenantId}`;
  await db
    .insert(provider)
    .values({
      id: providerId,
      tenantId,
      name: "test-inference-provider",
      plugin: "anthropic",
      // Material resolution pins each credential's origin to its provider's
      // API base URL and fails closed on a null one, so seed a concrete origin.
      apiBaseUrl: "https://api.anthropic.com",
    })
    .onConflictDoNothing();
  for (const credentialId of credentialIds) {
    await db
      .insert(credential)
      .values({
        id: credentialId,
        tenantId,
        providerId,
        name: credentialId,
        type: "api_key",
        secret: `${credentialId}-secret`,
        status: "active",
        principalId: null,
      })
      .onConflictDoNothing();
  }
}

export async function deployWorkflowSourceForTest(
  env: DeployFlowEnv,
  opts: DeployWorkflowSourceForTestOpts,
): Promise<DeployWorkflowSourceForTestHandle> {
  // Reap every tracked deployment whose run is already terminal before
  // standing up the new one: the new deploy proves the previous test is
  // done with those deployments, so their parked workflow-process children
  // (~330MB each) are dead weight. Fire-and-forget via the production
  // `agent.undeploy` path; see `reapTerminalDeployments`.
  reapTerminalDeployments(env);

  const deploymentDomain = opts.deploymentDomain ?? DEFAULT_DEPLOYMENT_DOMAIN;
  const workflowRunRef = opts.workflowRunRef ?? DEFAULT_WORKFLOW_RUN_REF;
  const entry = opts.entry ?? DEFAULT_WORKFLOW_ENTRY;
  const agentAddress =
    opts.agentAddress ??
    deriveRunAddress({ runId: opts.anchorRunId, domain: deploymentDomain });

  const hubPrincipal: WorkflowRunHubPrincipal = { kind: "hub" };
  const sourceAssetId = `ast_${opts.anchorRunId.replace(/[^a-zA-Z0-9]/g, "_")}_src`;
  const sourceRepoId: RepoId = { kind: "workflow", id: sourceAssetId };

  // Bundle the entry module to a self-contained `.mjs` in a throwaway scratch
  // dir, then seed it as raw source under the source asset. The scratch dir is
  // only needed for the Bun.build input, so it is removed once the bundle is in
  // hand.
  const scratchDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "source-fixture-"),
  );
  let workflowJs: string;
  try {
    workflowJs = await bundleWorkflowEntry(scratchDir, opts.entryModule);
  } finally {
    await fs.promises.rm(scratchDir, { recursive: true, force: true });
  }

  await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
  const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
    hubPrincipal,
    sourceRepoId,
    DEFAULT_ASSET_REF,
    {
      files: {
        "package.json": JSON.stringify({
          name: SOURCE_FIXTURE_PACKAGE_NAME,
          version: SOURCE_FIXTURE_PACKAGE_VERSION,
          interchange: {
            workflow: entry,
            ...(opts.loops !== undefined ? { loops: opts.loops } : {}),
            ...(opts.actions !== undefined ? { actions: opts.actions } : {}),
          },
        }),
        "workflow.mjs": workflowJs,
        ...opts.extraSourceFiles,
      },
      message: `deployWorkflowSourceForTest: seed source package for ${opts.anchorRunId}`,
    },
  );
  const commitSha = writeResult.commitSha;

  const source: WorkflowDefinitionAssetSource = {
    kind: "asset",
    assetId: sourceAssetId,
    package: { format: "source", commitSha },
  };

  // Deliver the source asset's git pack on the frame: the sidecar indexes it
  // and checks the pinned subtree out of it.
  const resolveAttachment = async (
    assetId: string,
  ): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
    if (assetId !== sourceAssetId) {
      throw new Error(
        `deployWorkflowSourceForTest: unexpected attachment request ${assetId}`,
      );
    }
    const tipSha = await env.hub.agentRepoStore.repoStore.resolveRef(
      hubPrincipal,
      sourceRepoId,
      DEFAULT_ASSET_REF,
    );
    if (tipSha === null) {
      throw new Error(
        "deployWorkflowSourceForTest: source asset has no commit",
      );
    }
    const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
      hubPrincipal,
      sourceRepoId,
      DEFAULT_ASSET_REF,
    );
    return { pack, ref, commitSha: tipSha };
  };

  const committed =
    await env.hub.agentRepoStore.repoStore.openCommittedReadsAtCommit(
      hubPrincipal,
      sourceRepoId,
      commitSha,
    );
  if (committed === null) {
    throw new Error(
      "deployWorkflowSourceForTest: could not open committed reads at commit",
    );
  }
  const reads = committedReadsToSourceTree(committed);

  const approvals =
    opts.approvals === "approve-probed"
      ? ({ kind: "approve-probed" } as const)
      : opts.approvals;

  const allocationTarget = env.hub.prepareAllocationIdentity(
    opts.anchorRunId,
    agentAddress,
    opts.sidecarId,
  );
  const approved = await installAndApproveWorkflowDefinition({
    source,
    entry,
    assetId: opts.definitionAssetId,
    approvals,
    router: {
      sendProbe: (args) =>
        env.hub.router.sendProbeToAllocation(allocationTarget, args),
    },
    db: opts.db,
    reads,
    registryName: "npmjs",
    registryConfig: { url: "https://registry.test" },
    resolveAttachment,
  });
  if (!approved.approval.ok) {
    throw new Error(
      `deployWorkflowSourceForTest: install/approve gate did not approve ` +
        `(reason: ${approved.approval.reason}): ${JSON.stringify(approved.approval)}\n` +
        env.sidecarDiagnostics(),
    );
  }

  // Compute the per-step sources the way production does when the fixture does
  // not override them, so an omitting fixture exercises the real source pin
  // (including the loop-body recursion) rather than a hand-supplied map.
  const sources =
    opts.sources ??
    buildInertProjectionStepSources({
      projection: approved.projection,
      config: opts.config,
      operatorApprovals: approved.approval.approvedSurface,
    });

  // Pre-register: every inference source references a registered credential by
  // id. Seed a tenant-owned credential per referenced credentialId so the deploy
  // resolves them into the unified material cell (the mock inference server
  // ignores the secret value). Idempotent across a test's repeated deploys.
  await seedInferenceCredentials(opts.db, opts.tenantId, sources, opts.config);

  const deployResult = await deployCodeSourcedWorkflow({
    approved,
    source,
    resolveAttachment,
    sidecarAllocationRouter: env.hub.router,
    allocationTarget,
    agentAddress,
    config: opts.config,
    sources,
    db: opts.db,
    tenantId: opts.tenantId,
    anchorRunId: opts.anchorRunId,
    deploymentDomain,
    // The deploy resolves each pinned source's credentialId to material; the
    // seeded secrets are plaintext, so default to the noop cipher unless a test
    // supplies its own.
    credentialCipher: opts.credentialCipher ?? createNoopCredentialCipher(),
  });

  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: deriveDeploymentId(agentAddress),
  };
  const handle: DeploymentHandle = {
    anchorRunId: opts.anchorRunId,
    workflowDefinition: {
      id: approved.projection.id,
      triggers: [{ type: "mail", to: agentAddress }],
      steps: {},
      stepOrder: [...approved.projection.stepOrder],
    },
    workflowRunRepoId,
    workflowRunRef,
    mailAddress: agentAddress,
  };
  env.registerDeployment(handle);

  return {
    anchorRunId: opts.anchorRunId,
    workflowRunRepoId,
    workflowRunRef,
    mailAddress: agentAddress,
    approved,
    publicKey: deployResult.publicKey,
  };
}

/**
 * Resolve a deployment handle by id. Throws if no deployment has been
 * registered under that id so a typo or stale id surfaces as a loud
 * failure rather than a silent no-op.
 */
function requireDeployment(
  env: DeployFlowEnv,
  anchorRunId: string,
): DeploymentHandle {
  const handle = env.deployments.get(anchorRunId);
  if (handle === undefined) {
    throw new Error(
      `deploy-flow env: no deployment registered for ${anchorRunId}; call deployWorkflowSourceForTest or registerDeployment first`,
    );
  }
  return handle;
}

export type { WorkflowRunEvent };

// ---- Reaping completed deployments' workflow-process children ----
//
// A workflow-process child (~330MB of module graph) stays alive for the
// whole test file once its deployment's run completes: the sidecar keeps
// deployments deployed (warm) until an undeploy or the sidecar exits, and
// the fixture only tears the sidecar down in the file's afterAll. A file
// that deploys several workflows therefore accumulates one parked child
// per completed deployment, and the workflow lane's peak RSS is set by
// those parked children (e.g. child-workflow-roundtrip peaks at ~2.7GiB
// with six parked children where the single-deployment baseline is ~1GiB).
//
// The fixture reaps a completed deployment through the production
// `agent.undeploy` wire path (the same frame the hub sends for a real
// undeploy) when a NEW deployment is registered. At that point every
// previously-completed deployment is provably done -- each test deploys
// its own workflow and no test re-triggers a completed deployment after a
// later deploy in the same file -- so killing its child is unobservable to
// test logic. The undeploy is fire-and-forget so the next deploy is never
// delayed by the reap, and a failed/timed-out undeploy leaves the child
// parked until teardown (the pre-existing behavior), so the reap is
// strictly best-effort. Terminal marking happens only in the read helpers
// below (a run whose LAST event is terminal is by definition finished);
// an externally-registered handle that is never read is never reaped.
const terminalDeployments = new WeakMap<DeploymentHandle, boolean>();
const reapedDeployments = new WeakSet<DeploymentHandle>();

function reapTerminalDeployments(env: DeployFlowEnv): void {
  for (const handle of env.deployments.values()) {
    if (terminalDeployments.get(handle) !== true) continue;
    if (reapedDeployments.has(handle)) continue;
    reapedDeployments.add(handle);
    void env.hub.router
      .sendAgentUndeploy(
        handle.mailAddress,
        "test fixture: deployment run complete; reaping parked child",
      )
      .catch(() => {
        // Best-effort: leave the child parked until teardown.
      });
  }
}

/**
 * Read every event under `runs/<runId>/events/` from the deployment's
 * workflow-run repo and return them in ascending `seq` order. Returns
 * an empty array when the run has not yet committed any events or the
 * repo has not yet been created (e.g. the deployment hasn't taken the
 * multi-step branch yet). Delegates to the shared hub-side
 * `WorkflowRunReader` so the fixture and the REST route project the
 * substrate through one reader.
 */
export async function readWorkflowRunEvents(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
): Promise<WorkflowRunEvent[]> {
  const handle = requireDeployment(env, anchorRunId);
  const reader = createWorkflowRunReader(env.hub.agentRepoStore.repoStore);
  const events = await reader.readRunEvents(
    handle.workflowRunRepoId,
    handle.workflowRunRef,
    runId,
  );
  // The log is seq-ordered, so a terminal last event means the run is
  // finished: no further event can follow it. Marking here lets tests that
  // poll for terminal state via this helper (instead of
  // `waitForWorkflowRunComplete`) still reap the deployment's parked child
  // at the next deploy.
  const last = events.at(-1);
  if (last !== undefined && WORKFLOW_RUN_TERMINAL_TYPES.has(last.type)) {
    terminalDeployments.set(handle, true);
  }
  return events;
}

/**
 * Options for `waitForWorkflowRunComplete`. Mirrors the shape of
 * `waitFor` so the helper composes the same diagnostic surface.
 */
export type WaitForWorkflowRunCompleteOpts = {
  /**
   * Bound on the wait. Omitted, the helper polls until the terminal event
   * lands and never throws; a caller that must distinguish "not terminal yet"
   * from a fault supplies it and discriminates on
   * `isWorkflowRunCompleteTimeout`.
   */
  timeoutMs?: number;
  diagnostics?: () => string;
};

/** Terminal event discriminators the kind handler recognises. */
export const WORKFLOW_RUN_TERMINAL_TYPES: ReadonlySet<string> = new Set([
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
]);

/**
 * `code` marker on the error `waitForWorkflowRunComplete` throws when its
 * budget lapses with no terminal event on the log.
 */
export const WORKFLOW_RUN_COMPLETE_TIMEOUT_CODE =
  "workflow_run_complete_timeout";

/** The timeout `waitForWorkflowRunComplete` throws, carrying its marker. */
export interface WorkflowRunCompleteTimeout extends Error {
  readonly code: typeof WORKFLOW_RUN_COMPLETE_TIMEOUT_CODE;
}

/**
 * True only for that timeout. A caller that retries
 * `waitForWorkflowRunComplete` has to tell "no terminal event yet, read
 * again" apart from a real fault -- a substrate read error, a deployment the
 * fixture never registered -- so it can surface the fault instead of
 * retrying through it until its own budget lapses and reports a hang.
 *
 * Discriminate on this, never on the error's message. The message carries
 * interpolated diagnostics and exists to be read by a human, so matching it
 * would reclassify every fault as a timeout the first time it is reworded.
 */
export function isWorkflowRunCompleteTimeout(
  err: unknown,
): err is WorkflowRunCompleteTimeout {
  return (
    err instanceof Error &&
    hasCode(err) &&
    err.code === WORKFLOW_RUN_COMPLETE_TIMEOUT_CODE
  );
}

/**
 * Poll the deployment's workflow-run event log until the run's
 * terminal event lands. Returns the terminal event. Carries no deadline
 * unless the caller supplies `timeoutMs`.
 */
export async function waitForWorkflowRunComplete(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
  opts: WaitForWorkflowRunCompleteOpts = {},
): Promise<WorkflowRunEvent> {
  const { timeoutMs, diagnostics } = opts;
  const registration = registerWait(
    `waitForWorkflowRunComplete(${anchorRunId}/${runId})`,
  );
  try {
    const start = Date.now();
    for (;;) {
      throwIfEnvTornDown(registration);
      const events = await readWorkflowRunEvents(env, anchorRunId, runId);
      const terminal = events.find((e) =>
        WORKFLOW_RUN_TERMINAL_TYPES.has(e.type),
      );
      if (terminal !== undefined) {
        const handle = env.deployments.get(anchorRunId);
        if (handle !== undefined) {
          terminalDeployments.set(handle, true);
        }
        return terminal;
      }
      if (timeoutMs !== undefined && Date.now() - start > timeoutMs) {
        const diag = diagnostics?.();
        const ctx = diag ? `\n${diag}` : "";
        throw Object.assign(
          new Error(
            `waitForWorkflowRunComplete timed out after ${String(timeoutMs)}ms for ${anchorRunId}/${runId}${ctx}`,
          ),
          { code: WORKFLOW_RUN_COMPLETE_TIMEOUT_CODE },
        );
      }
      await new Promise((r) => setTimeout(r, 50));
    }
  } finally {
    deregisterWait(registration);
  }
}

/**
 * Options for `fireMailTrigger`. `messageId` defaults to a stable
 * synthesized id so the FIFO test can supply distinct ids per call
 * without colliding on the dedup index.
 */
export type FireMailTriggerOpts = {
  /**
   * RFC 2822 `Message-Id` of the synthesized mail. The fixture
   * supplies a stable default when omitted; the FIFO crash-replay
   * test overrides per call.
   */
  messageId?: string;
  /** Mail body (conversation text). Defaults to a placeholder. */
  content?: string;
  /** Sender address. Defaults to a test-stable user address. */
  from?: string;
  /**
   * Per-run grants delivered ahead of the trigger mail, mirroring the
   * production trigger route: the hub sends the run's `run.grants` frame
   * before the mail so the run's `runs/<runId>/grants.json` lands before
   * dispatch. Defaults to an empty set, which still materializes the file
   * -- every mail-born run carries a grants file, so the supervisor's
   * `onRunStart` barrier and a spawned child both resolve it rather than
   * failing closed on its absence.
   */
  grants?: WireGrantRule[];
  /**
   * Attachments MIME-encoded into the signed conversation message, exactly as
   * the production trigger route encodes them. Delivered to the deployed run
   * as non-text inbound content.
   */
  attachments?: MessageAttachment[];
  /**
   * RFC 2822 `In-Reply-To` header of the synthesized mail. Set it to a prior
   * message's `Message-Id` to thread this inbound onto an existing connector
   * conversation: the connector router treats the message as a continuation
   * when its `In-Reply-To` matches the thread's last message id. Omitted (no
   * header) by default, which starts a fresh thread.
   */
  inReplyTo?: string;
  /**
   * RFC 2822 `References` header (a message-id chain) of the synthesized mail.
   * The connector router also treats an inbound as a continuation when its
   * `References` includes the thread root. Omitted (no header) by default.
   */
  references?: string[];
};

/**
 * `code` marker on the errors `fireMailTrigger` throws when the hub declines
 * to route a frame at the target address -- `sendRunGrants` or `routeMail`
 * returned false, meaning the address had neither a live connection nor a
 * disconnect queue to ride.
 */
export const MAIL_TRIGGER_UNROUTABLE_CODE = "mail_trigger_unroutable";

/** The unroutable-address failure `fireMailTrigger` throws, carrying its marker. */
export interface MailTriggerUnroutableError extends Error {
  readonly code: typeof MAIL_TRIGGER_UNROUTABLE_CODE;
}

/**
 * True only for that failure. A caller that re-fires a trigger across a
 * reconnect has to tell "the address is not routable yet, fire again" apart
 * from a real fault -- a signing failure, a malformed address -- so it can
 * surface the fault instead of re-firing through it until its own budget
 * lapses and reports a hang.
 *
 * Discriminate on this, never on the error's message. The message interpolates
 * the address and exists to be read by a human, so matching it would
 * reclassify every fault as unroutable the first time it is reworded.
 */
export function isMailTriggerUnroutableError(
  err: unknown,
): err is MailTriggerUnroutableError {
  return (
    err instanceof Error &&
    hasCode(err) &&
    err.code === MAIL_TRIGGER_UNROUTABLE_CODE
  );
}

/**
 * Construct a signed mail message and route it via the hub's
 * `routeMail` path -- the same surface the existing deploy-flow
 * integration test uses to fire a mail at the agent. Returns the
 * `Message-Id` the helper chose so the caller can correlate the
 * downstream `RunStarted` against the message that triggered it.
 */
export async function fireMailTrigger(
  env: DeployFlowEnv,
  address: string,
  opts: FireMailTriggerOpts = {},
): Promise<{ messageId: string }> {
  const messageId =
    opts.messageId ??
    `<wf-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}@integration.interchange>`;
  const content = opts.content ?? "Hello.";
  const from = opts.from ?? "user@integration.interchange";

  const keyPair = await generateKeyPair();
  const crypto = createEd25519Crypto(keyPair);
  const headers: MessageHeaders = {
    from,
    to: [address],
    cc: undefined,
    date: new Date(),
    messageId,
    subject: undefined,
    inReplyTo: opts.inReplyTo,
    references: opts.references,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
  const signedContent = assembleSignedContent({
    kind: "conversation",
    text: content,
    ...(opts.attachments && opts.attachments.length > 0
      ? { attachments: opts.attachments }
      : {}),
  });
  const signature = await createDetachedSignatureFromProvider(
    signedContent,
    crypto,
  );
  const rawMessage = assembleMessage(headers, signedContent, signature);
  const base64 = base64Encode(rawMessage);

  // Deliver the run's grants before the trigger mail. The runId is the local
  // part of the deployment's mail address (not the per-message Message-ID);
  // derive it through the same shared helper the production route and sidecar
  // use, so this fixture cannot mask a divergence by hand-picking the right
  // value.
  const runId = deriveWorkflowRunId(address);
  // Co-deliver the trigger signer's public key on the run's grants barrier,
  // exactly as production does: the HTTP trigger route resolves and co-delivers
  // the principal key, so the recipient caches the sender's key before the mail
  // arrives and the admission verdict is clean (valid signature + address
  // match). Without this, the recipient caches no key, the verdict is unknown,
  // and strict inbound enforcement rejects the trigger so the run never starts.
  // The identity address is the MIME From, which this fixture also reuses as the
  // authenticated sender on routeMail, so the from-match holds.
  const senderIdentities = [
    { address: from, publicKey: hexEncode(keyPair.publicKey) },
  ];
  const grantsDelivered = env.hub.router.sendRunGrants(
    address,
    runId,
    opts.grants ?? [],
    senderIdentities,
  );
  if (!grantsDelivered) {
    throw Object.assign(
      new Error(
        `fireMailTrigger: sendRunGrants returned false for ${address}; address is not routable on the hub`,
      ),
      { code: MAIL_TRIGGER_UNROUTABLE_CODE },
    );
  }

  // Route the trigger the way the production route does, stamping the
  // signed-under address as the authenticated sender. This fixture reuses
  // that same address as the MIME From, so it is not a From-independence
  // check.
  const delivered = env.hub.router.routeMail(address, base64, from);
  if (!delivered) {
    throw Object.assign(
      new Error(
        `fireMailTrigger: routeMail returned false for ${address}; address is not routable on the hub`,
      ),
      { code: MAIL_TRIGGER_UNROUTABLE_CODE },
    );
  }
  return { messageId };
}

/**
 * Deliver a workflow-run signal through the production hub →
 * sidecar → supervisor → workflow-process child pipeline. The hub
 * router's `sendSignalDeliver` ships a `signal.deliver` wire frame to
 * the sidecar holding the deployment; the sidecar's hub-link routes
 * the frame into the deployment's supervisor, which forwards a
 * `signal.deliver` control IPC payload to the workflow-process child.
 * The child commits the resulting `SignalReceived` event through its
 * own substrate -- the single writer of the workflow-run repo on the
 * sidecar side -- so the workflow-run pack-push pipeline that
 * propagates the commit to the hub never sees a concurrent writer at
 * the workflow-run ref. The host-side substrate write the previous
 * implementation performed is the race the wire path eliminates by
 * construction.
 *
 * The returned `signalId` is the value the producer minted; the
 * workflow-run state machine's `observedSignalIds` dedup key matches
 * against this value.
 */
export async function injectSignal(
  env: DeployFlowEnv,
  anchorRunId: string,
  runId: string,
  signalName: string,
  payload: unknown,
): Promise<{ signalId: string }> {
  const handle = requireDeployment(env, anchorRunId);
  const signalId = `sig_${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  env.hub.router.sendSignalDeliver({
    agentAddress: handle.mailAddress,
    runId,
    signalName,
    signalId,
    payload,
  });
  return { signalId };
}

/** Options for `initiateDrain`. */
export type InitiateDrainOpts = {
  /**
   * Wire `deadlineMs` carried on the drain control frame. Defaults
   * to the supervisor's own `DEFAULT_DRAIN_TIMEOUT_MS` (5_000) when
   * omitted, mirroring the production wiring's policy default.
   */
  deadlineMs?: number;
};

/**
 * Send a workflow-host drain control payload through the production
 * hub -> sidecar -> supervisor -> workflow-process child pipeline. The
 * hub router's `sendDrain` ships a `drain.deliver` wire frame to the
 * sidecar holding the deployment; the sidecar's hub-link routes the
 * frame into the deployment's supervisor, which forwards a `drain`
 * control IPC payload to the workflow-process child and arms one
 * `drainTimeout` accumulator per in-flight run. Cancel-mode in-flight
 * steps abort on the child side as the controller signal flips;
 * wait-mode steps continue. Each accumulator commits a signed
 * `CancelRequested{origin: "supervisor-drain"}` against the
 * workflow-run repo when the deadline expires.
 */
export function initiateDrain(
  env: DeployFlowEnv,
  anchorRunId: string,
  opts: InitiateDrainOpts = {},
): void {
  const handle = requireDeployment(env, anchorRunId);
  const deadlineMs = opts.deadlineMs ?? 5_000;
  env.hub.router.sendDrain({
    agentAddress: handle.mailAddress,
    deadlineMs,
  });
}

/**
 * Write a `processing/<receivedAt>-<messageId>.json` entry directly
 * into the deployment's workflow-run repo. The helper composes
 * `enqueueInbox` followed by `dequeueToProcessing` -- the same two
 * substrate primitives the supervisor uses on a normal mail trigger
 * fire -- so the resulting on-disk state is bit-identical to the
 * state a supervisor crash would leave behind after the dequeue
 * commit but before the matching `markConsumed`. The kind handler's
 * `validatePush` requires the inbox→processing transition to be
 * backed by a matching prior inbox entry, so any "direct" write
 * that bypassed the inbox would be rejected at the substrate
 * boundary; routing through the two primitives is the only honest
 * way to land the post-crash state.
 */
export async function simulateProcessingCrash(
  env: DeployFlowEnv,
  anchorRunId: string,
  address: string,
  messageId: string,
  receivedAt: number,
): Promise<void> {
  const handle = requireDeployment(env, anchorRunId);
  const principal: WorkflowRunHubPrincipal = { kind: "hub" };
  await enqueueInbox(
    env.hub.agentRepoStore.repoStore,
    principal,
    handle.workflowRunRepoId,
    {
      address,
      messageId,
      receivedAt,
      mailAuditRef: {
        store: "deploy-flow-env-simulated-crash",
        path: `${address}/${messageId}`,
      },
    },
  );
  const dequeued = await dequeueToProcessing(
    env.hub.agentRepoStore.repoStore,
    principal,
    handle.workflowRunRepoId,
    address,
  );
  if (dequeued === null) {
    throw new Error(
      `simulateProcessingCrash: dequeueToProcessing returned null after enqueueInbox; inbox is unexpectedly empty for ${address}/${messageId}`,
    );
  }
}

/**
 * Enumerate the run ids present under `runs/` in the deployment's
 * workflow-run repo's `refs/heads/main`. Returns an empty array when
 * the repo has not been initialised yet (no on-disk repoDir, no ref,
 * or no `runs/` tree); a corrupt repo, a present-but-malformed tree,
 * or any other unexpected isomorphic-git error propagates so the
 * caller sees the failure rather than treating it as "no runs yet".
 */
export async function listRunIds(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string[]> {
  const reader = createWorkflowRunReader(env.hub.agentRepoStore.repoStore);
  return reader.listRunIds(workflowRunRepoId, DEFAULT_WORKFLOW_RUN_REF);
}

/**
 * Read every blob under a specific claim-check sub-directory of the
 * deployment's workflow-run repo, against `refs/heads/events` (the
 * workflow-run substrate's claim-check ref). Returns an empty array
 * when the repo, ref, address subtree, or chosen sub-directory has
 * not been initialised yet; other isomorphic-git failures propagate.
 */
export async function readClaimCheckDir(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
  subdir: "inbox" | "processing" | "consumed",
): Promise<{ filename: string; bytes: Uint8Array }[]> {
  let repoDir: string;
  try {
    repoDir = env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
  } catch {
    return [];
  }
  let oid: string;
  try {
    oid = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: "refs/heads/events",
    });
  } catch (cause) {
    if (
      cause instanceof git.Errors.NotFoundError ||
      (cause instanceof Error && /ENOENT|not found/i.test(cause.message))
    ) {
      return [];
    }
    throw cause;
  }
  const filepath = `addresses/${encodeURIComponent(address)}/${subdir}`;
  let tree: Awaited<ReturnType<typeof git.readTree>>;
  try {
    tree = await git.readTree({ fs, dir: repoDir, oid, filepath });
  } catch (cause) {
    if (cause instanceof git.Errors.NotFoundError) return [];
    throw cause;
  }
  const out: { filename: string; bytes: Uint8Array }[] = [];
  for (const entry of tree.tree) {
    if (entry.type !== "blob") continue;
    const blob = await git.readBlob({ fs, dir: repoDir, oid: entry.oid });
    out.push({ filename: entry.path, bytes: blob.blob });
  }
  return out;
}

/**
 * Poll until at least one run id is present under `runs/` and return
 * the first one found. Used by integration tests that don't know the runId
 * upfront because the supervisor mints it. Carries no deadline unless the
 * caller supplies `timeoutMs`.
 */
export async function waitForFirstRunId(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<string> {
  const { timeoutMs, diagnostics } = opts;
  const registration = registerWait(
    `waitForFirstRunId(${workflowRunRepoId.id})`,
  );
  try {
    const start = Date.now();
    for (;;) {
      throwIfEnvTornDown(registration);
      const ids = await listRunIds(env, workflowRunRepoId);
      const first = ids[0];
      if (first !== undefined) return first;
      if (timeoutMs !== undefined && Date.now() - start > timeoutMs) {
        const diag = diagnostics?.();
        const ctx = diag ? `\n${diag}` : "";
        throw new Error(
          `waitForFirstRunId timed out after ${String(timeoutMs)}ms for ${workflowRunRepoId.id}${ctx}`,
        );
      }
      await new Promise((r) => setTimeout(r, 50));
    }
  } finally {
    deregisterWait(registration);
  }
}

// =========================================================================
// Hub-link disconnect / reconnect helpers
// =========================================================================
//
// These drive the sidecar's hub WebSocket through a drop and its automatic
// reconnect so a survival test can assert a deployed workflow keeps running
// across the reconnect. The in-process hub is normally lossless with no way
// to sever the link; `startHub` now captures every live server-side
// `WsHandle` (`env.hub.liveHandles`). The reconnect revalidates the sidecar's
// allocation identity and current generation before restoring its route.

/**
 * Force-close every live server-side hub WebSocket, severing the sidecar's
 * hub link. The sidecar's `hub-link` observes the close and begins its
 * `DEFAULT_RECONNECT_DELAY_MS` reconnect cycle. Throws if no handle is
 * live, so a test that expected an established link fails loudly rather
 * than dropping nothing.
 *
 * This is the raw drop, with no settle: it may sever the link while a
 * workflow-run pack push is mid-flight. The interrupted-pack regression
 * test wants exactly that; every survival test that must NOT race an
 * in-flight push should use `settleThenDrop` instead.
 */
export function dropHubLink(env: DeployFlowEnv): void {
  const handles = [...env.hub.liveHandles];
  if (handles.length === 0) {
    throw new Error(
      "dropHubLink: no live hub WebSocket handle to close; the sidecar link is not established",
    );
  }
  for (const handle of handles) {
    handle.close();
  }
}

/** Options for `waitForReconnect`. */
export type WaitForReconnectOpts = {
  /**
   * Ceiling on the reconnect wait. Omitted, the helper waits for the address
   * to return to the routing index however long that takes.
   */
  timeoutMs?: number;
};

/**
 * Poll until `address` is routable on the hub again. A deployment address can
 * only re-enter the hub's routing index after its reconnect passes durable
 * identity revalidation and the current allocation-generation fence, so
 * "routable again" is a sound proxy for completed reconnect registration.
 *
 * Returns nothing, and deliberately does not report how long the wait took.
 * Which delay the sidecar cycled through is pinned at the seam that receives
 * it, so an elapsed figure from here would be evidence of nothing a caller
 * may assert on -- reporting one would only invite a caller to bound it.
 */
export async function waitForReconnect(
  env: DeployFlowEnv,
  address: string,
  opts: WaitForReconnectOpts = {},
): Promise<void> {
  const registration = registerWait(`waitForReconnect(${address})`);
  try {
    await waitFor(
      () => env.hub.router.getRoutableAddresses().includes(address),
      {
        ...(opts.timeoutMs !== undefined ? { timeoutMs: opts.timeoutMs } : {}),
        diagnostics: env.sidecarDiagnostics,
      },
    );
  } finally {
    deregisterWait(registration);
  }
}

/** Options for `settleThenDrop`. */
export type SettleThenDropOpts = {
  /**
   * Length of the no-new-pack quiet window that must elapse before the drop
   * fires. Defaults to `500`. The helper waits until no workflow-run pack
   * has been accepted for this long, treating that as the pack-push pipeline
   * having drained.
   */
  quietMs?: number;
  /**
   * Ceiling on the settle wait. Omitted, the helper waits for the quiet
   * window however long the pack stream takes to reach it. Supplied, it
   * throws instead of dropping into an in-flight push once the ceiling
   * passes.
   */
  timeoutMs?: number;
};

/**
 * Wait for the workflow-run pack-push pipeline to go quiet, then drop the
 * hub link. "Quiet" is `quietMs` with no newly-accepted workflow-run pack
 * (`env.hub.workflowRunPackReceipts`), which is the hub-side, cross-process
 * proxy for the sidecar's pack-push pipeline having drained
 * (`flushWorkflowRunPushes` / `notifySettled` live inside the sidecar
 * subprocess and cannot be awaited from the harness process). This is the
 * default drop for survival tests: it guarantees no pack push is mid-flight
 * when the link is severed, so the test exercises reconnect survival rather
 * than an interrupted pack. Use the raw `dropHubLink` when an interrupted
 * push is the thing under test.
 *
 * `address` is accepted for symmetry with the other reconnect helpers and
 * to document which deployment the drop targets; the quiescence signal is
 * hub-wide, and in the single-deployment survival tests the sidecar holds
 * exactly one link, so a hub-wide quiet window is equivalent to a
 * per-deployment one.
 */
export async function settleThenDrop(
  env: DeployFlowEnv,
  address: string,
  opts: SettleThenDropOpts = {},
): Promise<void> {
  const quietMs = opts.quietMs ?? 500;
  const { timeoutMs } = opts;
  const registration = registerWait(`settleThenDrop(${address})`);
  try {
    const start = Date.now();
    let lastCount = env.hub.workflowRunPackReceipts.count;
    let lastChange = Date.now();
    for (;;) {
      throwIfEnvTornDown(registration);
      const current = env.hub.workflowRunPackReceipts.count;
      if (current !== lastCount) {
        lastCount = current;
        lastChange = Date.now();
      }
      if (Date.now() - lastChange >= quietMs) break;
      if (timeoutMs !== undefined && Date.now() - start > timeoutMs) {
        throw new Error(
          `settleThenDrop: workflow-run pack stream did not go quiet for ${String(quietMs)}ms within ${String(timeoutMs)}ms for ${address}` +
            `\n${env.sidecarDiagnostics()}`,
        );
      }
      await new Promise((r) => setTimeout(r, 50));
    }
  } finally {
    deregisterWait(registration);
  }
  dropHubLink(env);
}

/**
 * Wait for the workflow-run pack-push pipeline to go quiet (the same
 * quiescence signal `settleThenDrop` uses) WITHOUT dropping the hub link.
 * Used before a mid-run child SIGKILL so no pack push is mid-flight when
 * the child dies -- killing mid-push risks stranding pack state and
 * flaking the respawn. "Quiet" is `quietMs` with no newly-accepted
 * workflow-run pack (`env.hub.workflowRunPackReceipts`). Carries no deadline
 * unless the caller supplies `timeoutMs`.
 */
export async function settleWorkflowRunPacks(
  env: DeployFlowEnv,
  opts: { quietMs?: number; timeoutMs?: number } = {},
): Promise<void> {
  const quietMs = opts.quietMs ?? 500;
  const { timeoutMs } = opts;
  const registration = registerWait(
    `settleWorkflowRunPacks(quietMs=${String(quietMs)})`,
  );
  try {
    const start = Date.now();
    let lastCount = env.hub.workflowRunPackReceipts.count;
    let lastChange = Date.now();
    for (;;) {
      throwIfEnvTornDown(registration);
      const current = env.hub.workflowRunPackReceipts.count;
      if (current !== lastCount) {
        lastCount = current;
        lastChange = Date.now();
      }
      if (Date.now() - lastChange >= quietMs) return;
      if (timeoutMs !== undefined && Date.now() - start > timeoutMs) {
        throw new Error(
          `settleWorkflowRunPacks: pack stream did not go quiet for ${String(quietMs)}ms within ${String(timeoutMs)}ms\n${env.sidecarDiagnostics()}`,
        );
      }
      await new Promise((r) => setTimeout(r, 50));
    }
  } finally {
    deregisterWait(registration);
  }
}

/**
 * Enumerate the live workflow-process child pids under the sidecar
 * subprocess. The supervisor spawns each child by launching the sidecar's
 * `bin/workflow-child` bun binary; the child is a descendant of the
 * sidecar (`env.sidecar.proc.pid`), identified by `bin/workflow-child` in
 * its argv. The process tree is walked transitively (the sidecar itself is
 * a `bun run` process, so depth is not assumed).
 *
 * Non-throwing by design: returns `[]` when no child is up -- during the
 * respawn backoff gap there is legitimately none, so a poll can watch a
 * child appear or disappear without an empty result being an error.
 */
export function listWorkflowHostChildren(env: DeployFlowEnv): number[] {
  const sidecarPid = env.sidecar.proc.pid;
  if (sidecarPid === undefined) return [];
  // `-o args=` gives the full argv (including the script path) on both
  // darwin and linux; `command`/`comm` are darwin-only / truncated.
  const result = Bun.spawnSync(["ps", "-A", "-o", "pid=,ppid=,args="]);
  const text = new TextDecoder().decode(result.stdout);
  const childrenOf = new Map<number, number[]>();
  const argsOf = new Map<number, string>();
  for (const line of text.split("\n")) {
    const match = /^\s*(\d+)\s+(\d+)\s+(.+)$/.exec(line);
    if (match === null) continue;
    const [, pidStr, ppidStr, args] = match;
    if (pidStr === undefined || ppidStr === undefined || args === undefined) {
      continue;
    }
    const pid = Number.parseInt(pidStr, 10);
    const ppid = Number.parseInt(ppidStr, 10);
    argsOf.set(pid, args);
    const siblings = childrenOf.get(ppid) ?? [];
    siblings.push(pid);
    childrenOf.set(ppid, siblings);
  }
  const found: number[] = [];
  const seen = new Set<number>();
  const queue = [sidecarPid];
  while (queue.length > 0) {
    const current = queue.shift();
    if (current === undefined || seen.has(current)) continue;
    seen.add(current);
    for (const child of childrenOf.get(current) ?? []) {
      queue.push(child);
      const args = argsOf.get(child) ?? "";
      // `workflow-probe-child` does not contain the `workflow-child`
      // substring, but exclude it explicitly so an argv layout change
      // cannot silently target the probe.
      if (
        args.includes("bin/workflow-child") &&
        !args.includes("workflow-probe-child")
      ) {
        found.push(child);
      }
    }
  }
  return found;
}

/**
 * SIGKILL every live workflow-process child under the sidecar and return
 * the killed pids. Throws if none is found: the caller kills a running
 * child on purpose, so a mis-discovery must fail loudly rather than
 * silently no-op. The sidecar process itself is left untouched -- only its
 * child dies, so the in-process supervisor's respawn (not a sidecar
 * restart) is what recovers the deployment.
 */
export function killWorkflowHostChild(env: DeployFlowEnv): number[] {
  const pids = listWorkflowHostChildren(env);
  if (pids.length === 0) {
    throw new Error(
      `killWorkflowHostChild: no live workflow-process child under sidecar pid ${String(env.sidecar.proc.pid)}\n${env.sidecarDiagnostics()}`,
    );
  }
  for (const pid of pids) process.kill(pid, "SIGKILL");
  return pids;
}
