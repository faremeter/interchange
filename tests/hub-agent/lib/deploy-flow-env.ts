// Integration-test fixture for the hub-agent deploy-flow surface: a real hub
// WebSocket server, a mock inference HTTP server, and a real sidecar
// subprocess. The fixture owns the full lifecycle (tempdirs, hub init,
// mock-inference boot, sidecar spawn, stderr drain, teardown).
//
//   let env: DeployFlowEnv;
//   beforeAll(async () => { env = await startDeployFlowEnv(); });
//   afterAll(async () => { await env.teardown(); });
//
// Exposes the hub handle, the inference request capture, the sidecar handle,
// and `sidecarDiagnostics()`, which renders sidecar stderr plus hub
// state-pack receive failures. The shared constants below are exported so
// tests that exercise the same agent across lifecycle steps reuse them.

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
  readWorkflowRunRefTips,
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
  deriveWorkflowRunRepoId,
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

// The production hub-link reconnect backoff (`DEFAULT_RECONNECT_DELAY_MS` in
// `@intx/hub-agent`'s hub-link). A test that needs the real delayed-reconnect
// cycle pins it via
// `sidecarEnv: { SIDECAR_RECONNECT_DELAY_MS: PRODUCTION_RECONNECT_DELAY_MS }`.
// Load-bearing only where the drop interrupts a mid-flight pack push: the
// recovery assumes the disconnect is fully processed before the reconnect
// opens, so a faster reconnect could reopen the link mid-teardown.
export const PRODUCTION_RECONNECT_DELAY_MS = "3000";

// Default reconnect backoff for spawned sidecars: short enough that the
// reconnect-survival suite does not burn 3s per dropped link, long enough that
// a drop still lands as a genuine disconnect.
const TEST_RECONNECT_DELAY_MS = "250";

// Reap grace (SIGTERM, then SIGKILL) so a slow sidecar cannot wedge afterAll.
const SIDECAR_REAP_GRACE_MS = 10_000;
// Reap grace for the sidecar's descendants after SIGKILL. A straggler still
// alive past this is stuck in an uninterruptible kernel state; fail loudly
// rather than leak a ~300MB process into the next test file.
const CHILD_REAP_GRACE_MS = 2_000;
// Second sidecar identity for tests that run two sidecars on one hub; the
// caller spawns it via `startSidecarSubprocess` with these in `extraEnv`.
export const SECOND_SIDECAR_ID = "sc-integration-2";
export const SECOND_TOKEN = "test-token-2";
export const PRIMARY_ALLOCATION_TARGET = {
  allocationId: "allocation-integration-1",
  generation: 1,
} as const;

// ---- In-flight harness waits ----
//
// Every wait helper below registers on entry and deregisters in a `finally`.
// `teardown()` dumps sidecar output when anything is still registered, then
// stops what it found.
//
// An outstanding wait at teardown IS a wedge: when the runner's per-test
// budget lapses, bun abandons the test body's promise but nothing aborts the
// poll loop it was suspended in; left running, the loop polls into an env
// teardown has dismantled. `stopOutstandingWaits` ends such a loop at its
// next iteration instead.
//
// The registry is module-scoped because `waitFor` takes no env; `--no-isolate`
// shares one registry per worker across files, so `currentWaitMark` fences an
// env to registrations made after its creation.

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
 * Throw when the env a wait belongs to was torn down while the wait was in
 * flight. Every poll loop calls this at the top of each iteration so a stopped
 * loop neither reads dismantled state nor returns a result the dismantling
 * produced.
 */
function throwIfEnvTornDown(record: InFlightWait): void {
  if (!record.envTornDown) return;
  throw new Error(
    `deploy-flow env: torn down while ${record.label} was still in flight; ` +
      `the wait was stopped instead of polling on against a dismantled env`,
  );
}

// Cap on a `waitFor` label's predicate source: long enough to distinguish
// predicates, short enough to stay readable.
const PREDICATE_LABEL_MAX_CHARS = 140;

/**
 * Render a `waitFor` predicate for its label. The predicate source text is the
 * only thing that distinguishes one bare `waitFor` call from the next.
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
 * Waits registered at or after `mark` that have not deregistered, one per
 * line, or `null` when there are none.
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
 * Stop every wait registered at or after `mark`: each poll loop throws at its
 * next iteration instead of polling on into a dismantled env. Returns the
 * report for the same mark, taken before the stop. A wait that returned or
 * threw already deregistered, so only abandoned waits remain.
 */
export function stopOutstandingWaits(mark: number): string | null {
  const report = renderOutstandingWaitReport(mark);
  for (const wait of inFlightWaits) {
    if (wait.seq >= mark) wait.envTornDown = true;
  }
  return report;
}

/**
 * Run `fn` under the in-flight registry, for retry loops `waitFor` cannot
 * express: one that performs an action per iteration, or one whose exit is an
 * elapsed window rather than a state. A loop that polls through one of the
 * helpers here is stopped through that helper; one that polls on its own
 * calls `checkTornDown` at the top of each iteration, which throws once
 * teardown starts.
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
 * Poll `predicate` until it is true. No deadline unless the caller supplies
 * `timeoutMs`, so a slow machine makes the wait slower and never fails it; a
 * hang is failed by the lane budget.
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
  // Tool id on assistant `tool_use` blocks, answered id on user `tool_result`
  // blocks; the mock uses it to detect a request that already carries a result.
  id?: string;
  tool_use_id?: string;
  // A `tool_result` block's payload: the Anthropic adapter serializes it as an
  // array of `{ type: "text", text }` blocks, which the mock flattens.
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
 * Opt-in tool-call behavior for the mock inference server: the first request
 * whose `tools` carries `toolName` yields a `tool_use` turn; later requests
 * (carrying the tool_result) get the ordinary text turn.
 */
export type MockToolCall = {
  toolName: string;
  input: Record<string, unknown>;
};

/** One scripted assistant turn: a plain text reply or a single tool call. */
export type ScriptedTurn =
  | { text: string }
  | { toolUse: { name: string; input: Record<string, unknown> } };

export type StartMockInferenceOpts = {
  toolCall?: MockToolCall;
  /**
   * When true, `toolCall` fires on the first turn of EVERY run (any request
   * whose history carries no tool_result) instead of once across the mock's
   * lifetime. Lets one env drive the same tool across several runs.
   */
  toolCallEachRun?: boolean;
  /**
   * When true, the reply echoes the last user message's text as `echo:<text>`
   * so a test can assert the inbound mail body reached inference.
   */
  echoUserMessage?: boolean;
  /**
   * Persistent tool-call behavior for the approval capstone: re-issue the
   * `tool_use` on every request whose history lacks a `tool_result` answering
   * the named tool, and only reply once the result lands (the reply being
   * `${resultPrefix}<result>`). On the broken resume rail no result ever
   * arrives and this loops; on the fixed rail the approved call runs, its
   * result lands, and this reply completes the run.
   */
  approvalToolCall?: {
    toolName: string;
    input: Record<string, unknown>;
    resultPrefix: string;
  };
  /**
   * A fixed, ordered script of assistant turns, dispatched by how many tool
   * results the request's history carries: turn N returns once N results are
   * present. Exhausted scripts fall back to the ordinary text turn. Mutually
   * exclusive with `toolCall`/`approvalToolCall`.
   */
  scriptedTurns?: ScriptedTurn[];
};

/**
 * Recover the last user message's plain text from an Anthropic-style request:
 * `content` is either a bare string or an array of `{ type: "text", text }`
 * blocks; both are flattened.
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
 * Text of the first `tool_result` block in the request's history, or `null`
 * when none is present. Its presence tells the mock the tool has run.
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
 * Count `tool_result` blocks in the history. Each completed tool call appends
 * exactly one, so the count indexes the scripted turns.
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

// Mock inference server: returns a canned Anthropic-style SSE response naming
// the tools it was given, so tests can assert the harness passed the
// deploy-tree tools through to inference. With `opts.toolCall`, the first
// request exposing the named tool returns a `tool_use` turn.
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
      // The adapter encodes tool names for the provider wire charset; decode
      // back to the qualified names the tests assert on.
      for (const tool of body.tools ?? []) {
        tool.name = decodeToolName(tool.name);
      }
      requests.push(body);

      const toolNames = (body.tools ?? []).map((t) => t.name);
      const wantsToolCall =
        opts.toolCall !== undefined &&
        toolNames.includes(opts.toolCall.toolName) &&
        // Default: emit once per mock lifetime. `toolCallEachRun`: emit on any
        // request whose history has no tool_result yet.
        (opts.toolCallEachRun === true
          ? firstToolResultText(body) === null
          : !toolCallEmitted);

      let events: string[];
      const approval = opts.approvalToolCall;
      if (opts.scriptedTurns !== undefined) {
        // Each tool_use gets a distinct id so the child correlates each result
        // to its own call across turns.
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
        // Persistent (no latch): under the broken resume rail no result ever
        // arrives and this loops; under the fixed rail the approved call runs,
        // its result lands, and this reply completes the run.
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
 * Build a plain npm-package tarball (package.json + index.mjs) for serving as
 * a workflow's external dependency from an in-process registry.
 */
export async function buildSyntheticNpmPackageTarball(
  registerTempDir: (dir: string) => void,
  opts: { packageName: string; version: string; moduleSource: string },
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
   * Every delivered `mail.outbound` frame the sidecar forwarded, keyed by the
   * signing sender. `raw` is the full signed MIME as delivered, so a test can
   * read the real headers via `parseHeaderSection`; no durable row is minted.
   */
  outboundMail: {
    senderAddress: string;
    recipients: string[];
    raw: Uint8Array;
  }[];
  hubDataDir: string;
  /**
   * Every server-side `WsHandle` open against this hub, added on `onOpen` and
   * removed on `onClose`. The reconnect helpers force-close these to sever the
   * sidecar's link.
   */
  liveHandles: Set<WsHandle>;
  /**
   * Monotonic count of workflow-run packs the hub accepted, shared by the
   * router callback and the settle helpers (which watch for a quiet window so
   * no pack push is mid-flight when the link drops).
   */
  workflowRunPackReceipts: { count: number };
  /**
   * Opt-in, arm-once mid-pack interrupt for the workflow-run push: the first
   * `refs/heads/main` pack is applied durably, then every live link is dropped
   * BEFORE the ack, so the sidecar's pending transfer rejects and latches
   * "Connection lost". Recorded fields let a test observe that it fired.
   */
  interrupt: {
    armed: boolean;
    interruptedRef: string | null;
    interruptedCommitSha: string | null;
  };
};

// In-process hub WebSocket server wired against a real AgentRepoStore and
// SessionService. It wires no tool-package registry surface; the DB stub
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
  // Live server-side WsHandles, for the reconnect helpers to force-close.
  const liveHandles = new Set<WsHandle>();
  // Mutable box shared by the router callback (bumps) and the settle helpers
  // (read), so the count is always current.
  const workflowRunPackReceipts = { count: 0 };

  // Local so the sender-key lookup closures below narrow away `undefined`.
  const senderKeyResolution = opts.senderKeyResolution;

  // Arm-once mid-pack interrupt state, shared by reference between the pack
  // lookup below and the returned env.
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
    withExecutableWorkflowRun: async (_target, send) => send(),
    requestTimeoutMs: 10_000,
    hubPublicKey: hexEncode(hubSigningKey.publicKey),
    // Verify the sidecar's handshake token against the fixed integration ids.
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
        // Mirror the production fallback branch: catch receive failures and
        // surface them as a structured "corrupt" rejection so a transient does
        // not propagate as an unhandled rejection. No path_violation
        // distinction; this fixture never exercises tree-validator rejection.
        try {
          await agentRepoStore.receiveAgentStatePack(
            { kind: "agent-state", id: agentId },
            pack,
            ref,
            commitSha,
          );
        } catch (err) {
          // Capture the error so sidecarDiagnostics surfaces it on timeouts.
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
        // Interrupted-pack mode: apply durably, then drop every live link
        // before the ack so the sidecar's push rejects and latches "Connection
        // lost". Restricted to the main ref so the claim-check ref keeps
        // flowing.
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
        // Record the receipt so the settle helpers can watch for quiet.
        workflowRunPackReceipts.count += 1;
        return { accepted: true };
      },
      // Capture delivered outbound mail (sender, recipients, raw signed MIME)
      // so a test can read the threading headers; no durable row is minted.
      persistMail({ senderAddress, recipients, raw }) {
        outboundMail.push({ senderAddress, recipients, raw });
        return Promise.resolve([]);
      },
      // The hub's own history tips, as in production.
      readWorkflowRunRefTips: (agentAddress) =>
        readWorkflowRunRefTips(agentRepoStore.repoStore, agentAddress),
      // Only the approval capstone wires this; otherwise the handler drops the
      // frame with a warning.
      ...(opts.registerSignalCorrelation !== undefined
        ? { registerSignalCorrelation: opts.registerSignalCorrelation }
        : {}),
      // Only the federated-mail capstone supplies it; otherwise mail routes
      // without grant materialization.
      ...(opts.materializeMailTriggeredRunGrants !== undefined
        ? {
            materializeMailTriggeredRunGrants:
              opts.materializeMailTriggeredRunGrants,
          }
        : {}),
      // Resolve a signed sender's durable key so the materializer path
      // co-delivers it, as production wires it (apps/hub/src/server.ts);
      // without it the mail resolves `unknown` and strict enforcement drops
      // it. Only wired when a test supplies its db + principal key store.
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
  // Production stamps the hub-approved wire hash on deploy frames before they
  // reach the sidecar, so no harness-side stamping is needed here.
  router.events.on("agent.event", ({ agentAddress, sessionId, event }) => {
    agentEvents.push({ addr: agentAddress, sid: sessionId, event });
  });
  router.events.on("agent.deploy.ack", ({ agentAddress, publicKey }) => {
    deployAcks.set(agentAddress, publicKey);
  });

  // DB stub for the narrow surface the deploy tests exercise: a tenant lookup
  // and the session_asset audit writes. Other members throw so the test fails
  // loudly if production drifts into a dependency the stub does not cover.
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
      // Update the allocation identity so routed operations bind to the
      // deployment under test.
      Object.assign(primaryIdentity, {
        anchorRunId,
        workflowRunAddress: address,
      });
    },
    prepareAllocationIdentity(anchorRunId, address, sidecarId) {
      // The fallback infers the sidecar from which one is connected, which is
      // a fact about timing rather than intent: a reconnecting primary rebinds
      // ITS identity to `address`, so a deployment meant for the secondary
      // would land on the primary. A two-sidecar caller must say which it
      // means.
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
   * The env the subprocess was spawned with (the object handed to
   * `Bun.spawn`), which `assertPinnedSidecarEnvReached` checks pins against.
   */
  env: Readonly<Record<string, string | undefined>>;
};

// The env the fixture hands a spawned sidecar, separated from the spawn so a
// test can read it without paying for a subprocess. `extraEnv` is written last
// and may override any fixture-owned key.
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
    // Fixed test key so the sidecar boots with a real cipher and e2e exercises
    // the at-rest credential sealing rather than a noop.
    SIDECAR_CREDENTIAL_ENCRYPTION_KEY:
      "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
    // Fast backoff; a test that pins the production delay overrides it via
    // `sidecarEnv`.
    SIDECAR_RECONNECT_DELAY_MS: TEST_RECONNECT_DELAY_MS,
    ...(opts.extraEnv ?? {}),
  };
}

// Spawn a real sidecar subprocess pointed at the supplied hub. `extraEnv` is
// merged into the process env after the standard variables; callers use it to
// inject opt-in flags (e.g. `SIDECAR_WORKFLOW_RUN_SHADOW`).
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

  // Drain stderr (and stdout) into a rolling buffer for diagnostics.
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
 * A caller that passes `sidecarEnv` claims the subprocess runs with those
 * variables; nothing downstream re-states the claim, so a break in the
 * `sidecarEnv` -> `extraEnv` -> spawned-env chain leaves the subprocess on a
 * fixture default that looks deliberate -- e.g. a reconnect pin could silently
 * run at the short test delay. Keyed off the caller's own variables, so a
 * caller that passes no `sidecarEnv` has no keys and cannot fire it.
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
 * Live PIDs in the process subtree rooted at `rootPid`, excluding the root.
 * Walked transitively over `ps -A -o pid=,ppid=` so no depth is assumed: every
 * descendant of the sidecar is a process the test owns.
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
 * reap its process subtree.
 *
 * The sidecar installs no SIGTERM handler, so under contention a plain SIGTERM
 * can leave `proc.exited` unresolved; the bound keeps that from wedging
 * `afterAll`, and the exit must complete before the caller removes the data
 * directory (an rm racing a subprocess that still holds file handles surfaces
 * EBUSY/EACCES). Descendants are snapshotted BEFORE the kill: once the sidecar
 * exits, the kernel re-parents them to init and they can no longer be
 * attributed to it, so orphaned workflow-process children (~300MB each) would
 * leak into the next test file.
 *
 * Safe to call twice on the same handle: `proc.kill()` is a no-op and
 * `proc.exited` is resolved on a finished subprocess.
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
 * Per-deployment handle tracked by the env, populated by
 * `deployWorkflowSourceForTest` and consulted by the read/signal helpers so
 * tests never thread the workflow-run repo identity themselves.
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
   * failing wait. Covers every registered sidecar.
   */
  sidecarDiagnostics: () => string;
  /** Per-deployment handles populated by `deployWorkflowSourceForTest`. */
  deployments: Map<string, DeploymentHandle>;
  /**
   * Register an externally-constructed deployment handle (one driven outside
   * `deployWorkflowSourceForTest`) so the helpers can resolve it by
   * `anchorRunId`.
   */
  registerDeployment(handle: DeploymentHandle): void;
  /**
   * Register a sidecar the test spawned itself so `sidecarDiagnostics`
   * reports its output. Diagnostics only: `teardown()` terminates the
   * primary sidecar and no other.
   */
  registerSidecar(handle: SidecarHandle): void;
  /** Run a retry loop under the in-flight wait registry. See `retrying`. */
  retrying: typeof retrying;
  teardown: () => Promise<void>;
};

export type StartDeployFlowEnvOpts = {
  /**
   * Extra env vars written last into the sidecar env; wins over every
   * fixture-owned key. Each variable named here is verified against the
   * spawned env, so a value that does not arrive throws. See
   * `assertPinnedSidecarEnvReached`.
   */
  sidecarEnv?: Record<string, string>;
  /** Tool-call behavior for the mock inference server. See `MockToolCall`. */
  inferenceToolCall?: MockToolCall;
  /** When true, `inferenceToolCall` drives the tool on every run. */
  inferenceToolCallEachRun?: boolean;
  /**
   * Persistent tool-call behavior for the approval capstone. See
   * `StartMockInferenceOpts.approvalToolCall`.
   */
  inferenceApprovalToolCall?: StartMockInferenceOpts["approvalToolCall"];
  /** Fixed, ordered script of assistant turns. See `StartMockInferenceOpts.scriptedTurns`. */
  inferenceScriptedTurns?: ScriptedTurn[];
  /** When true, the mock echoes the last user message as `echo:<text>`. */
  inferenceEchoUserMessage?: boolean;
  /**
   * Co-write hook for the `signal.correlation.register` frame a suspending
   * step emits. Only the approval capstone supplies it; otherwise the frame
   * is dropped with a warning.
   */
  registerSignalCorrelation?: SidecarLookups["registerSignalCorrelation"];
  /**
   * Materializer for a mail-triggered run's grants, called by
   * `deliverMailToRecipient` to stage the receiving run's grants before
   * forwarding the mail. Only the federated-mail capstone supplies it;
   * otherwise mail routes without materialization.
   */
  materializeMailTriggeredRunGrants?: SidecarLookups["materializeMailTriggeredRunGrants"];
  /**
   * A db + principal key store for resolving a signed sender's durable public
   * key, wired as `resolveSenderKey` / `resolveSenderKeyStrict` exactly as
   * production does (apps/hub/src/server.ts). When set, the materializer path
   * co-delivers the key on the grants barrier so a same-hub sender verifies
   * `clean` instead of the `unknown` strict enforcement drops. Only the
   * federated-mail capstone supplies it.
   */
  senderKeyResolution?: {
    db: DBExecutor;
    principalKeyStore: PrincipalKeyStore;
  };
};

// Compose the full env: hub server, mock inference, sidecar subprocess. Owns
// every tempdir and tears them all down in `teardown()`. Returns once the
// sidecar has registered with the hub.
export async function startDeployFlowEnv(
  opts: StartDeployFlowEnvOpts = {},
): Promise<DeployFlowEnv> {
  // Fence this env's waits from earlier ones in the same worker.
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

  // Check the caller's pin against what the process actually got, after the
  // spawn, so a break at either wiring hop is caught rather than reproduced.
  if (opts.sidecarEnv !== undefined) {
    assertPinnedSidecarEnvReached(opts.sidecarEnv, sidecar.env);
  }

  // Primary sidecar plus every registered handle, in spawn order, so
  // `sidecarDiagnostics` covers a restart test's replacement process.
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
    // A wedged test is ended by the runner's budget with nothing printed, so
    // this hook (run by `afterAll`) is the remaining place to report what the
    // sidecar said. The registry is the gate: a wedged test leaves its helper
    // registered, a clean test leaves nothing, so a clean run prints nothing.
    const outstanding = stopOutstandingWaits(waitMark);
    if (outstanding !== null) {
      const diagnostics = sidecarDiagnostics();
      process.stderr.write(
        `\n${outstanding}\n${diagnostics.length > 0 ? `${diagnostics}\n` : ""}`,
      );
    }
    // Close every live hub-side handle, then terminate the sidecar and reap
    // its subtree BEFORE removing its data dir (an rm racing a subprocess
    // still holding file handles surfaces EBUSY/EACCES). Server stops are
    // bounded because a dropped link can leave Bun a phantom connection its
    // `server.stop` would wait on forever.
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
// Helpers shared by the Phase I end-to-end tests. Each composes against the
// real production paths in `@intx/workflow-deploy`, `@intx/workflow-host`,
// and the workflow-run kind handler in `@intx/hub-sessions`; none reach into
// stubs.

const DEFAULT_DEPLOYMENT_DOMAIN = "integration.interchange";
const DEFAULT_WORKFLOW_RUN_REF = "refs/heads/main";

/**
 * A deployment's anchor id plus the workflow-run repo identity the other
 * helpers consult; returned (extended) by `deployWorkflowSourceForTest`.
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
   * when a test runs two sidecars; omitted, the identity is inferred from
   * which sidecar is connected, which races a reconnect.
   */
  sidecarId?: string;
  /** The source entry module text to bundle; deployed BY SOURCE-REF. */
  entryModule: string;
  /** The path inside the source package that exports `workflow`. */
  entry?: string;
  /** The `interchange.loops` module path, for fixtures that ship loop fns. */
  loops?: string;
  /** The `interchange.actions` module path, for fixtures that ship handlers. */
  actions?: string;
  /** Extra source files seeded alongside the bundle under the source asset. */
  extraSourceFiles?: Record<string, string>;

  /** The real test DB: the install/approve freeze and anchor insert write through it. */
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
   * Per-step inference sources. Omit to compute them the way production does
   * (`buildInertProjectionStepSources`, which recurses into loop bodies) so an
   * omitting fixture exercises the real source-pin path.
   */
  sources?: Record<string, InferenceSource[]>;

  /** Credential delivery for a credential-consuming fixture. */
  credentialCipher?: CredentialCipher;
};

/**
 * Handle returned by `deployWorkflowSourceForTest`. Carries the same fields as
 * `DeployWorkflowHandle` plus the frozen approve result and the deploy's
 * public key.
 */
export type DeployWorkflowSourceForTestHandle = DeployWorkflowHandle & {
  approved: InstallAndApproveResult;
  publicKey: string;
};

/**
 * Deploy a workflow BY SOURCE-REF against the env's hub: bundle the entry,
 * seed it as a `workflow`-kind source asset, install + probe + gate + freeze
 * against the real DB, then emit the source-ref deploy frame and write the
 * anchor `workflow_run` row. Registers the handle so the Phase I helpers
 * resolve it by `anchorRunId`. The caller owns the DB lifecycle
 * (`createTestDb`) and seeds the tenant/principal/definition asset in its own
 * `beforeAll`.
 */
// `credential.providerId` needs a real provider row, so seed one per tenant.
const TEST_INFERENCE_PROVIDER_PREFIX = "prov-test-inference-";

/**
 * Seed a tenant-owned credential for each inference-source `credentialId` the
 * deploy references (top-level `sources` and the `config` pool). The
 * pre-register deploy resolves each id through the credential table, so an
 * unseeded id fails the deploy closed. The mock ignores the secret, so a
 * stable placeholder suffices; `onConflictDoNothing` keeps repeated deploys
 * idempotent.
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
      // Material resolution pins the origin to the provider's API base URL and
      // fails closed on null, so seed a concrete one.
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
  // Reap terminal deployments' parked workflow-process children (~330MB each)
  // via the production `agent.undeploy` path; see `reapTerminalDeployments`.
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

  // Bundle to a self-contained `.mjs` in a throwaway scratch dir (only needed
  // for the Bun.build input), then seed it as raw source under the asset.
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

  // Deliver the source asset's git pack on the frame for the sidecar to index.
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

  // Compute sources the way production does when the fixture does not override
  // them, exercising the real source pin (including loop-body recursion).
  const sources =
    opts.sources ??
    buildInertProjectionStepSources({
      projection: approved.projection,
      config: opts.config,
      operatorApprovals: approved.approval.approvedSurface,
    });

  // Seed a credential per referenced credentialId so the deploy resolves them
  // into the unified material cell.
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
    // Seeded secrets are plaintext, so default to the noop cipher unless a test
    // supplies its own.
    credentialCipher: opts.credentialCipher ?? createNoopCredentialCipher(),
  });

  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: deriveWorkflowRunRepoId(agentAddress),
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
// A workflow-process child (~330MB) stays alive once its deployment's run
// completes -- the sidecar keeps deployments warm until an undeploy or the
// sidecar exits, and the fixture only tears the sidecar down in afterAll -- so
// a file that deploys several workflows accumulates one parked child per
// completed deployment, setting the lane's peak RSS.
//
// When a NEW deployment is registered, every previously-completed deployment
// is provably done (each test deploys its own workflow and no test re-triggers
// a completed one), so the fixture reaps it through the production
// `agent.undeploy` wire path, fire-and-forget so the next deploy is never
// delayed; a failed undeploy leaves the child parked until teardown. Terminal
// marking happens only in the read helpers below, so an externally-registered
// handle that is never read is never reaped.
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
 * Read every event under `runs/<runId>/events/` in ascending `seq` order.
 * Empty when the run has committed nothing or the repo does not exist yet.
 * Delegates to the shared hub-side `WorkflowRunReader`.
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
  // A terminal last event (the log is seq-ordered) means the run is finished;
  // mark it so the next deploy reaps the parked child.
  const last = events.at(-1);
  if (last !== undefined && WORKFLOW_RUN_TERMINAL_TYPES.has(last.type)) {
    terminalDeployments.set(handle, true);
  }
  return events;
}

/**
 * Options for `waitForWorkflowRunComplete`, mirroring `waitFor`.
 */
export type WaitForWorkflowRunCompleteOpts = {
  /**
   * Bound on the wait. Omitted, the helper polls until the terminal event
   * lands and never throws; a caller that must distinguish "not terminal yet"
   * from a fault discriminates on `isWorkflowRunCompleteTimeout`.
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
 * True only for that timeout. A caller that retries the wait must tell "no
 * terminal event yet, read again" apart from a real fault, so it surfaces the
 * fault instead of retrying through it. Discriminate on this, never on the
 * error's message.
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
 * Options for `fireMailTrigger`. `messageId` defaults to a synthesized id so a
 * caller can supply distinct ids per call without colliding on the dedup index.
 */
export type FireMailTriggerOpts = {
  /** RFC 2822 `Message-Id` of the synthesized mail. */
  messageId?: string;
  /** Mail body (conversation text). Defaults to a placeholder. */
  content?: string;
  /** Sender address. Defaults to a test-stable user address. */
  from?: string;
  /**
   * Per-run grants delivered ahead of the trigger mail, mirroring the
   * production route: the `run.grants` frame lands before dispatch. Defaults
   * to an empty set, which still materializes `grants.json` so the
   * supervisor's `onRunStart` barrier resolves it rather than failing closed.
   */
  grants?: WireGrantRule[];
  /** Attachments MIME-encoded into the signed message, as the production route does. */
  attachments?: MessageAttachment[];
  /**
   * RFC 2822 `In-Reply-To` header, set to a prior message's `Message-Id` to
   * thread this inbound onto an existing conversation. Omitted by default.
   */
  inReplyTo?: string;
  /**
   * RFC 2822 `References` header (a message-id chain); the router also treats
   * an inbound as a continuation when it includes the thread root.
   */
  references?: string[];
};

/**
 * `code` marker on the errors `fireMailTrigger` throws when `sendRunGrants`
 * or `routeMail` returned false -- the address had neither a live connection
 * nor a disconnect queue to ride.
 */
export const MAIL_TRIGGER_UNROUTABLE_CODE = "mail_trigger_unroutable";

/** The unroutable-address failure `fireMailTrigger` throws, carrying its marker. */
export interface MailTriggerUnroutableError extends Error {
  readonly code: typeof MAIL_TRIGGER_UNROUTABLE_CODE;
}

/**
 * True only for that failure. A caller that re-fires across a reconnect must
 * tell "not routable yet, fire again" apart from a real fault, so it surfaces
 * the fault instead of re-firing through it. Discriminate on this, never on
 * the error's message.
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
 * Construct a signed mail message and route it via the hub's `routeMail` path.
 * Returns the `Message-Id` the helper chose so the caller can correlate the
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
  // part of the mail address; derive it through the same shared helper
  // production uses so the fixture cannot mask a divergence.
  const runId = deriveWorkflowRunId(address);
  // Co-deliver the signer's public key on the grants barrier, as the HTTP
  // trigger route does, so the recipient's admission verdict is clean rather
  // than the cache-miss `unknown` strict enforcement rejects.
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

  // Route the trigger as the production route does, stamping the signed-under
  // address as the authenticated sender (same address as the MIME From).
  const delivered = await env.hub.router.routeMail(address, base64, from);
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
 * Deliver a workflow-run signal through the production hub → sidecar →
 * supervisor → workflow-process child pipeline. The child commits the
 * resulting `SignalReceived` event through its own substrate -- the single
 * writer of the workflow-run repo on the sidecar side -- so the pack-push
 * pipeline never sees a concurrent writer at the workflow-run ref.
 *
 * The returned `signalId` is the value the producer minted; the state
 * machine's `observedSignalIds` dedup key matches against it.
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
  await env.hub.router.sendSignalDeliver({
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
   * Wire `deadlineMs` on the drain control frame, defaulting to the
   * supervisor's `DEFAULT_DRAIN_TIMEOUT_MS` (5_000).
   */
  deadlineMs?: number;
};

/**
 * Send a workflow-host drain control payload through the production
 * hub -> sidecar -> supervisor -> workflow-process child pipeline. Cancel-mode
 * in-flight steps abort as the controller signal flips; wait-mode steps
 * continue. Each accumulator commits a signed
 * `CancelRequested{origin: "supervisor-drain"}` when the deadline expires.
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
 * Land a `processing/<receivedAt>-<messageId>.json` entry in the deployment's
 * workflow-run repo, composing `enqueueInbox` + `dequeueToProcessing` -- the
 * same primitives the supervisor uses on a mail trigger -- so the state is
 * bit-identical to a supervisor crash after the dequeue commit but before
 * `markConsumed`. A direct write would be rejected by `validatePush`, which
 * requires the transition to be backed by a prior inbox entry.
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
 * Enumerate the run ids under `runs/` in the workflow-run repo's
 * `refs/heads/main`. Empty when the repo or ref does not exist yet; real
 * errors propagate so the caller sees the failure rather than "no runs yet".
 */
export async function listRunIds(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
): Promise<string[]> {
  const reader = createWorkflowRunReader(env.hub.agentRepoStore.repoStore);
  return reader.listRunIds(workflowRunRepoId, DEFAULT_WORKFLOW_RUN_REF);
}

/**
 * Read every blob under a claim-check sub-directory of the workflow-run repo,
 * against `refs/heads/events` (the substrate's claim-check ref). Empty when
 * the repo, ref, address subtree, or sub-directory does not exist yet; other
 * failures propagate.
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
 * Poll until at least one run id is present under `runs/` and return the
 * first one found. Used by tests that don't know the runId upfront because
 * the supervisor mints it.
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
// Drive the sidecar's hub WebSocket through a drop and its automatic reconnect
// so a survival test can assert a deployed workflow keeps running across it.
// The in-process hub is normally lossless; `startHub` captures every live
// server-side `WsHandle` (`env.hub.liveHandles`) for the helpers to close.

/**
 * Force-close every live server-side hub WebSocket, severing the sidecar's
 * link and starting its reconnect cycle. Throws if no handle is live, so a
 * test that expected an established link fails loudly. This is the raw drop:
 * it may sever the link mid-pack-push, which the interrupted-pack regression
 * test wants; survival tests that must not race an in-flight push should use
 * `settleThenDrop`.
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
 * Poll until `address` is routable on the hub again. An address only re-enters
 * the routing index after its reconnect passes identity revalidation and the
 * allocation-generation fence, so "routable again" is a sound proxy for
 * completed reconnect registration.
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
   * No-new-pack quiet window before the drop fires. Defaults to `500`; the
   * helper treats a quiet window as the pack-push pipeline having drained.
   */
  quietMs?: number;
  /**
   * Ceiling on the settle wait. Omitted, the helper waits however long the
   * pack stream takes; supplied, it throws instead of dropping into an
   * in-flight push.
   */
  timeoutMs?: number;
};

/**
 * Wait for the workflow-run pack-push pipeline to go quiet, then drop the hub
 * link. "Quiet" is `quietMs` with no newly-accepted pack
 * (`env.hub.workflowRunPackReceipts`), the hub-side proxy for the sidecar's
 * pipeline having drained (its `flushWorkflowRunPushes` cannot be awaited
 * cross-process). This is the default survival-test drop; use `dropHubLink`
 * when an interrupted push is the thing under test.
 *
 * `address` is accepted for symmetry; the quiescence signal is hub-wide, which
 * equals per-deployment for the single-link survival tests.
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
 * Wait for the pack-push pipeline to go quiet (the same signal `settleThenDrop`
 * uses) WITHOUT dropping the link. Used before a mid-run child SIGKILL so no
 * pack push is mid-flight when the child dies -- killing mid-push risks
 * stranding pack state and flaking the respawn.
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
 * Live workflow-process child pids under the sidecar subprocess, identified by
 * the `bin/workflow-child` path in their argv and found by a transitive walk of
 * the sidecar's process tree.
 * Returns `[]` when no child is up, which is legitimate during the respawn
 * backoff gap.
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
      // Exclude the probe explicitly so an argv layout change cannot silently
      // target it.
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
 * SIGKILL every live workflow-process child under the sidecar and return the
 * killed pids. Throws if none is found: the caller kills a running child on
 * purpose, so a mis-discovery must fail loudly. Only the child dies, so the
 * in-process supervisor's respawn recovers the deployment.
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
