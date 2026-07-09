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
// surfaces sidecar stderr and hub state-pack receive failures on `waitFor`
// timeouts.
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
  bridgeOrchestratorDeployContent,
  createAgentRepoStore,
  createSessionService,
  createSidecarRouter,
  createWorkflowRunReader,
  dequeueToProcessing,
  enqueueInbox,
  DEFAULT_ASSET_REF,
  parseAgentId,
  type AgentRepoStore,
  type AssetService,
  type RepoId,
  type SidecarRouter,
  type SessionService,
  type WorkflowRunEvent,
  type WorkflowRunHubPrincipal,
  type WsHandle,
} from "@intx/hub-sessions";
import { base64Encode, hexEncode } from "@intx/types";
import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import {
  createWorkflowDeployOrchestrator,
  type LaunchSessionFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { createDefaultDirectorRegistry } from "@intx/agent";
import { decodeToolName } from "@intx/inference";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { ApprovalSet } from "@intx/workflow-deploy";
import type { WorkflowDefinition } from "@intx/workflow";

export const AGENT_ADDRESS = "ins_test-agent@integration.interchange";
export const AGENT_ID = "ins_test-agent";
export const SESSION_ID = "ses_integration-1";
export const SIDECAR_ID = "sc-integration-1";
export const TOKEN = "test-token";

const TENANT_ID = "tenant-1";
const REGISTRY_NAME = "workspace-builtins";
const ASSET_ID = `ast_${REGISTRY_NAME.replace(/-/g, "_")}`;
const TARBALL_FILENAME = "tools-mail-0.1.2.tgz";

export { TENANT_ID, REGISTRY_NAME, ASSET_ID, TARBALL_FILENAME };

export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<void> {
  const { timeoutMs = 10_000, diagnostics } = opts;
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      throw new Error(`waitFor timed out after ${timeoutMs}ms${ctx}`);
    }
    await new Promise((r) => setTimeout(r, 50));
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

export type StartMockInferenceOpts = {
  toolCall?: MockToolCall;
  /**
   * When true, the assistant reply echoes the last user message's text
   * as `echo:<text>` instead of the tool-names text turn. This lets a
   * test assert the agent's `agent.send` actually received the inbound
   * mail body (the body reaches inference as the user turn, so the echo
   * reflects it). Mutually exclusive in spirit with `toolCall`, which
   * drives a different reply shape.
   */
  echoUserMessage?: boolean;
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

  const toolUseTurn = (call: MockToolCall): string[] => [
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
        id: "toolu_mock_1",
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
        !toolCallEmitted &&
        toolNames.includes(opts.toolCall.toolName);

      let events: string[];
      if (wantsToolCall && opts.toolCall !== undefined) {
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

// Synthetic @intx/tools-mail tarball
//
// The integration test's loader path is unmodified production code; it
// imports the tarball's `interchange.tools` entry as a real ESM module.
// The tarball ships a minimal `sidecar-bundle.js` that exports an
// AnnotatedToolFactory with the same `id` shape as the real bundle
// (`@intx/tools-mail/sidecar-bundle`) and a single `mail_send` definition.
// The loader prefixes the definition name with the bundle id to yield the
// `@intx/tools-mail/sidecar-bundle:mail_send` tool the model ends up
// seeing.
export async function buildSyntheticToolsMailTarball(
  registerTempDir: (dir: string) => void,
  opts: { transportBacked?: boolean } = {},
): Promise<Uint8Array> {
  const stagingDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "tools-mail-fixture-"),
  );
  registerTempDir(stagingDir);
  const packageDir = path.join(stagingDir, "package");
  await fs.promises.mkdir(packageDir, { recursive: true });

  await fs.promises.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: "@intx/tools-mail",
      version: "0.1.2",
      type: "module",
      interchange: { tools: "./sidecar-bundle.js" },
    }),
  );

  // Two bundle shapes, selected by `opts.transportBacked`:
  //
  // Default (filesystem-only): the tool's `run` writes a sentinel file
  // into the agent's `env.workdir` so a spawned-child test can prove the
  // tool executed IN THE CHILD by observing the file in the per-step
  // workspace. It declares `requires: []` and never touches the agent's
  // transport. Tests that never drive the model to call the tool never
  // invoke `run`, so the write is inert for them.
  //
  // Transport-backed (the 4.6 milestone's OUTBOUND proof): the tool calls
  // `env.transport.send(...)` -- the SAME supervisor-backed transport the
  // unified child wires for a step agent -- so the call exercises the
  // real OUTBOUND chain (mail tool -> supervisor-backed transport ->
  // outbound bridge -> `outbound.message` IPC -> supervisor `sendOutbound`
  // -> host transport signed send -> `SendReceipt`). It declares
  // `requires: ["transport", "address"]` so the loader populates those
  // env slots, sends to the `to` argument, and writes the sentinel ONLY
  // after a successful receipt. A broken outbound path rejects inside
  // `send`, so no sentinel is written and the run fails -- making the
  // sentinel a load-bearing proof of the signed-outbound composition.
  const bundleSource = opts.transportBacked
    ? `
import fs from "node:fs";
import path from "node:path";
const factory = (env) => ({
  definitions: [
    {
      name: "mail_send",
      description: "Send a mail message",
      inputSchema: {
        type: "object",
        properties: { to: { type: "string" }, body: { type: "string" } },
        required: ["to", "body"],
      },
    },
  ],
  run: async (call, signal) => {
    const args = call.arguments ?? {};
    const to = typeof args.to === "string" ? args.to : env.address;
    const filename = typeof args.body === "string" ? args.body : "tool-ran.txt";
    const receipt = await env.transport.send({
      to,
      type: "conversation.message",
      content: "Reply produced by the unified-host step agent.",
    }, signal);
    await fs.promises.mkdir(env.workdir, { recursive: true });
    await fs.promises.writeFile(
      path.join(env.workdir, filename),
      JSON.stringify({ messageId: receipt.messageId, status: receipt.status }),
    );
    return { callId: call.id, content: "sent " + receipt.messageId };
  },
});
export const mail = Object.assign(factory, {
  id: "@intx/tools-mail/sidecar-bundle",
  requires: ["transport", "address"],
});
`.trimStart()
    : `
import fs from "node:fs";
import path from "node:path";
const factory = (env) => ({
  definitions: [
    {
      name: "mail_send",
      description: "Send a mail message",
      inputSchema: {
        type: "object",
        properties: { to: { type: "string" }, body: { type: "string" } },
        required: ["to", "body"],
      },
    },
  ],
  run: async (call, signal) => {
    const args = call.arguments ?? {};
    const filename = typeof args.body === "string" ? args.body : "tool-ran.txt";
    const content = typeof args.to === "string" ? args.to : "ok";
    await fs.promises.mkdir(env.workdir, { recursive: true });
    await fs.promises.writeFile(path.join(env.workdir, filename), content);
    return { callId: call.id, content: "wrote " + filename };
  },
});
export const mail = Object.assign(factory, {
  id: "@intx/tools-mail/sidecar-bundle",
  requires: [],
});
`.trimStart();
  await fs.promises.writeFile(
    path.join(packageDir, "sidecar-bundle.js"),
    bundleSource,
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
  router: SidecarRouter;
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
   */
  outboundMail: { senderAddress: string; recipients: string[] }[];
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
};

// Hub WebSocket server (in-process) wired against a real AgentRepoStore
// and SessionService.
//
// The hub seeds a `package-registry` asset repo with a synthetic
// `@intx/tools-mail@0.1.2` tarball so the session-service tool-package
// resolver path exercises the real registry walker end-to-end.
export async function startHub(
  registerTempDir: (dir: string) => void,
  opts: { transportBackedMailTool?: boolean } = {},
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

  const hubDataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "hub-data-"),
  );
  registerTempDir(hubDataDir);

  const hubSigningKey = await generateKeyPair();
  const agentRepoStore = createAgentRepoStore({
    dataDir: hubDataDir,
    signingKey: hubSigningKey,
  });

  const router = createSidecarRouter({
    requestTimeoutMs: 10_000,
    hubPublicKey: hexEncode(hubSigningKey.publicKey),
    lookups: {
      // Answer a reconnecting sidecar's ownership challenge for a deployment
      // address with the Ed25519 key that address acked at deploy time.
      // Without this, `handleReconnect` hits its `lookupKey === undefined`
      // guard and closes the socket every attempt, so the sidecar loops in a
      // 3s reconnect cycle and never re-enters routing. Every deployment acks
      // its own key (captured into `deployAcks` by the `agent.deploy.ack`
      // listener below), and the sidecar re-signs the reconnect challenge
      // with that same key, so a `deployAcks` lookup is the correct oracle.
      async lookupPublicKey(agentAddress) {
        return deployAcks.get(agentAddress) ?? null;
      },
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
      // persistence. Recording the signing sender is enough for the
      // integration assertions; no durable row is minted, so this returns
      // an empty result set.
      persistMail({ senderAddress, recipients }) {
        outboundMail.push({ senderAddress, recipients });
        return Promise.resolve([]);
      },
    },
  });
  router.events.on("agent.event", ({ agentAddress, sessionId, event }) => {
    agentEvents.push({ addr: agentAddress, sid: sessionId, event });
  });
  router.events.on("agent.deploy.ack", ({ agentAddress, publicKey }) => {
    deployAcks.set(agentAddress, publicKey);
  });

  const tarballBytes = await buildSyntheticToolsMailTarball(registerTempDir, {
    ...(opts.transportBackedMailTool === true ? { transportBacked: true } : {}),
  });
  await agentRepoStore.repoStore.initRepo({
    kind: "package-registry",
    id: ASSET_ID,
  });
  await agentRepoStore.repoStore.writeTree(
    { kind: "hub" },
    { kind: "package-registry", id: ASSET_ID },
    DEFAULT_ASSET_REF,
    {
      files: {
        [`tarballs/${TARBALL_FILENAME}`]: tarballBytes,
      },
      message: "Seed tools-mail tarball",
    },
  );

  // The AssetService and DB stubs satisfy the narrow surface that the
  // session-service tool-package path actually consults: blob reads for
  // the resolver, tenant-walk and asset list for the registry map build,
  // and a session_asset insert/delete for audit. The other members of the
  // AssetService and DB interfaces throw on access so the test fails
  // loudly if the production code drifts into a dependency the stub does
  // not cover.
  const assetRow = {
    id: ASSET_ID,
    tenantId: TENANT_ID,
    kind: "package-registry" as const,
    name: REGISTRY_NAME,
    displayName: null,
    creatorPrincipalId: null,
    createdAt: new Date(),
    updatedAt: new Date(),
  };
  const assetService: AssetService = {
    createAsset: () => {
      throw new Error("deploy-flow: AssetService.createAsset not used");
    },
    populateAsset: () => {
      throw new Error("deploy-flow: AssetService.populateAsset not used");
    },
    attachAsset: () => {
      throw new Error("deploy-flow: AssetService.attachAsset not used");
    },
    listAgentAssets: async (_agentId: string) => [],
    readAssetBlob: async ({ assetId, path: p }) => {
      if (assetId !== ASSET_ID) {
        throw new Error(`deploy-flow: unexpected readAssetBlob ${assetId}`);
      }
      if (p !== `tarballs/${TARBALL_FILENAME}`) {
        throw new Error(`deploy-flow: unexpected blob path ${p}`);
      }
      return tarballBytes;
    },
    listAssetBlobs: async ({ assetId, dir: d }) => {
      if (assetId !== ASSET_ID) {
        throw new Error(`deploy-flow: unexpected listAssetBlobs ${assetId}`);
      }
      if (d !== "tarballs") {
        throw new Error(`deploy-flow: unexpected list dir ${d}`);
      }
      return [TARBALL_FILENAME];
    },
  };
  const fakeDb = {
    query: {
      tenant: {
        findFirst: async (_args: unknown) =>
          ({ parentId: null }) as { parentId: string | null },
      },
      asset: {
        findMany: async (_args: unknown) => [assetRow],
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
    agentRepoStore,
    assetService,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the stub satisfies the narrow surface the session-service tool-package path actually calls (query.tenant.findFirst, query.asset.findMany, insert/delete), but cannot structurally satisfy the full drizzle PgDatabase type
    db: fakeDb as unknown as NonNullable<
      Parameters<typeof createSessionService>[0]["db"]
    >,
    toolPackageRegistries: {
      httpRegistries: new Map(),
      defaultRegistry: REGISTRY_NAME,
      scopeRouting: [{ scope: "@intx", registry: REGISTRY_NAME }],
    },
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
  };
}

export type SidecarHandle = {
  proc: Subprocess;
  dataDir: string;
  /** Rolling stderr buffer; capped at 500 chunks. */
  stderr: readonly string[];
};

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

  // `extraEnv` is written last and may override any key the fixture
  // sets. Callers use it to inject opt-in flags, but the override
  // contract is uniform across every fixture-owned key so a future
  // caller can also point the sidecar at a different hub or data
  // directory without the fixture silently winning.
  const env: Record<string, string | undefined> = {
    PATH: process.env["PATH"],
    HOME: process.env["HOME"],
    TMPDIR: process.env["TMPDIR"],
    HUB_WS_URL: `ws://localhost:${String(hubPort)}/ws`,
    SIDECAR_ID,
    SIDECAR_TOKEN: TOKEN,
    SIDECAR_DATA_DIR: dataDir,
    ...(opts.extraEnv ?? {}),
  };

  const proc = Bun.spawn(["bun", "run", "apps/sidecar/src/index.ts"], {
    cwd: path.resolve(import.meta.dir, "../../.."),
    env,
    stdout: "pipe",
    stderr: "pipe",
  });

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

  return { proc, dataDir, stderr };
}

/**
 * Per-deployment handle tracked by the env. Populated by
 * `deployWorkflow`; consulted by `readWorkflowRunEvents`,
 * `waitForWorkflowRunComplete`, `injectSignal`, and
 * `simulateProcessingCrash` so the Phase I integration tests never
 * thread the workflow-run repo identity themselves.
 */
export type DeploymentHandle = {
  deploymentId: string;
  workflowDefinition: WorkflowDefinition;
  workflowRunRepoId: RepoId;
  workflowRunRef: string;
  mailAddress: string;
};

export type DeployFlowEnv = {
  hub: HubEnv;
  inference: MockInference;
  sidecar: SidecarHandle;
  sidecarDiagnostics: () => string;
  /** Per-deployment handles populated by `deployWorkflow`. */
  deployments: Map<string, DeploymentHandle>;
  /**
   * Register an externally-constructed deployment handle on the env.
   * Tests call this when the deployment was driven outside
   * `deployWorkflow` (e.g. a pre-staged repo state) so the env's
   * helpers can resolve the handle by `deploymentId`.
   */
  registerDeployment(handle: DeploymentHandle): void;
  teardown: () => Promise<void>;
};

export type StartDeployFlowEnvOpts = {
  /**
   * Extra env vars written last into the sidecar subprocess env. Wins
   * over every fixture-owned key, including `HUB_WS_URL`,
   * `SIDECAR_ID`, `SIDECAR_TOKEN`, `SIDECAR_DATA_DIR`, and the
   * inherited `PATH`/`HOME`/`TMPDIR`, so callers can both inject new
   * flags and override any fixture default.
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
   * When true, the mock inference server echoes the last user message's
   * text as `echo:<text>` so a test can assert the inbound mail body
   * reached the agent's `agent.send` as the step input. See
   * `StartMockInferenceOpts.echoUserMessage`.
   */
  inferenceEchoUserMessage?: boolean;
  /**
   * When true, the synthetic `@intx/tools-mail` tarball the hub seeds
   * ships the transport-backed `mail_send` bundle: its `run` calls
   * `env.transport.send(...)` (the supervisor-backed transport the
   * unified child wires) and writes its workspace sentinel only after a
   * successful `SendReceipt`. This drives the real OUTBOUND signed-send
   * chain through the spawned child, proving the 4.3 path composes with
   * the rest of the single-agent lifecycle. The default filesystem-only
   * bundle is unchanged for tests that only assert in-child tool
   * execution.
   */
  transportBackedMailTool?: boolean;
};

/**
 * Stop a `Bun.serve` server, bounding the wait so teardown cannot hang.
 *
 * A server-initiated WebSocket close (`ws.close()` from the hub upgrade
 * callback, which the reconnect helpers use to drop the hub link) does
 * NOT fire the server-side `onClose`, so Bun keeps counting the dropped
 * connection as live. `server.stop(true)` and `server.stop(false)` both
 * then wait forever for that phantom connection to drain -- a reproducible
 * Bun/Hono behavior, not a product concern (the sidecar has already been
 * killed and every tracked handle closed by the time this runs). This is
 * a deliberate teardown bound, not an error swallow: the server's only
 * remaining job is releasing its port, which the exiting test process
 * reclaims regardless. Tests that never drop the hub link resolve the
 * `stop(true)` promptly and never hit the bound.
 */
async function stopServerBounded(
  server: ReturnType<typeof Bun.serve>,
): Promise<void> {
  const STOP_TIMEOUT_MS = 1_000;
  await Promise.race([
    server.stop(true),
    new Promise<void>((resolve) => setTimeout(resolve, STOP_TIMEOUT_MS)),
  ]);
}

// Compose the full deploy-flow env: hub server, mock inference, sidecar
// subprocess. Owns every tempdir these subsystems open and tears them
// all down in `teardown()`.
//
// Returns once the sidecar has registered with the hub.
export async function startDeployFlowEnv(
  opts: StartDeployFlowEnvOpts = {},
): Promise<DeployFlowEnv> {
  const tempDirs: string[] = [];
  const registerTempDir = (dir: string): void => {
    tempDirs.push(dir);
  };

  const hub = await startHub(registerTempDir, {
    ...(opts.transportBackedMailTool === true
      ? { transportBackedMailTool: true }
      : {}),
  });
  const inference = startMockInference({
    ...(opts.inferenceToolCall !== undefined
      ? { toolCall: opts.inferenceToolCall }
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

  const sidecarDiagnostics = (): string => {
    const parts: string[] = [];
    if (sidecar.stderr.length > 0) {
      parts.push(`sidecar stderr:\n${sidecar.stderr.slice(-300).join("")}`);
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
    if (deployments.has(handle.deploymentId)) {
      throw new Error(
        `deploy-flow env: deployment ${handle.deploymentId} is already registered`,
      );
    }
    deployments.set(handle.deploymentId, handle);
  };

  const teardown = async (): Promise<void> => {
    // Close every tracked hub-side WebSocket handle before killing the
    // sidecar so no live link lingers. Then wait for the sidecar
    // subprocess to fully exit before removing its data directory. The
    // earlier shape (kill, then immediately rm) raced the sidecar's
    // still-open file handles inside `sidecar-data-*`; on a slow CI host
    // the rm would observe EBUSY, EACCES, or partial removal, which the
    // `.catch(() => {})` shrouded. Errors must surface from the rm, so
    // the catch is dropped here. The server stops are bounded
    // (`stopServerBounded`) because a test that dropped the hub link
    // leaves Bun with a phantom connection its `server.stop` would wait
    // on forever.
    deployments.clear();
    for (const handle of hub.liveHandles) {
      handle.close();
    }
    hub.liveHandles.clear();
    sidecar.proc.kill();
    await sidecar.proc.exited;
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
 * Options accepted by `deployWorkflow`. The helper composes the
 * workflow-deploy orchestrator against the env's hub substrate and routes
 * per-step launches through `env.hub.sessionService.stageWorkflowStep`, the
 * stage-only path production's multi-step branch runs.
 */
export type DeployWorkflowOpts = {
  /**
   * Harness configuration shared across every step's launch. The
   * orchestrator overrides `agentAddress`, `agentId`, and `systemPrompt`
   * per step in the multi-step branch.
   */
  config: HarnessConfig;
  /**
   * Deploy-tree content shared across every step's launch. The
   * orchestrator overrides `systemPrompt` per step in the multi-step
   * branch from the step's agent definition.
   */
  deployContent: { systemPrompt: string };
  /**
   * Stable identifier the branch concatenates into derived agent
   * addresses. Required.
   */
  deploymentId: string;
  /**
   * Mail-domain for the deployment. Defaults to the integration test's
   * canonical domain so callers do not have to thread the domain through.
   */
  deploymentDomain?: string;
  /** Tool-package pins to ship with every step's deploy. */
  toolPackagePins?: readonly ToolPackagePin[];
  /**
   * Flat set of grant-shape strings the operator has approved for this
   * deployment. Every grant the capability walk surfaces must be in
   * this set.
   */
  operatorApprovals: ApprovalSet;
  /**
   * Optional per-deployment `workflow-run` ref override. Defaults to
   * `refs/heads/main`, mirroring the sidecar wiring's default.
   */
  workflowRunRef?: string;
  /**
   * Optional override for the deployment's mail address. Defaults to
   * `ins_<deploymentId>@<deploymentDomain>`.
   */
  deploymentMailAddress?: string;
};

/**
 * Handle returned by `deployWorkflow`. Carries the deployment id the
 * orchestrator settled on plus the workflow-run repo identity the
 * other helpers consult.
 */
export type DeployWorkflowHandle = {
  deploymentId: string;
  workflowRunRepoId: RepoId;
  workflowRunRef: string;
  mailAddress: string;
};

/**
 * Build a workflow-deploy orchestrator wired against the env's hub
 * substrate and run it against the supplied workflow. The orchestrator
 * derives per-step addresses and routes each launch through the
 * `launchSession` callback.
 *
 * Registers the resulting handle on `env.deployments` so the other
 * Phase I helpers (event reads, signal injection, drain initiation,
 * processing-crash simulation) can resolve the deployment by id.
 */
export async function deployWorkflow(
  env: DeployFlowEnv,
  workflow: WorkflowDefinition,
  opts: DeployWorkflowOpts,
): Promise<DeployWorkflowHandle> {
  const workflowRunRef = opts.workflowRunRef ?? DEFAULT_WORKFLOW_RUN_REF;

  const deploymentId = opts.deploymentId;
  const deploymentDomain = opts.deploymentDomain ?? DEFAULT_DEPLOYMENT_DOMAIN;
  const mailAddress =
    opts.deploymentMailAddress ?? `ins_${deploymentId}@${deploymentDomain}`;

  // The deployment supplies its own substrate-safe `deploymentId`, which
  // is the workflow-run repo slug the supervisor commits under.
  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: deploymentId,
  };

  // Route every per-step launch through the session service, mirroring
  // the production multi-step branch, which drives the orchestrator's
  // per-step launch callback against `stageWorkflowStep` (stage-only, no
  // warm harness). The orchestrator's `DeployContent` widens
  // `toolPackageManifest` to `unknown`; `bridgeOrchestratorDeployContent`
  // narrows and validates it back to the hub-sessions shape -- the same
  // bridge production uses.
  const launchSession: LaunchSessionFn = async (orchestratorParams) => {
    await env.hub.sessionService.stageWorkflowStep({
      agentAddress: orchestratorParams.agentAddress,
      agentId: orchestratorParams.agentId,
      instanceId: orchestratorParams.instanceId,
      config: orchestratorParams.config,
      deployContent: bridgeOrchestratorDeployContent(
        orchestratorParams.deployContent,
      ),
      ...(orchestratorParams.toolPackagePins !== undefined
        ? { toolPackagePins: orchestratorParams.toolPackagePins }
        : {}),
    });
  };

  const workflowRepo: WorkflowRepoWriter = {
    async writeWorkflowRepo(args) {
      const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
      const principal: WorkflowRunHubPrincipal = { kind: "hub" };
      const files: Record<string, string> = {};
      for (const [k, v] of args.files) {
        files[k] = v;
      }
      await env.hub.agentRepoStore.repoStore.writeTree(
        principal,
        repoId,
        DEFAULT_ASSET_REF,
        {
          files,
          message: `deployWorkflow: write workflow repo ${args.workflowRepoId}`,
        },
      );
    },
  };

  const orchestrator = createWorkflowDeployOrchestrator({
    directorRegistry: createDefaultDirectorRegistry(),
    workflowRepo,
    launchSession,
  });

  await orchestrator.deployWorkflow({
    workflow,
    deploymentId,
    deploymentDomain,
    config: opts.config,
    deployContent: opts.deployContent,
    operatorApprovals: opts.operatorApprovals,
    ...(opts.toolPackagePins !== undefined
      ? { toolPackagePins: opts.toolPackagePins }
      : {}),
  });

  const handle: DeploymentHandle = {
    deploymentId,
    workflowDefinition: workflow,
    workflowRunRepoId,
    workflowRunRef,
    mailAddress,
  };
  env.registerDeployment(handle);

  return {
    deploymentId,
    workflowRunRepoId,
    workflowRunRef,
    mailAddress,
  };
}

/**
 * Resolve a deployment handle by id. Throws if no deployment has been
 * registered under that id so a typo or stale id surfaces as a loud
 * failure rather than a silent no-op.
 */
function requireDeployment(
  env: DeployFlowEnv,
  deploymentId: string,
): DeploymentHandle {
  const handle = env.deployments.get(deploymentId);
  if (handle === undefined) {
    throw new Error(
      `deploy-flow env: no deployment registered for ${deploymentId}; call deployWorkflow or registerDeployment first`,
    );
  }
  return handle;
}

export type { WorkflowRunEvent };

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
  deploymentId: string,
  runId: string,
): Promise<WorkflowRunEvent[]> {
  const handle = requireDeployment(env, deploymentId);
  const reader = createWorkflowRunReader(env.hub.agentRepoStore.repoStore);
  return reader.readRunEvents(
    handle.workflowRunRepoId,
    handle.workflowRunRef,
    runId,
  );
}

/**
 * Options for `waitForWorkflowRunComplete`. Mirrors the shape of
 * `waitFor` so the helper composes the same diagnostic surface.
 */
export type WaitForWorkflowRunCompleteOpts = {
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
 * Poll the deployment's workflow-run event log until the run's
 * terminal event lands. Returns the terminal event.
 */
export async function waitForWorkflowRunComplete(
  env: DeployFlowEnv,
  deploymentId: string,
  runId: string,
  opts: WaitForWorkflowRunCompleteOpts = {},
): Promise<WorkflowRunEvent> {
  const { timeoutMs = 10_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const events = await readWorkflowRunEvents(env, deploymentId, runId);
    const terminal = events.find((e) =>
      WORKFLOW_RUN_TERMINAL_TYPES.has(e.type),
    );
    if (terminal !== undefined) return terminal;
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      throw new Error(
        `waitForWorkflowRunComplete timed out after ${String(timeoutMs)}ms for ${deploymentId}/${runId}${ctx}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
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
};

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
    inReplyTo: undefined,
    references: undefined,
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
  });
  const signature = await createDetachedSignatureFromProvider(
    signedContent,
    crypto,
  );
  const rawMessage = assembleMessage(headers, signedContent, signature);
  const base64 = base64Encode(rawMessage);
  const delivered = env.hub.router.routeMail(address, base64);
  if (!delivered) {
    throw new Error(
      `fireMailTrigger: routeMail returned false for ${address}; address is not routable on the hub`,
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
  deploymentId: string,
  runId: string,
  signalName: string,
  payload: unknown,
): Promise<{ signalId: string }> {
  const handle = requireDeployment(env, deploymentId);
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
  deploymentId: string,
  opts: InitiateDrainOpts = {},
): void {
  const handle = requireDeployment(env, deploymentId);
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
  deploymentId: string,
  address: string,
  messageId: string,
  receivedAt: number,
): Promise<void> {
  const handle = requireDeployment(env, deploymentId);
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
 * the first one found, or throw on timeout. Used by integration tests
 * that don't know the runId upfront because the supervisor mints it.
 */
export async function waitForFirstRunId(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<string> {
  const { timeoutMs = 10_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const ids = await listRunIds(env, workflowRunRepoId);
    const first = ids[0];
    if (first !== undefined) return first;
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      throw new Error(
        `waitForFirstRunId timed out after ${String(timeoutMs)}ms for ${workflowRunRepoId.id}${ctx}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
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
// `WsHandle` (`env.hub.liveHandles`) and answers the reconnect ownership
// challenge (`lookups.lookupPublicKey` backed by `deployAcks`), which is
// what makes a dropped link reconnect instead of looping on a closed socket.

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
   * Ceiling on the reconnect wait. Defaults to `20_000`, comfortably above
   * the observed ~3s reconnect (the sidecar's 3s `DEFAULT_RECONNECT_DELAY_MS`
   * plus a handshake).
   */
  timeoutMs?: number;
};

/**
 * Poll until `address` is routable on the hub again, then return the
 * elapsed milliseconds. A keyed deployment address can only re-enter the
 * hub's routing index by passing the reconnect ownership challenge (the
 * plain `register` path leaves a keyed address unrouted until challenged),
 * so "routable again" is a sound proxy for a completed challenge/response.
 */
export async function waitForReconnect(
  env: DeployFlowEnv,
  address: string,
  opts: WaitForReconnectOpts = {},
): Promise<number> {
  const timeoutMs = opts.timeoutMs ?? 20_000;
  const start = Date.now();
  await waitFor(() => env.hub.router.getRoutableAddresses().includes(address), {
    timeoutMs,
    diagnostics: env.sidecarDiagnostics,
  });
  return Date.now() - start;
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
   * Ceiling on the settle wait. Defaults to `10_000`. If the pack stream
   * never goes quiet within this window the helper throws rather than
   * dropping into an in-flight push.
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
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const start = Date.now();
  let lastCount = env.hub.workflowRunPackReceipts.count;
  let lastChange = Date.now();
  for (;;) {
    const current = env.hub.workflowRunPackReceipts.count;
    if (current !== lastCount) {
      lastCount = current;
      lastChange = Date.now();
    }
    if (Date.now() - lastChange >= quietMs) break;
    if (Date.now() - start > timeoutMs) {
      throw new Error(
        `settleThenDrop: workflow-run pack stream did not go quiet for ${String(quietMs)}ms within ${String(timeoutMs)}ms for ${address}` +
          `\n${env.sidecarDiagnostics()}`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  dropHubLink(env);
}
