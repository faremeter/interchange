// Integration test: full deploy lifecycle through SessionService.
//
// Spins up a real WS server (hub side), spawns a real sidecar subprocess,
// and exercises the complete agent lifecycle orchestrated by SessionService:
//
//   launchSession (write → pack → provision → deliver → start)
//     → message → sync → endSession
//
// The gap this test fills: nobody else tests that AgentRepoStore.writeDeployTree
// → createDeployPack → sendPack produces a packfile the sidecar actually
// accepts and materializes correctly.
//
// Inference is mocked by a tiny HTTP server that echoes the tools it receives,
// so we can assert the model saw the correct tool definitions without calling
// a real LLM.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createAgentRepoStore,
  createSessionService,
  createSidecarRouter,
  type SidecarRouter,
  type SessionService,
  type WsHandle,
} from "@interchange/hub";
import type { HarnessConfig } from "@interchange/types/runtime";
import { sanitizeAddress } from "../../apps/sidecar/src/session-manager";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@interchange/mime";
import { generateKeyPair, createNodeCrypto } from "@interchange/crypto-node";
import type { Subprocess } from "bun";
import git from "isomorphic-git";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseAgentId(agentAddress: string): string {
  const atIdx = agentAddress.indexOf("@");
  if (atIdx === -1) {
    throw new Error(`Invalid agent address: "${agentAddress}"`);
  }
  return agentAddress.substring(0, atIdx);
}

async function waitFor(
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

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

// ---------------------------------------------------------------------------
// Mock inference server
//
// Returns a canned assistant response that includes the tool names it was
// given in the request. This lets us assert that the harness passed the
// deploy-tree tools through to inference.
// ---------------------------------------------------------------------------

type InferenceTool = {
  name: string;
  description?: string;
  input_schema?: Record<string, unknown>;
};

type InferenceRequest = {
  tools?: InferenceTool[];
};

function startMockInference(): {
  server: ReturnType<typeof Bun.serve>;
  requests: InferenceRequest[];
} {
  const requests: InferenceRequest[] = [];

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const body = (await req.json()) as InferenceRequest;
      requests.push(body);

      const toolNames = (body.tools ?? []).map((t) => t.name);
      const text = `I see these tools: ${toolNames.join(", ")}`;

      // Return an Anthropic-style SSE streaming response.
      const events = [
        `event: message_start\ndata: ${JSON.stringify({
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
        })}\n\n`,
        `event: content_block_start\ndata: ${JSON.stringify({
          type: "content_block_start",
          index: 0,
          content_block: { type: "text", text: "" },
        })}\n\n`,
        `event: content_block_delta\ndata: ${JSON.stringify({
          type: "content_block_delta",
          index: 0,
          delta: { type: "text_delta", text },
        })}\n\n`,
        `event: content_block_stop\ndata: ${JSON.stringify({
          type: "content_block_stop",
          index: 0,
        })}\n\n`,
        `event: message_delta\ndata: ${JSON.stringify({
          type: "message_delta",
          delta: { stop_reason: "end_turn" },
          usage: { output_tokens: 20 },
        })}\n\n`,
        `event: message_stop\ndata: ${JSON.stringify({
          type: "message_stop",
        })}\n\n`,
      ];

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

// ---------------------------------------------------------------------------
// Hub WS server (in-process) with real AgentRepoStore and SessionService
// ---------------------------------------------------------------------------

type HubEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
  sessionService: SessionService;
  agentEvents: { addr: string; sid: string; event: unknown }[];
  deployAcks: Map<string, string>;
  statePacks: { agentAddress: string; ref: string; commitSha: string }[];
  hubDataDir: string;
};

async function startHub(): Promise<HubEnv> {
  const agentEvents: HubEnv["agentEvents"] = [];
  const deployAcks = new Map<string, string>();
  const statePacks: HubEnv["statePacks"] = [];

  const hubDataDir = await makeTempDir("hub-data-");
  const hubSigningKey = await generateKeyPair();
  const agentRepoStore = createAgentRepoStore({
    dataDir: hubDataDir,
    signingKey: hubSigningKey,
  });

  function hexEncode(bytes: Uint8Array): string {
    return Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }

  const router = createSidecarRouter({
    requestTimeoutMs: 10_000,
    hubPublicKey: hexEncode(hubSigningKey.publicKey),
    onAgentEvent(addr, sid, event) {
      agentEvents.push({ addr, sid, event });
    },
    async onAgentDeployAck(agentAddress, publicKey) {
      deployAcks.set(agentAddress, publicKey);
    },
    async onStatePackReceived(agentAddress, pack, ref, commitSha) {
      const agentId = parseAgentId(agentAddress);
      await agentRepoStore.receiveStatePack(agentId, pack, ref, commitSha);
      statePacks.push({ agentAddress, ref, commitSha });
      return { accepted: true };
    },
  });

  const sessionService = createSessionService({
    sidecarRouter: router,
    agentRepoStore,
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
          router.handleOpen(handle);
        },
        onMessage(evt, _ws) {
          if (typeof evt.data === "string") {
            router.handleMessage(handle, evt.data);
          }
        },
        onClose(_evt, _ws) {
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
    agentEvents,
    deployAcks,
    statePacks,
    hubDataDir,
  };
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

let hub: HubEnv;
let inference: ReturnType<typeof startMockInference>;
let sidecarProc: Subprocess;
let sidecarDataDir: string;
const sidecarStderr: string[] = [];

function sidecarDiagnostics(): string {
  if (sidecarStderr.length === 0) return "";
  return `sidecar stderr:\n${sidecarStderr.slice(-20).join("")}`;
}

const AGENT_ADDRESS = "test-agent@integration.interchange";
const AGENT_ID = "test-agent";
const SESSION_ID = "ses_integration-1";
const SIDECAR_ID = "sc-integration-1";
const TOKEN = "test-token";

const GREET_SKILL = {
  name: "greet",
  definition: {
    name: "greet",
    description: "Greet someone by name",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Name to greet" },
      },
      required: ["name"],
    },
  },
};

beforeAll(async () => {
  hub = await startHub();
  inference = startMockInference();
  sidecarDataDir = await makeTempDir("sidecar-data-");

  const hubPort = hub.server.port;

  sidecarProc = Bun.spawn(["bun", "run", "apps/sidecar/src/main.ts"], {
    cwd: path.resolve(import.meta.dir, "../.."),
    env: {
      PATH: process.env["PATH"],
      HOME: process.env["HOME"],
      TMPDIR: process.env["TMPDIR"],
      HUB_WS_URL: `ws://localhost:${hubPort}/ws`,
      SIDECAR_ID,
      SIDECAR_TOKEN: TOKEN,
      SIDECAR_DATA_DIR: sidecarDataDir,
    },
    stdout: "pipe",
    stderr: "pipe",
  });

  // Drain stderr into a rolling buffer for diagnostics on timeout.
  const stderr = sidecarProc.stderr as ReadableStream<Uint8Array>;
  (async () => {
    const reader = stderr.getReader();
    const decoder = new TextDecoder();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      sidecarStderr.push(decoder.decode(value));
      if (sidecarStderr.length > 50) sidecarStderr.shift();
    }
  })();

  // Wait for the sidecar to register with the hub.
  await waitFor(() => hub.router.getConnectedSidecars().length > 0, {
    diagnostics: sidecarDiagnostics,
  });
});

afterAll(async () => {
  sidecarProc?.kill();
  hub?.server.stop(true);
  inference?.server.stop(true);

  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch((_e) => {
      /* best effort cleanup */
    });
  }
});

describe("deploy flow integration", () => {
  test("sidecar registers with hub", () => {
    expect(hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });

  test("launchSession writes, packs, provisions, delivers, and starts", async () => {
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: AGENT_ID,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: AGENT_ADDRESS,
      systemPrompt: "Fallback prompt (should be overridden by deploy tree)",
      tools: [],
      grants: [],
      providers: [
        {
          provider: "anthropic",
          baseURL: `http://localhost:${inference.server.port}`,
          apiKey: "sk-mock",
        },
      ],
      defaultModel: "mock-model",
    };

    await hub.sessionService.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      config,
      deployContent: {
        systemPrompt:
          "You are an integration test agent. Use the greet tool when asked.",
        skills: [GREET_SKILL],
      },
    });

    // The deploy ack should have arrived (provision phase completed).
    const publicKey = hub.deployAcks.get(AGENT_ADDRESS);
    expect(publicKey).toBeDefined();
    if (publicKey === undefined) throw new Error("unreachable");
    expect(publicKey.length).toBeGreaterThan(0);

    // The agent should now be routable (session start completed).
    expect(hub.router.getRoutableAddresses()).toContain(AGENT_ADDRESS);

    // The deploy tree should have landed on the sidecar's disk.
    const agentDir = path.join(sidecarDataDir, sanitizeAddress(AGENT_ADDRESS));

    await waitFor(
      async () => {
        try {
          await fs.promises.access(path.join(agentDir, "deploy", "prompt.md"));
          return true;
        } catch {
          return false;
        }
      },
      { diagnostics: sidecarDiagnostics },
    );

    const prompt = await fs.promises.readFile(
      path.join(agentDir, "deploy", "prompt.md"),
      "utf-8",
    );
    expect(prompt).toContain("integration test agent");

    const toolJson = await fs.promises.readFile(
      path.join(agentDir, "deploy", "skills", "greet", "tool.json"),
      "utf-8",
    );
    const tool = JSON.parse(toolJson) as { name: string };
    expect(tool.name).toBe("greet");
  });

  test("send message and verify inference receives deploy tools", async () => {
    const requestsBefore = inference.requests.length;
    const eventsBefore = hub.agentEvents.length;

    const keyPair = await generateKeyPair();
    const crypto = createNodeCrypto(keyPair);
    const headers: MessageHeaders = {
      from: "user@integration.interchange",
      to: [AGENT_ADDRESS],
      cc: undefined,
      date: new Date(),
      messageId: "<test-msg-1@integration.interchange>",
      subject: undefined,
      inReplyTo: undefined,
      references: undefined,
      mimeVersion: "1.0",
      interchangeType: "conversation.message",
      interchangeCorrelationId: undefined,
      interchangeTenantId: undefined,
      interchangeAgentId: undefined,
      interchangeSessionId: SESSION_ID,
      interchangeOfferingId: undefined,
      interchangeSchemaVersion: undefined,
      traceparent: undefined,
      tracestate: undefined,
    };
    const signedContent = assembleSignedContent({
      kind: "conversation",
      text: "Hello, please greet Alice.",
    });
    const signature = await createDetachedSignatureFromProvider(
      signedContent,
      crypto,
    );
    const rawMessage = assembleMessage(headers, signedContent, signature);
    const base64 = Buffer.from(rawMessage).toString("base64");
    hub.router.routeMail(AGENT_ADDRESS, base64);

    await waitFor(() => inference.requests.length > requestsBefore, {
      diagnostics: sidecarDiagnostics,
    });

    const req = inference.requests[requestsBefore];
    if (req === undefined) throw new Error("unreachable");
    const tools = req.tools ?? [];
    const toolNames = tools.map((t) => t.name);

    expect(toolNames).toContain("greet");
    expect(toolNames).toContain("message_send");

    const greetTool = tools.find((t) => t.name === "greet");
    expect(greetTool).toBeDefined();
    if (greetTool === undefined) throw new Error("unreachable");
    expect(greetTool.description).toBe("Greet someone by name");
    expect(greetTool.input_schema).toEqual({
      type: "object",
      properties: {
        name: { type: "string", description: "Name to greet" },
      },
      required: ["name"],
    });

    await waitFor(() => hub.agentEvents.length > eventsBefore, {
      diagnostics: sidecarDiagnostics,
    });

    const firstEvent = hub.agentEvents[eventsBefore];
    if (firstEvent === undefined) throw new Error("unreachable");
    expect(firstEvent.addr).toBe(AGENT_ADDRESS);
    expect(firstEvent.sid).toBe(SESSION_ID);

    // The first event from a reactor turn is message.received, followed
    // by inference events. Verify it is a known reactor event type.
    const payload = firstEvent.event as { type: string };
    expect(payload.type).toBe("message.received");
  });

  test("sync request triggers state push to hub repo", async () => {
    const packCountBefore = hub.statePacks.length;
    hub.router.sendSyncRequest(AGENT_ADDRESS);

    await waitFor(() => hub.statePacks.length > packCountBefore, {
      diagnostics: sidecarDiagnostics,
    });

    const last = hub.statePacks[hub.statePacks.length - 1];
    if (last === undefined) throw new Error("unreachable");
    expect(last.agentAddress).toBe(AGENT_ADDRESS);
    expect(last.ref).toMatch(/^refs\//);
    expect(last.commitSha).toMatch(/^[0-9a-f]{40}$/);
    expect(hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);

    // Verify the pack was actually persisted in the hub's git repo.
    const hubAgentDir = path.join(
      hub.hubDataDir,
      "agents",
      parseAgentId(AGENT_ADDRESS),
    );
    const resolvedSha = await git.resolveRef({
      fs,
      dir: hubAgentDir,
      ref: last.ref,
    });
    expect(resolvedSha).toBe(last.commitSha);

    // Verify the commit object is readable (pack was properly indexed).
    const { commit } = await git.readCommit({
      fs,
      dir: hubAgentDir,
      oid: last.commitSha,
    });
    expect(commit.tree).toMatch(/^[0-9a-f]{40}$/);
  });

  test("endSession undeploys agent and cleans up sidecar", async () => {
    await hub.sessionService.endSession(AGENT_ADDRESS, "test_complete");

    // Agent should no longer be routable after ack.
    expect(hub.router.getRoutableAddresses()).not.toContain(AGENT_ADDRESS);

    // The ack is sent after deleteAgentDir completes, so the directory
    // is already gone by the time the promise resolves.
    const agentDir = path.join(sidecarDataDir, sanitizeAddress(AGENT_ADDRESS));
    const dirExists = await fs.promises
      .access(agentDir)
      .then(() => true)
      .catch(() => false);
    expect(dirExists).toBe(false);
  });
});
