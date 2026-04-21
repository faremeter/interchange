// Integration test: full deploy flow from hub to sidecar.
//
// Spins up a real WS server (hub side), spawns a real sidecar subprocess,
// deploys an agent, pushes a deploy pack with a prompt and skills, sends a
// message, and verifies that inference sees the deploy-tree tools.
//
// This test exercises the complete pack transport pipeline:
//   source repo → createDeployPack → WS frames → applyPack → readDeployTree
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
import git from "isomorphic-git";
import {
  createSidecarRouter,
  type SidecarRouter,
  type WsHandle,
} from "@interchange/hub";
import { createDeployPack } from "@interchange/storage-isogit";
import type { HarnessConfig } from "@interchange/types/runtime";
import type { Subprocess } from "bun";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
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

type InferenceRequest = {
  tools?: { name: string }[];
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
// Hub WS server (in-process)
// ---------------------------------------------------------------------------

type HubEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
  agentEvents: { addr: string; sid: string; event: unknown }[];
  deployAcks: Map<string, string>;
  statePacks: { agentAddress: string; commitSha: string }[];
};

function startHub(): HubEnv {
  const agentEvents: HubEnv["agentEvents"] = [];
  const deployAcks = new Map<string, string>();
  const statePacks: HubEnv["statePacks"] = [];

  const router = createSidecarRouter({
    requestTimeoutMs: 10_000,
    onAgentEvent(addr, sid, event) {
      agentEvents.push({ addr, sid, event });
    },
    async onAgentDeployAck(agentAddress, publicKey) {
      deployAcks.set(agentAddress, publicKey);
    },
    async onStatePackReceived(agentAddress, _pack, _ref, commitSha) {
      statePacks.push({ agentAddress, commitSha });
      return { accepted: true };
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

  return { server, router, agentEvents, deployAcks, statePacks };
}

// ---------------------------------------------------------------------------
// Source repo builder: creates a git repo with deploy content
// ---------------------------------------------------------------------------

async function createSourceRepo(
  promptText: string,
  skills: { name: string; definition: Record<string, unknown> }[],
): Promise<{ dir: string; ref: string }> {
  const dir = await makeTempDir("deploy-source-");
  await git.init({ fs, dir, defaultBranch: "main" });

  // deploy/prompt.md
  const promptDir = path.join(dir, "deploy");
  await fs.promises.mkdir(promptDir, { recursive: true });
  await fs.promises.writeFile(path.join(promptDir, "prompt.md"), promptText);
  await git.add({ fs, dir, filepath: "deploy/prompt.md" });

  // deploy/skills/<name>/tool.json
  for (const skill of skills) {
    const skillDir = path.join(dir, "deploy", "skills", skill.name);
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(skillDir, "tool.json"),
      JSON.stringify(skill.definition, null, 2),
    );
    await git.add({
      fs,
      dir,
      filepath: `deploy/skills/${skill.name}/tool.json`,
    });
  }

  await git.commit({
    fs,
    dir,
    message: "Initial deploy content",
    author: { name: "Test", email: "test@test.dev" },
  });

  return { dir, ref: "refs/heads/main" };
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

let hub: HubEnv;
let inference: ReturnType<typeof startMockInference>;
let sidecarProc: Subprocess;
let sidecarDataDir: string;

const AGENT_ADDRESS = "test-agent@integration.interchange";
const SIDECAR_ID = "sc-integration-1";
const TOKEN = "test-token";

beforeAll(async () => {
  hub = startHub();
  inference = startMockInference();
  sidecarDataDir = await makeTempDir("sidecar-data-");

  const hubPort = hub.server.port;

  sidecarProc = Bun.spawn(["bun", "run", "apps/sidecar/src/main.ts"], {
    cwd: path.resolve(import.meta.dir, "../.."),
    env: {
      ...process.env,
      HUB_WS_URL: `ws://localhost:${hubPort}/ws`,
      SIDECAR_ID,
      SIDECAR_TOKEN: TOKEN,
      SIDECAR_DATA_DIR: sidecarDataDir,
    },
    stdout: "pipe",
    stderr: "pipe",
  });

  // Wait for the sidecar to register with the hub.
  await waitFor(() => hub.router.getConnectedSidecars().length > 0);
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

  test("deploy agent to sidecar", async () => {
    const config: HarnessConfig = {
      sessionId: "ses_integration-1",
      agentId: "agent-integration",
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

    await hub.router.sendAgentDeploy(AGENT_ADDRESS, config);

    // Verify deploy ack was received with a public key.
    const publicKey = hub.deployAcks.get(AGENT_ADDRESS);
    expect(publicKey).toBeDefined();
    if (publicKey === undefined) throw new Error("unreachable");
    expect(typeof publicKey).toBe("string");
    expect(publicKey.length).toBeGreaterThan(0);
  });

  test("push deploy pack with prompt and skills", async () => {
    const source = await createSourceRepo(
      "You are an integration test agent. Use the greet tool when asked.",
      [
        {
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
        },
      ],
    );

    const { pack, commitSha } = await createDeployPack(source.dir, source.ref);

    await hub.router.sendPack(
      AGENT_ADDRESS,
      pack,
      "refs/heads/deploy",
      commitSha,
    );

    // Verify the deploy tree landed on the sidecar's disk.
    const agentDir = path.join(
      sidecarDataDir,
      AGENT_ADDRESS.replace(/@/g, "_at_").replace(/[^a-zA-Z0-9_-]/g, "_"),
    );

    await waitFor(async () => {
      try {
        await fs.promises.access(path.join(agentDir, "deploy", "prompt.md"));
        return true;
      } catch {
        return false;
      }
    });

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

  test("redeploy agent to pick up deploy tree", async () => {
    // The deploy pack was applied to disk but the running session was created
    // before the pack arrived. Undeploy and re-deploy so the new session reads
    // the deploy tree (prompt + skills) from disk.
    //
    // Both frames travel over the same WS connection, so the sidecar processes
    // the undeploy before the deploy. sendAgentDeploy awaits the deploy ack,
    // which confirms the new session is running.
    hub.router.sendAgentUndeploy(AGENT_ADDRESS, "redeploy");

    const config: HarnessConfig = {
      sessionId: "ses_integration-2",
      agentId: "agent-integration",
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

    await hub.router.sendAgentDeploy(AGENT_ADDRESS, config);

    // Verify the second deploy ack was received.
    const publicKey = hub.deployAcks.get(AGENT_ADDRESS);
    expect(publicKey).toBeDefined();
  });

  test("send message and verify inference receives deploy tools", async () => {
    const requestsBefore = inference.requests.length;

    // Send a user message to the deployed agent.
    await hub.router.sendMessage(
      AGENT_ADDRESS,
      "ses_integration-2",
      "Hello, please greet Alice.",
    );

    // Wait for the mock inference server to receive a request.
    await waitFor(() => inference.requests.length > requestsBefore);

    const req = inference.requests[inference.requests.length - 1];
    if (req === undefined) throw new Error("unreachable");
    const toolNames = (req.tools ?? []).map((t) => t.name);

    // The inference request should include both the built-in message tools
    // and the deploy-tree "greet" tool.
    expect(toolNames).toContain("greet");
    expect(toolNames).toContain("message_send");

    // Wait for agent events to arrive at the hub (inference.done).
    await waitFor(() => hub.agentEvents.length > 0);
    expect(hub.agentEvents.length).toBeGreaterThan(0);
  });

  test("sync request triggers state push", async () => {
    const packCountBefore = hub.statePacks.length;
    hub.router.sendSyncRequest(AGENT_ADDRESS);

    // Wait for the hub to receive the state pack from the sidecar.
    await waitFor(() => hub.statePacks.length > packCountBefore);

    const last = hub.statePacks[hub.statePacks.length - 1];
    expect(last?.agentAddress).toBe(AGENT_ADDRESS);
    expect(hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
  });
});
