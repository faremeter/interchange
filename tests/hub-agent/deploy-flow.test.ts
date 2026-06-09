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
import * as tar from "tar";
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import {
  createAgentRepoStore,
  createSessionService,
  createSidecarRouter,
  DEFAULT_ASSET_REF,
  type AssetService,
  type SidecarRouter,
  type SessionService,
  type WsHandle,
} from "@intx/hub-sessions";
import type { HarnessConfig } from "@intx/types/runtime";
import { hexEncode } from "@intx/types";
import { createAgentRepoStore as createSidecarRepoStore } from "@intx/hub-agent";
import {
  assembleSignedContent,
  assembleMessage,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import { generateKeyPair, createNodeCrypto } from "@intx/crypto-node";
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
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this is a test mock server that only receives requests from the sidecar under test; the shape is known
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
// Synthetic @intx/tools-mail tarball
//
// The integration test's loader path is unmodified production code; it
// imports the tarball's `interchange.tools` entry as a real ESM module.
// To keep the fixture self-contained (no workspace dependency tree) the
// tarball ships a minimal `sidecar-bundle.js` that exports an
// AnnotatedToolFactory with the same `id` shape as the real bundle
// (`@intx/tools-mail/sidecar-bundle`) and a single `mail_send`
// definition. The loader prefixes the definition name with the bundle
// id to yield the `@intx/tools-mail/sidecar-bundle:mail_send` tool the
// model ends up seeing.
// ---------------------------------------------------------------------------

async function buildSyntheticToolsMailTarball(): Promise<Uint8Array> {
  const stagingDir = await makeTempDir("tools-mail-fixture-");
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

  const bundleSource = `
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
  run: async (call, signal) => ({ callId: call.id, content: "ok" }),
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
  statePackReceiveFailures: { agentAddress: string; error: string }[];
  hubDataDir: string;
};

async function startHub(): Promise<HubEnv> {
  const agentEvents: HubEnv["agentEvents"] = [];
  const deployAcks = new Map<string, string>();
  const statePacks: HubEnv["statePacks"] = [];
  const statePackReceiveFailures: HubEnv["statePackReceiveFailures"] = [];

  const hubDataDir = await makeTempDir("hub-data-");
  const hubSigningKey = await generateKeyPair();
  const agentRepoStore = createAgentRepoStore({
    dataDir: hubDataDir,
    signingKey: hubSigningKey,
  });

  const router = createSidecarRouter({
    requestTimeoutMs: 10_000,
    hubPublicKey: hexEncode(hubSigningKey.publicKey),
    lookups: {
      async receiveStatePack(repoId, pack, ref, commitSha) {
        if (repoId.kind !== "agent-state") {
          throw new Error(
            `deploy-flow test mock received unsupported repo kind ${JSON.stringify(repoId.kind)}`,
          );
        }
        const agentAddress = repoId.id;
        const agentId = parseAgentId(agentAddress);
        // Mirror createHubSessionLookups' fallback branch only: catch
        // every receive failure and surface it as a structured
        // "corrupt" rejection, so a transient (e.g. the agent
        // directory being torn down concurrently with an in-flight
        // pack write) does not propagate as an unhandled rejection
        // through the WebSocket message handler. The production
        // lookups distinguish a path_violation prefix and report that
        // as a separate reason; this mock does not, because this test
        // never exercises tree-validator rejection.
        try {
          await agentRepoStore.receiveStatePack(
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
    },
  });
  router.events.on("agent.event", ({ agentAddress, sessionId, event }) => {
    agentEvents.push({ addr: agentAddress, sid: sessionId, event });
  });
  router.events.on("agent.deploy.ack", ({ agentAddress, publicKey }) => {
    deployAcks.set(agentAddress, publicKey);
  });

  // Initialize a `package-registry` asset repo on the hub-side
  // RepoStore and seed it with a synthetic `@intx/tools-mail` tarball.
  // The session-service's tool-package path reads the asset's tree
  // through both the AssetService (for the resolver's per-tarball
  // lookups) and the RepoStore (for the resolver-derived asset pack
  // sent to the sidecar), so both surfaces must agree on the asset
  // contents. Building the asset against the real RepoStore is the
  // shortest path that keeps those surfaces in sync.
  const REGISTRY_NAME = "workspace-builtins";
  const ASSET_ID = `ast_${REGISTRY_NAME.replace(/-/g, "_")}`;
  const TARBALL_FILENAME = "tools-mail-0.1.2.tgz";
  const TENANT_ID = "tenant-1";
  const tarballBytes = await buildSyntheticToolsMailTarball();
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
  // session-service tool-package path actually consults: blob reads
  // for the resolver, tenant-walk and asset list for the registry map
  // build, and a session_asset insert/delete for audit. The other
  // members of the AssetService and DB interfaces throw on access so
  // the test fails loudly if the production code drifts into a
  // dependency the stub does not cover.
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
    statePackReceiveFailures,
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
  const parts: string[] = [];
  if (sidecarStderr.length > 0) {
    parts.push(`sidecar stderr:\n${sidecarStderr.slice(-20).join("")}`);
  }
  const failures = hub?.statePackReceiveFailures ?? [];
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
}

const AGENT_ADDRESS = "test-agent@integration.interchange";
const AGENT_ID = "test-agent";
const SESSION_ID = "ses_integration-1";
const SIDECAR_ID = "sc-integration-1";
const TOKEN = "test-token";

beforeAll(async () => {
  hub = await startHub();
  inference = startMockInference();
  sidecarDataDir = await makeTempDir("sidecar-data-");

  const hubPort = hub.server.port;

  sidecarProc = Bun.spawn(["bun", "run", "apps/sidecar/src/index.ts"], {
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
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- Subprocess is declared without generics; with stderr: "pipe" the actual type is ReadableStream<Uint8Array>
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
      sources: [
        {
          id: "anthropic:mock-model",
          provider: "anthropic",
          baseURL: `http://localhost:${inference.server.port}`,
          apiKey: "sk-mock",
          model: "mock-model",
        },
      ],
      defaultSource: "anthropic:mock-model",
    };

    await hub.sessionService.launchSession({
      agentAddress: AGENT_ADDRESS,
      agentId: AGENT_ID,
      instanceId: AGENT_ID,
      config,
      deployContent: {
        systemPrompt: "You are an integration test agent.",
      },
      // The hub is wired with a `workspace-builtins` package-registry
      // asset containing a synthetic `@intx/tools-mail@0.1.2` tarball;
      // pinning here exercises the resolver → asset-pack fan-out →
      // sidecar loader path end-to-end. The model-facing tool surfaces
      // below as `@intx/tools-mail/sidecar-bundle:mail_send`.
      toolPackagePins: [{ name: "@intx/tools-mail", version: "0.1.2" }],
    });

    // The deploy ack should have arrived (provision phase completed).
    const publicKey = hub.deployAcks.get(AGENT_ADDRESS);
    expect(publicKey).toBeDefined();
    if (publicKey === undefined) throw new Error("unreachable");
    expect(publicKey.length).toBeGreaterThan(0);

    // The agent should now be routable (session start completed).
    expect(hub.router.getRoutableAddresses()).toContain(AGENT_ADDRESS);

    // The deploy tree should have landed on the sidecar's disk.
    const agentDir = createSidecarRepoStore({
      dataDir: sidecarDataDir,
    }).getAgentDir(AGENT_ADDRESS);

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
  });

  test("send message and inference receives the asset-backed mail tool", async () => {
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
      text: "Hello.",
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
    // The synthetic tarball seeded into the workspace-builtins
    // package-registry asset publishes a single `mail_send`
    // definition under the `@intx/tools-mail/sidecar-bundle` factory
    // id; the loader prefixes the definition name with the factory id
    // to yield the qualified tool name the model sees.
    expect(toolNames).toContain("@intx/tools-mail/sidecar-bundle:mail_send");

    // reactor.start may or may not have arrived before eventsBefore was
    // captured (it depends on how fast contextStore.load() resolves), so
    // wait until we see an inference.start event rather than assuming
    // it is the very first new event.
    function hasEventType(
      event: unknown,
      type: string,
    ): event is { type: string } {
      return (
        typeof event === "object" &&
        event !== null &&
        "type" in event &&
        event.type === type
      );
    }

    await waitFor(
      () =>
        hub.agentEvents
          .slice(eventsBefore)
          .some((e) => hasEventType(e.event, "inference.start")),
      { diagnostics: sidecarDiagnostics },
    );

    const inferenceStartEvent = hub.agentEvents
      .slice(eventsBefore)
      .find((e) => hasEventType(e.event, "inference.start"));
    if (inferenceStartEvent === undefined) throw new Error("unreachable");
    expect(inferenceStartEvent.addr).toBe(AGENT_ADDRESS);
    expect(inferenceStartEvent.sid).toBe(SESSION_ID);
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
    const agentDir = createSidecarRepoStore({
      dataDir: sidecarDataDir,
    }).getAgentDir(AGENT_ADDRESS);
    const dirExists = await fs.promises
      .access(agentDir)
      .then(() => true)
      .catch(() => false);
    expect(dirExists).toBe(false);
  });
});
