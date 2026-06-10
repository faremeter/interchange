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
import { Hono } from "hono";
import { upgradeWebSocket, websocket } from "hono/bun";
import type { Subprocess } from "bun";

import {
  createAgentRepoStore,
  createSessionService,
  createSidecarRouter,
  DEFAULT_ASSET_REF,
  parseAgentId,
  type AssetService,
  type SidecarRouter,
  type SessionService,
  type WsHandle,
} from "@intx/hub-sessions";
import { hexEncode } from "@intx/types";
import { generateKeyPair } from "@intx/crypto-node";

export const AGENT_ADDRESS = "test-agent@integration.interchange";
export const AGENT_ID = "test-agent";
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

export type InferenceRequest = {
  tools?: InferenceTool[];
};

export type MockInference = {
  server: ReturnType<typeof Bun.serve>;
  requests: InferenceRequest[];
};

// Mock inference server
//
// Returns a canned Anthropic-style SSE assistant response that includes the
// tool names it was given in the request. This lets tests assert that the
// harness passed the deploy-tree tools through to inference.
export function startMockInference(): MockInference {
  const requests: InferenceRequest[] = [];

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- this is a test mock server that only receives requests from the sidecar under test; the shape is known
      const body = (await req.json()) as InferenceRequest;
      requests.push(body);

      const toolNames = (body.tools ?? []).map((t) => t.name);
      const text = `I see these tools: ${toolNames.join(", ")}`;

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

export type HubEnv = {
  server: ReturnType<typeof Bun.serve>;
  router: SidecarRouter;
  sessionService: SessionService;
  agentEvents: { addr: string; sid: string; event: unknown }[];
  deployAcks: Map<string, string>;
  statePacks: { agentAddress: string; ref: string; commitSha: string }[];
  statePackReceiveFailures: { agentAddress: string; error: string }[];
  hubDataDir: string;
};

// Hub WebSocket server (in-process) wired against a real AgentRepoStore
// and SessionService.
//
// The hub seeds a `package-registry` asset repo with a synthetic
// `@intx/tools-mail@0.1.2` tarball so the session-service tool-package
// resolver path exercises the real registry walker end-to-end.
export async function startHub(
  registerTempDir: (dir: string) => void,
): Promise<HubEnv> {
  const agentEvents: HubEnv["agentEvents"] = [];
  const deployAcks = new Map<string, string>();
  const statePacks: HubEnv["statePacks"] = [];
  const statePackReceiveFailures: HubEnv["statePackReceiveFailures"] = [];

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
      async receiveStatePack(repoId, pack, ref, commitSha) {
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

  const tarballBytes = await buildSyntheticToolsMailTarball(registerTempDir);
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

export type SidecarHandle = {
  proc: Subprocess;
  dataDir: string;
  /** Rolling stderr buffer; capped at 50 chunks. */
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
      if (stderr.length > 50) stderr.shift();
    }
  })();

  return { proc, dataDir, stderr };
}

export type DeployFlowEnv = {
  hub: HubEnv;
  inference: MockInference;
  sidecar: SidecarHandle;
  sidecarDiagnostics: () => string;
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
};

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

  const hub = await startHub(registerTempDir);
  const inference = startMockInference();

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
      parts.push(`sidecar stderr:\n${sidecar.stderr.slice(-20).join("")}`);
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

  const teardown = async (): Promise<void> => {
    // Wait for the sidecar subprocess to fully exit before removing
    // its data directory. The earlier shape (kill, then immediately
    // rm) raced the sidecar's still-open file handles inside
    // `sidecar-data-*`; on a slow CI host the rm would observe EBUSY,
    // EACCES, or partial removal, which the `.catch(() => {})`
    // shrouded. Errors must surface from the rm, so the catch is
    // dropped here.
    sidecar.proc.kill();
    await sidecar.proc.exited;
    hub.server.stop(true);
    inference.server.stop(true);
    for (const d of tempDirs.splice(0)) {
      await fs.promises.rm(d, { recursive: true, force: true });
    }
  };

  return { hub, inference, sidecar, sidecarDiagnostics, teardown };
}
