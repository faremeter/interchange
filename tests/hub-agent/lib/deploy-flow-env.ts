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
  createAgentRepoStore,
  createSessionService,
  createSidecarRouter,
  dequeueToProcessing,
  enqueueInbox,
  DEFAULT_ASSET_REF,
  parseAgentId,
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
  type AgentRepoStore,
  type AssetService,
  type RepoId,
  type SidecarRouter,
  type SessionService,
  type WorkflowRunHubPrincipal,
  type WsHandle,
} from "@intx/hub-sessions";
import { hexEncode } from "@intx/types";
import { createNodeCrypto, generateKeyPair } from "@intx/crypto-node";
import {
  createWorkflowDeployOrchestrator,
  type LaunchSessionFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";
import { createDefaultDirectorRegistry } from "@intx/agent";
import type { HarnessConfig } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { ApprovalSet } from "@intx/workflow-deploy";
import type { WorkflowDefinition } from "@intx/workflow";
import { deriveTrivialDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";

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
  agentRepoStore: AgentRepoStore;
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
    agentRepoStore,
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
    // Wait for the sidecar subprocess to fully exit before removing
    // its data directory. The earlier shape (kill, then immediately
    // rm) raced the sidecar's still-open file handles inside
    // `sidecar-data-*`; on a slow CI host the rm would observe EBUSY,
    // EACCES, or partial removal, which the `.catch(() => {})`
    // shrouded. Errors must surface from the rm, so the catch is
    // dropped here.
    deployments.clear();
    sidecar.proc.kill();
    await sidecar.proc.exited;
    hub.server.stop(true);
    inference.server.stop(true);
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
 * workflow-deploy orchestrator against the env's hub substrate and
 * routes per-step launches through `env.hub.sessionService.launchSession`
 * so the trivial-branch round-trip and the multi-step branch both
 * exercise the production code paths.
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
  /** Pre-existing per-agent address binding for the trivial branch. */
  trivialBindings?: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
  };
  /**
   * Stable identifier the multi-step branch concatenates into derived
   * agent addresses. Required when `trivialBindings` is absent.
   */
  deploymentId?: string;
  /**
   * Mail-domain for the deployment. Required when `trivialBindings` is
   * absent. Defaults to the integration test's canonical domain so
   * callers that exercise the multi-step branch with the default
   * fixture wiring do not have to thread the domain through.
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
   * the trivial bindings' `agentAddress` (trivial branch) or
   * `ins_<deploymentId>@<deploymentDomain>` (multi-step branch).
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
 * substrate and run it against the supplied workflow. Trivial
 * single-step workflows route through `env.hub.sessionService.launchSession`
 * (which itself invokes the orchestrator's trivial branch); multi-step
 * workflows derive per-step addresses and route each launch through
 * the same `launchSession` callback.
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

  let deploymentId: string;
  let mailAddress: string;
  if (opts.trivialBindings !== undefined) {
    if (workflow.stepOrder.length !== 1) {
      throw new Error(
        `deployWorkflow: trivialBindings supplied for a ${String(workflow.stepOrder.length)}-step workflow; trivial deploy requires exactly one step`,
      );
    }
    deploymentId = opts.trivialBindings.agentAddress;
    mailAddress =
      opts.deploymentMailAddress ?? opts.trivialBindings.agentAddress;
  } else {
    const explicit = opts.deploymentId;
    if (explicit === undefined) {
      throw new Error(
        "deployWorkflow: deploymentId is required when trivialBindings is absent",
      );
    }
    deploymentId = explicit;
    const deploymentDomain = opts.deploymentDomain ?? DEFAULT_DEPLOYMENT_DOMAIN;
    mailAddress =
      opts.deploymentMailAddress ?? `ins_${deploymentId}@${deploymentDomain}`;
  }

  // The supervisor's trivial branch projects the agent address into
  // a substrate-safe slug via `deriveTrivialDeploymentId` before
  // writing workflow-run events (see
  // `apps/sidecar/src/workflow-host-wiring.ts`). The fixture must
  // report the same slug so downstream helpers query the repo the
  // supervisor actually committed to. Multi-step deployments supply
  // their own substrate-safe `deploymentId` and pass through
  // unchanged.
  const workflowRunRepoSlug =
    opts.trivialBindings !== undefined
      ? deriveTrivialDeploymentId(opts.trivialBindings.agentAddress)
      : deploymentId;
  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: workflowRunRepoSlug,
  };

  // Route every per-step launch through the session service. The
  // session service's `launchSession` itself routes through the
  // orchestrator's trivial branch, so this preserves the bit-identical
  // trivial round-trip the existing deploy-flow test asserts.
  //
  // `launchSession`'s `deployContent` parameter widens
  // `toolPackageManifest` to `unknown` in the orchestrator's surface
  // shape; the session-service's `bridgeOrchestratorDeployContent`
  // narrows it back at the inner boundary, so the cast here only
  // crosses the structural-shape gap between the orchestrator's
  // `OrchestratorDeployContent` and the session-service's
  // `DeployContent`.
  const launchSession: LaunchSessionFn = async (orchestratorParams) => {
    const deployContent = orchestratorParams.deployContent;
    await env.hub.sessionService.launchSession({
      agentAddress: orchestratorParams.agentAddress,
      agentId: orchestratorParams.agentId,
      instanceId: orchestratorParams.instanceId,
      config: orchestratorParams.config,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the session-service's launchSession invokes the orchestrator internally and re-narrows `toolPackageManifest` via arktype; this fixture forwards the orchestrator-shaped deploy content as-is
      deployContent: deployContent as Parameters<
        SessionService["launchSession"]
      >[0]["deployContent"],
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
    config: opts.config,
    deployContent: opts.deployContent,
    operatorApprovals: opts.operatorApprovals,
    ...(opts.trivialBindings !== undefined
      ? { trivialBindings: opts.trivialBindings }
      : {}),
    ...(opts.trivialBindings === undefined
      ? {
          deploymentId,
          deploymentDomain: opts.deploymentDomain ?? DEFAULT_DEPLOYMENT_DOMAIN,
        }
      : {}),
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

/**
 * A workflow-run event as committed under `runs/<runId>/events/<seq>.json`.
 * The discriminator field is `type`; the per-type body is opaque to
 * this helper.
 */
export type WorkflowRunEvent = {
  seq: number;
  type: string;
  body: Record<string, unknown>;
};

/**
 * Read every event under `runs/<runId>/events/` from the deployment's
 * workflow-run repo and return them in ascending `seq` order. Returns
 * an empty array when the run has not yet committed any events or the
 * repo has not yet been created (e.g. the deployment hasn't taken the
 * multi-step branch yet).
 */
export async function readWorkflowRunEvents(
  env: DeployFlowEnv,
  deploymentId: string,
  runId: string,
): Promise<WorkflowRunEvent[]> {
  const handle = requireDeployment(env, deploymentId);
  const repoDir = env.hub.agentRepoStore.repoStore.getRepoDir(
    handle.workflowRunRepoId,
  );
  let oid: string;
  try {
    oid = await git.resolveRef({
      fs,
      dir: repoDir,
      ref: handle.workflowRunRef,
    });
  } catch {
    return [];
  }
  const eventsDir = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_EVENTS_DIR}`;
  let tree: Awaited<ReturnType<typeof git.readTree>>;
  try {
    tree = await git.readTree({
      fs,
      dir: repoDir,
      oid,
      filepath: eventsDir,
    });
  } catch {
    return [];
  }
  const events: WorkflowRunEvent[] = [];
  for (const entry of tree.tree) {
    if (entry.type !== "blob") continue;
    const m = /^(0|[1-9][0-9]*)\.json$/.exec(entry.path);
    if (m === null || m[1] === undefined) continue;
    const seq = Number.parseInt(m[1], 10);
    const blob = await git.readBlob({
      fs,
      dir: repoDir,
      oid: entry.oid,
    });
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow-run kind handler validates the parsed shape at push time; readers downstream of validatePush observe Record<string, unknown>
    const parsed = JSON.parse(new TextDecoder().decode(blob.blob)) as Record<
      string,
      unknown
    >;
    const type = parsed["type"];
    if (typeof type !== "string") {
      throw new Error(
        `readWorkflowRunEvents: event at ${entry.path} is missing a string \`type\` field`,
      );
    }
    events.push({ seq, type, body: parsed });
  }
  events.sort((a, b) => a.seq - b.seq);
  return events;
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
  const crypto = createNodeCrypto(keyPair);
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
  const base64 = Buffer.from(rawMessage).toString("base64");
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
      ref: "refs/heads/main",
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
  let tree: Awaited<ReturnType<typeof git.readTree>>;
  try {
    tree = await git.readTree({ fs, dir: repoDir, oid, filepath: "runs" });
  } catch (cause) {
    if (cause instanceof git.Errors.NotFoundError) return [];
    throw cause;
  }
  return tree.tree
    .filter((entry) => entry.type === "tree")
    .map((entry) => entry.path);
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
