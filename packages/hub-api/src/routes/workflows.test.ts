import { describe, test, expect, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { type, type Type } from "arktype";

import { createInMemoryGrantStore } from "@intx/authz";
import { base64Decode, ErrorResponse } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import {
  deriveDeploymentAddress,
  deriveWorkflowRunRepoId,
} from "@intx/workflow-deploy";

import { createApp } from "../app";
import {
  createSidecarEmitter,
  type AssetService,
  type DeployWorkflowDefinitionParams,
  type DeployWorkflowDefinitionResult,
  type EventCollectorRegistry,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { GetSession } from "../session";

const TENANT_ID = "tnt_test";
const PRINCIPAL_ID = "prn_test";
const USER_ID = "usr_test";
const DOMAIN = "test.example.com";
const ASSET_ID = "ast_workflow";
const DEPLOYMENT_ID = "dep_abc";

// The sidecar's deploy router keys the workflow-run repo by the
// sanitized deployment address, NOT the bare deployment id (see
// `deriveDeploymentId` -> `deriveWorkflowRunRepoId` in
// apps/sidecar/src/workflow-host-wiring.ts). The read routes must
// reconstruct the same id from `(deploymentId, tenantDomain)`; the
// run-observe tests build their on-disk repo under this derived id so a
// passing test proves the read side addresses the same repo the write
// side committed to. Keying the repo by the bare DEPLOYMENT_ID (the
// pre-fix behavior) would make these tests pass against the buggy
// bare-id reader and fail against the corrected derivation.
const WORKFLOW_RUN_REPO_ID = deriveWorkflowRunRepoId(
  deriveDeploymentAddress({
    deploymentId: DEPLOYMENT_ID,
    deploymentDomain: DOMAIN,
  }),
);

const testTenant = {
  id: TENANT_ID,
  name: "Test",
  slug: "test",
  domain: DOMAIN,
  parentId: null,
  config: null,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

const testPrincipal = {
  id: PRINCIPAL_ID,
  tenantId: TENANT_ID,
  kind: "user" as const,
  refId: USER_ID,
  status: "active" as const,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

const workflowAssetRow = {
  id: ASSET_ID,
  tenantId: TENANT_ID,
  kind: "workflow",
  name: "demo-workflow",
  displayName: null,
  creatorPrincipalId: PRINCIPAL_ID,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

const deploymentRow = {
  id: DEPLOYMENT_ID,
  tenantId: TENANT_ID,
  definitionAssetId: ASSET_ID,
  status: "deployed" as const,
  createdAt: new Date("2025-01-02"),
};

const WORKFLOW_JSON = JSON.stringify({
  id: "wf_demo",
  triggers: [{ type: "manual" }],
  stepOrder: ["plan", "wait"],
  steps: {
    plan: {
      kind: "step",
      id: "plan",
      agent: {
        id: "planner",
        systemPrompt: "plan it",
        toolFactories: [],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "m" }] },
      },
      after: [],
    },
    wait: { kind: "awaitSignal", id: "wait", name: "go", after: ["plan"] },
  },
});

function makeGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-test",
    resource: "workflow:*",
    action: "read",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
    ...overrides,
  };
}

type MockDBOpts = {
  assetRow?: typeof workflowAssetRow | undefined;
  deploymentRow?: typeof deploymentRow | undefined;
  deploymentList?: (typeof deploymentRow)[];
};

function createMockDB(opts: MockDBOpts) {
  const list = opts.deploymentList ?? [];
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: { findFirst: async () => testTenant },
      principal: { findFirst: async () => testPrincipal },
      asset: { findFirst: async () => opts.assetRow },
      workflowDeployment: { findFirst: async () => opts.deploymentRow },
    },
    select: () => ({
      from: () => ({
        where: () => ({
          orderBy: () => Promise.resolve(list),
        }),
      }),
    }),
    insert: () => ({ values: () => Promise.resolve() }),
  } as unknown as Parameters<typeof createApp>[0]["db"];
}

function createMockGetSession(): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: USER_ID,
      email: "test@example.com",
      emailVerified: true,
      name: "Test User",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId: USER_ID,
      token: "tok_test",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

type SignalCall = Parameters<SidecarRouter["sendSignalDeliver"]>[0];
type RouteMailCall = { address: string; rawMessage: string };

function createMockSidecarRouter(
  signalCalls: SignalCall[],
  routeMailCalls: RouteMailCall[] = [],
  routeMailResult = true,
): SidecarRouter {
  function notImpl(name: string): never {
    throw new Error(`mock: sidecarRouter.${name} not implemented`);
  }
  return {
    handleOpen: () => notImpl("handleOpen"),
    handleMessage: () => notImpl("handleMessage"),
    handleClose: () => notImpl("handleClose"),
    routeMail: (address, rawMessage) => {
      routeMailCalls.push({ address, rawMessage });
      return routeMailResult;
    },
    sendAgentDeploy: () => notImpl("sendAgentDeploy"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
    sendPack: () => notImpl("sendPack"),
    sendProvisionStep: () => notImpl("sendProvisionStep"),
    bindStepRoute: () => notImpl("bindStepRoute"),
    unbindStepRoute: () => notImpl("unbindStepRoute"),
    sendSyncRequest: () => notImpl("sendSyncRequest"),
    sendSignalDeliver: (opts) => {
      signalCalls.push(opts);
    },
    sendDrain: () => notImpl("sendDrain"),
    subscribeAgent: () => notImpl("subscribeAgent"),
    dispatchAgentEvent: () => undefined,
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => [],
    getConnectorState: () => null,
    events: createSidecarEmitter(),
  };
}

function createMockSessionService(
  deployCalls: DeployWorkflowDefinitionParams[],
  result?: DeployWorkflowDefinitionResult,
): SessionService {
  function notImpl(name: string): never {
    throw new Error(`mock: sessionService.${name} not implemented`);
  }
  return {
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    deployInstanceAtHead: () => notImpl("deployInstanceAtHead"),
    deployWorkflowDefinition: (params) => {
      deployCalls.push(params);
      if (result === undefined) {
        throw new Error("deploy failed");
      }
      return Promise.resolve(result);
    },
    deploySingleStepAtHead: () => notImpl("deploySingleStepAtHead"),
    sendUserMessage: () => notImpl("sendUserMessage"),
    endSession: () => notImpl("endSession"),
  };
}

function createMockAssetService(workflowJson: string | null): AssetService {
  function notImpl(name: string): never {
    throw new Error(`mock: assetService.${name} not implemented`);
  }
  return {
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    attachAsset: () => notImpl("attachAsset"),
    listAgentAssets: () => notImpl("listAgentAssets"),
    readAssetBlob: async () => {
      if (workflowJson === null) {
        throw new Error("no blob");
      }
      return new TextEncoder().encode(workflowJson);
    },
    listAssetBlobs: () => notImpl("listAssetBlobs"),
  };
}

function createStubRepoStore(repoDirById?: Map<string, string>): RepoStore {
  // The deploy/signal/trigger routes never read the repoStore; only the
  // run-observe routes do, via `getRepoDir`. Tests that exercise those
  // routes pass a `repoDirById` map pointing at a constructed on-disk
  // workflow-run repo. The remaining methods throw so any drift onto a
  // substrate method these routes do not own fails loudly.
  const unused = () =>
    Promise.reject(new Error("stub repoStore is not wired in workflow tests"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    writeTreeDelta: unused,
    receivePack: unused,
    createPack: unused,
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: (repoId) => {
      const dir = repoDirById?.get(repoId.id);
      if (dir === undefined) {
        throw new Error(
          `stub repoStore has no dir for ${repoId.id} in workflow tests`,
        );
      }
      return dir;
    },
    subscribe: () => {
      throw new Error("stub repoStore is not wired in workflow tests");
    },
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
  function notImpl(name: string): never {
    throw new Error(`mock: eventCollectors.${name} not implemented`);
  }
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

type TestAppOpts = {
  db?: MockDBOpts;
  grants?: GrantRule[];
  signalCalls?: SignalCall[];
  routeMailCalls?: RouteMailCall[];
  routeMailResult?: boolean;
  deployCalls?: DeployWorkflowDefinitionParams[];
  deployResult?: DeployWorkflowDefinitionResult;
  workflowJson?: string | null;
  repoDirById?: Map<string, string>;
};

function createTestApp(opts: TestAppOpts = {}) {
  const db = createMockDB(
    opts.db ?? { assetRow: workflowAssetRow, deploymentRow },
  );
  return createApp({
    getSession: createMockGetSession(),
    authHandler: () => new Response("", { status: 404 }),
    db,
    grantStore: createInMemoryGrantStore(opts.grants ?? [makeGrant()]),
    sidecarRouter: createMockSidecarRouter(
      opts.signalCalls ?? [],
      opts.routeMailCalls ?? [],
      opts.routeMailResult ?? true,
    ),
    sessionService: createMockSessionService(
      opts.deployCalls ?? [],
      opts.deployResult,
    ),
    eventCollectors: createMockEventCollectors(),
    assetService: createMockAssetService(
      opts.workflowJson === undefined ? WORKFLOW_JSON : opts.workflowJson,
    ),
    repoStore: createStubRepoStore(opts.repoDirById),
    maxTarballBytes: 10_000_000,
  });
}

function base(tenantId = TENANT_ID): string {
  return `/api/tenants/${tenantId}/workflows`;
}

function authedPost(path: string, body: unknown): Request {
  return new Request(`http://localhost${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function errorCode(res: Response): Promise<string> {
  const parsed = ErrorResponse(await res.json());
  if (parsed instanceof type.errors) {
    throw new Error(`unexpected error body: ${parsed.summary}`);
  }
  return parsed.error.code;
}

const TriggerBody = type({
  deploymentId: "string",
  address: "string",
  messageId: "string",
});

const RunListBody = type({ runIds: "string[]" });

const RunEventsBody = type({
  runId: "string",
  events: type({ seq: "number", type: "string", body: "object" }).array(),
});

function assertBody<T extends Type>(schema: T, raw: unknown): T["infer"] {
  const parsed = schema(raw);
  if (parsed instanceof type.errors) {
    throw new Error(`unexpected response body: ${parsed.summary}`);
  }
  return parsed;
}

describe("POST /workflows/instances", () => {
  test("deploys a workflow and returns the deployment record", async () => {
    const deployCalls: DeployWorkflowDefinitionParams[] = [];
    const app = createTestApp({
      grants: [makeGrant({ action: "create" })],
      deployCalls,
      deployResult: {
        deploymentId: DEPLOYMENT_ID,
        deploymentAddress: `ins_${DEPLOYMENT_ID}@${DOMAIN}`,
        publicKey: "pubkey",
      },
    });

    const res = await app.fetch(
      authedPost(`${base()}/instances`, {
        assetId: ASSET_ID,
        sources: [
          {
            id: "src",
            provider: "anthropic",
            baseURL: "https://api.example",
            apiKey: "secret",
            model: "m",
          },
        ],
        defaultSource: "src",
      }),
    );

    expect(res.status).toBe(201);
    const json = await res.json();
    expect(json).toMatchObject({
      id: DEPLOYMENT_ID,
      definitionAssetId: ASSET_ID,
    });

    expect(deployCalls).toHaveLength(1);
    const call = deployCalls[0];
    if (call === undefined) throw new Error("missing deploy call");
    expect(call.deploymentDomain).toBe(DOMAIN);
    expect(call.definitionAssetId).toBe(ASSET_ID);
    expect(call.definition.id).toBe("wf_demo");
    expect(call.definition.stepOrder).toEqual(["plan", "wait"]);
  });

  test("rejects a caller without the workflow create grant", async () => {
    const app = createTestApp({ grants: [] });
    const res = await app.fetch(
      authedPost(`${base()}/instances`, {
        assetId: ASSET_ID,
        sources: [
          {
            id: "src",
            provider: "anthropic",
            baseURL: "https://api.example",
            apiKey: "secret",
            model: "m",
          },
        ],
        defaultSource: "src",
      }),
    );
    expect(res.status).toBe(403);
  });

  test("reports a sidecar deploy failure as 502 sidecar_unavailable", async () => {
    // Omitting deployResult makes the session-service mock throw, which
    // is the sidecar-unavailable path.
    const app = createTestApp({
      grants: [makeGrant({ action: "create" })],
    });
    const res = await app.fetch(
      authedPost(`${base()}/instances`, {
        assetId: ASSET_ID,
        sources: [
          {
            id: "src",
            provider: "anthropic",
            baseURL: "https://api.example",
            apiKey: "secret",
            model: "m",
          },
        ],
        defaultSource: "src",
      }),
    );
    expect(res.status).toBe(502);
    expect(await errorCode(res)).toBe("sidecar_unavailable");
  });

  test("reports a missing post-deploy projection row as 500, not 502", async () => {
    const app = createTestApp({
      grants: [makeGrant({ action: "create" })],
      db: { assetRow: workflowAssetRow, deploymentRow: undefined },
      deployResult: {
        deploymentId: DEPLOYMENT_ID,
        deploymentAddress: `ins_${DEPLOYMENT_ID}@${DOMAIN}`,
        publicKey: "pubkey",
      },
    });
    const res = await app.fetch(
      authedPost(`${base()}/instances`, {
        assetId: ASSET_ID,
        sources: [
          {
            id: "src",
            provider: "anthropic",
            baseURL: "https://api.example",
            apiKey: "secret",
            model: "m",
          },
        ],
        defaultSource: "src",
      }),
    );
    expect(res.status).toBe(500);
    expect(await errorCode(res)).toBe("deployment_projection_missing");
  });

  test("returns 404 when the workflow asset is missing", async () => {
    const app = createTestApp({
      grants: [makeGrant({ action: "create" })],
      db: { assetRow: undefined },
    });
    const res = await app.fetch(
      authedPost(`${base()}/instances`, {
        assetId: ASSET_ID,
        sources: [
          {
            id: "src",
            provider: "anthropic",
            baseURL: "https://api.example",
            apiKey: "secret",
            model: "m",
          },
        ],
        defaultSource: "src",
      }),
    );
    expect(res.status).toBe(404);
  });
});

describe("GET /workflows/instances", () => {
  test("lists the tenant's workflow deployments", async () => {
    const app = createTestApp({
      db: { deploymentList: [deploymentRow] },
    });
    const res = await app.fetch(
      new Request(`http://localhost${base()}/instances`),
    );
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toEqual([
      {
        id: DEPLOYMENT_ID,
        tenantId: TENANT_ID,
        definitionAssetId: ASSET_ID,
        status: "deployed",
        createdAt: deploymentRow.createdAt.toISOString(),
      },
    ]);
  });
});

describe("POST /workflows/:deploymentId/signals", () => {
  function manageGrant(): GrantRule {
    return makeGrant({
      resource: `workflow-run:${DEPLOYMENT_ID}`,
      action: "manage",
    });
  }

  test("delivers a caller-supplied signal to the deployment supervisor", async () => {
    const signalCalls: SignalCall[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      signalCalls,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/signals`, {
        runId: "run-1",
        signalName: "go",
        signalId: "sig-caller-1",
        payload: { ok: true },
      }),
    );

    expect(res.status).toBe(202);
    expect(signalCalls).toHaveLength(1);
    const call = signalCalls[0];
    if (call === undefined) throw new Error("missing signal call");
    expect(call.agentAddress).toBe(`ins_${DEPLOYMENT_ID}@${DOMAIN}`);
    expect(call.runId).toBe("run-1");
    expect(call.signalName).toBe("go");
    // The signalId is the caller-supplied stable id, never server-minted.
    expect(call.signalId).toBe("sig-caller-1");
    expect(call.payload).toEqual({ ok: true });
  });

  test("accepts a payload-less signal with 202", async () => {
    const signalCalls: SignalCall[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      signalCalls,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/signals`, {
        runId: "run-1",
        signalName: "go",
        signalId: "sig-caller-2",
      }),
    );

    expect(res.status).toBe(202);
    expect(signalCalls).toHaveLength(1);
    const call = signalCalls[0];
    if (call === undefined) throw new Error("missing signal call");
    expect(call.signalId).toBe("sig-caller-2");
    expect(call.payload).toBeUndefined();
  });

  test("rejects a caller without the workflow-run manage grant", async () => {
    const signalCalls: SignalCall[] = [];
    const app = createTestApp({
      grants: [makeGrant({ resource: "workflow:*", action: "read" })],
      signalCalls,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/signals`, {
        runId: "run-1",
        signalName: "go",
        signalId: "sig-caller-1",
        payload: null,
      }),
    );

    expect(res.status).toBe(403);
    expect(signalCalls).toHaveLength(0);
  });

  test("rejects a blank caller-supplied signalId at the boundary", async () => {
    const signalCalls: SignalCall[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      signalCalls,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/signals`, {
        runId: "run-1",
        signalName: "go",
        signalId: "",
        payload: null,
      }),
    );

    expect(res.status).toBe(400);
    expect(signalCalls).toHaveLength(0);
  });

  test("returns 404 when the deployment does not exist", async () => {
    const app = createTestApp({
      grants: [manageGrant()],
      db: { deploymentRow: undefined },
    });
    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/signals`, {
        runId: "run-1",
        signalName: "go",
        signalId: "sig-1",
        payload: null,
      }),
    );
    expect(res.status).toBe(404);
  });
});

describe("POST /workflows/:deploymentId/mail", () => {
  function manageGrant(): GrantRule {
    return makeGrant({
      resource: `workflow-run:${DEPLOYMENT_ID}`,
      action: "manage",
    });
  }

  test("fires a run by routing mail to the deployment address", async () => {
    const routeMailCalls: RouteMailCall[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      routeMailCalls,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(202);
    const json = assertBody(TriggerBody, await res.json());
    expect(json.deploymentId).toBe(DEPLOYMENT_ID);
    expect(json.address).toBe(`ins_${DEPLOYMENT_ID}@${DOMAIN}`);
    expect(json.messageId.length).toBeGreaterThan(0);

    expect(routeMailCalls).toHaveLength(1);
    const call = routeMailCalls[0];
    if (call === undefined) throw new Error("missing routeMail call");
    expect(call.address).toBe(`ins_${DEPLOYMENT_ID}@${DOMAIN}`);
    // The wire payload is base64-encoded MIME carrying the body text.
    const decoded = new TextDecoder().decode(base64Decode(call.rawMessage));
    expect(decoded).toContain("kick off");
    // A run trigger is threading-less: no In-Reply-To / References.
    expect(decoded).not.toContain("In-Reply-To");
    expect(decoded).not.toContain("References:");
  });

  test("surfaces an unroutable deployment address as 409", async () => {
    const routeMailCalls: RouteMailCall[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      routeMailCalls,
      routeMailResult: false,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(409);
    expect(await errorCode(res)).toBe("deployment_unreachable");
    expect(routeMailCalls).toHaveLength(1);
  });

  test("rejects a caller without the workflow-run manage grant", async () => {
    const routeMailCalls: RouteMailCall[] = [];
    const app = createTestApp({
      grants: [makeGrant({ resource: "workflow:*", action: "read" })],
      routeMailCalls,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(403);
    expect(routeMailCalls).toHaveLength(0);
  });

  test("returns 404 when the deployment does not exist", async () => {
    const routeMailCalls: RouteMailCall[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      routeMailCalls,
      db: { deploymentRow: undefined },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(404);
    expect(routeMailCalls).toHaveLength(0);
  });
});

// On-disk workflow-run repos backing the run-observe route tests. Each
// repo is laid out by hand with isomorphic-git so the reader projects a
// real tree; the substrate's push-time validation is exercised
// elsewhere and is not the unit under test here.
const runRepoDirs: string[] = [];

afterAll(async () => {
  for (const dir of runRepoDirs.splice(0)) {
    await fs.promises.rm(dir, { recursive: true, force: true });
  }
});

async function buildRunRepo(
  repoId: string,
  files: Record<string, string>,
): Promise<Map<string, string>> {
  const dir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "wf-run-route-"),
  );
  runRepoDirs.push(dir);
  await git.init({ fs, dir, defaultBranch: "main" });
  for (const [rel, content] of Object.entries(files)) {
    const full = path.join(dir, rel);
    await fs.promises.mkdir(path.dirname(full), { recursive: true });
    await fs.promises.writeFile(full, content);
    await git.add({ fs, dir, filepath: rel });
  }
  await git.commit({
    fs,
    dir,
    message: "seed run events",
    author: { name: "test", email: "test@example.com" },
  });
  return new Map([[repoId, dir]]);
}

describe("GET /workflows/:deploymentId/runs", () => {
  function readGrant(): GrantRule {
    return makeGrant({
      resource: `workflow-run:${DEPLOYMENT_ID}`,
      action: "read",
    });
  }

  test("lists the run ids for the deployment", async () => {
    const repoDirById = await buildRunRepo(WORKFLOW_RUN_REPO_ID, {
      "runs/run-a/events/0.json": JSON.stringify({ type: "RunStarted" }),
      "runs/run-b/events/0.json": JSON.stringify({ type: "RunStarted" }),
    });
    const app = createTestApp({
      grants: [readGrant()],
      repoDirById,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      new Request(`http://localhost${base()}/${DEPLOYMENT_ID}/runs`),
    );
    expect(res.status).toBe(200);
    const json = assertBody(RunListBody, await res.json());
    expect([...json.runIds].sort()).toEqual(["run-a", "run-b"]);
  });

  test("reads the sidecar-derived repo id, not the bare deployment id", async () => {
    // The derived workflow-run repo id must differ from the bare
    // deployment id for this guard to bite; the deployment address
    // carries `@` and `.`, which the sanitizer rewrites to `-`.
    expect(WORKFLOW_RUN_REPO_ID).not.toBe(DEPLOYMENT_ID);

    // A repo committed only under the BARE deployment id (the pre-fix
    // write target the buggy reader looked at) must NOT be found: the
    // corrected reader addresses the sanitized id, so this run is
    // invisible. This is the test that fails against the bare-id reader
    // (it would return ["run-x"]) and passes against the fix.
    const bareRepo = await buildRunRepo(DEPLOYMENT_ID, {
      "runs/run-x/events/0.json": JSON.stringify({ type: "RunStarted" }),
    });
    const app = createTestApp({
      grants: [readGrant()],
      repoDirById: bareRepo,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      new Request(`http://localhost${base()}/${DEPLOYMENT_ID}/runs`),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ runIds: [] });
  });

  test("returns an empty list when no run has committed events", async () => {
    const repoDirById = new Map<string, string>();
    const app = createTestApp({
      grants: [readGrant()],
      repoDirById,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      new Request(`http://localhost${base()}/${DEPLOYMENT_ID}/runs`),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ runIds: [] });
  });

  test("rejects a caller without the workflow-run read grant", async () => {
    const app = createTestApp({
      grants: [makeGrant({ resource: "workflow:*", action: "read" })],
      db: { deploymentRow },
    });
    const res = await app.fetch(
      new Request(`http://localhost${base()}/${DEPLOYMENT_ID}/runs`),
    );
    expect(res.status).toBe(403);
  });

  test("returns 404 for an unknown deployment", async () => {
    const app = createTestApp({
      grants: [readGrant()],
      db: { deploymentRow: undefined },
    });
    const res = await app.fetch(
      new Request(`http://localhost${base()}/${DEPLOYMENT_ID}/runs`),
    );
    expect(res.status).toBe(404);
  });
});

describe("GET /workflows/:deploymentId/runs/:runId/events", () => {
  function readGrant(): GrantRule {
    return makeGrant({
      resource: `workflow-run:${DEPLOYMENT_ID}`,
      action: "read",
    });
  }

  test("returns the seq-ordered event projection", async () => {
    const repoDirById = await buildRunRepo(WORKFLOW_RUN_REPO_ID, {
      "runs/run-1/events/0.json": JSON.stringify({
        type: "RunStarted",
        consumedMessageId: "m1",
      }),
      "runs/run-1/events/2.json": JSON.stringify({ type: "RunCompleted" }),
      "runs/run-1/events/1.json": JSON.stringify({
        type: "SignalAwaited",
        name: "approve",
      }),
    });
    const app = createTestApp({
      grants: [readGrant()],
      repoDirById,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      new Request(
        `http://localhost${base()}/${DEPLOYMENT_ID}/runs/run-1/events`,
      ),
    );
    expect(res.status).toBe(200);
    const json = assertBody(RunEventsBody, await res.json());
    expect(json.runId).toBe("run-1");
    expect(json.events.map((e) => [e.seq, e.type])).toEqual([
      [0, "RunStarted"],
      [1, "SignalAwaited"],
      [2, "RunCompleted"],
    ]);
  });

  test("returns an empty event list for an unknown run", async () => {
    const repoDirById = await buildRunRepo(WORKFLOW_RUN_REPO_ID, {
      "runs/run-1/events/0.json": JSON.stringify({ type: "RunStarted" }),
    });
    const app = createTestApp({
      grants: [readGrant()],
      repoDirById,
      db: { deploymentRow },
    });

    const res = await app.fetch(
      new Request(
        `http://localhost${base()}/${DEPLOYMENT_ID}/runs/missing/events`,
      ),
    );
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ runId: "missing", events: [] });
  });

  test("rejects a caller without the workflow-run read grant", async () => {
    const app = createTestApp({
      grants: [makeGrant({ resource: "workflow:*", action: "read" })],
      db: { deploymentRow },
    });
    const res = await app.fetch(
      new Request(
        `http://localhost${base()}/${DEPLOYMENT_ID}/runs/run-1/events`,
      ),
    );
    expect(res.status).toBe(403);
  });

  test("returns 404 for an unknown deployment", async () => {
    const app = createTestApp({
      grants: [readGrant()],
      db: { deploymentRow: undefined },
    });
    const res = await app.fetch(
      new Request(
        `http://localhost${base()}/${DEPLOYMENT_ID}/runs/run-1/events`,
      ),
    );
    expect(res.status).toBe(404);
  });
});
