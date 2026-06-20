import { describe, test, expect } from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";

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

function createMockSidecarRouter(signalCalls: SignalCall[]): SidecarRouter {
  function notImpl(name: string): never {
    throw new Error(`mock: sidecarRouter.${name} not implemented`);
  }
  return {
    handleOpen: () => notImpl("handleOpen"),
    handleMessage: () => notImpl("handleMessage"),
    handleClose: () => notImpl("handleClose"),
    routeMail: () => notImpl("routeMail"),
    sendAgentDeploy: () => notImpl("sendAgentDeploy"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSessionStart: () => notImpl("sendSessionStart"),
    sendSessionAbort: () => notImpl("sendSessionAbort"),
    sendGrantsUpdate: () => notImpl("sendGrantsUpdate"),
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
    sendPack: () => notImpl("sendPack"),
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
    launchSession: () => notImpl("launchSession"),
    deployWorkflowDefinition: (params) => {
      deployCalls.push(params);
      if (result === undefined) {
        throw new Error("deploy failed");
      }
      return Promise.resolve(result);
    },
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

function createStubRepoStore(): RepoStore {
  // The workflow routes read the definition via assetService, never the
  // repoStore. app.ts requires assetService and repoStore to move as a
  // unit, so a throwing stub keeps the XOR happy without exercising the
  // substrate from any path these tests drive.
  const unused = () =>
    Promise.reject(new Error("stub repoStore is not wired in workflow tests"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    receivePack: unused,
    createPack: unused,
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => {
      throw new Error("stub repoStore is not wired in workflow tests");
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
  deployCalls?: DeployWorkflowDefinitionParams[];
  deployResult?: DeployWorkflowDefinitionResult;
  workflowJson?: string | null;
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
    sidecarRouter: createMockSidecarRouter(opts.signalCalls ?? []),
    sessionService: createMockSessionService(
      opts.deployCalls ?? [],
      opts.deployResult,
    ),
    eventCollectors: createMockEventCollectors(),
    assetService: createMockAssetService(
      opts.workflowJson === undefined ? WORKFLOW_JSON : opts.workflowJson,
    ),
    repoStore: createStubRepoStore(),
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
