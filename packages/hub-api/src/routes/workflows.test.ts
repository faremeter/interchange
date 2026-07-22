import { describe, test, expect, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { type, type Type } from "arktype";

import { createInMemoryGrantStore, evaluateGrants } from "@intx/authz";
import { base64Decode, ErrorResponse } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import {
  asset as assetTable,
  grant as grantTable,
  principal as principalTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import {
  createDefaultDirectorRegistry,
  defineAgent,
  type AnnotatedToolFactory,
  type BaseEnv,
  type ToolDeclaration,
} from "@intx/agent";
import { defineWorkflow } from "@intx/workflow/definition";
import {
  deriveDeploymentAddress,
  deriveWorkflowRunRepoId,
  walkCapabilities,
} from "@intx/workflow-deploy";

import { createApp } from "../app";
import { deriveRunRuntimeGrantRows } from "../run-grant-materialization";
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

const workflowAssetRow: typeof assetTable.$inferSelect = {
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

// A workflow whose sole step declares two tools, one approval-gated. The
// capability walk lifts these into the run's tool grants: the trigger route
// materializes one `tool:<name>` grant row per declared tool, carrying the
// effect the tool's static declaration requested.
const WORKFLOW_JSON_WITH_TOOLS = JSON.stringify({
  id: "wf_tools",
  triggers: [{ type: "manual" }],
  stepOrder: ["work"],
  steps: {
    work: {
      kind: "step",
      id: "work",
      agent: {
        id: "worker",
        systemPrompt: "do work",
        toolFactories: [
          {
            id: "fac",
            definitions: [
              { name: "read_file" },
              { name: "run_shell", approval: "ask" },
            ],
          },
        ],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "m" }] },
      },
      after: [],
    },
  },
});

// A workflow whose sole step is an ACTION declaring an effect requirement.
// The capability walk lifts `effect.requires` into `effect:<cap>` grant
// strings (see `collectActionGrants` in `@intx/workflow-deploy`), which the
// trigger route must materialize as run grant rows: the action EffectContext
// authorizes `effect:<cap>`/`invoke` fail-closed at runtime, so a missing row
// makes the action throw.
const WORKFLOW_JSON_WITH_EFFECT = JSON.stringify({
  id: "wf_effect",
  triggers: [{ type: "manual" }],
  stepOrder: ["commit"],
  steps: {
    commit: {
      kind: "action",
      id: "commit",
      handler: "commit",
      effect: { requires: ["git:commit"] },
      after: [],
    },
  },
});

// A workflow declaring one invoker-sourced grant requirement. At trigger
// time the requirement resolves against the run's triggerer, so a triggerer
// who holds `secret:vault`/`use` gets a materialized run grant and one who
// lacks it is rejected fail-closed -- two runs by principals of differing
// authority produce different run.grants.
const WORKFLOW_JSON_WITH_INVOKER_GRANT = JSON.stringify({
  id: "wf_invoker_grant",
  triggers: [{ type: "manual" }],
  stepOrder: ["work"],
  steps: {
    work: {
      kind: "step",
      id: "work",
      agent: {
        id: "worker",
        systemPrompt: "do work",
        toolFactories: [],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "m" }] },
      },
      after: [],
    },
  },
  grantRequirements: [
    { resource: "secret:vault", action: "use", source: "invoker" },
  ],
});

// A workflow declaring one creator-sourced grant requirement. It resolves
// against the workflow asset's creator principal, not the triggerer.
const WORKFLOW_JSON_WITH_CREATOR_GRANT = JSON.stringify({
  id: "wf_creator_grant",
  triggers: [{ type: "manual" }],
  stepOrder: ["work"],
  steps: {
    work: {
      kind: "step",
      id: "work",
      agent: {
        id: "worker",
        systemPrompt: "do work",
        toolFactories: [],
        capabilities: [],
        inference: { sources: [{ provider: "anthropic", model: "m" }] },
      },
      after: [],
    },
  },
  grantRequirements: [
    { resource: "secret:vault", action: "use", source: "creator" },
  ],
});

const CREATOR_PRINCIPAL_ID = "prn_creator";

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

// An insert the mock DB records: the drizzle table object it targeted and
// the row values. Tests inspect `inserts` to assert what was (or was not)
// committed. Insert order is preserved so a transaction body's writes are
// visible in the order they ran.
type InsertRecord = { table: unknown; values: unknown };

type MockDBOpts = {
  assetRow?: typeof workflowAssetRow | undefined;
  deploymentRow?: typeof deploymentRow | undefined;
  deploymentList?: (typeof deploymentRow)[];
  inserts?: InsertRecord[];
};

function createMockDB(opts: MockDBOpts) {
  const list = opts.deploymentList ?? [];
  const inserts = opts.inserts ?? [];
  // The mock records each insert and supports both call shapes the routes
  // use: a bare `.values(v)` (awaited directly) and the conflict-tolerant
  // `.values(v).onConflictDoNothing().returning()`. The `.returning()` here
  // always yields the inserted row (the no-conflict path); the mock does not
  // model a primary-key conflict, so the reconciliation branch is exercised
  // against a real database, not here.
  const insert = (table: unknown) => ({
    values: (values: unknown) => {
      inserts.push({ table, values });
      return Object.assign(Promise.resolve(), {
        onConflictDoNothing: () => ({
          returning: () => Promise.resolve([values]),
        }),
      });
    },
  });
  // The list-route select resolves through `.orderBy`; the commit's
  // already-materialized guard select resolves through `.limit`, returning
  // no rows so the mock always exercises the first-commit insert path (a
  // redelivery no-op is covered against a real database).
  const select = () => ({
    from: () => ({
      where: () => ({
        orderBy: () => Promise.resolve(list),
        limit: () => Promise.resolve([]),
      }),
    }),
  });
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: { findFirst: async () => testTenant },
      principal: { findFirst: async () => testPrincipal },
      asset: { findFirst: async () => opts.assetRow },
      workflowDeployment: { findFirst: async () => opts.deploymentRow },
    },
    select,
    insert,
    transaction: async (
      fn: (tx: {
        insert: typeof insert;
        select: typeof select;
      }) => Promise<void>,
    ) => {
      await fn({ insert, select });
    },
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
type RunGrantsCall = {
  address: string;
  runId: string;
  stepGrants: Parameters<SidecarRouter["sendRunGrants"]>[2];
};
// Every send the mock records, in call order, so tests can assert that a
// `run.grants` send precedes the trigger `mail`.
type SendCall = { kind: "run.grants" | "mail"; address: string };

function createMockSidecarRouter(
  signalCalls: SignalCall[],
  routeMailCalls: RouteMailCall[] = [],
  routeMailResult = true,
  runGrantsCalls: RunGrantsCall[] = [],
  sendOrder: SendCall[] = [],
  runGrantsResult = true,
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
      sendOrder.push({ kind: "mail", address });
      return routeMailResult;
    },
    sendRunGrants: (address, runId, stepGrants) => {
      runGrantsCalls.push({ address, runId, stepGrants });
      sendOrder.push({ kind: "run.grants", address });
      return runGrantsResult;
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
    commitPackedTip: () => {
      throw new Error("stub repoStore is not wired in workflow tests");
    },
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
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
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
  runGrantsCalls?: RunGrantsCall[];
  sendOrder?: SendCall[];
  runGrantsResult?: boolean;
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
      opts.runGrantsCalls ?? [],
      opts.sendOrder ?? [],
      opts.runGrantsResult ?? true,
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

// Shapes the trigger route inserts. Validated at the test boundary rather
// than cast so the mock DB's `unknown` insert values are narrowed safely.
const PrincipalInsert = type({
  id: "string",
  kind: "string",
  refId: "string",
  tenantId: "string",
  status: "string",
  "+": "ignore",
});

const GrantInsert = type({
  id: "string",
  principalId: "string",
  resource: "string",
  action: "string",
  effect: "string",
  origin: "string",
  "+": "ignore",
});

const WorkflowRunInsert = type({
  id: "string",
  deploymentId: "string",
  tenantId: "string",
  principalId: "string",
  status: "string",
  "+": "ignore",
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
      db: { deploymentRow, assetRow: workflowAssetRow },
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
      db: { deploymentRow, assetRow: workflowAssetRow },
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

  test("mints a run principal, anchors the run, and sends run.grants before the mail", async () => {
    const runGrantsCalls: RunGrantsCall[] = [];
    const routeMailCalls: RouteMailCall[] = [];
    const sendOrder: SendCall[] = [];
    const inserts: InsertRecord[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      runGrantsCalls,
      routeMailCalls,
      sendOrder,
      db: { deploymentRow, assetRow: workflowAssetRow, inserts },
      workflowJson: WORKFLOW_JSON_WITH_TOOLS,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(202);
    const json = assertBody(TriggerBody, await res.json());

    // The grants ride the wire ahead of the trigger mail, so FIFO ordering
    // lands them at the sidecar before the run dispatches.
    expect(sendOrder.map((s) => s.kind)).toEqual(["run.grants", "mail"]);

    // The run.grants frame carries the run id (= the mail's Message-ID) and
    // the definition-pure tool grants the capability walk lifted.
    expect(runGrantsCalls).toHaveLength(1);
    const grantsCall = runGrantsCalls[0];
    if (grantsCall === undefined) throw new Error("missing run.grants call");
    expect(grantsCall.address).toBe(`ins_${DEPLOYMENT_ID}@${DOMAIN}`);
    expect(grantsCall.runId).toBe(json.messageId);

    const byResource = new Map(
      grantsCall.stepGrants.map((g) => [g.resource, g]),
    );
    expect([...byResource.keys()].sort()).toEqual([
      "tool:read_file",
      "tool:run_shell",
    ]);
    for (const g of grantsCall.stepGrants) {
      expect(g.action).toBe("invoke");
      expect(g.origin).toBe("creator");
      expect(g.principalId).not.toBeNull();
    }
    // The approval-gated tool keeps its `ask` effect; the plain tool is
    // `allow`.
    expect(byResource.get("tool:read_file")?.effect).toBe("allow");
    expect(byResource.get("tool:run_shell")?.effect).toBe("ask");

    // A `kind: "workflow"` run principal keyed on the runId is committed,
    // alongside its grant rows.
    const principalInserts = inserts.filter((i) => i.table === principalTable);
    expect(principalInserts).toHaveLength(1);
    const principalRow = assertBody(
      PrincipalInsert,
      principalInserts[0]?.values,
    );
    expect(principalRow).toMatchObject({
      kind: "workflow",
      refId: json.messageId,
      tenantId: TENANT_ID,
      status: "active",
    });

    // A workflow_run row keyed on the same runId anchors the run, carrying
    // the minted principal and the deployment it belongs to.
    const runInserts = inserts.filter((i) => i.table === workflowRunTable);
    expect(runInserts).toHaveLength(1);
    const runRow = assertBody(WorkflowRunInsert, runInserts[0]?.values);
    expect(runRow).toMatchObject({
      id: json.messageId,
      deploymentId: DEPLOYMENT_ID,
      tenantId: TENANT_ID,
      principalId: principalRow.id,
      status: "running",
    });

    const grantInserts = inserts.filter((i) => i.table === grantTable);
    expect(grantInserts).toHaveLength(2);
    // The committed grant principal matches the minted run principal.
    for (const gi of grantInserts) {
      const grantRow = assertBody(GrantInsert, gi.values);
      expect(grantRow.principalId).toBe(principalRow.id);
    }
  });

  test("materializes action effect grants in the frame and DB", async () => {
    const runGrantsCalls: RunGrantsCall[] = [];
    const inserts: InsertRecord[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      runGrantsCalls,
      db: { deploymentRow, assetRow: workflowAssetRow, inserts },
      workflowJson: WORKFLOW_JSON_WITH_EFFECT,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(202);

    // The action's `effect.requires` capability rides the run.grants frame as
    // an `effect:git:commit` / invoke / allow grant. This is the row the
    // action EffectContext authorizes fail-closed at runtime; without it the
    // action throws.
    expect(runGrantsCalls).toHaveLength(1);
    const grantsCall = runGrantsCalls[0];
    if (grantsCall === undefined) throw new Error("missing run.grants call");
    const framed = grantsCall.stepGrants.find(
      (g) => g.resource === "effect:git:commit",
    );
    expect(framed).toBeDefined();
    expect(framed?.action).toBe("invoke");
    expect(framed?.effect).toBe("allow");
    expect(framed?.origin).toBe("creator");

    // The same grant is committed to the DB alongside the run principal.
    const effectInserts = inserts
      .filter((i) => i.table === grantTable)
      .map((i) => assertBody(GrantInsert, i.values))
      .filter((g) => g.resource === "effect:git:commit");
    expect(effectInserts).toHaveLength(1);
    const committed = effectInserts[0];
    if (committed === undefined) throw new Error("missing effect grant insert");
    expect(committed.action).toBe("invoke");
    expect(committed.effect).toBe("allow");
    expect(committed.origin).toBe("creator");
  });

  test("leaves no orphaned run principal or grants on a 409", async () => {
    const runGrantsCalls: RunGrantsCall[] = [];
    const routeMailCalls: RouteMailCall[] = [];
    const inserts: InsertRecord[] = [];
    const app = createTestApp({
      grants: [manageGrant()],
      runGrantsCalls,
      routeMailCalls,
      routeMailResult: false,
      db: { deploymentRow, assetRow: workflowAssetRow, inserts },
      workflowJson: WORKFLOW_JSON_WITH_TOOLS,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(409);
    expect(await errorCode(res)).toBe("deployment_unreachable");

    // The run.grants frame was attempted, but the unroutable mail means the
    // run never starts -- so no run principal and no grant rows are written.
    expect(routeMailCalls).toHaveLength(1);
    expect(inserts.filter((i) => i.table === principalTable)).toHaveLength(0);
    expect(inserts.filter((i) => i.table === grantTable)).toHaveLength(0);
  });

  test("triggerers of differing authority get different declared run grants", async () => {
    // Run 1: the triggerer holds the delegatable `secret:vault`/`use`, so the
    // invoker-sourced requirement materializes onto the run.
    const authorizedGrants = runGrantsForTrigger([
      manageGrant(),
      makeGrant({ resource: "secret:vault", action: "use", origin: "system" }),
    ]);
    // Run 2: a triggerer lacking that authority. Same requirement, but nothing
    // to delegate, so the requirement fails closed and no run starts.
    const unauthorizedRunGrantsCalls: RunGrantsCall[] = [];
    const unauthorizedInserts: InsertRecord[] = [];
    const unauthorizedApp = createTestApp({
      grants: [manageGrant()],
      runGrantsCalls: unauthorizedRunGrantsCalls,
      db: {
        deploymentRow,
        assetRow: workflowAssetRow,
        inserts: unauthorizedInserts,
      },
      workflowJson: WORKFLOW_JSON_WITH_INVOKER_GRANT,
    });

    const authorized = await authorizedGrants;
    expect(authorized.status).toBe(202);
    const authorizedVault = authorized.runGrantsCalls[0]?.stepGrants.find(
      (g) => g.resource === "secret:vault",
    );
    expect(authorizedVault).toBeDefined();
    expect(authorizedVault?.action).toBe("use");
    expect(authorizedVault?.origin).toBe("invoker");

    const unauthorizedRes = await unauthorizedApp.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );
    expect(unauthorizedRes.status).toBe(403);
    expect(await errorCode(unauthorizedRes)).toBe("insufficient_grants");
    // The two runs' materialized grants differ: one carries the vault grant,
    // the other never gets sent because the run is rejected.
    expect(unauthorizedRunGrantsCalls).toHaveLength(0);
    expect(
      unauthorizedInserts.filter((i) => i.table === grantTable),
    ).toHaveLength(0);
  });

  async function runGrantsForTrigger(grants: GrantRule[]): Promise<{
    status: number;
    runGrantsCalls: RunGrantsCall[];
  }> {
    const runGrantsCalls: RunGrantsCall[] = [];
    const app = createTestApp({
      grants,
      runGrantsCalls,
      db: { deploymentRow, assetRow: workflowAssetRow, inserts: [] },
      workflowJson: WORKFLOW_JSON_WITH_INVOKER_GRANT,
    });
    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );
    return { status: res.status, runGrantsCalls };
  }

  test("resolves a creator-sourced requirement against the asset creator", async () => {
    const runGrantsCalls: RunGrantsCall[] = [];
    const app = createTestApp({
      grants: [
        manageGrant(),
        // The creator (not the triggerer) holds the authority. The route
        // collects the creator's grants keyed on the asset's creatorPrincipalId.
        makeGrant({
          resource: "secret:vault",
          action: "use",
          origin: "system",
          principalId: CREATOR_PRINCIPAL_ID,
        }),
      ],
      runGrantsCalls,
      db: {
        deploymentRow,
        assetRow: {
          ...workflowAssetRow,
          creatorPrincipalId: CREATOR_PRINCIPAL_ID,
        },
        inserts: [],
      },
      workflowJson: WORKFLOW_JSON_WITH_CREATOR_GRANT,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(202);
    const vault = runGrantsCalls[0]?.stepGrants.find(
      (g) => g.resource === "secret:vault",
    );
    expect(vault).toBeDefined();
    expect(vault?.action).toBe("use");
    expect(vault?.origin).toBe("creator");
  });

  test("fails closed when a creator-sourced requirement has a null-creator asset", async () => {
    const runGrantsCalls: RunGrantsCall[] = [];
    const inserts: InsertRecord[] = [];
    const app = createTestApp({
      grants: [
        manageGrant(),
        // The TRIGGERER holds the creator-source resource. A creator-sourced
        // requirement must resolve against the asset creator alone; with a
        // null creator there is nothing to resolve against, and the route
        // must NOT fall back to the triggerer's own authority. This grant is
        // the bait: if the route ever collected the triggerer's grants as a
        // creator fallback, this run would satisfy the requirement and start.
        makeGrant({
          resource: "secret:vault",
          action: "use",
          origin: "system",
        }),
      ],
      runGrantsCalls,
      db: {
        deploymentRow,
        // A creator-sourced requirement against an asset with no recorded
        // creator (the FK is `set null` on principal deletion). No fallback
        // principal is invented; the requirement cannot be satisfied.
        assetRow: { ...workflowAssetRow, creatorPrincipalId: null },
        inserts,
      },
      workflowJson: WORKFLOW_JSON_WITH_CREATOR_GRANT,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${DEPLOYMENT_ID}/mail`, { content: "kick off" }),
    );

    expect(res.status).toBe(403);
    expect(await errorCode(res)).toBe("insufficient_grants");
    expect(runGrantsCalls).toHaveLength(0);
    expect(inserts.filter((i) => i.table === grantTable)).toHaveLength(0);
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

describe("deriveRunRuntimeGrantRows tool-mark floor", () => {
  function makeFactory(
    id: string,
    definitions: readonly ToolDeclaration[],
  ): AnnotatedToolFactory<BaseEnv> {
    const factory = (_env: BaseEnv) => ({
      definitions: [],
      run: () =>
        Promise.resolve({ callId: "", content: "", isError: false as const }),
    });
    return Object.assign(factory, {
      id,
      requires: [] as readonly string[],
      definitions,
    });
  }

  test("an ask tool floor beats a workflow-declared allow at run time", async () => {
    const toolName = "run_shell";
    const agent = defineAgent({
      id: "ag_floor",
      systemPrompt: "an ask-marked tool",
      tools: [
        makeFactory("@intx/tools-posix/sidecar-bundle", [
          { name: toolName, approval: "ask" },
        ]),
      ],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
    });
    const workflow = defineWorkflow({
      id: "wf_floor",
      agent,
      trigger: { type: "manual" },
    });

    const now = new Date("2026-01-01T00:00:00Z");
    const runPrincipalId = "prn_run_floor";
    const walk = walkCapabilities(workflow, createDefaultDirectorRegistry());

    // The tool's `ask` mark materializes as an `ask` floor grant on the run
    // principal.
    const floorRows = deriveRunRuntimeGrantRows(
      walk,
      TENANT_ID,
      runPrincipalId,
      now,
    );
    const floor = floorRows.find((r) => r.resource === `tool:${toolName}`);
    if (floor === undefined) {
      throw new Error("expected a derived floor row for the ask-marked tool");
    }
    expect(floor.effect).toBe("ask");

    // A workflow that declares a competing `allow` grant for the same
    // `tool:<name>/invoke` lands at equal specificity on the same principal.
    // The declaration must not be able to lower the tool below its `ask` floor.
    const declaredAllow: GrantRule = {
      id: "grant-declared-allow",
      resource: `tool:${toolName}`,
      action: "invoke",
      effect: "allow",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: runPrincipalId,
    };
    const grants: GrantRule[] = [
      ...floorRows.map((r) => ({
        id: r.id,
        resource: r.resource,
        action: r.action,
        effect: r.effect,
        origin: r.origin,
        conditions: r.conditions,
        expiresAt: r.expiresAt,
        roleId: null,
        principalId: r.principalId,
      })),
      declaredAllow,
    ];

    const result = await evaluateGrants(grants, `tool:${toolName}`, "invoke");
    expect(result.effect).toBe("ask");
  });
});
