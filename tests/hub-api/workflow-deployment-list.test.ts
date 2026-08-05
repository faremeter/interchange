import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createInMemoryGrantStore } from "@intx/authz";
import { createApp, type GetSession } from "@intx/hub-api";
import {
  createSidecarEmitter,
  type AssetService,
  type EventCollectorRegistry,
  type RepoStore,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { GrantRule } from "@intx/types/authz";
import { workflowDefinition, workflowRun } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Exercises GET /workflows/instances against a real migrated schema. The list
// enumerates each deployment's anchor run -- the workflow_run whose id equals
// its deployment_id -- and synthesizes the deployment shape from the run and
// its definition. Only
// a real database exercises the `id = deployment_id` identity predicate (which
// excludes child and folded runs), the deliberate absence of a run-status
// filter (a torn-down deployment still lists as "deployed", preserving the old
// projection's contract), and the definition inner-join that resolves the
// deployment's asset.

const TENANT_ID = "tnt_wfl";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";
const ASSET_ID = "ast_wf";
const DEFINITION_ID = "wfd_wf";

function createMockGetSession(userId: string): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: userId,
      email: "test@example.com",
      emailVerified: true,
      name: "Test User",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId,
      token: "tok_test",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

function notImpl(name: string): never {
  throw new Error(`mock: ${name} not implemented`);
}

function createMockSidecarRouter(): SidecarRouter {
  return {
    handleOpen: () => notImpl("handleOpen"),
    handleMessage: () => notImpl("handleMessage"),
    handleClose: () => notImpl("handleClose"),
    routeMail: () => notImpl("routeMail"),
    sendRunGrants: () => notImpl("sendRunGrants"),
    sendAgentDeploy: () => notImpl("sendAgentDeploy"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
    sendCredentialsUpdate: () => notImpl("sendCredentialsUpdate"),
    sendPack: () => notImpl("sendPack"),
    sendProvisionStep: () => notImpl("sendProvisionStep"),
    bindStepRoute: () => notImpl("bindStepRoute"),
    unbindStepRoute: () => notImpl("unbindStepRoute"),
    sendSyncRequest: () => notImpl("sendSyncRequest"),
    sendSignalDeliver: () => notImpl("sendSignalDeliver"),
    sendDrain: () => notImpl("sendDrain"),
    subscribeAgent: () => notImpl("subscribeAgent"),
    dispatchAgentEvent: () => undefined,
    getConnectedSidecars: () => [],
    getRoutableAddresses: () => [],
    getConnectorState: () => null,
    events: createSidecarEmitter(),
  };
}

function createMockSessionService(): SessionService {
  return {
    stageWorkflowStep: () => notImpl("stageWorkflowStep"),
    deployInstanceAtHead: () => notImpl("deployInstanceAtHead"),
    deployWorkflowDefinition: () => notImpl("deployWorkflowDefinition"),
    deploySingleStepAtHead: () => notImpl("deploySingleStepAtHead"),
    sendUserMessage: () => notImpl("sendUserMessage"),
    endSession: () => notImpl("endSession"),
  };
}

function createMockEventCollectors(): EventCollectorRegistry {
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

// The deploy surface mounts only when the asset service and repo store are
// both present; the list route reads neither, so these stubs exist only to
// satisfy the mount gate and throw if any route unexpectedly reaches them.
function createStubAssetService(): AssetService {
  return {
    createAsset: () => notImpl("createAsset"),
    populateAsset: () => notImpl("populateAsset"),
    readAssetBlob: () => notImpl("readAssetBlob"),
    listAssetBlobs: () => notImpl("listAssetBlobs"),
  };
}

function createStubRepoStore(): RepoStore {
  const unused = () => Promise.reject(new Error("stub repoStore not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    writeTreeDelta: unused,
    receivePack: unused,
    createPack: unused,
    commitPackedTip: () => notImpl("commitPackedTip"),
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => notImpl("getRepoDir"),
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
    subscribe: () => notImpl("subscribe"),
  };
}

// The deploy list gates on `workflow:*` read.
const LIST_GRANT: GrantRule = {
  id: "grant-list",
  resource: "workflow:*",
  action: "read",
  effect: "allow",
  origin: "system",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: ACTOR_PRINCIPAL_ID,
};

let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  h = await createTestDb();
});

afterAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.close();
});

beforeEach(async () => {
  if (!harnessDbEnvAvailable()) return;
  await h.reset();
  await seedTenants(h.db, [{ id: TENANT_ID }]);
  await seedPrincipal(h.db, {
    id: ACTOR_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
    refId: ACTOR_USER_ID,
  });
  await seedAsset(h.db, {
    id: ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "demo-workflow",
    creatorPrincipalId: ACTOR_PRINCIPAL_ID,
  });
  // The deployment's first-class definition, carrying the asset the list
  // resolves the deployment's definitionAssetId from.
  await h.db.insert(workflowDefinition).values({
    id: DEFINITION_ID,
    tenantId: TENANT_ID,
    name: "demo-def",
    assetId: ASSET_ID,
  });
});

function buildApp(): ReturnType<typeof createApp> {
  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([LIST_GRANT]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: createStubAssetService(),
    repoStore: createStubRepoStore(),
    maxTarballBytes: 10_000_000,
  });
}

// A deployment's anchor run -- the workflow_run whose id equals the
// deployment id. The list reads the run, not the projection.
async function seedAnchor(opts: {
  id: string;
  createdAt: Date;
  status?: "running" | "completed" | "failed" | "cancelled";
  definitionId?: string;
}): Promise<void> {
  await h.db.insert(workflowRun).values({
    id: opts.id,
    tenantId: TENANT_ID,
    deploymentId: opts.id,
    definitionId: opts.definitionId ?? DEFINITION_ID,
    address: `ins_${opts.id}@wf.example`,
    status: opts.status ?? "running",
    createdAt: opts.createdAt,
  });
}

type ListRow = {
  id: string;
  tenantId: string;
  definitionAssetId: string;
  status: string;
  createdAt: string;
};

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

async function fetchList(
  app: ReturnType<typeof createApp>,
): Promise<{ res: Response; rows: ListRow[] }> {
  const res = await app.request(
    `/api/tenants/${TENANT_ID}/workflows/instances`,
  );
  if (res.status !== 200) return { res, rows: [] };
  const body: unknown = await res.json();
  if (!Array.isArray(body)) throw new Error("expected array body");
  const rows = body.map((d) => {
    if (!isObject(d)) throw new Error("bad row");
    return {
      id: String(d["id"]),
      tenantId: String(d["tenantId"]),
      definitionAssetId: String(d["definitionAssetId"]),
      status: String(d["status"]),
      createdAt: String(d["createdAt"]),
    };
  });
  return { res, rows };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "GET /workflows/instances (anchor-run list)",
  () => {
    test("lists an anchor run as a deployed deployment", async () => {
      await seedAnchor({
        id: "dep_a",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      const { rows } = await fetchList(buildApp());
      expect(rows).toEqual([
        {
          id: "dep_a",
          tenantId: TENANT_ID,
          definitionAssetId: ASSET_ID,
          status: "deployed",
          createdAt: "2025-03-01T00:00:00.000Z",
        },
      ]);
    });

    test("orders most recent first and excludes non-anchor runs", async () => {
      await seedAnchor({
        id: "dep_old",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await seedAnchor({
        id: "dep_new",
        createdAt: new Date("2025-03-03T00:00:00.000Z"),
      });
      // A child run of dep_old: its deployment_id is non-null but does NOT
      // equal its own id, so the anchor-identity predicate excludes it.
      await h.db.insert(workflowRun).values({
        id: "run_child",
        tenantId: TENANT_ID,
        deploymentId: "dep_old",
        definitionId: DEFINITION_ID,
        status: "running",
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      // A folded-agent run: address-bearing but deployment_id null, so it is
      // not a deployment anchor and the predicate excludes it.
      await h.db.insert(workflowRun).values({
        id: "run_folded",
        tenantId: TENANT_ID,
        deploymentId: null,
        definitionId: DEFINITION_ID,
        address: "run_folded@wf.example",
        status: "running",
        createdAt: new Date("2025-03-04T00:00:00.000Z"),
      });

      const { rows } = await fetchList(buildApp());
      expect(rows.map((r) => r.id)).toEqual(["dep_new", "dep_old"]);
    });

    test("lists a torn-down deployment as still deployed", async () => {
      // The anchor run has flipped to a terminal run status. The old projection
      // never tracked teardown, so the list applies no run-status filter and
      // still reports the deployment as "deployed".
      await seedAnchor({
        id: "dep_gone",
        status: "cancelled",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      const { rows } = await fetchList(buildApp());
      expect(rows).toEqual([
        {
          id: "dep_gone",
          tenantId: TENANT_ID,
          definitionAssetId: ASSET_ID,
          status: "deployed",
          createdAt: "2025-03-01T00:00:00.000Z",
        },
      ]);
    });

    test("surfaces a corrupt definition with no asset as a 500", async () => {
      // An anchor run whose definition names no asset is corrupt: the deployment
      // contract cannot represent a null asset, so the list fails loudly rather
      // than emitting null into a string field.
      await h.db.insert(workflowDefinition).values({
        id: "wfd_null",
        tenantId: TENANT_ID,
        name: "no-asset",
        assetId: null,
      });
      await seedAnchor({
        id: "dep_corrupt",
        definitionId: "wfd_null",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      const { res } = await fetchList(buildApp());
      expect(res.status).toBe(500);
    });
  },
);
