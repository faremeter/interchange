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
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
} from "@intx/hub-sessions";
import type { GrantRule } from "@intx/types/authz";
import {
  agentInstance,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAgent, seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Exercises the fold-aware GET /instances list against a real migrated schema.
// The list merges two keyset heads -- legacy agent_instance rows and folded
// workflow_run rows -- into a single page. Only a real database exercises the
// cross-table merge order, the origin-agent join that surfaces a folded run,
// and cursor resumption across both tables.

const TENANT_ID = "tnt_list";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";
const AGENT_A = "agt_a";
const AGENT_B = "agt_b";

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

// The list route gates on `workflow-run:*` read.
const LIST_GRANT: GrantRule = {
  id: "grant-list",
  resource: "workflow-run:*",
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
  await seedAgent(h.db, {
    id: AGENT_A,
    tenantId: TENANT_ID,
    creatorPrincipalId: ACTOR_PRINCIPAL_ID,
    name: "agent-a",
  });
  await seedAgent(h.db, {
    id: AGENT_B,
    tenantId: TENANT_ID,
    creatorPrincipalId: ACTOR_PRINCIPAL_ID,
    name: "agent-b",
  });
  // One folded definition per agent -- `workflow_definition.originAgentId` is
  // uniquely indexed, so a definition is 1:1 with its origin agent and many
  // folded runs of that agent share it.
  await h.db.insert(workflowDefinition).values([
    {
      id: definitionIdFor(AGENT_A),
      tenantId: TENANT_ID,
      name: "def-a",
      originAgentId: AGENT_A,
    },
    {
      id: definitionIdFor(AGENT_B),
      tenantId: TENANT_ID,
      name: "def-b",
      originAgentId: AGENT_B,
    },
  ]);
});

function definitionIdFor(agentId: string): string {
  return `wfd_${agentId}`;
}

function buildApp(): ReturnType<typeof createApp> {
  return createApp({
    getSession: createMockGetSession(ACTOR_USER_ID),
    authHandler: () => new Response("", { status: 404 }),
    db: h.db,
    grantStore: createInMemoryGrantStore([LIST_GRANT]),
    sidecarRouter: createMockSidecarRouter(),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

async function insertInstance(opts: {
  id: string;
  agentId: string;
  status?: "deployed" | "running" | "updating" | "error" | "stopped";
  createdAt: Date;
}): Promise<void> {
  await seedPrincipal(h.db, {
    id: `prn_${opts.id}`,
    tenantId: TENANT_ID,
    kind: "agent",
    refId: opts.id,
  });
  await h.db.insert(agentInstance).values({
    id: opts.id,
    agentId: opts.agentId,
    tenantId: TENANT_ID,
    principalId: `prn_${opts.id}`,
    address: `${opts.id}@list.example`,
    status: opts.status ?? "running",
    createdAt: opts.createdAt,
  });
}

// A folded run owning a routing address, anchored on its origin agent's shared
// definition (or an explicit definition for the corruption case). principalId
// is left null -- the list never reads it.
async function insertFoldedRun(opts: {
  id: string;
  originAgentId: string;
  status?: "running" | "completed" | "failed" | "cancelled";
  createdAt: Date;
  address?: string | null;
  definitionId?: string;
}): Promise<void> {
  await h.db.insert(workflowRun).values({
    id: opts.id,
    tenantId: TENANT_ID,
    definitionId: opts.definitionId ?? definitionIdFor(opts.originAgentId),
    address:
      opts.address === undefined ? `${opts.id}@list.example` : opts.address,
    status: opts.status ?? "running",
    createdAt: opts.createdAt,
  });
}

// A definition that names no origin agent (backfill corruption). Its id is
// caller-supplied so the run can anchor on it; `originAgentId` null is allowed
// alongside the unique index, which is partial on non-null values.
async function insertOriginlessDefinition(id: string): Promise<void> {
  await h.db.insert(workflowDefinition).values({
    id,
    tenantId: TENANT_ID,
    name: `def-${id}`,
    originAgentId: null,
  });
}

type ListRow = { id: string; definitionId: string; status: string };

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

async function fetchList(
  app: ReturnType<typeof createApp>,
  query = "",
): Promise<{ ids: string[]; rows: ListRow[]; nextCursor: string | null }> {
  const res = await app.request(
    `/api/tenants/${TENANT_ID}/agents/instances${query}`,
  );
  expect(res.status).toBe(200);
  const body: unknown = await res.json();
  if (!isObject(body)) throw new Error("expected object body");
  const data = body["data"];
  if (!Array.isArray(data)) throw new Error("expected data array");
  const nextCursorRaw = body["nextCursor"];
  const nextCursor = typeof nextCursorRaw === "string" ? nextCursorRaw : null;
  const rows = data.map((d) => {
    if (!isObject(d)) throw new Error("bad row");
    return {
      id: String(d["id"]),
      definitionId: String(d["definitionId"]),
      status: String(d["status"]),
    };
  });
  return { ids: rows.map((r) => r.id), rows, nextCursor };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "GET /agents/instances (fold-aware list)",
  () => {
    test("lists a folded run alongside a legacy instance", async () => {
      await insertInstance({
        id: "ins_1",
        agentId: AGENT_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_2",
        originAgentId: AGENT_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      const { ids } = await fetchList(buildApp());
      // Newest first: the folded run (later createdAt) leads.
      expect(ids).toEqual(["ins_2", "ins_1"]);
    });

    test("filters folded runs by their origin agent", async () => {
      await insertFoldedRun({
        id: "ins_a",
        originAgentId: AGENT_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_b",
        originAgentId: AGENT_B,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      const { ids } = await fetchList(
        buildApp(),
        `?definitionId=${definitionIdFor(AGENT_A)}`,
      );
      expect(ids).toEqual(["ins_a"]);
    });

    test("maps a folded run's status onto the instance status filter", async () => {
      await insertFoldedRun({
        id: "ins_run",
        originAgentId: AGENT_A,
        status: "running",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_done",
        originAgentId: AGENT_A,
        status: "completed",
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_failed",
        originAgentId: AGENT_A,
        status: "failed",
        createdAt: new Date("2025-03-03T00:00:00.000Z"),
      });

      const running = await fetchList(buildApp(), "?status=running");
      expect(running.rows).toEqual([
        {
          id: "ins_run",
          definitionId: definitionIdFor(AGENT_A),
          status: "running",
        },
      ]);

      const stopped = await fetchList(buildApp(), "?status=stopped");
      expect(stopped.rows).toEqual([
        {
          id: "ins_done",
          definitionId: definitionIdFor(AGENT_A),
          status: "stopped",
        },
      ]);

      const errored = await fetchList(buildApp(), "?status=error");
      expect(errored.rows).toEqual([
        {
          id: "ins_failed",
          definitionId: definitionIdFor(AGENT_A),
          status: "error",
        },
      ]);

      // No folded run ever presents as `deployed`, so the run query is skipped.
      const deployed = await fetchList(buildApp(), "?status=deployed");
      expect(deployed.ids).toEqual([]);
    });

    test("excludes a deployment-anchored run and a run whose definition names no agent", async () => {
      // Address-less: a native deployment-anchored run, not a folded instance.
      await insertFoldedRun({
        id: "ins_native",
        originAgentId: AGENT_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
        address: null,
      });
      // Address present but the definition names no origin agent: backfill
      // corruption; the origin-agent join drops it.
      await insertOriginlessDefinition("wfd_corrupt");
      await insertFoldedRun({
        id: "ins_corrupt",
        originAgentId: AGENT_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
        definitionId: "wfd_corrupt",
      });
      const { ids } = await fetchList(buildApp());
      expect(ids).toEqual([]);
    });

    test("orders a createdAt tie across the two tables by id and pages it cleanly", async () => {
      const tie = new Date("2025-03-01T00:00:00.000Z");
      // Same createdAt across the two tables; id DESC breaks the tie, so
      // `ins_z` (run) precedes `ins_a` (instance).
      await insertInstance({ id: "ins_a", agentId: AGENT_A, createdAt: tie });
      await insertFoldedRun({
        id: "ins_z",
        originAgentId: AGENT_A,
        createdAt: tie,
      });

      const first = await fetchList(buildApp(), "?limit=1");
      expect(first.ids).toEqual(["ins_z"]);
      expect(first.nextCursor).not.toBeNull();

      const second = await fetchList(
        buildApp(),
        `?limit=1&cursor=${encodeURIComponent(first.nextCursor ?? "")}`,
      );
      expect(second.ids).toEqual(["ins_a"]);
    });

    test("resumes an interleaved page across both tables without drops or duplicates", async () => {
      // Interleave the two tables in createdAt order so a two-page fetch must
      // resume both heads at the seam.
      await insertFoldedRun({
        id: "ins_4run",
        originAgentId: AGENT_A,
        createdAt: new Date("2025-03-04T00:00:00.000Z"),
      });
      await insertInstance({
        id: "ins_3ins",
        agentId: AGENT_A,
        createdAt: new Date("2025-03-03T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_2run",
        originAgentId: AGENT_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      await insertInstance({
        id: "ins_1ins",
        agentId: AGENT_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });

      const expectedOrder = ["ins_4run", "ins_3ins", "ins_2run", "ins_1ins"];
      const collected: string[] = [];
      let cursor: string | null = null;
      for (let page = 0; page < 5; page++) {
        const q: string =
          cursor === null
            ? "?limit=2"
            : `?limit=2&cursor=${encodeURIComponent(cursor)}`;
        const res = await fetchList(buildApp(), q);
        collected.push(...res.ids);
        cursor = res.nextCursor;
        if (cursor === null) break;
      }
      expect(collected).toEqual(expectedOrder);
      // No duplicates across the seam.
      expect(new Set(collected).size).toBe(expectedOrder.length);
    });

    test("pages a run of rows deeper than the limit in each head without drops", async () => {
      // Three runs and three instances densely interleaved by createdAt, walked
      // at limit 2. Each head holds more than `limit` rows, so every page but
      // the last truncates both heads and resumes them mid-run -- the case the
      // merge's head-cap argument rests on.
      const rows: { id: string; kind: "run" | "instance"; day: number }[] = [
        { id: "ins_6run", kind: "run", day: 6 },
        { id: "ins_5ins", kind: "instance", day: 5 },
        { id: "ins_4run", kind: "run", day: 4 },
        { id: "ins_3ins", kind: "instance", day: 3 },
        { id: "ins_2run", kind: "run", day: 2 },
        { id: "ins_1ins", kind: "instance", day: 1 },
      ];
      for (const r of rows) {
        const createdAt = new Date(`2025-03-0${r.day}T00:00:00.000Z`);
        if (r.kind === "run") {
          await insertFoldedRun({
            id: r.id,
            originAgentId: AGENT_A,
            createdAt,
          });
        } else {
          await insertInstance({ id: r.id, agentId: AGENT_A, createdAt });
        }
      }

      const expectedOrder = rows.map((r) => r.id);
      const collected: string[] = [];
      let cursor: string | null = null;
      for (let page = 0; page < 6; page++) {
        const q: string =
          cursor === null
            ? "?limit=2"
            : `?limit=2&cursor=${encodeURIComponent(cursor)}`;
        const res = await fetchList(buildApp(), q);
        expect(res.ids.length).toBeLessThanOrEqual(2);
        collected.push(...res.ids);
        cursor = res.nextCursor;
        if (cursor === null) break;
      }
      expect(collected).toEqual(expectedOrder);
      expect(new Set(collected).size).toBe(expectedOrder.length);
    });
  },
);
