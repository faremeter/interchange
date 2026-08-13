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
import { workflowDefinition, workflowRun } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";

// Exercises the GET /workflows/runs list against a real migrated schema. The
// list surfaces the tenant's top-level workflow runs -- a run that owns a
// routing address and self-anchors (`anchorRunId === id`). Only a real database
// exercises the definition join that surfaces a run and cursor resumption over
// the keyset.

const TENANT_ID = "tnt_list";
const ACTOR_PRINCIPAL_ID = "prn_actor";
const ACTOR_USER_ID = "usr_actor";
const DEF_A = "def_a";
const DEF_B = "def_b";

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
  // Two definitions; many runs share each one.
  await h.db.insert(workflowDefinition).values([
    {
      id: definitionIdFor(DEF_A),
      tenantId: TENANT_ID,
      name: "def-a",
    },
    {
      id: definitionIdFor(DEF_B),
      tenantId: TENANT_ID,
      name: "def-b",
    },
  ]);
});

function definitionIdFor(key: string): string {
  return `wfd_${key}`;
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

// A top-level run: it owns a routing address and self-anchors
// (`anchorRunId === id`) -- the shape the list surfaces. principalId is left
// null -- the list never reads it.
async function insertTopLevelRun(opts: {
  id: string;
  definitionKey: string;
  status?: "deployed" | "running" | "completed" | "failed" | "cancelled";
  createdAt: Date;
  address?: string | null;
  definitionId?: string;
}): Promise<void> {
  await h.db.insert(workflowRun).values({
    id: opts.id,
    tenantId: TENANT_ID,
    definitionId: opts.definitionId ?? definitionIdFor(opts.definitionKey),
    anchorRunId: opts.id,
    address:
      opts.address === undefined ? `${opts.id}@list.example` : opts.address,
    status: opts.status ?? "running",
    createdAt: opts.createdAt,
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
    `/api/tenants/${TENANT_ID}/workflows/runs${query}`,
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
  "GET /workflows/runs (top-level run list)",
  () => {
    test("lists runs newest first", async () => {
      await insertTopLevelRun({
        id: "run_1",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_2",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      const { ids } = await fetchList(buildApp());
      // Newest first: the later createdAt leads.
      expect(ids).toEqual(["run_2", "run_1"]);
    });

    test("filters runs by their definition", async () => {
      await insertTopLevelRun({
        id: "run_a",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_b",
        definitionKey: DEF_B,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      const { ids } = await fetchList(
        buildApp(),
        `?definitionId=${definitionIdFor(DEF_A)}`,
      );
      expect(ids).toEqual(["run_a"]);
    });

    test("maps a run's status onto the run-view status filter", async () => {
      await insertTopLevelRun({
        id: "run_deployed",
        definitionKey: DEF_A,
        status: "deployed",
        createdAt: new Date("2025-02-28T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_run",
        definitionKey: DEF_A,
        status: "running",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_done",
        definitionKey: DEF_A,
        status: "completed",
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_failed",
        definitionKey: DEF_A,
        status: "failed",
        createdAt: new Date("2025-03-03T00:00:00.000Z"),
      });

      const running = await fetchList(buildApp(), "?status=running");
      expect(running.rows).toEqual([
        {
          id: "run_run",
          definitionId: definitionIdFor(DEF_A),
          status: "running",
        },
      ]);

      const stopped = await fetchList(buildApp(), "?status=stopped");
      expect(stopped.rows).toEqual([
        {
          id: "run_done",
          definitionId: definitionIdFor(DEF_A),
          status: "stopped",
        },
      ]);

      const errored = await fetchList(buildApp(), "?status=error");
      expect(errored.rows).toEqual([
        {
          id: "run_failed",
          definitionId: definitionIdFor(DEF_A),
          status: "error",
        },
      ]);

      // A deployed anchor is a top-level run, so the `deployed` filter now
      // returns it.
      const deployed = await fetchList(buildApp(), "?status=deployed");
      expect(deployed.rows).toEqual([
        {
          id: "run_deployed",
          definitionId: definitionIdFor(DEF_A),
          status: "deployed",
        },
      ]);
    });

    test("excludes an address-less child park row", async () => {
      // A top-level anchor, listed.
      await insertTopLevelRun({
        id: "run_anchor",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      // A lazy child park row: it anchors on its parent (`anchorRunId !== id`)
      // and carries no address, so it is not a top-level run. The self-FK on
      // `anchor_run_id` forces the parent anchor to be seeded first (above).
      await h.db.insert(workflowRun).values({
        id: "run_child",
        tenantId: TENANT_ID,
        definitionId: definitionIdFor(DEF_A),
        anchorRunId: "run_anchor",
        address: null,
        status: "running",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      const { ids } = await fetchList(buildApp());
      expect(ids).toEqual(["run_anchor"]);
    });

    test("orders a createdAt tie by id and pages it cleanly", async () => {
      const tie = new Date("2025-03-01T00:00:00.000Z");
      // Same createdAt; id DESC breaks the tie, so `run_z` precedes `run_a`.
      await insertTopLevelRun({
        id: "run_a",
        definitionKey: DEF_A,
        createdAt: tie,
      });
      await insertTopLevelRun({
        id: "run_z",
        definitionKey: DEF_A,
        createdAt: tie,
      });

      const first = await fetchList(buildApp(), "?limit=1");
      expect(first.ids).toEqual(["run_z"]);
      expect(first.nextCursor).not.toBeNull();

      const second = await fetchList(
        buildApp(),
        `?limit=1&cursor=${encodeURIComponent(first.nextCursor ?? "")}`,
      );
      expect(second.ids).toEqual(["run_a"]);
    });

    test("resumes a page across runs without drops or duplicates", async () => {
      await insertTopLevelRun({
        id: "run_4run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-04T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_3run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-03T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_2run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      await insertTopLevelRun({
        id: "run_1run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });

      const expectedOrder = ["run_4run", "run_3run", "run_2run", "run_1run"];
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

    test("pages rows deeper than the limit without drops", async () => {
      // Six runs densely ordered by createdAt, walked at limit 2, so every page
      // but the last truncates the keyset and resumes it mid-run.
      const rows: { id: string; day: number }[] = [
        { id: "run_6run", day: 6 },
        { id: "run_5run", day: 5 },
        { id: "run_4run", day: 4 },
        { id: "run_3run", day: 3 },
        { id: "run_2run", day: 2 },
        { id: "run_1run", day: 1 },
      ];
      for (const r of rows) {
        await insertTopLevelRun({
          id: r.id,
          definitionKey: DEF_A,
          createdAt: new Date(`2025-03-0${r.day}T00:00:00.000Z`),
        });
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
