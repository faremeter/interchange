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
// list surfaces the folded workflow_run rows that present as instances (an
// address plus an instance-kind definition). Only a real database exercises the
// definition join that surfaces a folded run and cursor resumption over the
// keyset.

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
  // Two instance-kind definitions; many folded runs share each one.
  await h.db.insert(workflowDefinition).values([
    {
      id: definitionIdFor(DEF_A),
      tenantId: TENANT_ID,
      name: "def-a",
      kind: "instance",
    },
    {
      id: definitionIdFor(DEF_B),
      tenantId: TENANT_ID,
      name: "def-b",
      kind: "instance",
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

// A folded run owning a routing address, anchored on a shared instance-kind
// definition (or an explicit definition for the corruption case). principalId
// is left null -- the list never reads it.
async function insertFoldedRun(opts: {
  id: string;
  definitionKey: string;
  status?: "running" | "completed" | "failed" | "cancelled";
  createdAt: Date;
  address?: string | null;
  definitionId?: string;
}): Promise<void> {
  await h.db.insert(workflowRun).values({
    id: opts.id,
    tenantId: TENANT_ID,
    definitionId: opts.definitionId ?? definitionIdFor(opts.definitionKey),
    address:
      opts.address === undefined ? `${opts.id}@list.example` : opts.address,
    status: opts.status ?? "running",
    createdAt: opts.createdAt,
  });
}

// A workflow-kind (native) definition -- the list gate excludes its runs from
// the instance surface. Its id is caller-supplied so a run can anchor on it;
// `kind` defaults to `workflow`.
async function insertNativeDefinition(id: string): Promise<void> {
  await h.db.insert(workflowDefinition).values({
    id,
    tenantId: TENANT_ID,
    name: `def-${id}`,
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
  "GET /workflows/runs (fold-aware list)",
  () => {
    test("lists folded runs newest first", async () => {
      await insertFoldedRun({
        id: "ins_1",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_2",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      const { ids } = await fetchList(buildApp());
      // Newest first: the later createdAt leads.
      expect(ids).toEqual(["ins_2", "ins_1"]);
    });

    test("filters folded runs by their definition", async () => {
      await insertFoldedRun({
        id: "ins_a",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_b",
        definitionKey: DEF_B,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      const { ids } = await fetchList(
        buildApp(),
        `?definitionId=${definitionIdFor(DEF_A)}`,
      );
      expect(ids).toEqual(["ins_a"]);
    });

    test("maps a folded run's status onto the instance status filter", async () => {
      await insertFoldedRun({
        id: "ins_run",
        definitionKey: DEF_A,
        status: "running",
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_done",
        definitionKey: DEF_A,
        status: "completed",
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_failed",
        definitionKey: DEF_A,
        status: "failed",
        createdAt: new Date("2025-03-03T00:00:00.000Z"),
      });

      const running = await fetchList(buildApp(), "?status=running");
      expect(running.rows).toEqual([
        {
          id: "ins_run",
          definitionId: definitionIdFor(DEF_A),
          status: "running",
        },
      ]);

      const stopped = await fetchList(buildApp(), "?status=stopped");
      expect(stopped.rows).toEqual([
        {
          id: "ins_done",
          definitionId: definitionIdFor(DEF_A),
          status: "stopped",
        },
      ]);

      const errored = await fetchList(buildApp(), "?status=error");
      expect(errored.rows).toEqual([
        {
          id: "ins_failed",
          definitionId: definitionIdFor(DEF_A),
          status: "error",
        },
      ]);

      // No folded run ever presents as `deployed`, so the run query is skipped.
      const deployed = await fetchList(buildApp(), "?status=deployed");
      expect(deployed.ids).toEqual([]);
    });

    test("excludes an address-less run and a run on a native definition", async () => {
      // Address-less: a native deployment-anchored run, not a folded instance.
      await insertFoldedRun({
        id: "ins_native",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
        address: null,
      });
      // Address present but the definition is workflow-kind (native): the
      // instance-kind list gate drops it.
      await insertNativeDefinition("wfd_corrupt");
      await insertFoldedRun({
        id: "ins_corrupt",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
        definitionId: "wfd_corrupt",
      });
      const { ids } = await fetchList(buildApp());
      expect(ids).toEqual([]);
    });

    test("orders a createdAt tie by id and pages it cleanly", async () => {
      const tie = new Date("2025-03-01T00:00:00.000Z");
      // Same createdAt; id DESC breaks the tie, so `ins_z` precedes `ins_a`.
      await insertFoldedRun({
        id: "ins_a",
        definitionKey: DEF_A,
        createdAt: tie,
      });
      await insertFoldedRun({
        id: "ins_z",
        definitionKey: DEF_A,
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

    test("resumes a page across runs without drops or duplicates", async () => {
      await insertFoldedRun({
        id: "ins_4run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-04T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_3run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-03T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_2run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-02T00:00:00.000Z"),
      });
      await insertFoldedRun({
        id: "ins_1run",
        definitionKey: DEF_A,
        createdAt: new Date("2025-03-01T00:00:00.000Z"),
      });

      const expectedOrder = ["ins_4run", "ins_3run", "ins_2run", "ins_1run"];
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
        { id: "ins_6run", day: 6 },
        { id: "ins_5run", day: 5 },
        { id: "ins_4run", day: 4 },
        { id: "ins_3run", day: 3 },
        { id: "ins_2run", day: 2 },
        { id: "ins_1run", day: 1 },
      ];
      for (const r of rows) {
        await insertFoldedRun({
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
