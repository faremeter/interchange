import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";
import { agentSession, workflowDefinition, workflowRun } from "@intx/db/schema";
import { findRoutableById, resolveRunSessionId } from "@intx/hub-sessions";

describe.skipIf(!harnessDbEnvAvailable())("findRoutableById (real DB)", () => {
  let h: TestDb;

  beforeAll(async () => {
    h = await createTestDb();
  });

  afterAll(async () => {
    await h.close();
  });

  beforeEach(async () => {
    await h.reset();
  });

  async function seedBase(): Promise<void> {
    await seedTenants(h.db, [{ id: "tnt_root" }]);
    await seedPrincipal(h.db, { id: "prn_creator", tenantId: "tnt_root" });
  }

  test("resolves a top-level run by id via its own definition", async () => {
    await seedBase();
    await h.db.insert(workflowDefinition).values({
      id: "wfd_folded",
      tenantId: "tnt_root",
      name: "folded",
    });
    await seedPrincipal(h.db, { id: "prn_run", tenantId: "tnt_root" });
    // A top-level run self-anchors (anchorRunId === id) and owns an address.
    await h.db.insert(workflowRun).values({
      id: "run_folded",
      tenantId: "tnt_root",
      definitionId: "wfd_folded",
      anchorRunId: "run_folded",
      principalId: "prn_run",
      address: "run_folded@root.example",
      status: "running",
      createdAt: new Date("2026-01-01T00:00:00Z"),
    });

    const record = await findRoutableById(h.db, "run_folded", "tnt_root");
    expect(record?.id).toBe("run_folded");
    // definitionId is the run's own definition.
    expect(record?.definitionId).toBe("wfd_folded");
    expect(record?.address).toBe("run_folded@root.example");
    expect(record?.status).toBe("running");
    // A run has no updatedAt column; a live run reports createdAt.
    expect(record?.updatedAt).toEqual(new Date("2026-01-01T00:00:00Z"));
  });

  test("a terminal run reports endedAt as its updatedAt", async () => {
    await seedBase();
    await h.db.insert(workflowDefinition).values({
      id: "wfd_done",
      tenantId: "tnt_root",
      name: "done",
    });
    await h.db.insert(workflowRun).values({
      id: "run_done",
      tenantId: "tnt_root",
      definitionId: "wfd_done",
      anchorRunId: "run_done",
      principalId: null,
      address: "run_done@root.example",
      status: "completed",
      createdAt: new Date("2026-01-01T00:00:00Z"),
      endedAt: new Date("2026-01-05T00:00:00Z"),
    });

    const record = await findRoutableById(h.db, "run_done", "tnt_root");
    expect(record?.status).toBe("completed");
    expect(record?.updatedAt).toEqual(new Date("2026-01-05T00:00:00Z"));
  });

  test("returns undefined for a deployment-anchored native run (no address)", async () => {
    await seedBase();
    await seedWorkflowRun(h.db, {
      id: "run_native",
      tenantId: "tnt_root",
      anchorRunId: null,
      principalId: null,
      address: null,
      status: "running",
    });
    expect(
      await findRoutableById(h.db, "run_native", "tnt_root"),
    ).toBeUndefined();
  });

  test("returns undefined for a child run (anchored on another run)", async () => {
    await seedBase();
    // A child park row anchors on its parent (anchorRunId !== id). Even if it
    // owned an address it is not a top-level run, so the read surface never
    // serves it.
    await h.db.insert(workflowDefinition).values({
      id: "wfd_child",
      tenantId: "tnt_root",
      name: "child-def",
    });
    // The parent anchor the child references must exist: `anchor_run_id`
    // carries a self-referential foreign key onto `workflow_run.id`.
    await h.db.insert(workflowRun).values({
      id: "run_parent",
      tenantId: "tnt_root",
      definitionId: "wfd_child",
      anchorRunId: "run_parent",
      principalId: null,
      address: "run_parent@root.example",
      status: "running",
    });
    await h.db.insert(workflowRun).values({
      id: "run_child",
      tenantId: "tnt_root",
      definitionId: "wfd_child",
      anchorRunId: "run_parent",
      principalId: null,
      address: "run_child@root.example",
      status: "running",
    });
    expect(
      await findRoutableById(h.db, "run_child", "tnt_root"),
    ).toBeUndefined();
  });

  test("returns undefined for an unknown id", async () => {
    await seedBase();
    expect(
      await findRoutableById(h.db, "ins_missing", "tnt_root"),
    ).toBeUndefined();
  });

  describe("resolveRunSessionId includeEnded", () => {
    async function seedRunPrincipal(): Promise<void> {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await seedPrincipal(h.db, { id: "prn_run", tenantId: "tnt_root" });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_run",
        tenantId: "tnt_root",
        name: "run",
      });
    }

    test("default resolves only a live session; a stopped run's ended one is null", async () => {
      await seedRunPrincipal();
      await h.db.insert(agentSession).values({
        id: "ses_ended",
        tenantId: "tnt_root",
        agentId: "wfd_run",
        principalId: "prn_run",
        status: "ended",
        endedAt: new Date("2026-01-05T00:00:00Z"),
      });

      expect(await resolveRunSessionId(h.db, "prn_run")).toBeNull();
      expect(
        await resolveRunSessionId(h.db, "prn_run", { includeEnded: true }),
      ).toBe("ses_ended");
    });

    test("returns null for a principal with no session, either way", async () => {
      await seedRunPrincipal();
      expect(await resolveRunSessionId(h.db, "prn_run")).toBeNull();
      expect(
        await resolveRunSessionId(h.db, "prn_run", { includeEnded: true }),
      ).toBeNull();
    });

    test("returns null for a null principal", async () => {
      expect(await resolveRunSessionId(h.db, null)).toBeNull();
      expect(
        await resolveRunSessionId(h.db, null, { includeEnded: true }),
      ).toBeNull();
    });
  });
});
