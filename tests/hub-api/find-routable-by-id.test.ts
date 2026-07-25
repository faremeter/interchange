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
import { seedPrincipal, seedTenants } from "@intx/test-harness/seed";
import {
  agent,
  agentInstance,
  agentSession,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
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
    await h.db.insert(agent).values({
      id: "agt_1",
      tenantId: "tnt_root",
      creatorPrincipalId: "prn_creator",
      name: "test-agent",
      systemPrompt: "p",
    });
  }

  test("resolves a legacy agent instance by id", async () => {
    await seedBase();
    await h.db.insert(agentSession).values({
      id: "ses_legacy",
      tenantId: "tnt_root",
      agentId: "agt_1",
      principalId: "prn_creator",
      status: "active",
    });
    await h.db.insert(agentInstance).values({
      id: "ins_legacy",
      agentId: "agt_1",
      tenantId: "tnt_root",
      principalId: "prn_creator",
      address: "ins_legacy@root.example",
      sessionId: "ses_legacy",
      status: "running",
      publicKey: "pk-instance",
    });

    const record = await findRoutableById(h.db, "ins_legacy", "tnt_root");
    expect(record?.kind).toBe("instance");
    expect(record?.id).toBe("ins_legacy");
    expect(record?.agentId).toBe("agt_1");
    expect(record?.address).toBe("ins_legacy@root.example");
    expect(record?.status).toBe("running");
    expect(record?.sessionId).toBe("ses_legacy");
  });

  test("resolves a folded run by id, agent via the definition's origin agent", async () => {
    await seedBase();
    await h.db.insert(workflowDefinition).values({
      id: "wfd_folded",
      tenantId: "tnt_root",
      name: "folded",
      originAgentId: "agt_1",
    });
    await seedPrincipal(h.db, { id: "prn_run", tenantId: "tnt_root" });
    await h.db.insert(agentSession).values({
      id: "ses_run",
      tenantId: "tnt_root",
      agentId: "agt_1",
      principalId: "prn_run",
      status: "active",
    });
    await h.db.insert(workflowRun).values({
      id: "ins_folded",
      tenantId: "tnt_root",
      definitionId: "wfd_folded",
      deploymentId: null,
      principalId: "prn_run",
      address: "ins_folded@root.example",
      status: "running",
      createdAt: new Date("2026-01-01T00:00:00Z"),
    });

    const record = await findRoutableById(h.db, "ins_folded", "tnt_root");
    expect(record?.kind).toBe("run");
    expect(record?.id).toBe("ins_folded");
    // agentId comes from the definition's originAgentId, not the run row.
    expect(record?.agentId).toBe("agt_1");
    expect(record?.address).toBe("ins_folded@root.example");
    expect(record?.status).toBe("running");
    // A run has no updatedAt column; a live run reports createdAt.
    expect(record?.updatedAt).toEqual(new Date("2026-01-01T00:00:00Z"));
    expect(record?.sessionId).toBe("ses_run");
  });

  test("a terminal run reports endedAt as its updatedAt", async () => {
    await seedBase();
    await h.db.insert(workflowDefinition).values({
      id: "wfd_done",
      tenantId: "tnt_root",
      name: "done",
      originAgentId: "agt_1",
    });
    await h.db.insert(workflowRun).values({
      id: "ins_done",
      tenantId: "tnt_root",
      definitionId: "wfd_done",
      deploymentId: null,
      principalId: null,
      address: "ins_done@root.example",
      status: "completed",
      createdAt: new Date("2026-01-01T00:00:00Z"),
      endedAt: new Date("2026-01-05T00:00:00Z"),
    });

    const record = await findRoutableById(h.db, "ins_done", "tnt_root");
    expect(record?.kind).toBe("run");
    expect(record?.status).toBe("completed");
    expect(record?.updatedAt).toEqual(new Date("2026-01-05T00:00:00Z"));
    expect(record?.sessionId).toBeNull();
  });

  test("serves a terminated instance (no endedAt filter)", async () => {
    await seedBase();
    await h.db.insert(agentInstance).values({
      id: "ins_stopped",
      agentId: "agt_1",
      tenantId: "tnt_root",
      principalId: "prn_creator",
      address: "ins_stopped@root.example",
      status: "stopped",
      endedAt: new Date("2026-01-05T00:00:00Z"),
    });

    const record = await findRoutableById(h.db, "ins_stopped", "tnt_root");
    expect(record?.kind).toBe("instance");
    expect(record?.status).toBe("stopped");
  });

  test("returns undefined for a deployment-anchored native run (no address)", async () => {
    await seedBase();
    await h.db.insert(workflowRun).values({
      id: "run_native",
      tenantId: "tnt_root",
      deploymentId: null,
      definitionId: null,
      principalId: null,
      address: null,
      status: "running",
    });
    expect(
      await findRoutableById(h.db, "run_native", "tnt_root"),
    ).toBeUndefined();
  });

  test("returns undefined for a run with an address but no origin agent", async () => {
    await seedBase();
    // A native definition (originAgentId null) that a run with an address
    // points at: backfill corruption. Resolves to not-found, not a bad record.
    await h.db.insert(workflowDefinition).values({
      id: "wfd_native",
      tenantId: "tnt_root",
      name: "native",
      originAgentId: null,
    });
    await h.db.insert(workflowRun).values({
      id: "ins_corrupt",
      tenantId: "tnt_root",
      definitionId: "wfd_native",
      deploymentId: null,
      principalId: null,
      address: "ins_corrupt@root.example",
      status: "running",
    });
    expect(
      await findRoutableById(h.db, "ins_corrupt", "tnt_root"),
    ).toBeUndefined();
  });

  test("returns undefined for a deployment anchor run (workflow-derived address)", async () => {
    await seedBase();
    // The deployment's anchor run owns a workflow-derived address and its id is
    // the deployment id. It is a routing/key anchor, not a folded instance, so
    // the read surface never serves it. The address alone excludes it: even
    // pointed at a definition that DOES name an origin agent -- so absent the
    // workflow-derived guard it would resolve as a run -- it still returns
    // not-found, and without the corruption warning the plain-address case
    // above emits.
    await h.db.insert(workflowDefinition).values({
      id: "wfd_anchor",
      tenantId: "tnt_root",
      name: "anchor-def",
      originAgentId: "agt_1",
    });
    await h.db.insert(workflowRun).values({
      id: "dep_anchor",
      tenantId: "tnt_root",
      definitionId: "wfd_anchor",
      deploymentId: null,
      principalId: null,
      address: "ins_dep_anchor@root.example",
      status: "running",
    });
    expect(
      await findRoutableById(h.db, "dep_anchor", "tnt_root"),
    ).toBeUndefined();
  });

  test("returns undefined for an unknown id", async () => {
    await seedBase();
    expect(
      await findRoutableById(h.db, "ins_missing", "tnt_root"),
    ).toBeUndefined();
  });

  test("throws when an id matches both an instance and a run", async () => {
    await seedBase();
    await h.db.insert(workflowDefinition).values({
      id: "wfd_dup",
      tenantId: "tnt_root",
      name: "dup",
      originAgentId: "agt_1",
    });
    await h.db.insert(agentInstance).values({
      id: "ins_dup",
      agentId: "agt_1",
      tenantId: "tnt_root",
      principalId: "prn_creator",
      address: "ins_dup_a@root.example",
      status: "running",
    });
    await h.db.insert(workflowRun).values({
      id: "ins_dup",
      tenantId: "tnt_root",
      definitionId: "wfd_dup",
      deploymentId: null,
      principalId: null,
      address: "ins_dup_b@root.example",
      status: "running",
    });

    await expect(findRoutableById(h.db, "ins_dup", "tnt_root")).rejects.toThrow(
      /both an agent instance and a workflow run/,
    );
  });

  describe("resolveRunSessionId includeEnded", () => {
    async function seedRunPrincipal(): Promise<void> {
      await seedTenants(h.db, [{ id: "tnt_root" }]);
      await seedPrincipal(h.db, { id: "prn_run", tenantId: "tnt_root" });
      await h.db.insert(agent).values({
        id: "agt_1",
        tenantId: "tnt_root",
        creatorPrincipalId: "prn_run",
        name: "a",
        systemPrompt: "p",
      });
    }

    test("default resolves only a live session; a stopped run's ended one is null", async () => {
      await seedRunPrincipal();
      await h.db.insert(agentSession).values({
        id: "ses_ended",
        tenantId: "tnt_root",
        agentId: "agt_1",
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
