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
import {
  agent,
  agentInstance,
  agentSession,
  workflowDefinition,
} from "@intx/db/schema";
import { resolveRoutableAddress } from "@intx/hub-sessions";

describe.skipIf(!harnessDbEnvAvailable())(
  "resolveRoutableAddress (real DB)",
  () => {
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

    test("resolves a legacy agent-instance address to kind instance", async () => {
      await seedBase();
      await h.db.insert(workflowDefinition).values({
        id: "wfd_i",
        tenantId: "tnt_root",
        name: "instance",
        originAgentId: "agt_1",
      });
      await h.db.insert(agentSession).values({
        id: "ses_i",
        tenantId: "tnt_root",
        agentId: "wfd_i",
        principalId: "prn_creator",
        status: "active",
      });
      await h.db.insert(agentInstance).values({
        id: "ins_legacy",
        agentId: "agt_1",
        tenantId: "tnt_root",
        principalId: "prn_creator",
        address: "ins_legacy@root.example",
        sessionId: "ses_i",
        status: "deployed",
        publicKey: "pk-instance",
      });

      const endpoint = await resolveRoutableAddress(
        h.db,
        "ins_legacy@root.example",
      );
      expect(endpoint?.kind).toBe("instance");
      expect(endpoint?.id).toBe("ins_legacy");
      expect(endpoint?.tenantId).toBe("tnt_root");
      expect(endpoint?.publicKey).toBe("pk-instance");
      expect(endpoint?.sessionId).toBe("ses_i");
    });

    test("resolves a folded workflow-run address to kind run, session via the run principal", async () => {
      await seedBase();
      // The run carries its own principal; its active session is keyed by that
      // principal (the run row has no session column).
      await seedPrincipal(h.db, { id: "prn_run", tenantId: "tnt_root" });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_folded",
        tenantId: "tnt_root",
        name: "folded",
        originAgentId: "agt_1",
      });
      await h.db.insert(agentSession).values({
        id: "ses_r",
        tenantId: "tnt_root",
        agentId: "wfd_folded",
        principalId: "prn_run",
        status: "active",
      });
      await seedWorkflowRun(h.db, {
        id: "run_folded",
        tenantId: "tnt_root",
        deploymentId: null,
        definitionId: "wfd_folded",
        principalId: "prn_run",
        address: "ins_folded@root.example",
        status: "running",
        publicKey: "pk-run",
      });

      const endpoint = await resolveRoutableAddress(
        h.db,
        "ins_folded@root.example",
      );
      expect(endpoint?.kind).toBe("run");
      expect(endpoint?.id).toBe("run_folded");
      expect(endpoint?.tenantId).toBe("tnt_root");
      expect(endpoint?.publicKey).toBe("pk-run");
      expect(endpoint?.sessionId).toBe("ses_r");
    });

    test("returns undefined for an unknown address", async () => {
      await seedBase();
      expect(
        await resolveRoutableAddress(h.db, "ins_missing@root.example"),
      ).toBeUndefined();
    });

    test("excludes a deployment anchor run (workflow-derived address)", async () => {
      await seedBase();
      // The anchor run owns a workflow-derived address in the run table. A
      // plain-resolution caller (mail persist, reconnect reaction) must not
      // match it -- the workflow-derived key path owns it -- so the guard
      // returns undefined before the address query runs.
      await seedWorkflowRun(h.db, {
        id: "dep_anchor",
        tenantId: "tnt_root",
        deploymentId: null,
        principalId: null,
        address: "ins_dep_anchor@root.example",
        status: "running",
      });
      expect(
        await resolveRoutableAddress(h.db, "ins_dep_anchor@root.example"),
      ).toBeUndefined();
    });

    test("skips a terminated run (endedAt set)", async () => {
      await seedBase();
      await seedWorkflowRun(h.db, {
        id: "run_dead",
        tenantId: "tnt_root",
        deploymentId: null,
        principalId: "prn_creator",
        address: "ins_dead@root.example",
        status: "cancelled",
        endedAt: new Date(0),
      });
      expect(
        await resolveRoutableAddress(h.db, "ins_dead@root.example"),
      ).toBeUndefined();
    });

    test("skips a terminated instance (endedAt set)", async () => {
      await seedBase();
      await h.db.insert(agentInstance).values({
        id: "ins_gone",
        agentId: "agt_1",
        tenantId: "tnt_root",
        principalId: "prn_creator",
        address: "ins_gone@root.example",
        status: "stopped",
        endedAt: new Date(0),
      });
      expect(
        await resolveRoutableAddress(h.db, "ins_gone@root.example"),
      ).toBeUndefined();
    });

    test("resolves a run with no principal to a null session", async () => {
      await seedBase();
      await seedWorkflowRun(h.db, {
        id: "run_noprincipal",
        tenantId: "tnt_root",
        deploymentId: null,
        principalId: null,
        address: "ins_noprincipal@root.example",
        status: "running",
      });
      const endpoint = await resolveRoutableAddress(
        h.db,
        "ins_noprincipal@root.example",
      );
      expect(endpoint?.kind).toBe("run");
      expect(endpoint?.sessionId).toBeNull();
    });

    test("resolves a run whose principal has no live session to a null session", async () => {
      await seedBase();
      // A principal with only an ended session leaves the run session-less.
      await seedPrincipal(h.db, {
        id: "prn_sessionless",
        tenantId: "tnt_root",
      });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_sessionless",
        tenantId: "tnt_root",
        name: "sessionless",
        originAgentId: "agt_1",
      });
      await h.db.insert(agentSession).values({
        id: "ses_ended",
        tenantId: "tnt_root",
        agentId: "wfd_sessionless",
        principalId: "prn_sessionless",
        status: "ended",
        endedAt: new Date(0),
      });
      await seedWorkflowRun(h.db, {
        id: "run_sessionless",
        tenantId: "tnt_root",
        deploymentId: null,
        definitionId: "wfd_sessionless",
        principalId: "prn_sessionless",
        address: "ins_sessionless@root.example",
        status: "running",
      });
      const endpoint = await resolveRoutableAddress(
        h.db,
        "ins_sessionless@root.example",
      );
      expect(endpoint?.kind).toBe("run");
      expect(endpoint?.sessionId).toBeNull();
    });

    test("throws when an address matches both an instance and a run", async () => {
      await seedBase();
      const address = "ins_dup@root.example";
      await h.db.insert(workflowDefinition).values({
        id: "wfd_dup",
        tenantId: "tnt_root",
        name: "dup",
        originAgentId: "agt_1",
      });
      await h.db.insert(agentSession).values({
        id: "ses_d",
        tenantId: "tnt_root",
        agentId: "wfd_dup",
        principalId: "prn_creator",
        status: "active",
      });
      await h.db.insert(agentInstance).values({
        id: "ins_dup",
        agentId: "agt_1",
        tenantId: "tnt_root",
        principalId: "prn_creator",
        address,
        sessionId: "ses_d",
        status: "deployed",
      });
      await seedWorkflowRun(h.db, {
        id: "run_dup",
        tenantId: "tnt_root",
        deploymentId: null,
        principalId: "prn_creator",
        address,
        status: "running",
      });

      await expect(resolveRoutableAddress(h.db, address)).rejects.toThrow(
        /both an agent instance/,
      );
    });
  },
);
