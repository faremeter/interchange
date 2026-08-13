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
import { agentSession, workflowDefinition } from "@intx/db/schema";
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
    }

    test("resolves a folded workflow-run address to kind run, session via the run principal", async () => {
      await seedBase();
      // The run carries its own principal; its active session is keyed by that
      // principal (the run row has no session column).
      await seedPrincipal(h.db, { id: "prn_run", tenantId: "tnt_root" });
      await h.db.insert(workflowDefinition).values({
        id: "wfd_folded",
        tenantId: "tnt_root",
        name: "folded",
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
        anchorRunId: null,
        definitionId: "wfd_folded",
        principalId: "prn_run",
        address: "run_folded@root.example",
        status: "running",
        publicKey: "pk-run",
      });

      const endpoint = await resolveRoutableAddress(
        h.db,
        "run_folded@root.example",
      );
      expect(endpoint?.id).toBe("run_folded");
      expect(endpoint?.tenantId).toBe("tnt_root");
      expect(endpoint?.publicKey).toBe("pk-run");
      expect(endpoint?.sessionId).toBe("ses_r");
    });

    test("returns undefined for an unknown address", async () => {
      await seedBase();
      expect(
        await resolveRoutableAddress(h.db, "run_missing@root.example"),
      ).toBeUndefined();
    });

    test("resolves a deployment anchor run to the unified run", async () => {
      await seedBase();
      // The collapse folds the deployment onto one self-anchored run that owns
      // its routing address, so resolving that address returns the run: there
      // is no longer a separate workflow-derived key path that hides it. This
      // anchor carries no own principal, so the resolved endpoint has no
      // session.
      await seedWorkflowRun(h.db, {
        id: "run_anchor",
        tenantId: "tnt_root",
        anchorRunId: null,
        principalId: null,
        address: "run_anchor@root.example",
        status: "running",
      });
      const endpoint = await resolveRoutableAddress(
        h.db,
        "run_anchor@root.example",
      );
      expect(endpoint?.id).toBe("run_anchor");
      expect(endpoint?.sessionId).toBeNull();
    });

    test("skips a terminated run (endedAt set)", async () => {
      await seedBase();
      await seedWorkflowRun(h.db, {
        id: "run_dead",
        tenantId: "tnt_root",
        anchorRunId: null,
        principalId: "prn_creator",
        address: "run_dead@root.example",
        status: "cancelled",
        endedAt: new Date(0),
      });
      expect(
        await resolveRoutableAddress(h.db, "run_dead@root.example"),
      ).toBeUndefined();
    });

    test("resolves a run with no principal to a null session", async () => {
      await seedBase();
      await seedWorkflowRun(h.db, {
        id: "run_noprincipal",
        tenantId: "tnt_root",
        anchorRunId: null,
        principalId: null,
        address: "run_noprincipal@root.example",
        status: "running",
      });
      const endpoint = await resolveRoutableAddress(
        h.db,
        "run_noprincipal@root.example",
      );
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
        anchorRunId: null,
        definitionId: "wfd_sessionless",
        principalId: "prn_sessionless",
        address: "run_sessionless@root.example",
        status: "running",
      });
      const endpoint = await resolveRoutableAddress(
        h.db,
        "run_sessionless@root.example",
      );
      expect(endpoint?.sessionId).toBeNull();
    });
  },
);
