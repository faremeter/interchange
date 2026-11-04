import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createRunGrantsLoader } from "@intx/hub-api";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedGrant,
  seedPrincipal,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

// The sidecar router sends what this loader returns ahead of each signal, so
// a decision committed before the signal reaches the run first.

describe.skipIf(!harnessDbEnvAvailable())(
  "createRunGrantsLoader (real DB)",
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
      await seedTenants(h.db, [{ id: "tnt" }]);
    });

    test("returns the run principal's committed grants in id order", async () => {
      await seedPrincipal(h.db, {
        id: "prn_run_a",
        tenantId: "tnt",
        kind: "workflow",
        refId: "run_a",
      });
      await seedWorkflowRun(h.db, {
        id: "run_a",
        tenantId: "tnt",
        principalId: "prn_run_a",
        status: "running",
      });
      for (const [id, effect] of [
        ["grt_2", "allow"],
        ["grt_1", "ask"],
      ] as const) {
        await seedGrant(h.db, {
          id,
          tenantId: "tnt",
          principalId: "prn_run_a",
          resource: `tool:${id}`,
          action: "invoke",
          effect,
          origin: "system",
        });
      }

      const grants = await createRunGrantsLoader({ db: h.db })("run_a");
      expect(grants?.map((g) => [g.id, g.effect])).toEqual([
        ["grt_1", "ask"],
        ["grt_2", "allow"],
      ]);
    });

    test("returns null for an unknown run and for a run with no principal", async () => {
      await seedWorkflowRun(h.db, {
        id: "run_child",
        tenantId: "tnt",
        status: "running",
      });
      const load = createRunGrantsLoader({ db: h.db });
      expect(await load("run_missing")).toBeNull();
      expect(await load("run_child")).toBeNull();
    });

    test("returns null for a run row linked to another run's principal", async () => {
      await seedPrincipal(h.db, {
        id: "prn_other",
        tenantId: "tnt",
        kind: "workflow",
        refId: "run_other",
      });
      await seedGrant(h.db, {
        id: "grt_other",
        tenantId: "tnt",
        principalId: "prn_other",
        resource: "tool:x",
        action: "use",
        effect: "allow",
        origin: "creator",
      });
      await seedWorkflowRun(h.db, {
        id: "run_a",
        tenantId: "tnt",
        principalId: "prn_other",
        status: "running",
      });
      expect(await createRunGrantsLoader({ db: h.db })("run_a")).toBeNull();
    });

    test("returns null for a run row linked to a principal that is not a workflow", async () => {
      await seedPrincipal(h.db, {
        id: "prn_user_a",
        tenantId: "tnt",
        kind: "user",
        refId: "run_a",
      });
      await seedGrant(h.db, {
        id: "grt_user_a",
        tenantId: "tnt",
        principalId: "prn_user_a",
        resource: "tool:x",
        action: "use",
        effect: "allow",
        origin: "creator",
      });
      await seedWorkflowRun(h.db, {
        id: "run_a",
        tenantId: "tnt",
        principalId: "prn_user_a",
        status: "running",
      });
      expect(await createRunGrantsLoader({ db: h.db })("run_a")).toBeNull();
    });

    test("returns null for a run whose principal is not linked yet", async () => {
      // The run principal exists, but no first trigger has linked it to the
      // run row, so the run has committed no grants.
      await seedWorkflowRun(h.db, {
        id: "run_unfired",
        tenantId: "tnt",
        status: "deployed",
      });
      await seedPrincipal(h.db, {
        id: "prn_run_unfired",
        tenantId: "tnt",
        kind: "workflow",
        refId: "run_unfired",
      });
      expect(
        await createRunGrantsLoader({ db: h.db })("run_unfired"),
      ).toBeNull();
    });
  },
);
