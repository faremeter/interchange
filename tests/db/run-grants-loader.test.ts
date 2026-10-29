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
      await seedWorkflowRun(h.db, {
        id: "run_a",
        tenantId: "tnt",
        status: "running",
      });
      await seedPrincipal(h.db, {
        id: "prn_run_a",
        tenantId: "tnt",
        kind: "workflow",
        refId: "run_a",
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
  },
);
