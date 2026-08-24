import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createApprovalStore, createSignalCorrelationStore } from "@intx/db";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedTenants,
  seedWorkflowRun,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const ASSET = "ast";
const DEPLOYMENT = "dep";

async function seedDeploymentDeps(h: TestDb): Promise<void> {
  await seedTenants(h.db, [{ id: TENANT }]);
  await seedAsset(h.db, {
    id: ASSET,
    tenantId: TENANT,
    kind: "workflow",
    name: ASSET,
  });
  // The deployment's anchor run -- the workflow_run whose id equals the
  // deployment id. The approval/correlation `anchor_run_id` FK and the child
  // runs' `anchor_run_id` both resolve to it.
  await seedWorkflowRun(h.db, {
    id: DEPLOYMENT,
    anchorRunId: DEPLOYMENT,
    tenantId: TENANT,
  });
}

// Anchor the run an approval/correlation references, so the runId FK to
// workflow_run resolves. The store rows carry a runId whose only referent is
// this row.
async function seedRun(h: TestDb, runId: string): Promise<void> {
  await seedWorkflowRun(h.db, {
    id: runId,
    anchorRunId: DEPLOYMENT,
    tenantId: TENANT,
  });
}

function approvalRow(correlationId: string) {
  return {
    id: `apr_${correlationId}`,
    tenantId: TENANT,
    anchorRunId: DEPLOYMENT,
    runId: `run_${correlationId}`,
    agentAddress: `addr_${correlationId}`,
    correlationId,
    toolDefinition: {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
    },
    toolArguments: { amount: 100 },
    timeoutAt: new Date(Date.now() + 60_000),
  };
}

describe.skipIf(!harnessDbEnvAvailable())("approval-store (real DB)", () => {
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

  test("resolves a pending approval exactly once", async () => {
    await seedDeploymentDeps(h);
    await seedRun(h, "run_corr-1");
    const store = createApprovalStore(h.db);
    await store.create(approvalRow("corr-1"));

    const first = await store.resolve("corr-1", {
      status: "approved",
      scope: "once",
      resolvedAt: new Date(),
    });
    expect(first).not.toBeNull();
    expect(first?.status).toBe("approved");
    expect(first?.scope).toBe("once");
    expect(first?.resolvedAt).not.toBeNull();

    const second = await store.resolve("corr-1", {
      status: "rejected",
      resolvedAt: new Date(),
    });
    expect(second).toBeNull();
  });

  test("finds an approval by correlation id", async () => {
    await seedDeploymentDeps(h);
    await seedRun(h, "run_corr-find");
    const store = createApprovalStore(h.db);
    await store.create(approvalRow("corr-find"));

    const found = await store.findByCorrelationId("corr-find");
    expect(found?.id).toBe("apr_corr-find");

    const missing = await store.findByCorrelationId("nope");
    expect(missing).toBeNull();
  });

  test("finds an approval by its primary key", async () => {
    await seedDeploymentDeps(h);
    await seedRun(h, "run_corr-byid");
    const store = createApprovalStore(h.db);
    await store.create(approvalRow("corr-byid"));

    const found = await store.findById("apr_corr-byid");
    expect(found?.correlationId).toBe("corr-byid");

    const missing = await store.findById("apr_nope");
    expect(missing).toBeNull();
  });

  test("lists a run's approvals newest first, scoped to the run and tenant", async () => {
    await seedDeploymentDeps(h);
    const RUN = "run_list";
    const OTHER_RUN = "run_list_other";
    await seedRun(h, RUN);
    await seedRun(h, OTHER_RUN);
    const store = createApprovalStore(h.db);

    // Fix the runId (approvalRow ties it to the correlation id) and stamp
    // distinct createdAt out of insertion order, so a passing order proves the
    // `desc(createdAt)` sort rather than insertion order.
    const on = (corr: string, runId: string, createdAt: Date) => ({
      ...approvalRow(corr),
      runId,
      createdAt,
    });
    await store.create(on("c-mid", RUN, new Date("2025-01-02T00:00:00Z")));
    await store.create(on("c-old", RUN, new Date("2025-01-01T00:00:00Z")));
    await store.create(on("c-new", RUN, new Date("2025-01-03T00:00:00Z")));
    // Newest overall, but on a different run -- must be excluded.
    await store.create(
      on("c-other", OTHER_RUN, new Date("2025-01-04T00:00:00Z")),
    );

    const rows = await store.listByRunId(TENANT, RUN);
    expect(rows.map((r) => r.correlationId)).toEqual([
      "c-new",
      "c-mid",
      "c-old",
    ]);

    // Scoped by tenant: another tenant sees none of this run's approvals.
    expect(await store.listByRunId("tnt_other", RUN)).toEqual([]);
  });

  test("round-trips an approval's tool snapshot", async () => {
    await seedDeploymentDeps(h);
    await seedRun(h, "run_corr-snap");
    const store = createApprovalStore(h.db);

    const row = approvalRow("corr-snap");
    const inserted = await store.create(row);
    expect(inserted.toolDefinition).toEqual(row.toolDefinition);
    expect(inserted.toolArguments).toEqual(row.toolArguments);

    const found = await store.findByCorrelationId("corr-snap");
    expect(found?.toolDefinition).toEqual(row.toolDefinition);
    expect(found?.toolArguments).toEqual(row.toolArguments);
  });
});

function correlationRow(correlationId: string) {
  return {
    correlationId,
    tenantId: TENANT,
    anchorRunId: DEPLOYMENT,
    agentAddress: "addr-1",
    runId: "run-1",
    signalName: "sig-1",
    kind: "approval" as const,
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "signal-correlation-store (real DB)",
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

    test("claims a correlation for terminal delivery exactly once", async () => {
      await seedDeploymentDeps(h);
      await seedRun(h, "run-1");
      const store = createSignalCorrelationStore(h.db);
      await store.register(correlationRow("c-1"));

      const first = await store.claimTerminal("c-1", new Date(), "sigid-1");
      expect(first).not.toBeNull();
      expect(first?.resolvedAt).not.toBeNull();
      expect(first?.signalId).toBe("sigid-1");

      const second = await store.claimTerminal("c-1", new Date(), "sigid-2");
      expect(second).toBeNull();

      const route = await store.resolveRoute("c-1");
      expect(route?.signalId).toBe("sigid-1");
      expect(route?.runId).toBe("run-1");
    });

    test("resolveRoute returns null for an unknown correlation", async () => {
      await seedTenants(h.db, [{ id: TENANT }]);
      const store = createSignalCorrelationStore(h.db);
      expect(await store.resolveRoute("ghost")).toBeNull();
    });
  },
);
