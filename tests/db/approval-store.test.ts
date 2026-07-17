import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import {
  createApprovalStore,
  createSignalCorrelationStore,
  parseApprovalRow,
} from "@intx/db";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAgent,
  seedAgentInstance,
  seedPrincipal,
  seedTenants,
} from "@intx/test-harness/seed";

const TENANT = "tnt";
const PRINCIPAL = "prin";
const AGENT = "agt";
const INSTANCE = "inst";

async function seedApprovalDeps(h: TestDb): Promise<void> {
  await seedTenants(h.db, [{ id: TENANT }]);
  await seedPrincipal(h.db, { id: PRINCIPAL, tenantId: TENANT });
  await seedAgent(h.db, {
    id: AGENT,
    tenantId: TENANT,
    creatorPrincipalId: PRINCIPAL,
  });
  await seedAgentInstance(h.db, {
    id: INSTANCE,
    tenantId: TENANT,
    agentId: AGENT,
    principalId: PRINCIPAL,
  });
}

function approvalRow(correlationId: string) {
  return {
    id: `apr_${correlationId}`,
    tenantId: TENANT,
    instanceId: INSTANCE,
    agentId: AGENT,
    originPrincipalId: PRINCIPAL,
    correlationId,
    originKind: "creator" as const,
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
    await seedApprovalDeps(h);
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
    await seedApprovalDeps(h);
    const store = createApprovalStore(h.db);
    await store.create(approvalRow("corr-find"));

    const found = await store.findByCorrelationId("corr-find");
    expect(found?.id).toBe("apr_corr-find");

    const missing = await store.findByCorrelationId("nope");
    expect(missing).toBeNull();
  });

  test("round-trips an approval with a null tool snapshot", async () => {
    await seedApprovalDeps(h);
    const store = createApprovalStore(h.db);

    const inserted = await store.create(approvalRow("corr-null"));
    expect(inserted.toolDefinition).toBeNull();
    expect(inserted.toolArguments).toBeNull();

    const parsed = parseApprovalRow(inserted);
    expect(parsed.toolDefinition).toBeNull();
    expect(parsed.toolArguments).toBeNull();
  });
});

function correlationRow(correlationId: string) {
  return {
    correlationId,
    tenantId: TENANT,
    deploymentId: "dep-1",
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
      await seedTenants(h.db, [{ id: TENANT }]);
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
