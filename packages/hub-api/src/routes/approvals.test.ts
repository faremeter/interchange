import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { createInMemoryGrantStore } from "@intx/authz";
import {
  ApprovalResponse,
  ErrorResponse,
  type SidecarAllocationStatus,
} from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import type { ApprovalStore, SignalCorrelationStore, DB } from "@intx/db";
import { sidecarAllocation, workflowRun } from "@intx/db/schema";
import type { WorkflowRunLifecycle } from "@intx/hub-sessions";

import { createApp } from "../app";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
  type WorkflowDispatchService,
} from "@intx/hub-sessions";
import type { GetSession } from "../session";

const TENANT_ID = "tnt_test";
const OTHER_TENANT_ID = "tnt_other";
const PRINCIPAL_ID = "prn_test";
const USER_ID = "usr_test";
const DOMAIN = "test.example.com";
const DEPLOYMENT_ID = "dep_abc";
const APPROVAL_ID = "apr_1";
const CORRELATION_ID = "corr_1";
const RUN_ID = "run_1";
const AGENT_ADDRESS = `ins_${DEPLOYMENT_ID}@${DOMAIN}`;

const testTenant = {
  id: TENANT_ID,
  name: "Test",
  slug: "test",
  domain: DOMAIN,
  parentId: null,
  config: null,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

const testPrincipal = {
  id: PRINCIPAL_ID,
  tenantId: TENANT_ID,
  kind: "user" as const,
  refId: USER_ID,
  status: "active" as const,
  createdAt: new Date("2025-01-01"),
  updatedAt: new Date("2025-01-01"),
};

type ParsedApproval = Awaited<ReturnType<ApprovalStore["findById"]>>;

function pendingApproval(
  overrides: Partial<NonNullable<ParsedApproval>> = {},
): NonNullable<ParsedApproval> {
  return {
    id: APPROVAL_ID,
    tenantId: TENANT_ID,
    deploymentId: DEPLOYMENT_ID,
    runId: RUN_ID,
    agentAddress: AGENT_ADDRESS,
    correlationId: CORRELATION_ID,
    toolDefinition: {
      name: "charge_card",
      description: "Charge the customer's card",
      inputSchema: { type: "object" },
    },
    toolArguments: { amount: 100 },
    scope: null,
    status: "pending",
    timeoutAt: null,
    resolvedAt: null,
    createdAt: new Date("2025-01-02"),
    updatedAt: new Date("2025-01-02"),
    ...overrides,
  };
}

function makeGrant(overrides: Partial<GrantRule> = {}): GrantRule {
  return {
    id: "grant-test",
    resource: `approval:${DEPLOYMENT_ID}`,
    action: "resolve",
    effect: "allow",
    origin: "system",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: PRINCIPAL_ID,
    ...overrides,
  };
}

function createMockDB(
  approvalList: NonNullable<ParsedApproval>[] = [],
  sidecarAllocationStatus?: SidecarAllocationStatus,
  allocationLocks: string[] = [],
  workflowRunStatuses: readonly ("running" | "completed")[] = [
    "running",
    "running",
  ],
  operationOrder: string[] = [],
): DB["db"] {
  // The resolver locks an exclusive allocation inside `db.transaction`; the
  // injected stores ignore the rest of the tx handle. The list route reads
  // `db.query.approval.findMany`; the mock ignores the where/order (tenant
  // scoping and keyset ordering are exercised by the real-DB tests) and returns
  // the supplied rows, which the route parses and formats.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: { findFirst: async () => testTenant },
      principal: { findFirst: async () => testPrincipal },
      approval: { findMany: async () => approvalList },
    },
    transaction: async (fn: (tx: unknown) => Promise<unknown>) => {
      let workflowRunLockIndex = 0;
      return fn({
        select: () => ({
          from: (table: unknown) => ({
            where: () => ({
              limit: () => ({
                for: async (lock: string) => {
                  if (table === sidecarAllocation) {
                    allocationLocks.push(lock);
                    operationOrder.push("allocation-lock");
                    return sidecarAllocationStatus === undefined
                      ? []
                      : [{ id: "sal_test", status: sidecarAllocationStatus }];
                  }
                  if (table === workflowRun) {
                    const index = workflowRunLockIndex;
                    workflowRunLockIndex += 1;
                    operationOrder.push(
                      index === 0 ? "anchor-lock" : "target-lock",
                    );
                    const status = workflowRunStatuses[index];
                    return status === undefined ? [] : [{ status }];
                  }
                  throw new Error("mock: unexpected table lock");
                },
              }),
            }),
          }),
        }),
      });
    },
  } as unknown as DB["db"];
}

type ResolveCall = {
  status: string;
  scope: string | undefined;
};

type MockApprovalStoreOpts = {
  approval: NonNullable<ParsedApproval> | null;
  transactionalApproval?: NonNullable<ParsedApproval> | null;
  resolveResult?: NonNullable<ParsedApproval> | null;
  resolveCalls: ResolveCall[];
  operationOrder?: string[];
};

function createMockApprovalStore(opts: MockApprovalStoreOpts): ApprovalStore {
  function notImpl(name: string): never {
    throw new Error(`mock: approvalStore.${name} not implemented`);
  }
  return {
    create: () => notImpl("create"),
    createIfAbsent: () => notImpl("createIfAbsent"),
    findByCorrelationId: () => notImpl("findByCorrelationId"),
    findById: async (id, tx) => {
      if (id !== APPROVAL_ID) return null;
      return tx !== undefined && opts.transactionalApproval !== undefined
        ? opts.transactionalApproval
        : opts.approval;
    },
    resolve: async (_correlationId, args) => {
      opts.operationOrder?.push("resolve");
      opts.resolveCalls.push({ status: args.status, scope: args.scope });
      return opts.resolveResult === undefined
        ? {
            ...pendingApproval(),
            status: args.status,
            scope: args.scope ?? null,
            resolvedAt: args.resolvedAt,
          }
        : opts.resolveResult;
    },
  };
}

type ClaimCall = { correlationId: string; signalId: string | null };

type MockSignalStoreOpts = {
  claimResult: { agentAddress: string; runId: string } | null;
  claimCalls: ClaimCall[];
  operationOrder?: string[];
};

function createMockSignalCorrelationStore(
  opts: MockSignalStoreOpts,
): SignalCorrelationStore {
  function notImpl(name: string): never {
    throw new Error(`mock: signalCorrelationStore.${name} not implemented`);
  }
  return {
    register: () => notImpl("register"),
    registerIfAbsent: () => notImpl("registerIfAbsent"),
    resolveRoute: () => notImpl("resolveRoute"),
    claimTerminal: async (correlationId, _resolvedAt, signalId) => {
      opts.operationOrder?.push("claim");
      opts.claimCalls.push({ correlationId, signalId });
      if (opts.claimResult === null) return null;
      return {
        correlationId,
        tenantId: TENANT_ID,
        deploymentId: DEPLOYMENT_ID,
        agentAddress: opts.claimResult.agentAddress,
        runId: opts.claimResult.runId,
        signalName: "sig",
        kind: "approval" as const,
        signalId,
        resolvedAt: _resolvedAt,
        createdAt: new Date("2025-01-02"),
      };
    },
  };
}

function createMockGetSession(): GetSession {
  const now = new Date("2025-01-01");
  return async () => ({
    user: {
      id: USER_ID,
      email: "test@example.com",
      emailVerified: true,
      name: "Test User",
      createdAt: now,
      updatedAt: now,
    },
    session: {
      id: "session_test",
      userId: USER_ID,
      token: "tok_test",
      expiresAt: new Date("2999-01-01"),
      createdAt: now,
      updatedAt: now,
    },
  });
}

type SignalCall = Parameters<SidecarRouter["sendSignalDeliver"]>[0];

function createMockSidecarRouter(
  signalCalls: SignalCall[],
  deliverThrows = false,
): SidecarRouter {
  function notImpl(name: string): never {
    throw new Error(`mock: sidecarRouter.${name} not implemented`);
  }
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
    sendSignalDeliver: (opts) => {
      signalCalls.push(opts);
      if (deliverThrows) {
        throw new Error("no sidecar connected");
      }
    },
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
  function notImpl(name: string): never {
    throw new Error(`mock: sessionService.${name} not implemented`);
  }
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
  function notImpl(name: string): never {
    throw new Error(`mock: eventCollectors.${name} not implemented`);
  }
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

type WorkflowSignalDispatchEnqueue = Parameters<
  WorkflowDispatchService["enqueueSignal"]
>[0];

function createMockWorkflowDispatchService(
  enqueues: WorkflowSignalDispatchEnqueue[],
  transactionalEnqueues: boolean[],
  wakeCalls: string[],
  operationOrder: string[] = [],
): WorkflowDispatchService {
  function notImpl(name: string): never {
    throw new Error(`mock: workflowDispatchService.${name} not implemented`);
  }
  return {
    enqueue: () => notImpl("enqueue"),
    async enqueueSignal(args, tx) {
      operationOrder.push("enqueue");
      enqueues.push(args);
      transactionalEnqueues.push(tx !== undefined);
      const now = args.now ?? new Date();
      return {
        created: true,
        dispatch: {
          id: args.id,
          anchorRunId: args.anchorRunId,
          messageId: args.signal.signalId,
          kind: "signal",
          rawMessage: new TextEncoder().encode(JSON.stringify(args.signal)),
          stepGrants: [],
          status: "pending",
          acknowledgedGeneration: null,
          attemptCount: 0,
          nextAttemptAt: now,
          deliveryLeaseId: null,
          deliveryLeaseExpiresAt: null,
          failureCode: null,
          failureMessage: null,
          createdAt: now,
          updatedAt: now,
          acknowledgedAt: null,
          settledAt: null,
        },
      };
    },
    acknowledge: async () => notImpl("acknowledge"),
    settle: async () => notImpl("settle"),
    requeueForReadyAllocation: async () => notImpl("requeueForReadyAllocation"),
    reconcileNext: async () => notImpl("reconcileNext"),
    reconcileUntilIdle: async () => notImpl("reconcileUntilIdle"),
    wake: () => {
      wakeCalls.push("wake");
    },
  };
}

type TestAppOpts = {
  approval?: NonNullable<ParsedApproval> | null;
  transactionalApproval?: NonNullable<ParsedApproval> | null;
  approvalList?: NonNullable<ParsedApproval>[];
  resolveResult?: NonNullable<ParsedApproval> | null;
  claimResult?: { agentAddress: string; runId: string } | null;
  grants?: GrantRule[];
  signalCalls?: SignalCall[];
  resolveCalls?: ResolveCall[];
  claimCalls?: ClaimCall[];
  deliverThrows?: boolean;
  hasSidecarAllocation?: boolean;
  sidecarAllocationStatus?: SidecarAllocationStatus;
  workflowSignalEnqueues?: WorkflowSignalDispatchEnqueue[];
  transactionalEnqueues?: boolean[];
  dispatchWakeCalls?: string[];
  allocationLocks?: string[];
  runLifecycles?: {
    topLevel: WorkflowRunLifecycle;
    target: WorkflowRunLifecycle;
  };
  workflowRunStatuses?: readonly ("running" | "completed")[];
  operationOrder?: string[];
};

function createTestApp(opts: TestAppOpts = {}) {
  const approval =
    opts.approval === undefined ? pendingApproval() : opts.approval;
  const claimResult =
    opts.claimResult === undefined
      ? { agentAddress: AGENT_ADDRESS, runId: RUN_ID }
      : opts.claimResult;
  return createApp({
    getSession: createMockGetSession(),
    authHandler: () => new Response("", { status: 404 }),
    db: createMockDB(
      opts.approvalList ?? [],
      opts.hasSidecarAllocation
        ? (opts.sidecarAllocationStatus ?? "allocated")
        : undefined,
      opts.allocationLocks,
      opts.workflowRunStatuses,
      opts.operationOrder,
    ),
    grantStore: createInMemoryGrantStore(opts.grants ?? [makeGrant()]),
    approvalStore: createMockApprovalStore({
      approval,
      ...(opts.transactionalApproval !== undefined
        ? { transactionalApproval: opts.transactionalApproval }
        : {}),
      ...(opts.resolveResult !== undefined
        ? { resolveResult: opts.resolveResult }
        : {}),
      resolveCalls: opts.resolveCalls ?? [],
      ...(opts.operationOrder !== undefined
        ? { operationOrder: opts.operationOrder }
        : {}),
    }),
    signalCorrelationStore: createMockSignalCorrelationStore({
      claimResult,
      claimCalls: opts.claimCalls ?? [],
      ...(opts.operationOrder !== undefined
        ? { operationOrder: opts.operationOrder }
        : {}),
    }),
    sidecarRouter: createMockSidecarRouter(
      opts.signalCalls ?? [],
      opts.deliverThrows ?? false,
    ),
    ...(opts.workflowSignalEnqueues !== undefined
      ? {
          workflowDispatchService: createMockWorkflowDispatchService(
            opts.workflowSignalEnqueues,
            opts.transactionalEnqueues ?? [],
            opts.dispatchWakeCalls ?? [],
            opts.operationOrder,
          ),
        }
      : {}),
    ...(opts.workflowSignalEnqueues !== undefined
      ? {
          readRunLifecycles: async () => {
            opts.operationOrder?.push("git-lifecycle");
            return opts.runLifecycles ?? { topLevel: "live", target: "live" };
          },
        }
      : {}),
    sessionService: createMockSessionService(),
    eventCollectors: createMockEventCollectors(),
    assetService: null,
    repoStore: null,
    maxTarballBytes: 10_000_000,
  });
}

function base(tenantId = TENANT_ID): string {
  return `/api/tenants/${tenantId}/approvals`;
}

function authedPost(path: string, body: unknown): Request {
  return new Request(`http://localhost${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function authedGet(path: string): Request {
  return new Request(`http://localhost${path}`, { method: "GET" });
}

async function errorCode(res: Response): Promise<string> {
  const parsed = ErrorResponse(await res.json());
  if (parsed instanceof type.errors) {
    throw new Error(`unexpected error body: ${parsed.summary}`);
  }
  return parsed.error.code;
}

const ApprovalListBody = type({
  data: ApprovalResponse.array(),
  nextCursor: "string | null",
});

async function listBody(res: Response): Promise<typeof ApprovalListBody.infer> {
  const parsed = ApprovalListBody(await res.json());
  if (parsed instanceof type.errors) {
    throw new Error(`unexpected list body: ${parsed.summary}`);
  }
  return parsed;
}

describe("POST /approvals/:approvalId/approve", () => {
  test("claims, resolves once, and delivers the approved decision", async () => {
    const signalCalls: SignalCall[] = [];
    const resolveCalls: ResolveCall[] = [];
    const claimCalls: ClaimCall[] = [];
    const app = createTestApp({ signalCalls, resolveCalls, claimCalls });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toMatchObject({
      id: APPROVAL_ID,
      status: "approved",
      scope: "once",
    });

    expect(claimCalls).toEqual([
      { correlationId: CORRELATION_ID, signalId: expect.any(String) },
    ]);
    expect(resolveCalls).toEqual([{ status: "approved", scope: "once" }]);

    expect(signalCalls).toHaveLength(1);
    const call = signalCalls[0];
    if (call === undefined) throw new Error("missing signal call");
    expect(call.agentAddress).toBe(AGENT_ADDRESS);
    expect(call.runId).toBe(RUN_ID);
    expect(call.signalName).toBe(`__signal__:${CORRELATION_ID}`);
    // The delivered signalId is the same value persisted on the claim.
    expect(call.signalId).toBe(claimCalls[0]?.signalId ?? "");
    expect(call.payload).toEqual({ outcome: "approved" });
  });

  test("atomically enqueues an exclusive approval decision for replay", async () => {
    const signalCalls: SignalCall[] = [];
    const claimCalls: ClaimCall[] = [];
    const enqueues: WorkflowSignalDispatchEnqueue[] = [];
    const transactionalEnqueues: boolean[] = [];
    const wakeCalls: string[] = [];
    const allocationLocks: string[] = [];
    const operationOrder: string[] = [];
    const app = createTestApp({
      hasSidecarAllocation: true,
      signalCalls,
      claimCalls,
      workflowSignalEnqueues: enqueues,
      transactionalEnqueues,
      dispatchWakeCalls: wakeCalls,
      allocationLocks,
      operationOrder,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(200);
    expect(signalCalls).toHaveLength(0);
    expect(transactionalEnqueues).toEqual([true]);
    expect(allocationLocks).toEqual(["update"]);
    expect(operationOrder).toEqual([
      "allocation-lock",
      "git-lifecycle",
      "anchor-lock",
      "target-lock",
      "claim",
      "resolve",
      "enqueue",
    ]);
    expect(wakeCalls).toEqual(["wake"]);
    expect(enqueues).toHaveLength(1);
    const enqueue = enqueues[0];
    if (enqueue === undefined) throw new Error("missing signal enqueue");
    expect(enqueue.id).toBe(
      `dispatch:${DEPLOYMENT_ID}:${claimCalls[0]?.signalId ?? ""}`,
    );
    expect(enqueue.anchorRunId).toBe(DEPLOYMENT_ID);
    expect(enqueue.signal).toMatchObject({
      agentAddress: AGENT_ADDRESS,
      runId: RUN_ID,
      signalName: `__signal__:${CORRELATION_ID}`,
      signalId: claimCalls[0]?.signalId,
      payload: { outcome: "approved" },
    });
  });

  for (const terminalRun of ["topLevel", "target"] as const) {
    test(`does not resolve when the durable ${terminalRun} run is terminal`, async () => {
      const claimCalls: ClaimCall[] = [];
      const resolveCalls: ResolveCall[] = [];
      const enqueues: WorkflowSignalDispatchEnqueue[] = [];
      const operationOrder: string[] = [];
      const app = createTestApp({
        hasSidecarAllocation: true,
        claimCalls,
        resolveCalls,
        workflowSignalEnqueues: enqueues,
        runLifecycles: {
          topLevel: terminalRun === "topLevel" ? "terminal" : "live",
          target: terminalRun === "target" ? "terminal" : "live",
        },
        operationOrder,
      });

      const res = await app.fetch(
        authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
      );

      expect(res.status).toBe(409);
      expect(await errorCode(res)).toBe("workflow_run_not_running");
      expect(operationOrder).toEqual(["allocation-lock", "git-lifecycle"]);
      expect(claimCalls).toEqual([]);
      expect(resolveCalls).toEqual([]);
      expect(enqueues).toEqual([]);
    });
  }

  for (const terminalRun of ["anchor", "target"] as const) {
    test(`does not resolve when the SQL ${terminalRun} run is terminal`, async () => {
      const claimCalls: ClaimCall[] = [];
      const resolveCalls: ResolveCall[] = [];
      const enqueues: WorkflowSignalDispatchEnqueue[] = [];
      const operationOrder: string[] = [];
      const app = createTestApp({
        hasSidecarAllocation: true,
        claimCalls,
        resolveCalls,
        workflowSignalEnqueues: enqueues,
        workflowRunStatuses:
          terminalRun === "anchor"
            ? ["completed", "running"]
            : ["running", "completed"],
        operationOrder,
      });

      const res = await app.fetch(
        authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
      );

      expect(res.status).toBe(409);
      expect(await errorCode(res)).toBe("workflow_run_not_running");
      expect(operationOrder).toEqual([
        "allocation-lock",
        "git-lifecycle",
        "anchor-lock",
        "target-lock",
      ]);
      expect(claimCalls).toEqual([]);
      expect(resolveCalls).toEqual([]);
      expect(enqueues).toEqual([]);
    });
  }

  test("preserves already-resolved precedence after a terminal lifecycle check", async () => {
    const claimCalls: ClaimCall[] = [];
    const resolveCalls: ResolveCall[] = [];
    const enqueues: WorkflowSignalDispatchEnqueue[] = [];
    const app = createTestApp({
      transactionalApproval: pendingApproval({
        status: "approved",
        scope: "once",
        resolvedAt: new Date("2025-01-03"),
      }),
      hasSidecarAllocation: true,
      claimCalls,
      resolveCalls,
      workflowSignalEnqueues: enqueues,
      runLifecycles: { topLevel: "live", target: "terminal" },
    });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(409);
    expect(await errorCode(res)).toBe("already_resolved");
    expect(claimCalls).toEqual([]);
    expect(resolveCalls).toEqual([]);
    expect(enqueues).toEqual([]);
  });

  for (const sidecarAllocationStatus of [
    "releasing",
    "released",
    "failed",
  ] satisfies SidecarAllocationStatus[]) {
    test(`does not resolve an approval for a ${sidecarAllocationStatus} allocation`, async () => {
      const claimCalls: ClaimCall[] = [];
      const resolveCalls: ResolveCall[] = [];
      const enqueues: WorkflowSignalDispatchEnqueue[] = [];
      const app = createTestApp({
        hasSidecarAllocation: true,
        sidecarAllocationStatus,
        claimCalls,
        resolveCalls,
        workflowSignalEnqueues: enqueues,
      });

      const res = await app.fetch(
        authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
      );

      expect(res.status).toBe(409);
      expect(await errorCode(res)).toBe("deployment_unreachable");
      expect(claimCalls).toEqual([]);
      expect(resolveCalls).toEqual([]);
      expect(enqueues).toEqual([]);
    });
  }

  test("does not resolve an exclusive approval without durable dispatch", async () => {
    const claimCalls: ClaimCall[] = [];
    const resolveCalls: ResolveCall[] = [];
    const app = createTestApp({
      hasSidecarAllocation: true,
      claimCalls,
      resolveCalls,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(503);
    expect(await errorCode(res)).toBe("workflow_dispatch_unavailable");
    expect(claimCalls).toHaveLength(0);
    expect(resolveCalls).toHaveLength(0);
  });

  test("returns 409 and does not deliver on a double approve", async () => {
    const signalCalls: SignalCall[] = [];
    const app = createTestApp({ claimResult: null, signalCalls });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(409);
    expect(await errorCode(res)).toBe("already_resolved");
    expect(signalCalls).toHaveLength(0);
  });

  test("reports an approval resolved before its allocation was released", async () => {
    const claimCalls: ClaimCall[] = [];
    const resolveCalls: ResolveCall[] = [];
    const enqueues: WorkflowSignalDispatchEnqueue[] = [];
    const allocationLocks: string[] = [];
    const app = createTestApp({
      transactionalApproval: pendingApproval({
        status: "approved",
        scope: "once",
        resolvedAt: new Date("2025-01-03"),
      }),
      hasSidecarAllocation: true,
      sidecarAllocationStatus: "released",
      claimCalls,
      resolveCalls,
      workflowSignalEnqueues: enqueues,
      allocationLocks,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(409);
    expect(await errorCode(res)).toBe("already_resolved");
    expect(allocationLocks).toEqual(["update"]);
    expect(claimCalls).toEqual([]);
    expect(resolveCalls).toEqual([]);
    expect(enqueues).toEqual([]);
  });

  test("rejects scope 'always' at the boundary without resolving", async () => {
    const signalCalls: SignalCall[] = [];
    const resolveCalls: ResolveCall[] = [];
    const claimCalls: ClaimCall[] = [];
    const app = createTestApp({ signalCalls, resolveCalls, claimCalls });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "always" }),
    );

    expect(res.status).toBe(400);
    expect(await errorCode(res)).toBe("unsupported_scope");
    expect(claimCalls).toHaveLength(0);
    expect(resolveCalls).toHaveLength(0);
    expect(signalCalls).toHaveLength(0);
  });

  test("returns 404 for an approval belonging to another tenant", async () => {
    const signalCalls: SignalCall[] = [];
    const app = createTestApp({
      approval: pendingApproval({ tenantId: OTHER_TENANT_ID }),
      signalCalls,
    });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(404);
    expect(signalCalls).toHaveLength(0);
  });

  test("returns 404 for an unknown approval", async () => {
    const app = createTestApp({ approval: null });
    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );
    expect(res.status).toBe(404);
  });

  test("returns 403 for an approver without the resolve grant", async () => {
    const signalCalls: SignalCall[] = [];
    const resolveCalls: ResolveCall[] = [];
    const app = createTestApp({ grants: [], signalCalls, resolveCalls });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );

    expect(res.status).toBe(403);
    expect(resolveCalls).toHaveLength(0);
    expect(signalCalls).toHaveLength(0);
  });

  test("a tenant-wide approval wildcard grant authorizes the approver", async () => {
    const app = createTestApp({
      grants: [makeGrant({ resource: "approval:*", action: "resolve" })],
    });
    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );
    expect(res.status).toBe(200);
  });

  test("surfaces a post-commit delivery failure loudly", async () => {
    const app = createTestApp({ deliverThrows: true });
    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/approve`, { scope: "once" }),
    );
    expect(res.status).toBe(500);
  });
});

describe("POST /approvals/:approvalId/reject", () => {
  test("resolves rejected and delivers the rejection message", async () => {
    const signalCalls: SignalCall[] = [];
    const resolveCalls: ResolveCall[] = [];
    const app = createTestApp({ signalCalls, resolveCalls });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/reject`, { message: "no thanks" }),
    );

    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toMatchObject({ status: "rejected" });

    // A rejection records no scope on the row.
    expect(resolveCalls).toEqual([{ status: "rejected", scope: undefined }]);

    expect(signalCalls).toHaveLength(1);
    const call = signalCalls[0];
    if (call === undefined) throw new Error("missing signal call");
    expect(call.payload).toEqual({ outcome: "rejected", message: "no thanks" });
  });

  test("delivers a rejection with no message", async () => {
    const signalCalls: SignalCall[] = [];
    const app = createTestApp({ signalCalls });

    const res = await app.fetch(
      authedPost(`${base()}/${APPROVAL_ID}/reject`, {}),
    );

    expect(res.status).toBe(200);
    const call = signalCalls[0];
    if (call === undefined) throw new Error("missing signal call");
    expect(call.payload).toEqual({ outcome: "rejected" });
  });
});

describe("GET /approvals/:approvalId", () => {
  test("returns the approval with its tool snapshot for a holder of the resolve grant", async () => {
    // The default grant is `approval:<deployment>` / `resolve` -- the same
    // capability the approve/reject routes require. Reading must accept it; a
    // regression to action `read` would 403 the approver on the page they need.
    const app = createTestApp();
    const res = await app.fetch(authedGet(`${base()}/${APPROVAL_ID}`));

    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toMatchObject({
      id: APPROVAL_ID,
      status: "pending",
      toolDefinition: {
        name: "charge_card",
        description: "Charge the customer's card",
        inputSchema: { type: "object" },
      },
      toolArguments: { amount: 100 },
    });
  });

  test("returns a resolved approval with its terminal status", async () => {
    const app = createTestApp({
      approval: pendingApproval({
        status: "approved",
        resolvedAt: new Date("2025-01-03"),
      }),
    });
    const res = await app.fetch(authedGet(`${base()}/${APPROVAL_ID}`));

    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toMatchObject({ id: APPROVAL_ID, status: "approved" });
  });

  test("masks a cross-tenant approval as 404", async () => {
    const app = createTestApp({
      approval: pendingApproval({ tenantId: OTHER_TENANT_ID }),
    });
    const res = await app.fetch(authedGet(`${base()}/${APPROVAL_ID}`));

    expect(res.status).toBe(404);
    expect(await errorCode(res)).toBe("not_found");
  });

  test("returns 404 for an unknown approval id", async () => {
    const app = createTestApp({ approval: null });
    const res = await app.fetch(authedGet(`${base()}/apr_missing`));

    expect(res.status).toBe(404);
    expect(await errorCode(res)).toBe("not_found");
  });

  test("returns 403 when the caller holds no grant", async () => {
    const app = createTestApp({ grants: [] });
    const res = await app.fetch(authedGet(`${base()}/${APPROVAL_ID}`));

    expect(res.status).toBe(403);
    expect(await errorCode(res)).toBe("forbidden");
  });
});

describe("GET /approvals", () => {
  test("lists pending approvals with their snapshots for a tenant-wide grant", async () => {
    const app = createTestApp({
      grants: [makeGrant({ resource: "approval:*" })],
      approvalList: [pendingApproval()],
    });
    const res = await app.fetch(authedGet(base()));

    expect(res.status).toBe(200);
    const body = await listBody(res);
    expect(body.data).toHaveLength(1);
    expect(body.data[0]).toMatchObject({
      id: APPROVAL_ID,
      status: "pending",
      toolDefinition: { name: "charge_card" },
      toolArguments: { amount: 100 },
    });
    // Short page: no further cursor.
    expect(body.nextCursor).toBeNull();
  });

  test("returns an empty page when the tenant has no pending approvals", async () => {
    const app = createTestApp({
      grants: [makeGrant({ resource: "approval:*" })],
      approvalList: [],
    });
    const res = await app.fetch(authedGet(base()));

    expect(res.status).toBe(200);
    const body = await listBody(res);
    expect(body.data).toEqual([]);
    expect(body.nextCursor).toBeNull();
  });

  test("forbids a per-deployment approver: listing is a tenant-wide capability", async () => {
    // The default grant is `approval:<deployment>`, which does not match the
    // tenant-wide `approval:*` the list gate demands. Per-deployment approvers
    // read individual approvals by id, not the whole tenant's list.
    const app = createTestApp({ approvalList: [pendingApproval()] });
    const res = await app.fetch(authedGet(base()));

    expect(res.status).toBe(403);
    expect(await errorCode(res)).toBe("forbidden");
  });
});
