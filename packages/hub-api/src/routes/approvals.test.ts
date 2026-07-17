import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { createInMemoryGrantStore } from "@intx/authz";
import { ErrorResponse } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import type { ApprovalStore, SignalCorrelationStore, DB } from "@intx/db";

import { createApp } from "../app";
import {
  createSidecarEmitter,
  type EventCollectorRegistry,
  type SessionService,
  type SidecarRouter,
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
    toolDefinition: null,
    toolArguments: null,
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

function createMockDB(): DB["db"] {
  // The resolver only touches the db through `db.transaction`; the stores it
  // uses are injected as mocks, so the tx handle is never read here.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- drizzle PgDatabase type cannot be structurally satisfied in tests
  return {
    query: {
      tenant: { findFirst: async () => testTenant },
      principal: { findFirst: async () => testPrincipal },
    },
    transaction: async (fn: (tx: unknown) => Promise<unknown>) => fn({}),
  } as unknown as DB["db"];
}

type ResolveCall = {
  status: string;
  scope: string | undefined;
};

type MockApprovalStoreOpts = {
  approval: NonNullable<ParsedApproval> | null;
  resolveResult?: NonNullable<ParsedApproval> | null;
  resolveCalls: ResolveCall[];
};

function createMockApprovalStore(opts: MockApprovalStoreOpts): ApprovalStore {
  function notImpl(name: string): never {
    throw new Error(`mock: approvalStore.${name} not implemented`);
  }
  return {
    create: () => notImpl("create"),
    createIfAbsent: () => notImpl("createIfAbsent"),
    findByCorrelationId: () => notImpl("findByCorrelationId"),
    findById: async (id) => (id === APPROVAL_ID ? opts.approval : null),
    resolve: async (_correlationId, args) => {
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
    sendAgentDeploy: () => notImpl("sendAgentDeploy"),
    sendAgentUndeploy: () => notImpl("sendAgentUndeploy"),
    sendSourcesUpdate: () => notImpl("sendSourcesUpdate"),
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

type TestAppOpts = {
  approval?: NonNullable<ParsedApproval> | null;
  resolveResult?: NonNullable<ParsedApproval> | null;
  claimResult?: { agentAddress: string; runId: string } | null;
  grants?: GrantRule[];
  signalCalls?: SignalCall[];
  resolveCalls?: ResolveCall[];
  claimCalls?: ClaimCall[];
  deliverThrows?: boolean;
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
    db: createMockDB(),
    grantStore: createInMemoryGrantStore(opts.grants ?? [makeGrant()]),
    approvalStore: createMockApprovalStore({
      approval,
      ...(opts.resolveResult !== undefined
        ? { resolveResult: opts.resolveResult }
        : {}),
      resolveCalls: opts.resolveCalls ?? [],
    }),
    signalCorrelationStore: createMockSignalCorrelationStore({
      claimResult,
      claimCalls: opts.claimCalls ?? [],
    }),
    sidecarRouter: createMockSidecarRouter(
      opts.signalCalls ?? [],
      opts.deliverThrows ?? false,
    ),
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

async function errorCode(res: Response): Promise<string> {
  const parsed = ErrorResponse(await res.json());
  if (parsed instanceof type.errors) {
    throw new Error(`unexpected error body: ${parsed.summary}`);
  }
  return parsed.error.code;
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
