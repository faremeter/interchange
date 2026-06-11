import { describe, test, expect, beforeEach } from "bun:test";

import type { DB } from "@intx/db";
import { inferenceTurn, turnPart } from "@intx/db/schema";
import type { GrantRule, GrantStore } from "@intx/types/authz";
import type { InferenceEvent, InferenceSource } from "@intx/types/runtime";

import type { AgentRepoStore, DeployContent } from "./agent-repo";
import type { RepoStore } from "./repo-store";
import type { EventCollectorRegistry } from "./event-collector-registry";
import { createHubSessionOrchestrator } from "./hub-session-orchestrator";
import {
  createSidecarEmitter,
  type SidecarEventEmitter,
} from "./ws/sidecar-events";

// ---------------------------------------------------------------------------
// Fixtures and mocks
// ---------------------------------------------------------------------------

const TENANT_ID = "tnt_1";
const PRINCIPAL_ID = "prn_1";
const INSTANCE_ID = "ins_1";
const AGENT_ID = "agt_1";
const AGENT_ADDRESS = "ins_1@tenant.local";
const SESSION_ID = "ses_1";

type InstanceStatus = "deployed" | "running" | "updating" | "error" | "stopped";

type InstanceRow = {
  id: string;
  agentId: string;
  tenantId: string;
  principalId: string;
  address: string;
  status: InstanceStatus;
  sessionId: string | null;
  publicKey: string | null;
  createdAt: Date;
  updatedAt: Date;
  endedAt: Date | null;
};

function makeInstance(overrides: Partial<InstanceRow> = {}): InstanceRow {
  return {
    id: INSTANCE_ID,
    agentId: AGENT_ID,
    tenantId: TENANT_ID,
    principalId: PRINCIPAL_ID,
    address: AGENT_ADDRESS,
    status: "running",
    sessionId: SESSION_ID,
    publicKey: null,
    createdAt: new Date("2026-01-01"),
    updatedAt: new Date("2026-01-01"),
    endedAt: null,
    ...overrides,
  };
}

type UpdateCall = { table: string; set: Record<string, unknown> };

type MockDBOpts = {
  instance?: InstanceRow | undefined;
  recordUpdates?: UpdateCall[];
  /** When set, `resolveInstanceSources` queries fan out into these
   * tables; the helper returns empty arrays so the orchestrator's
   * credential push is a no-op. */
  emptyProviderTables?: boolean;
  /** When set, the inference_turn select returns this still-running turn so
   * findOpenTurn (reconnect adoption) resolves it. */
  openTurn?: { id: string } | undefined;
  /** Highest existing turn_part ordinal for the open turn (null = none). */
  maxOrdinal?: number | null;
};

function createMockDB(opts: MockDBOpts) {
  const updates = opts.recordUpdates ?? [];

  function tableName(t: unknown): string {
    if (t && typeof t === "object" && "name" in t && typeof t.name === "string")
      return t.name;
    return "<unknown>";
  }

  /* eslint-disable @typescript-eslint/no-unsafe-type-assertion --
   * drizzle PgDatabase type cannot be structurally satisfied in tests */
  return {
    query: {
      agentInstance: {
        findFirst: async () => opts.instance,
      },
      agent: { findFirst: async () => undefined },
      agentSession: { findFirst: async () => undefined },
      provider: { findFirst: async () => undefined, findMany: async () => [] },
      credential: {
        findFirst: async () => undefined,
        findMany: async () => [],
      },
      oauthClient: { findFirst: async () => undefined },
      tenant: { findFirst: async () => undefined },
    },
    update(t: unknown) {
      return {
        set(values: Record<string, unknown>) {
          return {
            where: async () => {
              updates.push({ table: tableName(t), set: values });
            },
          };
        },
      };
    },
    select() {
      return {
        from: (t: unknown) => {
          if (t === inferenceTurn) {
            const rows = opts.openTurn ? [opts.openTurn] : [];
            return {
              where: () => ({
                orderBy: () => ({ limit: () => Promise.resolve(rows) }),
                limit: () => Promise.resolve(rows),
              }),
            };
          }
          if (t === turnPart) {
            return {
              where: () => Promise.resolve([{ max: opts.maxOrdinal ?? null }]),
            };
          }
          return {
            where: () => ({
              limit: () => Promise.resolve([]),
              orderBy: () => ({ limit: () => Promise.resolve([]) }),
            }),
            innerJoin: () => ({
              where: () => ({ limit: () => Promise.resolve([]) }),
            }),
          };
        },
      };
    },
  } as unknown as DB["db"];
  /* eslint-enable @typescript-eslint/no-unsafe-type-assertion */
}

type RouterCall =
  | { kind: "sendGrantsUpdate"; addr: string; grants: GrantRule[] }
  | {
      kind: "sendSourcesUpdate";
      addr: string;
      sources: InferenceSource[];
      defaultSource: string;
    }
  | {
      kind: "sendPack";
      addr: string;
      pack: Uint8Array;
      ref: string;
      sha: string;
    }
  | { kind: "dispatchAgentEvent"; addr: string; event: unknown };

function createRouterFacade(): {
  facade: Parameters<typeof createHubSessionOrchestrator>[0]["router"];
  calls: RouterCall[];
} {
  const calls: RouterCall[] = [];
  return {
    calls,
    facade: {
      async sendGrantsUpdate(addr, grants) {
        calls.push({ kind: "sendGrantsUpdate", addr, grants });
      },
      async sendSourcesUpdate(addr, sources, defaultSource) {
        calls.push({ kind: "sendSourcesUpdate", addr, sources, defaultSource });
      },
      async sendPack(addr, pack, ref, sha) {
        calls.push({ kind: "sendPack", addr, pack, ref, sha });
      },
      dispatchAgentEvent(addr, event) {
        calls.push({ kind: "dispatchAgentEvent", addr, event });
      },
    },
  };
}

type CollectorCall =
  | {
      kind: "create";
      addr: string;
      sessionId: string;
      resumeTurn: { id: string; nextOrdinal: number } | undefined;
    }
  | { kind: "dispatch"; addr: string; event: InferenceEvent }
  | { kind: "abandon"; addr: string };

function createCollectorRegistry(
  initiallyHas: Set<string> = new Set<string>(),
): {
  registry: EventCollectorRegistry;
  calls: CollectorCall[];
  has: Set<string>;
} {
  const calls: CollectorCall[] = [];
  const has = new Set(initiallyHas);
  return {
    calls,
    has,
    registry: {
      create(addr, _tenantId, sessionId, _instanceId, resumeTurn) {
        has.add(addr);
        calls.push({ kind: "create", addr, sessionId, resumeTurn });
      },
      dispatch(addr, event) {
        calls.push({ kind: "dispatch", addr, event });
      },
      abandon(addr) {
        has.delete(addr);
        calls.push({ kind: "abandon", addr });
      },
      has: (addr) => has.has(addr),
      getStatus: () => undefined,
      getAccumulatedText: () => undefined,
      getCurrentTurnId: () => undefined,
      getLastTurnId: () => undefined,
    },
  };
}

function createGrantStoreStub(grants: GrantRule[] = []): GrantStore {
  return {
    collectGrants: async () => grants,
  };
}

type RepoCall = { kind: "createDeployPack"; agentId: string };

function createRepoStoreStub(): {
  store: AgentRepoStore;
  calls: RepoCall[];
} {
  const calls: RepoCall[] = [];
  return {
    calls,
    store: {
      async writeDeployTree(_agentId: string, _content: DeployContent) {
        throw new Error("mock: writeDeployTree not implemented");
      },
      async createDeployPack(agentId: string) {
        calls.push({ kind: "createDeployPack", agentId });
        return {
          pack: new Uint8Array([1, 2, 3]),
          commitSha: "c".repeat(40),
          ref: "refs/heads/deploy",
        };
      },
      async receiveStatePack() {
        throw new Error("mock: receiveStatePack not implemented");
      },
      getSigningPublicKey() {
        return new Uint8Array(32);
      },
      async getDeployRef() {
        return null;
      },
      repoStore: unusedRepoStore(),
    },
  };
}

function unusedRepoStore(): RepoStore {
  // The orchestrator tests do not touch the substrate; a throwing
  // stub keeps the AgentRepoStore surface fully populated without
  // pulling a real on-disk store into orchestrator-level unit tests.
  const unused = () =>
    Promise.reject(new Error("mock AgentRepoStore.repoStore is not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    receivePack: unused,
    createPack: unused,
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
  };
}

type Harness = {
  events: SidecarEventEmitter;
  router: ReturnType<typeof createRouterFacade>;
  collectors: ReturnType<typeof createCollectorRegistry>;
  repo: ReturnType<typeof createRepoStoreStub>;
  updates: UpdateCall[];
  dispose: () => void;
};

function setup(opts: MockDBOpts & { grants?: GrantRule[] } = {}): Harness {
  const updates: UpdateCall[] = [];
  const db = createMockDB({ ...opts, recordUpdates: updates });
  const events = createSidecarEmitter();
  const router = createRouterFacade();
  const collectors = createCollectorRegistry();
  const repo = createRepoStoreStub();
  const grantStore = createGrantStoreStub(opts.grants);

  const orchestrator = createHubSessionOrchestrator({
    events,
    router: router.facade,
    db,
    eventCollectors: collectors.registry,
    grantStore,
    agentRepoStore: repo.store,
  });

  return {
    events,
    router,
    collectors,
    repo,
    updates,
    dispose: orchestrator.dispose,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createHubSessionOrchestrator", () => {
  let harness: Harness;

  beforeEach(() => {
    harness = setup({ instance: makeInstance() });
  });

  describe("agent.event", () => {
    test("valid event is dispatched to the collector registry", () => {
      // reactor.start is a valid InferenceEvent that arktype's
      // parseInferenceEvent will accept; the orchestrator dispatches
      // it to the collector unchanged.
      harness.events.emit("agent.event", {
        agentAddress: AGENT_ADDRESS,
        sessionId: SESSION_ID,
        event: { type: "reactor.start", seq: 1, data: { tools: [] } },
      });
      const dispatched = harness.collectors.calls.find(
        (c) => c.kind === "dispatch",
      );
      expect(dispatched).toBeDefined();
    });

    test("invalid event is dropped (no dispatch call)", () => {
      harness.events.emit("agent.event", {
        agentAddress: AGENT_ADDRESS,
        sessionId: SESSION_ID,
        event: { not_a_real_event: true },
      });
      const dispatched = harness.collectors.calls.find(
        (c) => c.kind === "dispatch",
      );
      expect(dispatched).toBeUndefined();
    });
  });

  describe("sidecar.disconnect", () => {
    test("abandons every collector for the closed connection", () => {
      harness.events.emit("sidecar.disconnect", {
        agentAddresses: ["a@x", "b@x", "c@x"],
      });
      const abandonedAddrs = harness.collectors.calls
        .filter((c) => c.kind === "abandon")
        .map((c) => (c.kind === "abandon" ? c.addr : null));
      expect(abandonedAddrs).toEqual(["a@x", "b@x", "c@x"]);
    });
  });

  describe("agent.deploy.ack", () => {
    test("stores the public key on the active instance", async () => {
      await harness.events.emitAndAwait("agent.deploy.ack", {
        agentAddress: AGENT_ADDRESS,
        publicKey: "deadbeef",
      });
      expect(harness.updates).toHaveLength(1);
      expect(harness.updates[0]?.set).toEqual({ publicKey: "deadbeef" });
    });
  });

  describe("agent.reconnected", () => {
    test("refreshes grants and skips status update when already running", async () => {
      const grant: GrantRule = {
        id: "grant-1",
        resource: "x:y",
        action: "read",
        effect: "allow",
        origin: "system",
        conditions: null,
        expiresAt: null,
        roleId: null,
        principalId: PRINCIPAL_ID,
      };
      harness = setup({ instance: makeInstance(), grants: [grant] });

      await harness.events.emitAndAwait("agent.reconnected", {
        agentAddress: AGENT_ADDRESS,
      });

      const grantsCall = harness.router.calls.find(
        (c) => c.kind === "sendGrantsUpdate",
      );
      expect(grantsCall).toBeDefined();
      if (grantsCall?.kind === "sendGrantsUpdate") {
        expect(grantsCall.grants).toEqual([grant]);
      }

      // status was already "running", no update should fire
      expect(harness.updates).toHaveLength(0);
    });

    test("flips status to running and restores collector when missing", async () => {
      harness = setup({ instance: makeInstance({ status: "deployed" }) });

      await harness.events.emitAndAwait("agent.reconnected", {
        agentAddress: AGENT_ADDRESS,
      });

      const statusUpdate = harness.updates.find(
        (u) => u.set["status"] === "running",
      );
      expect(statusUpdate).toBeDefined();

      const created = harness.collectors.calls.find((c) => c.kind === "create");
      expect(created).toBeDefined();
      if (created?.kind === "create") {
        expect(created.addr).toBe(AGENT_ADDRESS);
        expect(created.sessionId).toBe(SESSION_ID);
      }
    });

    test("adopts the session's open running turn so trailing parts are not dropped", async () => {
      harness = setup({
        instance: makeInstance(),
        openTurn: { id: "turn_open" },
        maxOrdinal: 4,
      });

      await harness.events.emitAndAwait("agent.reconnected", {
        agentAddress: AGENT_ADDRESS,
      });

      const created = harness.collectors.calls.find((c) => c.kind === "create");
      expect(created).toBeDefined();
      if (created?.kind === "create") {
        expect(created.resumeTurn).toEqual({ id: "turn_open", nextOrdinal: 5 });
      }
    });

    test("starts the next part at ordinal 0 when the open turn has no parts", async () => {
      harness = setup({
        instance: makeInstance(),
        openTurn: { id: "turn_open" },
        maxOrdinal: null,
      });

      await harness.events.emitAndAwait("agent.reconnected", {
        agentAddress: AGENT_ADDRESS,
      });

      const created = harness.collectors.calls.find((c) => c.kind === "create");
      if (created?.kind === "create") {
        expect(created.resumeTurn).toEqual({ id: "turn_open", nextOrdinal: 0 });
      }
    });

    test("passes no resumeTurn when the session has no open turn", async () => {
      harness = setup({ instance: makeInstance(), openTurn: undefined });

      await harness.events.emitAndAwait("agent.reconnected", {
        agentAddress: AGENT_ADDRESS,
      });

      const created = harness.collectors.calls.find((c) => c.kind === "create");
      expect(created).toBeDefined();
      if (created?.kind === "create") {
        expect(created.resumeTurn).toBeUndefined();
      }
    });

    test("throws when the instance has no active session", async () => {
      harness = setup({ instance: makeInstance({ sessionId: null }) });

      await expect(
        harness.events.emitAndAwait("agent.reconnected", {
          agentAddress: AGENT_ADDRESS,
        }),
      ).rejects.toThrow(/no active session/);
    });

    test("throws when no active instance exists", async () => {
      harness = setup({ instance: undefined });

      await expect(
        harness.events.emitAndAwait("agent.reconnected", {
          agentAddress: AGENT_ADDRESS,
        }),
      ).rejects.toThrow(/No active instance/);
    });
  });

  describe("deploy.ref.stale", () => {
    test("creates a deploy pack and pushes it via the router", async () => {
      await harness.events.emitAndAwait("deploy.ref.stale", {
        agentAddress: AGENT_ADDRESS,
      });

      expect(harness.repo.calls).toEqual([
        { kind: "createDeployPack", agentId: "ins_1" },
      ]);

      const sendPack = harness.router.calls.find((c) => c.kind === "sendPack");
      expect(sendPack).toBeDefined();
      if (sendPack?.kind === "sendPack") {
        expect(sendPack.addr).toBe(AGENT_ADDRESS);
        expect(sendPack.ref).toBe("refs/heads/deploy");
      }
    });
  });

  describe("mail.persisted", () => {
    test("dispatches a mail.delivered agent event for the recipient", () => {
      const raw = new TextEncoder().encode(
        [
          "From: sender@x",
          "To: recipient@x",
          "Subject: hello",
          "",
          "body",
          "",
        ].join("\r\n"),
      );
      const createdAt = new Date("2026-01-01T00:00:00.000Z");

      harness.events.emit("mail.persisted", {
        id: "mail_1",
        raw,
        createdAt,
        direction: "inbound",
        instanceId: INSTANCE_ID,
        address: AGENT_ADDRESS,
      });

      const dispatched = harness.router.calls.find(
        (c) => c.kind === "dispatchAgentEvent",
      );
      expect(dispatched).toBeDefined();
      if (dispatched?.kind !== "dispatchAgentEvent") return;
      expect(dispatched.addr).toBe(AGENT_ADDRESS);

      const evt = dispatched.event;
      expect(evt).toMatchObject({
        type: "mail.delivered",
        data: {
          id: "mail_1",
          direction: "inbound",
          receivedAt: "2026-01-01T00:00:00.000Z",
        },
      });
    });
  });

  describe("dispose", () => {
    test("removes all subscriptions so later emits are inert", () => {
      harness.dispose();
      harness.events.emit("sidecar.disconnect", {
        agentAddresses: [AGENT_ADDRESS],
      });
      const abandoned = harness.collectors.calls.find(
        (c) => c.kind === "abandon",
      );
      expect(abandoned).toBeUndefined();
    });
  });
});
