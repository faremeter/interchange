import { describe, test, expect, beforeEach } from "bun:test";

import type { InferenceEvent } from "@intx/types/runtime";

import type { AgentRepoStore } from "./agent-repo";
import type { LocalRepoStore } from "./repo-store";
import type { EventCollectorRegistry } from "./event-collector-registry";

import { createHubSessionOrchestrator } from "./hub-session-orchestrator";
import {
  createSidecarEmitter,
  type SidecarEventEmitter,
} from "./ws/sidecar-events";

// ---------------------------------------------------------------------------
// Fixtures and mocks
// ---------------------------------------------------------------------------

const INSTANCE_ID = "run_1";
const AGENT_ADDRESS = "run_1@tenant.local";
const SESSION_ID = "ses_1";

type RouterCall = {
  kind: "dispatchAgentEvent";
  addr: string;
  event: unknown;
};

function createRouterFacade(): {
  facade: Parameters<typeof createHubSessionOrchestrator>[0]["router"];
  calls: RouterCall[];
} {
  const calls: RouterCall[] = [];
  return {
    calls,
    facade: {
      dispatchAgentEvent(addr, event) {
        calls.push({ kind: "dispatchAgentEvent", addr, event });
      },
    },
  };
}

type CollectorCall =
  | { kind: "create"; addr: string; sessionId: string }
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
      create(addr, _tenantId, sessionId, _runId) {
        has.add(addr);
        calls.push({ kind: "create", addr, sessionId });
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

function createRepoStoreStub(): { store: AgentRepoStore } {
  return {
    store: {
      async prepareWorkflowRunPack() {
        throw new Error("mock: prepareWorkflowRunPack not implemented");
      },
      repoStore: unusedRepoStore(),
    },
  };
}

function unusedRepoStore(): LocalRepoStore {
  // The orchestrator tests do not touch the substrate; a throwing
  // stub keeps the AgentRepoStore surface fully populated without
  // pulling a real on-disk store into orchestrator-level unit tests.
  const unused = () =>
    Promise.reject(new Error("mock AgentRepoStore.repoStore is not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    writeTreeDelta: unused,
    receivePack: unused,
    prepareReceivePack: unused,
    createPack: unused,
    commitPackedTip: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
    forgetPackedTip: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
    subscribe: () => {
      throw new Error("mock AgentRepoStore.repoStore is not wired");
    },
    removeRepo: unused,
  };
}

type Harness = {
  events: SidecarEventEmitter;
  router: ReturnType<typeof createRouterFacade>;
  collectors: ReturnType<typeof createCollectorRegistry>;
  repo: ReturnType<typeof createRepoStoreStub>;
  dispose: () => void;
};

function setup(): Harness {
  const events = createSidecarEmitter();
  const router = createRouterFacade();
  const collectors = createCollectorRegistry();
  const repo = createRepoStoreStub();

  const orchestrator = createHubSessionOrchestrator({
    events,
    router: router.facade,
    eventCollectors: collectors.registry,
  });

  return {
    events,
    router,
    collectors,
    repo,
    dispose: orchestrator.dispose,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createHubSessionOrchestrator", () => {
  let harness: Harness;

  beforeEach(() => {
    harness = setup();
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
        ownedAddresses: ["a@x", "b@x", "c@x"],
        allocated: [],
      });
      const abandonedAddrs = harness.collectors.calls
        .filter((c) => c.kind === "abandon")
        .map((c) => (c.kind === "abandon" ? c.addr : null));
      expect(abandonedAddrs).toEqual(["a@x", "b@x", "c@x"]);
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
        runId: INSTANCE_ID,
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
        ownedAddresses: [AGENT_ADDRESS],
        allocated: [],
      });
      const abandoned = harness.collectors.calls.find(
        (c) => c.kind === "abandon",
      );
      expect(abandoned).toBeUndefined();
    });
  });
});
