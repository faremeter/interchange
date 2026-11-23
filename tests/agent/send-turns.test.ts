// Integration tests for how @intx/agent settles a send on its own turn,
// driven by the @intx/inference-testing harness so that a turn's reply
// carries a real assistant turn from its own inference.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { type } from "arktype";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  createAgent,
  createDirectorRegistry,
  defineAgent,
  defineDirector,
  defineTool,
  type Agent,
  type BaseEnv,
} from "@intx/agent";
import { noopAuditStore } from "@intx/agent/testing";
import type { ReactorEmittedEvent } from "@intx/inference";
import { setupHarness, type Harness } from "@intx/inference-testing";
import { createInboundMessage } from "@intx/mime";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type {
  ContextStore,
  InboundMessage,
  InferenceSource,
  PersistedPendingOperation,
  ReactorDirector,
} from "@intx/types/runtime";
import { waitUntil } from "@intx/types/testing";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test-send-turns",
  model: "claude-3-5-sonnet",
};

// Every message infers. A model turn with tool calls runs them (the
// charge_card call parks on an approval); a model turn with text replies
// with it. A tool result infers again.
function director(): ReactorDirector {
  return {
    async decide(event, _state, caps) {
      if (event.type === "message.received") return caps.infer();
      if (event.type === "resume.execute_tools") {
        return caps.executeTools(event.calls, false, true);
      }
      if (event.type === "tool.done") return caps.infer();
      if (event.type === "inference.done") {
        const calls = event.turn.content.flatMap((block) =>
          block.type === "tool_call"
            ? [{ id: block.id, name: block.name, arguments: block.arguments }]
            : [],
        );
        if (calls.length > 0) return caps.executeTools(calls);
        const text = event.turn.content
          .map((block) => (block.type === "text" ? block.text : ""))
          .join("");
        return caps.reply(text);
      }
      return caps.wait();
    },
  };
}

// Script the model call that asks for the charge, deliver T, and wait for
// T to park on its approval.
async function parkT(
  agent: Agent,
  harness: Harness,
  events: ReactorEmittedEvent[],
): Promise<{ correlationId: string; turnId: string }> {
  harness.scenario.replyOnce("anthropic", {
    toolCalls: [{ callId: "call-charge", name: "charge_card", args: {} }],
  });
  agent.deliver(message("T"));
  await runUntil(harness, () =>
    events.some((e) => e.type === "reactor.gate.blocked"),
  );
  return gateOf(events);
}

const chargeTool = defineTool<BaseEnv>({
  id: "@intx-test/send-turns/charge_card",
  definitions: [{ name: "charge_card" }],
  factory: () => ({
    definitions: [
      {
        name: "charge_card",
        description: "Charge the card",
        inputSchema: { type: "object", properties: {} },
      },
    ],
    async run(call) {
      return { callId: call.id, content: "charged" };
    },
  }),
});

async function buildAgent(
  workdir: string,
  harness: Harness,
  wrapStorage: (store: ContextStore) => ContextStore = (store) => store,
): Promise<Agent> {
  const defined = defineDirector({
    id: "@intx-test/send-turns/director",
    configSchema: type({}),
    factory: () => director(),
  });
  const storage = wrapStorage(await createIsogitStore(workdir));
  return createAgent(
    defineAgent({
      id: "send-turns",
      systemPrompt: "send-turns test",
      tools: [chargeTool],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    }),
    {
      sources: [SOURCE],
      defaultSource: SOURCE.id,
      storage,
      workdir,
      audit: noopAuditStore(),
      authorize: async (resource) =>
        resource === "tool:charge_card"
          ? { effect: "ask", matchingGrants: [], resolvedBy: null }
          : { effect: "allow", matchingGrants: [], resolvedBy: null },
      directors: createDirectorRegistry({
        factories: [defined.factory],
        defaultId: defined.factory.id,
      }),
      readCurrentMaterial: (credentialId) => ({ secret: credentialId }),
      deps: harness.deps,
    },
  );
}

function message(content: string): InboundMessage {
  return createInboundMessage({
    from: "user@local",
    to: "agent@local",
    content,
    interchangeType: "conversation.message",
  });
}

function approval(correlationId: string): InboundMessage {
  return createInboundMessage({
    from: "signal@local",
    to: "agent@local",
    content: JSON.stringify({ outcome: "approved" }),
    correlationId,
  });
}

// The stream readers that collect started. The stream ends when the agent
// closes, and afterEach awaits each reader so a reader error fails the test.
const readers: Promise<void>[] = [];

function collect(agent: Agent): ReactorEmittedEvent[] {
  const events: ReactorEmittedEvent[] = [];
  readers.push(
    (async () => {
      for await (const event of agent.stream()) events.push(event);
    })(),
  );
  return events;
}

async function gateOf(events: ReactorEmittedEvent[]): Promise<{
  correlationId: string;
  turnId: string;
}> {
  await waitUntil(() => events.some((e) => e.type === "reactor.gate.blocked"));
  const blocked = events.find((e) => e.type === "reactor.gate.blocked");
  if (
    blocked?.type !== "reactor.gate.blocked" ||
    blocked.data.correlationId === undefined ||
    blocked.data.turnId === undefined
  ) {
    throw new Error("expected a gate with a correlation and a turn");
  }
  return {
    correlationId: blocked.data.correlationId,
    turnId: blocked.data.turnId,
  };
}

// Cleared when a test ends, so a drive loop of a failed test stops.
let driving = false;

// The harness streams a scripted reply only while its clock runs, and a
// run returns once the work in flight is quiet, which can be before the
// reactor's next model call starts: the store's file I/O runs between
// them. Run the clock, yield a task turn so that I/O can finish, and
// repeat until `done` holds.
async function runUntil(harness: Harness, done: () => boolean): Promise<void> {
  while (driving && !done()) {
    await harness.run();
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

// Run the clock until `promise` settles, then return its outcome.
async function settled<T>(harness: Harness, promise: Promise<T>): Promise<T> {
  let finished = false;
  promise.then(
    () => {
      finished = true;
    },
    () => {
      finished = true;
    },
  );
  await runUntil(harness, () => finished);
  return promise;
}

function textOf(turn: { content: { type: string; text?: string }[] }): string {
  return turn.content.map((block) => block.text ?? "").join("");
}

describe("@intx/agent sends settle on their own turn", () => {
  let workDir: string;
  let harness: Harness;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-send-turns-"));
    harness = setupHarness();
    driving = true;
  });

  afterEach(async () => {
    driving = false;
    // Each test closes its agent, which ends its stream.
    await Promise.all(readers.splice(0));
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("a foreign reply does not settle the send", async () => {
    const agent = await buildAgent(join(workDir, "ctx"), harness);
    const events = collect(agent);
    try {
      const parked = await parkT(agent, harness, events);

      // T's approval is queued ahead of the send's message, so T's resume
      // runs and replies while the send is active.
      harness.scenario.replyOnce("anthropic", { text: "T done" });
      harness.scenario.replyOnce("anthropic", { text: "S done" });
      agent.deliver(approval(parked.correlationId));
      const sent = agent.send(message("S"));

      const result = await settled(harness, sent);
      if (result.type !== "reply") {
        throw new Error(`expected a reply, got ${result.type}`);
      }
      expect(result.reply).toBe("S done");
      expect(textOf(result.turn)).toBe("S done");
      expect(result.turnId).not.toBe(parked.turnId);
    } finally {
      await agent.close();
    }
  });

  test("a send that resumes a park settles on the parked turn's reply", async () => {
    const agent = await buildAgent(join(workDir, "ctx"), harness);
    const events = collect(agent);
    try {
      const parked = await parkT(agent, harness, events);

      harness.scenario.replyOnce("anthropic", { text: "T done" });
      const sent = agent.send(approval(parked.correlationId));

      const result = await settled(harness, sent);
      if (result.type !== "reply") {
        throw new Error(`expected a reply, got ${result.type}`);
      }
      expect(result.reply).toBe("T done");
      expect(textOf(result.turn)).toBe("T done");
      expect(result.turnId).toBe(parked.turnId);
    } finally {
      await agent.close();
    }
  });

  test("a send's turn id is stamped on its inference and reply events", async () => {
    const agent = await buildAgent(join(workDir, "ctx"), harness);
    const events = collect(agent);
    try {
      harness.scenario.replyOnce("anthropic", { text: "hello" });
      const sent = agent.send(message("S"), { turnId: "turn-s" });
      const result = await settled(harness, sent);
      expect(result.type === "reply" && result.turnId).toBe("turn-s");

      await waitUntil(() => events.some((e) => e.type === "connector.reply"));
      const stamped = events.flatMap((e) =>
        e.type === "inference.start" ||
        e.type === "inference.done" ||
        e.type === "connector.reply"
          ? [[e.type, e.data.turnId]]
          : [],
      );
      expect(stamped).toEqual([
        ["inference.start", "turn-s"],
        ["inference.done", "turn-s"],
        ["connector.reply", "turn-s"],
      ]);
    } finally {
      await agent.close();
    }
  });

  test.each([
    ["under its own turn id", false],
    ["under the parked turn's id", true],
  ])(
    "a send that resumes a park during its suspend commit settles on the parked turn's reply %s",
    async (_name, namesParkedTurn) => {
      // The commit that persists the first approval park is held. A send
      // that carries that approval's correlation arrives while the commit is
      // held and resumes the parked turn: it joins the parked turn, or it
      // names that turn. The park is over before its commit ends, so the
      // reactor never announces it, and the send settles on the reply.
      const release = Promise.withResolvers<boolean>();
      let parkedOps: PersistedPendingOperation[] | undefined;
      let holding = false;
      let held = false;
      const holdParkCommit = (store: ContextStore): ContextStore =>
        new Proxy(store, {
          get(target, prop, receiver) {
            if (prop === "writeMetadata") {
              return async (
                metadata: Parameters<ContextStore["writeMetadata"]>[0],
                signal?: AbortSignal,
              ) => {
                if (
                  !held &&
                  metadata.pendingOperations.some(
                    (op) => op.suspendedCall !== undefined,
                  )
                ) {
                  parkedOps = metadata.pendingOperations;
                }
                return target.writeMetadata(metadata, signal);
              };
            }
            if (prop === "commit") {
              return async (options: Parameters<ContextStore["commit"]>[0]) => {
                if (parkedOps !== undefined && !held) {
                  held = true;
                  holding = true;
                  await release.promise;
                  holding = false;
                }
                return target.commit(options);
              };
            }
            const value: unknown = Reflect.get(target, prop, receiver);
            return typeof value === "function" ? value.bind(target) : value;
          },
        });

      const agent = await buildAgent(
        join(workDir, "ctx"),
        harness,
        holdParkCommit,
      );
      const events = collect(agent);
      try {
        harness.scenario.replyOnce("anthropic", {
          toolCalls: [{ callId: "call-charge", name: "charge_card", args: {} }],
        });
        agent.deliver(message("T"));
        await runUntil(harness, () => holding);
        const parked = parkedOps?.find((op) => op.suspendedCall !== undefined);
        if (parked?.turnId === undefined) throw new Error("expected a park");
        const { correlationId } = parked;

        harness.scenario.replyOnce("anthropic", { text: "T done" });
        const sent = agent.send(approval(correlationId), {
          turnId: namesParkedTurn ? parked.turnId : "turn-d",
        });
        await waitUntil(() =>
          events.some((e) => e.type === "message.correlated"),
        );
        release.resolve(true);

        const result = await settled(harness, sent);
        if (result.type !== "reply") {
          throw new Error(`expected a reply, got ${result.type}`);
        }
        expect(result.reply).toBe("T done");
        expect(events.some((e) => e.type === "reactor.gate.blocked")).toBe(
          false,
        );
      } finally {
        release.resolve(true);
        await agent.close();
      }
    },
  );
});
