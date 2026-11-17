import { afterEach, beforeEach, describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type } from "arktype";

import { createInboundMessage } from "@intx/mime";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type {
  ContextStore,
  InferenceSource,
  ReactorCapabilities,
  ReactorDirector,
  ReactorInboundEvent,
  ReactorState,
} from "@intx/types/runtime";

import type { ReactorEmittedEvent } from "@intx/inference";
import { waitUntil } from "@intx/types/testing";

import {
  AgentClosedError,
  createAgent,
  GateSuspendedWithoutCorrelationError,
  TurnEndedWithoutReplyError,
  TurnFailedError,
  type Agent,
  type SendResult,
} from "./agent";
import { defineAgent } from "./definition";
import { defineDirector } from "./director";
import {
  createDefaultDirectorRegistry,
  createDirectorRegistry,
} from "./director-registry";
import { AgentEnvError, type BaseEnv } from "./env";
import { noopAuditStore } from "./testing/audit-noop";
import { permissiveAuthorize } from "./testing/authorize-allow";
import { defineTool } from "./tool";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test",
  model: "claude-3-5-sonnet",
};

function stubContextStore(): ContextStore {
  // Env-validation tests reject before the agent touches the store; the
  // stub never has any of its methods called.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, never invoked on the validation path
  return {} as ContextStore;
}

function minimalDef() {
  return defineAgent({
    id: "agent",
    systemPrompt: "test",
    tools: [],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

function baseEnv(workdir: string): BaseEnv {
  return {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    readCurrentMaterial: (credentialId) => ({ secret: credentialId }),
    storage: stubContextStore(),
    workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };
}

describe("createAgent env validation", () => {
  test("throws AgentEnvError when storage is missing", async () => {
    const env = baseEnv("/tmp/agent-env-test-storage");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap to exercise validateEnv
    const bad = { ...env, storage: undefined as unknown as ContextStore };
    await expect(createAgent(minimalDef(), bad)).rejects.toBeInstanceOf(
      AgentEnvError,
    );
  });

  test("throws AgentEnvError when audit is missing", async () => {
    const env = baseEnv("/tmp/agent-env-test-audit");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap to exercise validateEnv
    const bad = { ...env, audit: undefined as unknown as BaseEnv["audit"] };
    await expect(createAgent(minimalDef(), bad)).rejects.toBeInstanceOf(
      AgentEnvError,
    );
  });

  test("throws AgentEnvError when authorize is missing", async () => {
    const env = baseEnv("/tmp/agent-env-test-authorize");
    const bad = {
      ...env,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap to exercise validateEnv
      authorize: undefined as unknown as BaseEnv["authorize"],
    };
    await expect(createAgent(minimalDef(), bad)).rejects.toBeInstanceOf(
      AgentEnvError,
    );
  });

  test("blames a tool factory whose required env key is missing", async () => {
    // Tool factory declaring `requires: ["transport"]`; env omits it.
    interface MailEnv extends BaseEnv {
      transport: unknown;
    }
    const sendFactory = defineTool<MailEnv>({
      id: "@intx/tools-mail/send",
      requires: ["transport"],
      definitions: [],
      factory: () => ({
        definitions: [],
        async run(call) {
          return { callId: call.id, content: "" };
        },
      }),
    });

    const def = defineAgent({
      id: "mail",
      systemPrompt: "mail",
      tools: [sendFactory],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    // The type system already rejects this call -- def requires MailEnv,
    // env is BaseEnv. Widen the def at the call site to exercise the
    // runtime presence check, which is the whole point of the test.
    const env = baseEnv("/tmp/agent-env-test-mail");

    let caught: unknown;
    try {
      await createAgent(
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bypass the type-level check to exercise runtime validateEnv
        def as unknown as Parameters<typeof createAgent<BaseEnv>>[0],
        env,
      );
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof AgentEnvError)) {
      throw new Error("expected AgentEnvError");
    }
    expect(caught.missing).toContain("transport");
    expect(caught.contributors).toContain("tool:@intx/tools-mail/send");
  });
});

describe("createAgent lock release on construction failure", () => {
  // The lock acquired against env.workdir must be released on every
  // failure path during construction. Otherwise a later createAgent
  // against the same workdir throws AgentContextLockError indefinitely
  // until the process exits. The narrow try/catch shape this test
  // pins guards against the regression where only the early tool /
  // source / director resolution path released the lock.
  function failingToolFactory() {
    return defineTool({
      id: "@intx-test/agent/failing",
      definitions: [],
      factory: () => {
        throw new Error("tool construction failed");
      },
    });
  }

  test("releases the workdir lock when tool factory construction throws", async () => {
    const tmpdir = `/tmp/agent-lock-leak-${Math.random().toString(36).slice(2)}`;
    const def = defineAgent({
      id: "agent",
      systemPrompt: "test",
      tools: [failingToolFactory()],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    let first: unknown;
    try {
      await createAgent(def, baseEnv(tmpdir));
    } catch (err) {
      first = err;
    }
    expect(first).toBeInstanceOf(Error);

    // The lock must be releasable; a second createAgent against the
    // same workdir hits the same failure and not AgentContextLockError.
    let second: unknown;
    try {
      await createAgent(def, baseEnv(tmpdir));
    } catch (err) {
      second = err;
    }
    expect(second).toBeInstanceOf(Error);
    if (!(second instanceof Error)) throw new Error("unreachable");
    expect(second.name).not.toBe("AgentContextLockError");
  });
});

describe("createAgent tool-rollback dispose handling", () => {
  // When a later tool factory throws (e.g. duplicate-name
  // DuplicateToolError), the agent disposes every bundle it has
  // already constructed so resources allocated at factory time do not
  // leak. The dispose contract is async, so a rejecting disposer must
  // not escape as an unhandled promise rejection -- the rollback path
  // discards the disposer's return value but has to wire any rejection
  // through a swallow handler. `void promise` would leave the
  // rejection in flight, which the surrounding synchronous try/catch
  // cannot observe and the runtime surfaces as
  // `unhandledRejection` -- producing the very noise the swallow
  // comment promises to prevent and, under a strict
  // `unhandledRejection: 'throw'` policy, crashing the process.

  test("absorbs an async disposer's rejection during rollback", async () => {
    let observedRejection: unknown = null;
    const onUnhandled = (err: unknown): void => {
      observedRejection = err;
    };
    process.on("unhandledRejection", onUnhandled);
    try {
      // Two factories that collide on tool name `dup`. The first
      // ships an async dispose that rejects after a microtask; the
      // second triggers the duplicate-name throw that drives the
      // rollback through the first's dispose.
      const firstFactory = defineTool({
        id: "@intx-test/agent/first",
        definitions: [{ name: "dup" }],
        factory: () => ({
          definitions: [
            {
              name: "dup",
              description: "first",
              inputSchema: { type: "object" },
            },
          ],
          async run(call) {
            return { callId: call.id, content: "" };
          },
          async dispose() {
            await Promise.resolve();
            throw new Error("async dispose boom");
          },
        }),
      });
      const secondFactory = defineTool({
        id: "@intx-test/agent/second",
        definitions: [{ name: "dup" }],
        factory: () => ({
          definitions: [
            {
              name: "dup",
              description: "second",
              inputSchema: { type: "object" },
            },
          ],
          async run(call) {
            return { callId: call.id, content: "" };
          },
        }),
      });

      const def = defineAgent({
        id: "agent",
        systemPrompt: "test",
        tools: [firstFactory, secondFactory],
        capabilities: [],
        inference: {
          sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
        },
      });

      const tmpdir = `/tmp/agent-dispose-rollback-${Math.random().toString(36).slice(2)}`;
      let caught: unknown;
      try {
        await createAgent(def, baseEnv(tmpdir));
      } catch (err) {
        caught = err;
      }
      // The original construction failure is what the caller sees;
      // the disposer's rejection is absorbed.
      expect(caught).toBeInstanceOf(Error);
      if (!(caught instanceof Error)) throw new Error("unreachable");
      expect(caught.name).toBe("DuplicateToolError");

      // One macrotask boundary is enough: the runtime reports an
      // unhandled rejection when the microtask queue drains, so a
      // rejection escaping the rollback above would already have
      // reached the handler by the time this resolves. Verified against
      // this runtime with a `void`-discarded rejection of the same
      // shape as the disposer's.
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(observedRejection).toBeNull();
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  test("disposes constructed bundles when a post-resolveTools step throws", async () => {
    // The intra-resolveTools rollback handles failures inside the
    // tool-walk loop. A later createAgent step (resolveDirector here,
    // exercised via a registry whose default factory throws) leaves
    // the bundles `resolveTools` already built unreferenced: the
    // caller never reaches the returned agent, so the per-`ToolBundle`
    // "caller owns lifetime" contract has no caller to honor it.
    // The outer try/finally in createAgent must dispose them.
    let disposeCalls = 0;
    const probingFactory = defineTool({
      id: "@intx-test/agent/probing",
      definitions: [{ name: "probe" }],
      factory: () => ({
        definitions: [
          {
            name: "probe",
            description: "noop",
            inputSchema: { type: "object" },
          },
        ],
        async run(call) {
          return { callId: call.id, content: "" };
        },
        async dispose() {
          disposeCalls += 1;
        },
      }),
    });

    const throwingDefault = defineDirector({
      id: "@intx-test/agent/throwing-default",
      configSchema: type({}),
      factory: () => {
        throw new Error("director construction failed");
      },
    });
    const directors = createDirectorRegistry({
      factories: [throwingDefault.factory],
      defaultId: throwingDefault.factory.id,
    });

    const def = defineAgent({
      id: "agent",
      systemPrompt: "test",
      tools: [probingFactory],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const tmpdir = `/tmp/agent-post-resolvetools-${Math.random().toString(36).slice(2)}`;
    let caught: unknown;
    try {
      await createAgent(def, { ...baseEnv(tmpdir), directors });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);

    // The bundle was constructed before the director throw, so it
    // must be disposed.
    expect(disposeCalls).toBe(1);
  });
});

describe("createAgent send() on reactor suspend", () => {
  // A director that parks the reactor on a gate every time it sees an
  // inbound message. Without a resume path the reactor stays parked, so
  // send() would hang unless handleEvent settles the active send on
  // `reactor.gate.blocked`. `correlationId` is threaded through so the
  // reject-vs-resolve branch is exercised by the same director.
  //
  // Each park mints a fresh gateId (`gate-suspend-test-N`) so two
  // messages against the same agent do not collide on a single gate --
  // the gate manager rejects a duplicate registration, which would
  // otherwise brick the reactor loop on the second park.
  function makeSuspendingDirector(opts: {
    correlationId: string | undefined;
    distinctPerPark?: boolean;
  }): ReactorDirector {
    let parkCount = 0;
    return {
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          parkCount += 1;
          return caps.suspend({
            type: "approval",
            gateId: `gate-suspend-test-${String(parkCount)}`,
            timeoutMs: 60_000,
            ...(opts.correlationId !== undefined
              ? {
                  correlationId: opts.distinctPerPark
                    ? `${opts.correlationId}-${String(parkCount)}`
                    : opts.correlationId,
                }
              : {}),
          });
        }
        return caps.done();
      },
    };
  }

  function makeDirectorRegistry(
    director: ReactorDirector,
  ): BaseEnv["directors"] {
    const defined = defineDirector({
      id: "@intx-test/agent/suspend-probe",
      configSchema: type({}),
      factory: () => director,
    });
    return createDirectorRegistry({
      factories: [defined.factory],
      defaultId: defined.factory.id,
    });
  }

  const SUSPEND_SOURCE: InferenceSource = {
    id: "anthropic:suspend-test",
    provider: "anthropic",
    baseURL: "http://localhost:1",
    credentialId: "test-key",
    model: "claude-test",
  };

  async function buildSuspendEnv(
    workdir: string,
    director: ReactorDirector,
  ): Promise<BaseEnv> {
    const storage = await createIsogitStore(workdir);
    return {
      sources: [SUSPEND_SOURCE],
      defaultSource: SUSPEND_SOURCE.id,
      readCurrentMaterial: (credentialId) => ({ secret: credentialId }),
      storage,
      workdir,
      audit: noopAuditStore(),
      authorize: permissiveAuthorize(),
      directors: makeDirectorRegistry(director),
    };
  }

  function suspendDef() {
    return defineAgent({
      id: "suspend-agent",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [
          { provider: SUSPEND_SOURCE.provider, model: SUSPEND_SOURCE.model },
        ],
      },
    });
  }

  function conversationMessage(): ReturnType<typeof createInboundMessage> {
    return createInboundMessage({
      from: "user@local",
      to: "agent@local",
      content: "trigger",
      interchangeType: "conversation.message",
    });
  }

  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-suspend-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("resolves with a suspended outcome when the reactor parks with a correlationId", async () => {
    const director = makeSuspendingDirector({ correlationId: "corr-123" });
    const env = await buildSuspendEnv(workDir, director);
    const agent = await createAgent(suspendDef(), env);
    try {
      const result: SendResult = await agent.send(conversationMessage());
      if (result.type !== "suspended") {
        throw new Error(`expected suspended outcome, got ${result.type}`);
      }
      expect(result.correlationId).toBe("corr-123");
    } finally {
      await agent.close();
    }
  });

  test("forwards the approval snapshot from an ask suspension into the SendResult", async () => {
    const chargeTool = defineTool<BaseEnv>({
      id: "@intx-test/agent/charge_card",
      definitions: [{ name: "charge_card" }],
      factory: () => ({
        definitions: [
          {
            name: "charge_card",
            description: "Charge the customer's card",
            inputSchema: { type: "object", properties: {} },
          },
        ],
        async run(call) {
          return { callId: call.id, content: "charged", isError: false };
        },
      }),
    });

    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          return caps.executeTools([
            {
              id: "call-charge",
              name: "charge_card",
              arguments: { amount: 100 },
            },
          ]);
        }
        return caps.done();
      },
    };

    const storage = await createIsogitStore(workDir);
    const env: BaseEnv = {
      sources: [SUSPEND_SOURCE],
      defaultSource: SUSPEND_SOURCE.id,
      readCurrentMaterial: (credentialId) => ({ secret: credentialId }),
      storage,
      workdir: workDir,
      audit: noopAuditStore(),
      // Ask for the wired tool so the authz extension builds and parks a
      // snapshot; allow anything else.
      authorize: async (resource) =>
        resource === "tool:charge_card"
          ? { effect: "ask", matchingGrants: [], resolvedBy: null }
          : { effect: "allow", matchingGrants: [], resolvedBy: null },
      directors: makeDirectorRegistry(director),
    };

    const def = defineAgent({
      id: "ask-snapshot-agent",
      systemPrompt: "test",
      tools: [chargeTool],
      capabilities: [],
      inference: {
        sources: [
          { provider: SUSPEND_SOURCE.provider, model: SUSPEND_SOURCE.model },
        ],
      },
    });

    const agent = await createAgent(def, env);
    try {
      const result: SendResult = await agent.send(conversationMessage());
      if (result.type !== "suspended") {
        throw new Error(`expected suspended outcome, got ${result.type}`);
      }
      expect(result.approvalSnapshot).toEqual({
        name: "charge_card",
        description: "Charge the customer's card",
        inputSchema: { type: "object", properties: {} },
        arguments: { amount: 100 },
      });
    } finally {
      await agent.close();
    }
  });

  test("rejects with GateSuspendedWithoutCorrelationError when the park carries no correlationId", async () => {
    const director = makeSuspendingDirector({ correlationId: undefined });
    const env = await buildSuspendEnv(workDir, director);
    const agent = await createAgent(suspendDef(), env);
    try {
      let caught: unknown;
      try {
        await agent.send(conversationMessage());
      } catch (err) {
        caught = err;
      }
      if (!(caught instanceof GateSuspendedWithoutCorrelationError)) {
        throw new Error(
          "expected GateSuspendedWithoutCorrelationError from a park without a correlationId",
        );
      }
      expect(caught.gateId).toBe("gate-suspend-test-1");
    } finally {
      await agent.close();
    }
  });

  test("a send after a prior bare-deliver park resolves against its own park", async () => {
    // A bare `deliver()` (no active send) parks the reactor and fires
    // `reactor.gate.blocked` while no send is in flight; a later send()
    // then parks on its own gate. The send must resolve against ITS OWN
    // correlation, not the earlier bare park's. Distinct per-park ids
    // (`corr-N`) make that attribution observable: the bare deliver
    // parks as `corr-1`, the send parks as `corr-2`, so a result of
    // `corr-2` proves the send did not ride the first park's stale
    // event.
    //
    // This pins the observable end-to-end attribution. The agent settles
    // a send only on events of the turn it follows, so the bare park's
    // `reactor.gate.blocked`, which carries another turn's id, cannot
    // settle it.
    const director = makeSuspendingDirector({
      correlationId: "corr",
      distinctPerPark: true,
    });
    const env = await buildSuspendEnv(workDir, director);
    const agent = await createAgent(suspendDef(), env);
    const stream = agent.stream();
    try {
      // Drive a bare deliver and wait for its park to land on the stream
      // so the gate.blocked event has already been dispatched before the
      // send is enqueued.
      agent.deliver(conversationMessage());
      for await (const event of stream) {
        if (event.type === "reactor.gate.blocked") break;
      }

      const result: SendResult = await agent.send(conversationMessage());
      if (result.type !== "suspended") {
        throw new Error(`expected suspended outcome, got ${result.type}`);
      }
      expect(result.correlationId).toBe("corr-2");
    } finally {
      await agent.close();
    }
  });
});

describe("createAgent send() follows its own turn", () => {
  // These tests run a real reactor with a scripted director and no model
  // call. The director answers a message by its content:
  //   "T"  parks on an approval for a charge_card call (ask)
  //   "M"  runs a tool whose result carries an async pending marker
  //   "W"  waits, "E" returns no action, "D" ends the reactor
  //   "X"  throws
  //   any other content replies "<content> done"
  // A resumed charge_card call parks again on `call-2` when it is `call-1`
  // and the director was asked to repark, else it replies "charged".
  const TURN_SOURCE: InferenceSource = {
    id: "anthropic:turn-test",
    provider: "anthropic",
    baseURL: "http://localhost:1",
    credentialId: "test-key",
    model: "claude-test",
  };

  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-turns-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  function scriptedDirector(opts: { repark?: boolean } = {}): ReactorDirector {
    let parks = 0;
    return {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          const content = event.message.content ?? "";
          if (content === "T") {
            parks += 1;
            return caps.executeTools([
              {
                id: `call-${String(parks)}`,
                name: "charge_card",
                arguments: {},
              },
            ]);
          }
          if (content === "M") {
            return caps.executeTools([
              { id: "call-marker", name: "send_note", arguments: {} },
            ]);
          }
          if (content === "W") return caps.wait();
          if (content === "E") return [];
          if (content === "D") return caps.done();
          if (content === "X") throw new Error("director blew up");
          return caps.reply(`${content} done`);
        }
        if (event.type === "resume.execute_tools") {
          return caps.executeTools(event.calls, false, true);
        }
        if (event.type === "tool.done") {
          if (event.result.callId === "call-marker") return caps.reply("sent");
          if (opts.repark === true && event.result.callId === "call-1") {
            parks += 1;
            return caps.executeTools([
              { id: "call-2", name: "charge_card", arguments: {} },
            ]);
          }
          return caps.reply("charged");
        }
        return caps.wait();
      },
    };
  }

  const toolsFactory = defineTool<BaseEnv>({
    id: "@intx-test/agent/turn-tools",
    definitions: [
      { name: "charge_card" },
      { name: "send_note" },
      { name: "note" },
    ],
    factory: () => ({
      definitions: [
        {
          name: "charge_card",
          description: "Charge the card",
          inputSchema: { type: "object", properties: {} },
        },
        {
          name: "send_note",
          description: "Send a note and wait for its answer",
          inputSchema: { type: "object", properties: {} },
        },
        {
          name: "note",
          description: "Write a note",
          inputSchema: { type: "object", properties: {} },
        },
      ],
      async run(call) {
        if (call.name === "send_note") {
          return {
            callId: call.id,
            content: "sent",
            pendingMarker: { status: "pending", correlationId: "corr-note" },
          };
        }
        return { callId: call.id, content: "charged" };
      },
    }),
  });

  async function buildAgent(director: ReactorDirector) {
    const defined = defineDirector({
      id: "@intx-test/agent/turn-probe",
      configSchema: type({}),
      factory: () => director,
    });
    const storage = await createIsogitStore(workDir);
    const env: BaseEnv = {
      sources: [TURN_SOURCE],
      defaultSource: TURN_SOURCE.id,
      readCurrentMaterial: (credentialId) => ({ secret: credentialId }),
      storage,
      workdir: workDir,
      audit: noopAuditStore(),
      authorize: async (resource) =>
        resource === "tool:charge_card"
          ? { effect: "ask", matchingGrants: [], resolvedBy: null }
          : { effect: "allow", matchingGrants: [], resolvedBy: null },
      directors: createDirectorRegistry({
        factories: [defined.factory],
        defaultId: defined.factory.id,
      }),
    };
    const def = defineAgent({
      id: "turn-agent",
      systemPrompt: "test",
      tools: [toolsFactory],
      capabilities: [],
      inference: {
        sources: [{ provider: TURN_SOURCE.provider, model: TURN_SOURCE.model }],
      },
    });
    return createAgent(def, env);
  }

  function message(
    content: string,
    correlationId?: string,
  ): ReturnType<typeof createInboundMessage> {
    return createInboundMessage({
      from: "user@local",
      to: "agent@local",
      content,
      interchangeType: "conversation.message",
      ...(correlationId !== undefined ? { correlationId } : {}),
    });
  }

  function approval(
    correlationId: string,
  ): ReturnType<typeof createInboundMessage> {
    return createInboundMessage({
      from: "signal@local",
      to: "agent@local",
      content: JSON.stringify({ outcome: "approved" }),
      correlationId,
    });
  }

  // Collect every event of the agent's stream in the background.
  function collect(agent: Agent): ReactorEmittedEvent[] {
    const events: ReactorEmittedEvent[] = [];
    void (async () => {
      for await (const event of agent.stream()) events.push(event);
    })();
    return events;
  }

  async function firstGate(
    events: ReactorEmittedEvent[],
  ): Promise<{ correlationId: string; turnId: string }> {
    let found: { correlationId: string; turnId: string } | undefined;
    await waitUntil(() => {
      for (const e of events) {
        if (
          e.type === "reactor.gate.blocked" &&
          e.data.correlationId !== undefined &&
          e.data.turnId !== undefined
        ) {
          found = {
            correlationId: e.data.correlationId,
            turnId: e.data.turnId,
          };
          return true;
        }
      }
      return false;
    });
    if (found === undefined) throw new Error("unreachable");
    return found;
  }

  async function rejection(promise: Promise<unknown>): Promise<unknown> {
    try {
      await promise;
    } catch (err) {
      return err;
    }
    throw new Error("expected the promise to reject");
  }

  test("a foreign park does not settle the send", async () => {
    const agent = await buildAgent(scriptedDirector());
    try {
      // T's message is queued ahead of the send's, so T parks while the
      // send is active.
      agent.deliver(message("T"));
      const result = await agent.send(message("S"));
      if (result.type !== "reply") {
        throw new Error(`expected a reply, got ${result.type}`);
      }
      expect(result.reply).toBe("S done");
    } finally {
      await agent.close();
    }
  });

  test("a joined send whose resumed turn parks again settles suspended with the new correlation", async () => {
    const agent = await buildAgent(scriptedDirector({ repark: true }));
    const events = collect(agent);
    try {
      agent.deliver(message("T"));
      const parked = await firstGate(events);

      const result = await agent.send(approval(parked.correlationId));
      if (result.type !== "suspended") {
        throw new Error(`expected suspended, got ${result.type}`);
      }
      expect(result.correlationId).not.toBe(parked.correlationId);
      expect(result.turnId).toBe(parked.turnId);
    } finally {
      await agent.close();
    }
  });

  test("a send whose turn ends unanswered rejects with TurnEndedWithoutReplyError", async () => {
    const agent = await buildAgent(scriptedDirector());
    try {
      for (const content of ["W", "E"]) {
        const err = await rejection(
          agent.send(message(content), { turnId: `turn-${content}` }),
        );
        if (!(err instanceof TurnEndedWithoutReplyError)) {
          throw new Error(`expected TurnEndedWithoutReplyError for ${content}`);
        }
        expect(err.turnId).toBe(`turn-${content}`);
        // The agent stays usable.
        const next = await agent.send(message("R"));
        expect(next.type === "reply" && next.reply).toBe("R done");
      }
      const done = await rejection(
        agent.send(message("D"), { turnId: "turn-D" }),
      );
      if (!(done instanceof TurnEndedWithoutReplyError)) {
        throw new Error("expected TurnEndedWithoutReplyError for done");
      }
      expect(done.turnId).toBe("turn-D");
    } finally {
      await agent.close();
    }
  });

  test("a correlated send that resumes nothing rejects with TurnEndedWithoutReplyError", async () => {
    const agent = await buildAgent(scriptedDirector());
    try {
      const sent = await agent.send(message("M"));
      expect(sent.type === "reply" && sent.reply).toBe("sent");

      // The marker has no live gate, so its answer resumes nothing.
      const err = await rejection(
        agent.send(message("answer", "corr-note"), { turnId: "turn-answer" }),
      );
      if (!(err instanceof TurnEndedWithoutReplyError)) {
        throw new Error("expected TurnEndedWithoutReplyError");
      }
      expect(err.turnId).toBe("turn-answer");
    } finally {
      await agent.close();
    }
  });

  test("a fatal shutdown rejects the active send, the queued send, and a later send", async () => {
    const agent = await buildAgent(scriptedDirector());
    const events = collect(agent);
    try {
      const fatal = agent.send(message("X"));
      const queued = agent.send(message("Q"));
      const fatalErr = await rejection(fatal);
      expect(fatalErr).toBeInstanceOf(Error);
      expect(String(fatalErr)).toContain("director blew up");
      // The fatal error settles the active send, so the queue starts the
      // next one while the reactor shuts down. Its delivery reaches a done
      // reactor and ends its turn failed.
      const queuedErr = await rejection(queued);
      if (!(queuedErr instanceof TurnFailedError)) {
        throw new Error(`expected TurnFailedError, got ${String(queuedErr)}`);
      }
      expect(queuedErr.kind).toBe("reactor_done");
      await waitUntil(() => events.some((e) => e.type === "reactor.done"));
      expect(await rejection(agent.send(message("L")))).toBeInstanceOf(
        AgentClosedError,
      );
    } finally {
      await agent.close();
    }
  });

  test("sends queued behind a fatal shutdown all reject, none hang", async () => {
    const agent = await buildAgent(scriptedDirector());
    try {
      const fatal = agent.send(message("X"));
      const queued = [
        agent.send(message("Q1")),
        agent.send(message("Q2")),
        agent.send(message("Q3")),
      ];
      await rejection(fatal);
      for (const send of queued) {
        const err = await rejection(send);
        if (!(err instanceof TurnFailedError)) {
          throw new Error(`expected TurnFailedError, got ${String(err)}`);
        }
        expect(err.kind).toBe("reactor_done");
      }
    } finally {
      await agent.close();
    }
  });

  test("reactor.done rejects the active send and a send still in the queue", async () => {
    // A delivered message holds the director until both sends are made:
    // the first is active with its message queued behind the held one, the
    // second waits in the send queue. The held message then ends the
    // reactor without a fatal error, so neither send's turn ever runs.
    const holdDecision = Promise.withResolvers<boolean>();
    const deciding = Promise.withResolvers<boolean>();
    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (
          event.type === "message.received" &&
          event.message.content === "H"
        ) {
          deciding.resolve(true);
          await holdDecision.promise;
          return caps.done();
        }
        return caps.reply("unexpected");
      },
    };
    const agent = await buildAgent(director);
    try {
      agent.deliver(message("H"));
      await deciding.promise;
      const active = agent.send(message("S1"));
      const waiting = agent.send(message("S2"));
      holdDecision.resolve(true);
      expect(await rejection(active)).toBeInstanceOf(AgentClosedError);
      expect(await rejection(waiting)).toBeInstanceOf(AgentClosedError);
    } finally {
      await agent.close();
    }
  });

  test("a queued send behind one naming an ended turn still runs", async () => {
    const agent = await buildAgent(scriptedDirector());
    try {
      await agent.send(message("A"), { turnId: "turn-a" });
      const named = agent.send(message("B"), { turnId: "turn-a" });
      const next = agent.send(message("C"));
      expect(String(await rejection(named))).toContain("already ended");
      const result = await next;
      expect(result.type === "reply" && result.reply).toBe("C done");
    } finally {
      await agent.close();
    }
  });

  test("a send that names the parked turn and carries its approval settles on it", async () => {
    const agent = await buildAgent(scriptedDirector());
    const events = collect(agent);
    try {
      agent.deliver(message("T"));
      const parked = await firstGate(events);
      const result = await agent.send(approval(parked.correlationId), {
        turnId: parked.turnId,
      });
      if (result.type !== "reply") {
        throw new Error(`expected a reply, got ${result.type}`);
      }
      expect(result.reply).toBe("charged");
      expect(result.turnId).toBe(parked.turnId);
    } finally {
      await agent.close();
    }
  });

  test("a joined send whose turn replied before it parked rejects with TurnEndedWithoutReplyError", async () => {
    // One batch runs a plain call and parks a charge_card call. The plain
    // call's result replies at once, so the turn has replied before the
    // send joins it. The resumed call ends the turn with no new reply.
    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          return caps.executeTools([
            { id: "call-note", name: "note", arguments: {} },
            { id: "call-charge", name: "charge_card", arguments: {} },
          ]);
        }
        if (event.type === "resume.execute_tools") {
          return caps.executeTools(event.calls, false, true);
        }
        if (event.type === "tool.done" && event.result.callId === "call-note") {
          return caps.reply("early");
        }
        return caps.wait();
      },
    };
    const agent = await buildAgent(director);
    const events = collect(agent);
    try {
      agent.deliver(message("P"));
      const parked = await firstGate(events);
      await waitUntil(() => events.some((e) => e.type === "connector.reply"));

      const err = await rejection(agent.send(approval(parked.correlationId)));
      if (!(err instanceof TurnEndedWithoutReplyError)) {
        throw new Error(
          `expected TurnEndedWithoutReplyError, got ${String(err)}`,
        );
      }
      expect(err.turnId).toBe(parked.turnId);
    } finally {
      await agent.close();
    }
  });

  test("each send gets its own turn, and a send that names an ended turn rejects", async () => {
    const agent = await buildAgent(scriptedDirector());
    try {
      const first = await agent.send(message("A"));
      const second = await agent.send(message("B"));
      if (first.type !== "reply" || second.type !== "reply") {
        throw new Error("expected replies");
      }
      expect(first.turnId).not.toBe(second.turnId);

      const named = await agent.send(message("C"), { turnId: "turn-c" });
      expect(named.type === "reply" && named.turnId).toBe("turn-c");
      const err = await rejection(
        agent.send(message("C2"), { turnId: "turn-c" }),
      );
      expect(String(err)).toContain("turn-c has already ended");

      const after = await agent.send(message("D2"));
      expect(after.type === "reply" && after.reply).toBe("D2 done");
    } finally {
      await agent.close();
    }
  });

  test("a stream consumer holds the reply when the send resolves", async () => {
    const agent = await buildAgent(scriptedDirector());
    const it = agent.stream()[Symbol.asyncIterator]();
    try {
      const result = await agent.send(message("S"));
      expect(result.type).toBe("reply");
      // The agent settles the send in the same synchronous step that
      // pushes the reply onto every consumer, so the reply is already in
      // this consumer's buffer. A buffered event resolves `next()` at
      // once, ahead of the already-resolved sentinel it races.
      const empty = Symbol("empty");
      const buffered: string[] = [];
      for (;;) {
        const next = await Promise.race([it.next(), Promise.resolve(empty)]);
        if (next === empty || next.done === true) break;
        buffered.push(next.value.type);
      }
      expect(buffered).toContain("connector.reply");
    } finally {
      await agent.close();
    }
  });

  test("a send whose own turn parks on the correlation its message carries settles suspended", async () => {
    // The message carries a correlation that no operation has, so it runs
    // under the send's own turn. That turn then parks on the same
    // correlation. The park is the send's own, so it settles the send.
    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          const correlationId = event.message.headers.interchangeCorrelationId;
          if (correlationId !== undefined) {
            return caps.suspend({
              type: "message_response",
              gateId: "gate-thread",
              timeoutMs: 60_000,
              correlationId,
            });
          }
          return caps.reply("plain");
        }
        return caps.wait();
      },
    };
    const agent = await buildAgent(director);
    try {
      const result = await agent.send(message("N", "corr-thread"), {
        turnId: "turn-n",
      });
      expect(result).toEqual({
        type: "suspended",
        correlationId: "corr-thread",
        turnId: "turn-n",
      });
      // The queue is free: the next send runs.
      const next = await agent.send(message("P"));
      expect(next.type === "reply" && next.reply).toBe("plain");
    } finally {
      await agent.close();
    }
  });

  test("a queued send that names the same turn as the send before it rejects, and its message runs under a fresh turn", async () => {
    // Naming another live turn is misuse. The second send starts when the
    // first send's reply settles it, before that turn ends, so the reactor
    // admits its message under a fresh turn and the named turn's end
    // rejects the second send.
    const agent = await buildAgent(scriptedDirector());
    const events = collect(agent);
    try {
      const first = agent.send(message("A"), { turnId: "turn-a" });
      const second = agent.send(message("B"), { turnId: "turn-a" });
      const third = agent.send(message("C"), { turnId: "turn-c" });

      const a = await first;
      expect(a.type === "reply" && a.reply).toBe("A done");
      expect(await rejection(second)).toBeInstanceOf(
        TurnEndedWithoutReplyError,
      );
      const c = await third;
      expect(c.type === "reply" && c.reply).toBe("C done");

      await waitUntil(() =>
        events.some(
          (e) => e.type === "connector.reply" && e.data.content === "B done",
        ),
      );
      const bReply = events.find(
        (e) => e.type === "connector.reply" && e.data.content === "B done",
      );
      const bTurn =
        bReply?.type === "connector.reply" ? bReply.data.turnId : "";
      expect(bTurn).not.toBe("turn-a");
      expect(bTurn).not.toBe("turn-c");
    } finally {
      await agent.close();
    }
  });
});
