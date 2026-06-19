// Integration coverage for compactor wiring through `createAgent`.
//
// The agent's job here is mechanical: thread `env.compactors` into the
// reactor assembly, surface the registered names to the director factory
// at construction as `agentContext.compactorNames`, and trust the
// reactor's existing `executeCompact` to resolve the name and run the
// compactor's `apply()`. These tests pin both halves:
//
//  - A registered compactor runs end-to-end when the director emits
//    `caps.compact(name, reason)` for it, and the director sees the
//    registered name on `agentContext.compactorNames` at construction.
//  - An unregistered name produces the reactor's existing fatal
//    "no compactor registered" error rather than silently dropping the
//    action.
//
// The directors here are hand-written test doubles: a real LLM is not
// needed to drive a compact action.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type } from "arktype";

import type { ReactorEmittedEvent } from "@intx/inference";
import { createInboundMessage } from "@intx/mime";
import { createIsogitStore } from "@intx/storage-isogit";
import type {
  Compactor,
  ConversationTurn,
  InferenceSource,
  ReactorCapabilities,
  ReactorDirector,
  ReactorInboundEvent,
  ReactorState,
  StrategyContext,
} from "@intx/types/runtime";

import { createAgent } from "./agent";
import { defineAgent } from "./definition";
import { defineDirector } from "./director";
import { createDirectorRegistry } from "./director-registry";
import type { BaseEnv } from "./env";
import { noopAuditStore } from "./testing/audit-noop";
import { permissiveAuthorize } from "./testing/authorize-allow";

// The compactor tests never call infer(); the URL is unreachable so any
// accidental infer() would fail fast rather than hang.
const SOURCE: InferenceSource = {
  id: "anthropic:compactor-test",
  provider: "anthropic",
  baseURL: "http://localhost:1",
  apiKey: "test-key",
  model: "claude-test",
};

interface RecordingCompactor extends Compactor {
  readonly calls: {
    turns: readonly ConversationTurn[];
    trigger: string;
  }[];
  /**
   * Resolves the first time `apply()` runs. The test awaits this
   * before delivering the follow-up message that lets the director
   * return `done()`, so the handshake is gated on the compact path
   * actually executing rather than on a wall-clock timer the reactor
   * must beat.
   */
  readonly firstApplyStarted: Promise<void>;
}

// A compactor that captures every (turns, ctx) pair its `apply()` saw
// and replaces the conversation with a single synthetic assistant turn.
// The single-turn output is enough for the reactor's commit cycle to
// distinguish "compaction ran" from "compaction skipped" via the
// resulting history length.
function makeRecordingCompactor(name: string): RecordingCompactor {
  const calls: {
    turns: readonly ConversationTurn[];
    trigger: string;
  }[] = [];
  const {
    promise: firstApplyStarted,
    resolve: signalFirstApply,
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Promise.withResolvers<void>() is the conventional shape for a fire-and-forget settled-signal
  } = Promise.withResolvers<void>();
  return {
    name,
    version: "1",
    calls,
    firstApplyStarted,
    async apply(turns: ConversationTurn[], ctx: StrategyContext) {
      calls.push({ turns: [...turns], trigger: ctx.trigger });
      signalFirstApply();
      const compacted: ConversationTurn = {
        role: "assistant",
        content: [{ type: "text", text: `compacted:${name}` }],
        model: SOURCE.model,
        timestamp: Date.now(),
      };
      return {
        output: [compacted],
        record: {
          strategy: name,
          version: "1",
          parameters: {},
          reason: ctx.trigger,
          decisions: { kept: 1, dropped: turns.length },
        },
      };
    },
  };
}

// A test director that emits `caps.compact(name, reason)` on the first
// inbound message and `caps.done()` on any later event. The compact
// cycle finishes without a follow-up action from the reactor, so the
// caller drives the director's terminal `done()` by delivering a
// second message -- the same shape the reactor-level compact test in
// `packages/inference/src/reactor.test.ts` uses.
//
// The director factory captures the `compactorNames` it received at
// construction so the test can assert the deployer's registry
// surfaced through to the director author the same way
// `toolDefinitions` does.
function defineCompactDirector(opts: {
  compactorName: string;
  reason: string;
  capturedNames: { value: readonly string[] | null };
}) {
  let messages = 0;
  const director: ReactorDirector = {
    async decide(
      event: ReactorInboundEvent,
      _state: ReactorState,
      caps: ReactorCapabilities,
    ) {
      if (event.type === "message.received") {
        messages++;
        if (messages === 1) {
          return caps.compact(opts.compactorName, opts.reason);
        }
        return caps.done();
      }
      return caps.done();
    },
  };
  const defined = defineDirector({
    id: "@intx-test/compactor/probe",
    configSchema: type({}),
    factory: (_config, _env, agent) => {
      opts.capturedNames.value = agent.compactorNames;
      return director;
    },
  });
  return createDirectorRegistry({
    factories: [defined.factory],
    defaultId: defined.factory.id,
  });
}

async function buildEnv(opts: {
  workdir: string;
  directors: BaseEnv["directors"];
  compactors?: Record<string, Compactor>;
}): Promise<BaseEnv> {
  const storage = await createIsogitStore(opts.workdir);
  return {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir: opts.workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: opts.directors,
    ...(opts.compactors !== undefined ? { compactors: opts.compactors } : {}),
  };
}

function inboundConversation(content: string) {
  return createInboundMessage({
    from: "user@local",
    to: "agent@local",
    content,
    interchangeType: "conversation.message",
  });
}

async function waitForReactorDone(
  stream: AsyncIterable<ReactorEmittedEvent>,
): Promise<void> {
  for await (const event of stream) {
    if (event.type === "reactor.done") return;
  }
}

async function collectUntilDone(
  stream: AsyncIterable<ReactorEmittedEvent>,
): Promise<ReactorEmittedEvent[]> {
  const events: ReactorEmittedEvent[] = [];
  for await (const event of stream) {
    events.push(event);
    if (event.type === "reactor.done") return events;
  }
  return events;
}

describe("createAgent compactor wiring", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-compactor-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("a registered compactor runs and its name surfaces on agentContext", async () => {
    const compactor = makeRecordingCompactor("test-compactor");
    const capturedNames: { value: readonly string[] | null } = { value: null };
    const directors = defineCompactDirector({
      compactorName: "test-compactor",
      reason: "test-trigger",
      capturedNames,
    });

    const def = defineAgent({
      id: "compactor-wired",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    const env = await buildEnv({
      workdir: workDir,
      directors,
      compactors: { "test-compactor": compactor },
    });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    try {
      agent.deliver(inboundConversation("trigger"));
      // Gate the follow-up delivery on the compact path actually
      // running rather than a wall-clock timer. The compact cycle
      // commits and waits for the next inbound event; once the
      // recording compactor's `apply()` has started, delivering the
      // second message lets the director return `done()` and the
      // reactor shut down cleanly.
      await compactor.firstApplyStarted;
      agent.deliver(inboundConversation("done"));
      await waitForReactorDone(stream);
    } finally {
      await agent.close();
    }

    // The director factory saw the registered name at construction.
    expect(capturedNames.value).toEqual(["test-compactor"]);

    // The reactor resolved the name against the env registry and called
    // the compactor exactly once with the inbound user turn and the
    // director's stated trigger.
    expect(compactor.calls.length).toBe(1);
    const call = compactor.calls[0];
    if (call === undefined) throw new Error("expected one compactor call");
    // The reactor namespaces the director-supplied reason with a
    // `director:` prefix before passing it to the compactor; the
    // contract under test here is that the director's reason reaches
    // the strategy unchanged, not the framing.
    expect(call.trigger).toContain("test-trigger");
    expect(call.turns.length).toBeGreaterThanOrEqual(1);
    const userTurn = call.turns[call.turns.length - 1];
    if (userTurn === undefined) throw new Error("expected user turn");
    expect(userTurn.role).toBe("user");
    // The inbound message's payload reached the compactor through the
    // reactor's commit-then-decide ordering; pin the content as well as
    // the role so a regression that fed the strategy an empty or
    // wrong-content turn collection cannot pass.
    const userBlocks = userTurn.content;
    const userText = userBlocks
      .map((block) => (block.type === "text" ? block.text : ""))
      .join("");
    expect(userText).toContain("trigger");

    // The compactor's output replaced the conversation: history reflects
    // the single synthetic assistant turn the compactor returned.
    const turns = await agent.history();
    expect(turns.length).toBe(1);
    const only = turns[0];
    if (only === undefined) throw new Error("expected one turn");
    expect(only.role).toBe("assistant");
    const block = only.content[0];
    if (block === undefined || block.type !== "text") {
      throw new Error("expected text block");
    }
    expect(block.text).toBe("compacted:test-compactor");
  });

  test("an unregistered compactor name produces the reactor's fatal error", async () => {
    const capturedNames: { value: readonly string[] | null } = { value: null };
    const directors = defineCompactDirector({
      compactorName: "missing",
      reason: "test",
      capturedNames,
    });

    const def = defineAgent({
      id: "compactor-missing",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: {
        sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
      },
    });

    // Build env without a `compactors` field at all: the agent threads
    // an undefined registry through and the reactor sees an empty
    // lookup, matching the deployer who simply never registered any.
    const env = await buildEnv({
      workdir: workDir,
      directors,
    });
    const agent = await createAgent(def, env);
    const stream = agent.stream();
    let events: ReactorEmittedEvent[] = [];
    try {
      agent.deliver(inboundConversation("trigger"));
      events = await collectUntilDone(stream);
    } finally {
      await agent.close();
    }

    // The director factory saw an empty list of registered names.
    expect(capturedNames.value).toEqual([]);

    // The reactor surfaced its existing "no compactor registered" fatal
    // error and shut down. The shape matches the reactor-level test
    // `"compact for an unknown name emits a fatal error and shuts down"`
    // in packages/inference/src/reactor.test.ts.
    const reactorError = events.find((e) => e.type === "reactor.error");
    if (reactorError === undefined || reactorError.type !== "reactor.error") {
      throw new Error("expected a reactor.error event");
    }
    expect(reactorError.data.fatal).toBe(true);
    expect(reactorError.data.error).toContain("missing");
    expect(reactorError.data.error).toContain("no compactor registered");
  });
});
