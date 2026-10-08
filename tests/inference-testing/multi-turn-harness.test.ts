// End-to-end round-trip tests for the @intx/inference-testing harness:
// complete turn-1 to turn-2 cycles through the production stack (real
// adapter, real `parseSSE`, `wire.*` bytes served through `deps.fetch`).
//
// Two driver styles: `harness.runInference` (auto-fires `onTool` handlers on
// `inference.tool_call.end`) for the round-trip variants, and direct
// production `runInference` with `deps: harness.deps` for the
// delayed-envelope and async-promise variants that observe dispatch timing
// before any auto-dispatch. Variants: Anthropic single tool call, OpenAI
// single tool call, delayed-envelope/async scheduling, and two parallel
// Anthropic tool calls in one turn.

import { afterEach, describe, expect, test } from "bun:test";

import { runInference } from "@intx/inference";
import { setupHarness, userTurn, wire } from "@intx/inference-testing";
import type { Harness } from "@intx/inference-testing";
import type {
  ConversationTurn,
  InferenceEvent,
  InferenceSource,
} from "@intx/types/runtime";

const ANTHROPIC_SOURCE: InferenceSource = {
  id: "anthropic:claude-test",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "test",
  model: "claude-test",
};

const OPENAI_SOURCE: InferenceSource = {
  id: "openai:gpt-test",
  provider: "openai",
  baseURL: "https://api.openai.com/v1",
  credentialId: "test",
  model: "gpt-test",
};

const TURN1_USAGE_HEAD = {
  input: 10,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const TURN1_USAGE_TAIL = {
  input: 0,
  output: 5,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const TURN2_USAGE_HEAD = {
  input: 15,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const TURN2_USAGE_TAIL = {
  input: 0,
  output: 8,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

let activeHarness: Harness | null = null;

afterEach(() => {
  if (activeHarness !== null) {
    activeHarness.dispose();
    activeHarness = null;
  }
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function parseRequestBody(
  request: Request,
): Promise<Record<string, unknown>> {
  const text = await request.text();
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error("captured request body was not a JSON object");
  }
  return parsed;
}

// Drives production runInference directly against the harness's deps,
// bypassing the auto-dispatch wrapper; the delayed-envelope and promise
// tests need to observe dispatch timing before any auto-dispatch fires.
async function collectInferenceManual(
  harness: Harness,
  turns: ConversationTurn[],
  nextSeq: () => number,
  source: InferenceSource,
): Promise<InferenceEvent[]> {
  const events: InferenceEvent[] = [];
  for await (const ev of runInference({
    turns,
    source,
    nextSeq,
    deps: harness.deps,
    readMaterial: () => ({ secret: "test-secret" }),
  })) {
    events.push(ev);
  }
  return events;
}

// Drives inference through `harness.runInference`; registered onTool handlers
// fire automatically on `inference.tool_call.end`.
async function collectInferenceAuto(
  harness: Harness,
  turns: ConversationTurn[],
  nextSeq: () => number,
  source: InferenceSource,
): Promise<InferenceEvent[]> {
  const events: InferenceEvent[] = [];
  for await (const ev of harness.runInference({
    turns,
    source,
    nextSeq,
  })) {
    events.push(ev);
  }
  return events;
}

describe("inference-testing harness — multi-turn round-trip", () => {
  test("Anthropic: drives turn-1 tool call, dispatches tool, and propagates result into turn-2 inference", async () => {
    const harness = setupHarness();
    activeHarness = harness;

    // Tool handler registration. The handler fires automatically via the
    // auto-dispatch path on `inference.tool_call.end`; results land on
    // `scenario.lastToolDispatch("weather")`.
    const handlerCalls: { args: unknown }[] = [];
    harness.scenario.onTool("weather", (args) => {
      handlerCalls.push({ args });
      return { temperatureF: 68, conditions: "fog" };
    });

    // Turn 1 wire bytes: a complete Anthropic response with one tool_use
    // block invoking `weather` with `{"location":"SF"}`.
    const turn1Stream = harness.scenario.createStream();
    const turn1Chunks = wire.completeResponse("anthropic", {
      toolCalls: [
        {
          callId: "call_weather_1",
          name: "weather",
          argsJSON: '{"location":"SF"}',
        },
      ],
      headUsage: TURN1_USAGE_HEAD,
      tailUsage: TURN1_USAGE_TAIL,
    });
    turn1Stream.enqueueAll(turn1Chunks, { startAt: 10 });
    const turn1Close = 10 + turn1Chunks.length;

    // Turn 2 wire bytes: a final assistant text response.
    const turn2Stream = harness.scenario.createStream();
    const turn2Chunks = wire.completeResponse("anthropic", {
      text: "It is 68F and foggy in SF.",
      headUsage: TURN2_USAGE_HEAD,
      tailUsage: TURN2_USAGE_TAIL,
    });
    turn2Stream.enqueueAll(turn2Chunks, { startAt: turn1Close + 50 });
    const turn2Close = turn1Close + 50 + turn2Chunks.length;

    // The matcher predicates are sync, so each clones and stashes its Request
    // for the async body assertions below. The harness never consumes the
    // body and each matcher fires at most once, so the clone is safe.
    let capturedTurn1Request: Request | null = null;
    let capturedTurn2Request: Request | null = null;
    harness.scenario.whenRequestMatches((req) => {
      capturedTurn1Request = req.clone();
      return true;
    }, turn1Stream);
    harness.scenario.whenRequestMatches((req) => {
      capturedTurn2Request = req.clone();
      return true;
    }, turn2Stream);

    // Drive turn 1 before advancing the clock, so the fetch parks in the
    // harness's waiting set and the matcher routes it.
    let seq = 0;
    const turn1Events = collectInferenceAuto(
      harness,
      [userTurn("What is the weather in SF?")],
      () => ++seq,
      ANTHROPIC_SOURCE,
    );
    await harness.advanceTo(turn1Close + 10);
    const events1 = await turn1Events;

    // The turn-1 iterator yielded the tool call the wire bytes encoded.
    const turn1End = events1.find((e) => e.type === "inference.tool_call.end");
    if (turn1End === undefined || turn1End.type !== "inference.tool_call.end") {
      throw new Error("expected an inference.tool_call.end event in turn 1");
    }
    expect(turn1End.data.name).toBe("weather");
    expect(turn1End.data.callId).toBe("call_weather_1");
    expect(turn1End.data.arguments).toEqual({ location: "SF" });

    const turn1Done = events1.find((e) => e.type === "inference.done");
    if (turn1Done === undefined || turn1Done.type !== "inference.done") {
      throw new Error("expected an inference.done event in turn 1");
    }
    const assistantTurn = turn1Done.data.turn;

    // Auto-dispatch fired the handler with the parsed arguments.
    expect(handlerCalls).toEqual([{ args: { location: "SF" } }]);
    expect(harness.scenario.lastToolDispatch("weather")).toEqual({
      temperatureF: 68,
      conditions: "fog",
    });

    // Turn 2 conversation: user turn + turn-1 assistant turn (tool_call) +
    // user turn with the `call_weather_1` tool_result.
    const turn2Conversation: ConversationTurn[] = [
      userTurn("What is the weather in SF?"),
      assistantTurn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_weather_1",
            content: [{ type: "text", text: "68F, fog" }],
          },
        ],
        timestamp: 0,
      },
    ];

    // Drive turn 2.
    const turn2Events = collectInferenceAuto(
      harness,
      turn2Conversation,
      () => ++seq,
      ANTHROPIC_SOURCE,
    );
    await harness.advanceTo(turn2Close + 10);
    const events2 = await turn2Events;

    // The turn-2 iterator yielded the final assistant text.
    const turn2Done = events2.find((e) => e.type === "inference.done");
    if (turn2Done === undefined || turn2Done.type !== "inference.done") {
      throw new Error("expected an inference.done event in turn 2");
    }
    const turn2Content = turn2Done.data.turn.content;
    const textBlock = turn2Content.find((c) => c.type === "text");
    if (textBlock === undefined || textBlock.type !== "text") {
      throw new Error("expected a text block in turn 2 final turn");
    }
    expect(textBlock.text).toBe("It is 68F and foggy in SF.");

    const turn2Delta = events2.find((e) => e.type === "inference.text.delta");
    if (
      turn2Delta === undefined ||
      turn2Delta.type !== "inference.text.delta"
    ) {
      throw new Error("expected an inference.text.delta event in turn 2");
    }
    expect(turn2Delta.data.token).toBe("It is 68F and foggy in SF.");

    // The turn-2 request body carries the tool_result block, proving the
    // round-trip propagated state through the production adapter.
    if (capturedTurn1Request === null) {
      throw new Error("turn-1 matcher never fired");
    }
    if (capturedTurn2Request === null) {
      throw new Error("turn-2 matcher never fired");
    }
    const turn2Body = await parseRequestBody(capturedTurn2Request);
    const messages = turn2Body["messages"];
    if (!Array.isArray(messages)) {
      throw new Error("turn-2 body did not carry a messages array");
    }
    expect(messages.length).toBe(3);

    const toolResultMessage = messages[2];
    if (!isRecord(toolResultMessage)) {
      throw new Error("turn-2 final message was not an object");
    }
    const content = toolResultMessage["content"];
    if (!Array.isArray(content)) {
      throw new Error("turn-2 tool_result message content was not an array");
    }
    const firstBlock = content[0];
    if (!isRecord(firstBlock)) {
      throw new Error("turn-2 tool_result block was not an object");
    }
    expect(firstBlock["type"]).toBe("tool_result");
    expect(firstBlock["tool_use_id"]).toBe("call_weather_1");
    expect(firstBlock["content"]).toEqual([{ type: "text", text: "68F, fog" }]);

    // The turn-1 body, by contrast, carried only the original user turn.
    const turn1Body = await parseRequestBody(capturedTurn1Request);
    const turn1Messages = turn1Body["messages"];
    if (!Array.isArray(turn1Messages)) {
      throw new Error("turn-1 body did not carry a messages array");
    }
    expect(turn1Messages.length).toBe(1);
  });

  test("OpenAI: drives turn-1 tool call, dispatches tool, and propagates result into turn-2 inference", async () => {
    const harness = setupHarness();
    activeHarness = harness;

    // The harness resolves the index-based placeholder callId at
    // tool_call.end; asserts use the resolved id.
    const handlerCalls: { args: unknown }[] = [];
    harness.scenario.onTool("lookup", (args) => {
      handlerCalls.push({ args });
      return { city: "SF", population: 815000 };
    });

    // Turn 1 wire bytes: tool-call sequence, usage chunk, [DONE].
    const turn1Stream = harness.scenario.createStream();
    const turn1Chunks: Uint8Array[] = [
      ...wire.openai.toolCallSequence(0, "call_lookup_1", "lookup", [
        '{"city":"SF"}',
      ]),
      wire.openai.usageChunk({
        promptTokens: TURN1_USAGE_HEAD.input,
        completionTokens: TURN1_USAGE_TAIL.output,
      }),
      wire.openai.done(),
    ];
    turn1Stream.enqueueAll(turn1Chunks, { startAt: 10 });
    const turn1Close = 10 + turn1Chunks.length;

    // Turn 2 wire bytes: final assistant text chunk + usage + done.
    const turn2Stream = harness.scenario.createStream();
    const turn2Chunks: Uint8Array[] = [
      wire.openai.chunk({ content: "SF has about 815k people." }),
      wire.openai.usageChunk({
        promptTokens: TURN2_USAGE_HEAD.input,
        completionTokens: TURN2_USAGE_TAIL.output,
      }),
      wire.openai.done(),
    ];
    turn2Stream.enqueueAll(turn2Chunks, { startAt: turn1Close + 50 });
    const turn2Close = turn1Close + 50 + turn2Chunks.length;

    let capturedTurn1Request: Request | null = null;
    let capturedTurn2Request: Request | null = null;
    harness.scenario.whenRequestMatches((req) => {
      capturedTurn1Request = req.clone();
      return true;
    }, turn1Stream);
    harness.scenario.whenRequestMatches((req) => {
      capturedTurn2Request = req.clone();
      return true;
    }, turn2Stream);

    let seq = 0;
    const turn1Events = collectInferenceAuto(
      harness,
      [userTurn("How big is SF?")],
      () => ++seq,
      OPENAI_SOURCE,
    );
    await harness.advanceTo(turn1Close + 10);
    const events1 = await turn1Events;

    const turn1End = events1.find((e) => e.type === "inference.tool_call.end");
    if (turn1End === undefined || turn1End.type !== "inference.tool_call.end") {
      throw new Error("expected an inference.tool_call.end event in turn 1");
    }
    expect(turn1End.data.name).toBe("lookup");
    expect(turn1End.data.callId).toBe("call_lookup_1");
    expect(turn1End.data.arguments).toEqual({ city: "SF" });

    const turn1Done = events1.find((e) => e.type === "inference.done");
    if (turn1Done === undefined || turn1Done.type !== "inference.done") {
      throw new Error("expected an inference.done event in turn 1");
    }
    const assistantTurn = turn1Done.data.turn;

    expect(handlerCalls).toEqual([{ args: { city: "SF" } }]);
    expect(harness.scenario.lastToolDispatch("lookup")).toEqual({
      city: "SF",
      population: 815000,
    });

    const turn2Conversation: ConversationTurn[] = [
      userTurn("How big is SF?"),
      assistantTurn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_lookup_1",
            content: [{ type: "text", text: "815000" }],
          },
        ],
        timestamp: 0,
      },
    ];

    const turn2Events = collectInferenceAuto(
      harness,
      turn2Conversation,
      () => ++seq,
      OPENAI_SOURCE,
    );
    await harness.advanceTo(turn2Close + 10);
    const events2 = await turn2Events;

    const turn2Delta = events2.find((e) => e.type === "inference.text.delta");
    if (
      turn2Delta === undefined ||
      turn2Delta.type !== "inference.text.delta"
    ) {
      throw new Error("expected an inference.text.delta event in turn 2");
    }
    expect(turn2Delta.data.token).toBe("SF has about 815k people.");

    if (capturedTurn1Request === null) {
      throw new Error("turn-1 matcher never fired");
    }
    if (capturedTurn2Request === null) {
      throw new Error("turn-2 matcher never fired");
    }

    // The turn-2 request body carried a `tool` role message referencing
    // `call_lookup_1` — OpenAI's tool-result wire shape.
    const turn2Body = await parseRequestBody(capturedTurn2Request);
    const messages = turn2Body["messages"];
    if (!Array.isArray(messages)) {
      throw new Error("turn-2 body did not carry a messages array");
    }
    const toolMessage = messages.find(
      (m): m is Record<string, unknown> => isRecord(m) && m["role"] === "tool",
    );
    if (toolMessage === undefined) {
      throw new Error("turn-2 body did not carry a tool role message");
    }
    expect(toolMessage["tool_call_id"]).toBe("call_lookup_1");
    expect(toolMessage["content"]).toBe("815000");

    // The turn-1 body carried only the user turn.
    const turn1Body = await parseRequestBody(capturedTurn1Request);
    const turn1Messages = turn1Body["messages"];
    if (!Array.isArray(turn1Messages)) {
      throw new Error("turn-1 body did not carry a messages array");
    }
    expect(turn1Messages.length).toBe(1);
  });

  test("Anthropic: delayed-envelope tool handler defers dispatch until virtualDelayMs elapses", async () => {
    const harness = setupHarness();
    activeHarness = harness;

    // The handler returns a `{ result, virtualDelayMs }` envelope; the
    // dispatch callback must fire at clock.now() + delay, not before.
    const HANDLER_DELAY_MS = 200;
    harness.scenario.onTool("weather", () => ({
      result: { temperatureF: 72, conditions: "clear" },
      virtualDelayMs: HANDLER_DELAY_MS,
    }));

    const turn1Stream = harness.scenario.createStream();
    const turn1Chunks = wire.completeResponse("anthropic", {
      toolCalls: [
        {
          callId: "call_weather_delayed",
          name: "weather",
          argsJSON: '{"location":"LA"}',
        },
      ],
      headUsage: TURN1_USAGE_HEAD,
      tailUsage: TURN1_USAGE_TAIL,
    });
    turn1Stream.enqueueAll(turn1Chunks, { startAt: 10 });
    const turn1Close = 10 + turn1Chunks.length;

    harness.scenario.whenRequestMatches(() => true, turn1Stream);

    let seq = 0;
    const turn1Events = collectInferenceManual(
      harness,
      [userTurn("What is the weather in LA?")],
      () => ++seq,
      ANTHROPIC_SOURCE,
    );
    await harness.advanceTo(turn1Close + 10);
    const events1 = await turn1Events;

    const turn1End = events1.find((e) => e.type === "inference.tool_call.end");
    if (turn1End === undefined || turn1End.type !== "inference.tool_call.end") {
      throw new Error("expected an inference.tool_call.end event in turn 1");
    }

    // Capture invocation time; the dispatch should run exactly HANDLER_DELAY_MS later.
    const invokedAt = harness.clock.now();
    const dispatched: { at: number; result: unknown }[] = [];
    harness.scenario.invokeTool(
      "weather",
      turn1End.data.arguments,
      (result) => {
        dispatched.push({ at: harness.clock.now(), result });
      },
    );

    // Immediately after invokeTool the dispatch must not have fired.
    expect(dispatched).toEqual([]);

    // Advance to one tick before the deadline: still no dispatch.
    await harness.advanceTo(invokedAt + HANDLER_DELAY_MS - 1);
    expect(dispatched).toEqual([]);

    // Advance exactly to the deadline: dispatch fires.
    await harness.advanceTo(invokedAt + HANDLER_DELAY_MS);
    expect(dispatched).toEqual([
      {
        at: invokedAt + HANDLER_DELAY_MS,
        result: { temperatureF: 72, conditions: "clear" },
      },
    ]);
  });

  test("Anthropic: promise-returning tool handler defers dispatch until the promise resolves", async () => {
    const harness = setupHarness();
    activeHarness = harness;

    // The handler returns a Promise gated by the test; run() awaits the
    // in-flight promise before declaring quiescence.
    const { promise: handlerGate, resolve: releaseHandler } =
      Promise.withResolvers<unknown>();
    harness.scenario.onTool("weather", async () => {
      const value = await handlerGate;
      return value;
    });

    const turn1Stream = harness.scenario.createStream();
    const turn1Chunks = wire.completeResponse("anthropic", {
      toolCalls: [
        {
          callId: "call_weather_async",
          name: "weather",
          argsJSON: '{"location":"NYC"}',
        },
      ],
      headUsage: TURN1_USAGE_HEAD,
      tailUsage: TURN1_USAGE_TAIL,
    });
    turn1Stream.enqueueAll(turn1Chunks, { startAt: 10 });
    const turn1Close = 10 + turn1Chunks.length;
    harness.scenario.whenRequestMatches(() => true, turn1Stream);

    let seq = 0;
    const turn1Events = collectInferenceManual(
      harness,
      [userTurn("What is the weather in NYC?")],
      () => ++seq,
      ANTHROPIC_SOURCE,
    );
    await harness.advanceTo(turn1Close + 10);
    const events1 = await turn1Events;

    const turn1End = events1.find((e) => e.type === "inference.tool_call.end");
    if (turn1End === undefined || turn1End.type !== "inference.tool_call.end") {
      throw new Error("expected an inference.tool_call.end event in turn 1");
    }

    const dispatched: unknown[] = [];
    harness.scenario.invokeTool(
      "weather",
      turn1End.data.arguments,
      (result) => {
        dispatched.push(result);
      },
    );

    // The handler is parked on the gate; the dispatch must not have fired.
    expect(dispatched).toEqual([]);

    // Release the gate; run() awaits the in-flight promise and surfaces the dispatch.
    releaseHandler({ temperatureF: 33, conditions: "snow" });

    await harness.run();
    expect(dispatched).toEqual([{ temperatureF: 33, conditions: "snow" }]);
  });

  test("Anthropic: parallel tool calls in a single turn round-trip both tool_results", async () => {
    const harness = setupHarness();
    activeHarness = harness;

    // The turn-1 wire bytes serve two tool_use blocks in one response.
    const weatherCalls: { args: unknown }[] = [];
    const timeCalls: { args: unknown }[] = [];
    harness.scenario.onTool("weather", (args) => {
      weatherCalls.push({ args });
      return { temperatureF: 60, conditions: "rain" };
    });
    harness.scenario.onTool("time", (args) => {
      timeCalls.push({ args });
      return { iso: "2026-05-16T12:00:00Z" };
    });

    const turn1Stream = harness.scenario.createStream();
    const turn1Chunks = wire.completeResponse("anthropic", {
      toolCalls: [
        {
          callId: "call_weather_par",
          name: "weather",
          argsJSON: '{"location":"PDX"}',
        },
        {
          callId: "call_time_par",
          name: "time",
          argsJSON: '{"zone":"UTC"}',
        },
      ],
      headUsage: TURN1_USAGE_HEAD,
      tailUsage: TURN1_USAGE_TAIL,
    });
    turn1Stream.enqueueAll(turn1Chunks, { startAt: 10 });
    const turn1Close = 10 + turn1Chunks.length;

    const turn2Stream = harness.scenario.createStream();
    const turn2Chunks = wire.completeResponse("anthropic", {
      text: "PDX: 60F rain at 12:00 UTC.",
      headUsage: TURN2_USAGE_HEAD,
      tailUsage: TURN2_USAGE_TAIL,
    });
    turn2Stream.enqueueAll(turn2Chunks, { startAt: turn1Close + 50 });
    const turn2Close = turn1Close + 50 + turn2Chunks.length;

    let capturedTurn2Request: Request | null = null;
    harness.scenario.whenRequestMatches(() => true, turn1Stream);
    harness.scenario.whenRequestMatches((req) => {
      capturedTurn2Request = req.clone();
      return true;
    }, turn2Stream);

    let seq = 0;
    const turn1Events = collectInferenceAuto(
      harness,
      [userTurn("PDX weather and time?")],
      () => ++seq,
      ANTHROPIC_SOURCE,
    );
    await harness.advanceTo(turn1Close + 10);
    const events1 = await turn1Events;

    // Both tool_call.end events surface with the right name/callId/args.
    const toolEnds = events1.filter(
      (e) => e.type === "inference.tool_call.end",
    );
    expect(toolEnds.length).toBe(2);
    const endsByName = new Map<string, (typeof toolEnds)[number]>();
    for (const ev of toolEnds) {
      if (ev.type !== "inference.tool_call.end") continue;
      endsByName.set(ev.data.name, ev);
    }
    const weatherEnd = endsByName.get("weather");
    const timeEnd = endsByName.get("time");
    if (
      weatherEnd === undefined ||
      weatherEnd.type !== "inference.tool_call.end" ||
      timeEnd === undefined ||
      timeEnd.type !== "inference.tool_call.end"
    ) {
      throw new Error("expected tool_call.end events for both tools");
    }
    expect(weatherEnd.data.callId).toBe("call_weather_par");
    expect(weatherEnd.data.arguments).toEqual({ location: "PDX" });
    expect(timeEnd.data.callId).toBe("call_time_par");
    expect(timeEnd.data.arguments).toEqual({ zone: "UTC" });

    const turn1Done = events1.find((e) => e.type === "inference.done");
    if (turn1Done === undefined || turn1Done.type !== "inference.done") {
      throw new Error("expected an inference.done event in turn 1");
    }
    const assistantTurn = turn1Done.data.turn;

    // Auto-dispatch fired both handlers once apiece, each with the right input.
    expect(weatherCalls).toEqual([{ args: { location: "PDX" } }]);
    expect(timeCalls).toEqual([{ args: { zone: "UTC" } }]);
    expect(harness.scenario.lastToolDispatch("weather")).toEqual({
      temperatureF: 60,
      conditions: "rain",
    });
    expect(harness.scenario.lastToolDispatch("time")).toEqual({
      iso: "2026-05-16T12:00:00Z",
    });

    // Turn 2: user turn + assistant turn (both tool_call blocks) + one user
    // turn carrying both tool_result blocks.
    const turn2Conversation: ConversationTurn[] = [
      userTurn("PDX weather and time?"),
      assistantTurn,
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call_weather_par",
            content: [{ type: "text", text: "60F, rain" }],
          },
          {
            type: "tool_result",
            callId: "call_time_par",
            content: [{ type: "text", text: "12:00 UTC" }],
          },
        ],
        timestamp: 0,
      },
    ];

    const turn2Events = collectInferenceAuto(
      harness,
      turn2Conversation,
      () => ++seq,
      ANTHROPIC_SOURCE,
    );
    await harness.advanceTo(turn2Close + 10);
    const events2 = await turn2Events;

    const turn2Delta = events2.find((e) => e.type === "inference.text.delta");
    if (
      turn2Delta === undefined ||
      turn2Delta.type !== "inference.text.delta"
    ) {
      throw new Error("expected an inference.text.delta event in turn 2");
    }
    expect(turn2Delta.data.token).toBe("PDX: 60F rain at 12:00 UTC.");

    if (capturedTurn2Request === null) {
      throw new Error("turn-2 matcher never fired");
    }
    const turn2Body = await parseRequestBody(capturedTurn2Request);
    const messages = turn2Body["messages"];
    if (!Array.isArray(messages)) {
      throw new Error("turn-2 body did not carry a messages array");
    }
    // The turn-2 messages array carries both tool_result blocks.
    const finalMessage = messages[messages.length - 1];
    if (!isRecord(finalMessage)) {
      throw new Error("turn-2 final message was not an object");
    }
    const finalContent = finalMessage["content"];
    if (!Array.isArray(finalContent)) {
      throw new Error("turn-2 final message content was not an array");
    }
    const toolResults = finalContent.filter(
      (b): b is Record<string, unknown> =>
        isRecord(b) && b["type"] === "tool_result",
    );
    expect(toolResults.length).toBe(2);
    const idToContent = new Map<unknown, unknown>();
    for (const tr of toolResults) {
      idToContent.set(tr["tool_use_id"], tr["content"]);
    }
    expect(idToContent.get("call_weather_par")).toEqual([
      { type: "text", text: "60F, rain" },
    ]);
    expect(idToContent.get("call_time_par")).toEqual([
      { type: "text", text: "12:00 UTC" },
    ]);
  });
});
