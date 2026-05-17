# @interchange/inference-testing

A deterministic test harness for the `@interchange/inference` streaming
pipeline. Replaces the global `fetch` with a virtual-clock-driven
`SimulatedStream`, lets tests script provider wire bytes via a typed DSL,
and exposes a small matcher suite for asserting against the collected
`InferenceEvent[]`.

## Quickstart

The end-to-end shape: build a harness, register a matcher that pairs a
request predicate with a scripted response stream, run inference, collect
events, assert.

```ts
import { describe, test } from "bun:test";

import { runInference } from "@interchange/inference";
import {
  expectEvents,
  setupHarness,
  wire,
} from "@interchange/inference-testing";

test("anthropic streams text and then completes", async () => {
  const harness = setupHarness();
  try {
    const response = harness.scenario.createStream();

    // Pair every fetch this harness sees with the scripted response.
    harness.scenario.whenRequestMatches(() => true, response);

    // Compose the response bytes the adapter will parse.
    const chunks = wire.completeResponse("anthropic", {
      text: "Hello, world!",
      headUsage: {
        input: 10,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      },
      tailUsage: {
        input: 0,
        output: 8,
        cacheRead: 0,
        cacheWrite: 0,
        thinking: 0,
      },
    });
    for (const [i, chunk] of chunks.entries()) {
      response.enqueueAt(i * 10, chunk);
    }
    response.closeAt(chunks.length * 10);

    // Run inference against the harness's `deps`. Collect events.
    let seq = 0;
    const events = [];
    const iterator = runInference({
      turns: [
        { role: "user", content: [{ type: "text", text: "Hi" }], timestamp: 0 },
      ],
      model: "claude-test",
      providerConfig: {
        provider: "anthropic",
        baseURL: "https://example",
        apiKey: "key",
      },
      nextSeq: () => seq++,
      deps: harness.deps,
    });

    const collect = (async () => {
      for await (const event of iterator) events.push(event);
    })();

    // Settle the virtual clock until quiescence (all chunks delivered,
    // tool handlers drained, response stream closed).
    await harness.run();
    await collect;

    // Assert the event sequence the harness produced.
    expectEvents(events).toMatchSequence([
      { type: "inference.start" },
      { type: "inference.text.delta", data: { token: "Hello, world!" } },
      { type: "inference.done" },
    ]);
  } finally {
    harness.dispose();
  }
});
```

## Scenario authoring

### Matchers

`scenario.whenRequestMatches(predicate, responseStream)` registers a
single-use matcher. The first fetch whose `Request` satisfies the predicate
receives the scripted response. Registration order determines priority;
ambiguous fetches that could route to multiple unbound matchers raise an
`AmbiguousRequestError`.

```ts
harness.scenario.whenRequestMatches(
  (req) => new URL(req.url).pathname === "/v1/messages",
  anthropicResponse,
);
harness.scenario.whenRequestMatches(
  (req) => new URL(req.url).pathname === "/chat/completions",
  openaiResponse,
);
```

The optional third argument shapes the `Response` envelope so tests can drive
the production HTTP error-classification branches in `runInference`
(`runInference` only inspects bodies when `response.ok` is false). The default
remains `status: 200` with `content-type: text/event-stream`; setting a non-2xx
`status` defaults `content-type` to `application/json` and adds any
caller-supplied `headers` on top (caller wins). A 429 rate-limit response with a
`retry-after` header, for example, drives the adapter's `extractRetryAfterMs`
path and surfaces as a `quota_exhausted` `inference.error`:

```ts
const errorStream = harness.scenario.createStream();
harness.scenario.whenRequestMatches((_req) => true, errorStream, {
  status: 429,
  headers: { "retry-after": "7" },
});
errorStream.enqueueAt(0, utf8('{"error":{"message":"rate limited"}}'));
errorStream.closeAt(1);
```

### Tool handlers

`scenario.onTool(name, handler)` registers a handler that runs when a tool
call is dispatched. The handler can return a value synchronously, return a
delayed envelope (`{ result, virtualDelayMs }`), or return a promise. The
harness's `run()` loop drains in-flight handlers before declaring
quiescence.

```ts
harness.scenario.onTool("calc", ({ a, b }) => ({ sum: a + b }));
harness.scenario.onTool("slow_search", async (args) => {
  return await someAsyncWork(args);
});
harness.scenario.onTool("rate_limited", (args) => ({
  result: { value: 42 },
  virtualDelayMs: 1000,
}));
```

Handlers fire automatically through `harness.runInference` (see the
[Tool round-trip](#tool-round-trip) section below); use
`scenario.invokeTool` only when a test wants to drive dispatch by hand.

### Abort scenarios

```ts
const controller = new AbortController();
harness.scenario.abortAt(50, controller); // abort at virtual t=50ms
harness.scenario.abortAfter(
  (event) => new TextDecoder().decode(event.bytes).includes("trigger"),
  controller,
);
```

`harness.abortBefore(streamId)` cancels still-pending chunks for a specific
stream and errors its controller — useful for modelling network failures
mid-response.

### Multi-turn

Compose multiple matchers and call `runInference` once per turn. The
harness preserves all state across calls until `dispose()`.

### Tool round-trip

The default driver `harness.runInference` wraps the production
`runInference`, injects `harness.deps` automatically, and fires registered
`onTool` handlers when it observes `inference.tool_call.end` events. Tests
do not need to call `scenario.invokeTool` to drive the round-trip:

```ts
const harness = setupHarness();
try {
  harness.scenario.onTool("weather", (args) => ({ temperatureF: 68 }));

  const turn1 = harness.scenario.createStream();
  turn1.enqueueAll(
    wire.completeResponse("anthropic", {
      toolCalls: [
        {
          callId: "call_w_1",
          name: "weather",
          argsJSON: '{"location":"SF"}',
        },
      ],
      headUsage,
      tailUsage,
    }),
    { startAt: 10 },
  );
  harness.scenario.whenRequestMatches(() => true, turn1);

  const turn2 = harness.scenario.createStream();
  turn2.enqueueAll(
    wire.completeResponse("anthropic", {
      text: "It is 68F in SF.",
      headUsage,
      tailUsage,
    }),
    { startAt: 100 },
  );
  harness.scenario.whenRequestMatches(() => true, turn2);

  let seq = 0;
  const events: InferenceEvent[] = [];
  const collect = (async () => {
    for await (const ev of harness.runInference({
      turns: [userTurn("weather?")],
      model: "claude-test",
      providerConfig,
      nextSeq: () => ++seq,
    })) {
      events.push(ev);
    }
  })();
  await harness.run();
  await collect;

  // The handler fired automatically; its result is captured here.
  expect(harness.scenario.lastToolDispatch("weather")).toEqual({
    temperatureF: 68,
  });
} finally {
  harness.dispose();
}
```

`test/integration/multi-turn-harness.test.ts` exercises the full
turn-1 → tool-dispatch → turn-2 round-trip with captured request bodies
proving the tool-result block propagates into the turn-2 wire payload.

## Wire DSL

Per-provider helpers compose into byte sequences:

```ts
const chunks = [
  ...wire.anthropic.thinkingBlock("Let me think..."),
  ...wire.anthropic.textBlock("Done thinking."),
  ...wire.anthropic.toolUseBlock("toolu_1", "search", '{"q":"x"}', 2),
  wire.anthropic.messageDelta({ stopReason: "end_turn", outputTokens: 42 }),
  wire.anthropic.messageStop(),
];
```

OpenAI exposes the same surface plus the `[DONE]` sentinel:

```ts
const chunks = [
  wire.openai.chunk({ content: "Hello" }),
  ...wire.openai.toolCallSequence(0, "call_x", "calc", ['{"x":', "1}"]),
  wire.openai.usageChunk({ promptTokens: 50, completionTokens: 10 }),
  wire.openai.done(),
];
```

Agnostic helpers compile to per-provider wire when the test only cares
about the message shape:

```ts
const chunks = wire.assistantText("anthropic", "Hi");
const tc = wire.toolCall("openai", "call_a", "search", '{"q":"x"}');
const u = wire.usage("anthropic", {
  input: 0,
  output: 7,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
});
```

The `raw()` escape hatch (`wire.anthropic.raw(...)`, `wire.openai.raw(...)`)
emits arbitrary bytes — use it for adversarial cases the structured helpers
cannot express. If a `raw()` use-case recurs, add a helper instead of
duplicating the byte literal.

## Matchers

```ts
expectEvents(events).toMatchSequence([
  { type: "inference.start" },
  { type: "inference.text.delta", data: { token: "Hello" } },
  { type: "inference.done" },
]);
expectToolCalls(events).toInclude({
  name: "search",
  arguments: { q: "x" },
});
expectToolCall("search").from(events).toHaveBeenCalledTimes(2);
```

`toMatchSequence` allows gaps between expected entries — events not named
in the partial are tolerated.

## Rules

The harness's determinism rests on a small set of contracts test authors
must respect. Violating any of them produces a stack trace pointing at the
offending site; the harness does not silently paper over breakage.

### Handler real-timer rule

Tool handlers and matcher predicates must not call `setTimeout`,
`setInterval`, `Bun.sleep`, `clock.now()` against a real-time clock, or
any other wall-clock-driven primitive. Microtasks (`Promise.resolve()`,
`queueMicrotask`) are fine — the harness's virtual clock yields the
underlying event loop between scheduled callbacks, so microtask
continuations resolve naturally.

When a handler blocks on a real-time primitive, the harness's wall-clock
watchdog (250ms default) trips and throws a `ClockWallClockOverrunError`
out of `run()` / `advanceTo()`. The throw carries the in-flight handler
count and the elapsed wall time so the offending site is easy to find.

### Matcher predicate purity rule

A `RequestPredicate` passed to `scenario.whenRequestMatches` must be:

- **synchronous** — enforced by the `(req: Request) => boolean` type
  signature; there is no place to `await`.
- **idempotent** — the harness scans the matcher table on every fetch,
  every `whenRequestMatches` call, and at quiescence checkpoints. A
  predicate may run any number of times against the same request.
- **side-effect-free** — predicates must not mutate harness state, mutate
  closed-over test state, or emit events.
- **independent of harness state** — predicates must not read
  `clock.now()`, `scenario.invokeTool`-managed registries, the matcher
  table, or anything else that changes between scan passes. Reading
  `req.url`, `req.method`, and `req.headers` is fine; everything else is
  a bug class.

A predicate that reads `clock.now()` to "only match the second fetch"
will misbehave: the harness calls the predicate during scan-on-quiescence
at a different virtual time than the fetch arrival. Match on request
fields, register multiple matchers in order, or use a separate counter
held by the test fixture if "first vs. second" semantics are required.

### `Dependencies` do-not-log/serialize note

The `Dependencies` object returned by `harness.deps` carries an internal
`[HarnessId]` symbol tag so `assertDeps()` can detect cross-harness
contamination. The symbol is enumerable via `Object.getOwnPropertySymbols`
(and the `Reflect.ownKeys` superset).

`JSON.stringify(deps)` is safe — it walks string keys only and produces
the serialized fetch function alone. Reflection-based serializers
(structuredClone in some implementations, debug-logging libraries that
walk symbols, custom audit collectors) leak the symbol tag across trust
boundaries.

Do not pass `Dependencies` through reflective serializers. Do not log
them across process boundaries. Do not expose them in test fixtures that
are themselves shared with code outside the test harness's boundary.

## Driving the clock

The harness exposes two complementary entry points for advancing the
virtual clock to quiescence.

### Choosing `run` vs `advanceTo`

- `harness.advanceTo(virtualMs, opts?)` advances the clock to a bounded
  virtual deadline, draining in-flight tool handlers along the way, and
  then asserts the waiting-fetch set is empty. Use it when the test
  knows the schedule's deadline — typically `closeTime + slack` derived
  from the times the test scheduled chunks at.
- `harness.run(opts?)` advances until the clock heap is empty AND
  quiescence holds (no parked fetches, no in-flight handlers). Use it
  for "drive everything to completion": tests that don't want to compute
  a deadline themselves and just want the harness to settle.

Both methods perform the same drain + quiescence check; the only
difference is the stopping criterion. Prefer `advanceTo` when the
schedule's deadline is part of the test's intent (e.g., asserting that
something has NOT fired by a particular virtual time); prefer `run`
otherwise.

### Wall-clock budget opt-out

`run()` and the in-flight drain inside `advanceTo()` race their progress
against a real-time watchdog so a tool handler blocked on a real-time
primitive (`setTimeout`, network I/O) surfaces as
`ClockWallClockOverrunError` instead of hanging the test. The default
budget is 250ms.

For tests that legitimately need to do real I/O inside a handler — for
example, an integration test that talks to a real downstream service
through the handler — pass `wallClockBudgetMs: Infinity` to opt out:

```ts
await harness.run({ wallClockBudgetMs: Infinity });
```

This should be rare. The default 250ms catches the common bug class
(`setTimeout` smuggled into a handler) without inconveniencing
microtask-only handlers.

## Architectural notes

`setupHarness()` returns a `Harness` whose `clock`, `deps`, and `scenario`
expose the testing surface. Its `run()` and `advanceTo()` drive the
virtual clock to quiescence — chunks fire, handlers drain, matchers scan
— and assert no fetch remains parked on a matcher (otherwise
`UnmatchedFetchError`).

Each `SimulatedStream` owns its own `ReadableStreamDefaultController`
captured at creation, so per-fetch state never leaks across calls. Tests
that need per-fetch abort isolation can attach an `AbortController` to
`scenario.abortAt` / `scenario.abortAfter` or use `harness.abortBefore`
to cancel pending chunks on a specific stream.

For deeper architectural detail see the dispatch plans in
`dispatch/intr-60-inference-testing/`.
