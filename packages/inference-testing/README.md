# @intx/inference-testing

A deterministic test harness for the `@intx/inference` streaming
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

import { runInference } from "@intx/inference";
import { expectEvents, setupHarness, wire } from "@intx/inference-testing";

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
      source: {
        id: "anthropic:claude-test",
        provider: "anthropic",
        baseURL: "https://example",
        apiKey: "key",
        model: "claude-test",
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

### Stall scenarios

`scenario.stall` wires up a parked-stream fixture for tests that exercise
the inference layer's per-call timeouts. The matched fetch is bound to a
stream the helper never feeds, so the SSE iterator parks on its first
read; in virtual time the test advances past the timeout threshold and
the inactivity (or total) timer in `runInference` aborts the underlying
fetch. The returned `StallHandle` exposes live abort telemetry so tests
can assert on `AbortController` propagation directly, not only on the
downstream `inference.error` event the abort produces.

```ts
const harness = setupHarness({ enableInferenceTimers: true });
const stall = harness.scenario.stall();

const events = runInference({
  /* ... */
  inferenceOptions: { inactivityTimeoutMs: 50, totalTimeoutMs: 10_000 },
  deps: harness.deps,
});

await harness.run();
await stall.awaitAbort;

expect(stall.aborted).toBe(true);
```

`stall.stream` is the underlying `SimulatedStream` in case a test wants
to release the stall by enqueueing chunks before the timeout fires.
`stall.aborted` flips `true` the moment the matched fetch's
`AbortSignal` fires (the inference layer's per-call timeout firing,
or a caller-supplied signal aborting). `dispose()` does not flip
`aborted` — it rejects the underlying fetch with an `Error` rather
than aborting its signal — but it does resolve `awaitAbort` so tests
that awaited the promise past dispose do not hang the test runner.

The harness has no special-case knowledge of stalls. The matcher
settles the fetch with a `Response` as soon as it fires; what parks
indefinitely is the response body's SSE iterator. `checkQuiescence`
therefore does not see an unmatched fetch and does not raise.

Pair `stall` with `setupHarness({ enableInferenceTimers: true })` — the
default no-op scheduler leaves the inference layer's timers inert so
that other test suites don't have to advance virtual time through the
ten-minute default total timeout on every call.

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

  const source = {
    id: "anthropic:claude-test",
    provider: "anthropic",
    baseURL: "https://example",
    apiKey: "key",
    model: "claude-test",
  };

  let seq = 0;
  const events: InferenceEvent[] = [];
  const collect = (async () => {
    for await (const ev of harness.runInference({
      turns: [userTurn("weather?")],
      source,
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

`tests/inference-testing/multi-turn-harness.test.ts` exercises the full
turn-1 → tool-dispatch → turn-2 round-trip with captured request bodies
proving the tool-result block propagates into the turn-2 wire payload.

## Session capture and replay

The harness primitives above (matchers, tool handlers, virtual clock)
script wire bytes the test author writes. Session capture and replay
go a step further: they record an entire conversation against a real
provider, then re-drive that captured wire and the captured tool I/O
through production `runInference` to catch regressions in the
orchestration layer above the adapter — multi-turn body construction,
conversation history threading, tool dispatch wiring, terminal
sequencing across turns.

Where the existing `runCompatReplay` (INTR-79) holds **one** exchange
constant to surface adapter regressions, session replay holds an
**entire conversation** constant: every exchange plus every tool
dispatch. If the production reactor changes how it serialises a
`tool_result` block, or how it threads previous turns into a new
request body, the corresponding exchange's body diverges from
capture and `SessionReplayMismatchError` fires with a diff.

### Terminology

- **Exchange.** One HTTP request/response with the provider. Wire
  level; curlable. `writeCapture` (imported from
  `@intx/inference-discovery`) writes the bytes.
- **Dispatch.** One tool-handler invocation by the reactor in response
  to `inference.tool_call.end`. Never crosses the network. Recorded
  as `{ args, result }`.

The two are wired together: dispatch N's result becomes a
`tool_result` block inside exchange N+1's request body. Freezing the
dispatch results is what makes a captured session deterministic at
replay time — without it, a flaky handler would make the next
exchange's body diverge and break replay.

### Capture format

```
packages/inference-testing/sessions/<name>/
├── session.json
├── exchanges/
│   ├── 0/
│   │   ├── request.json            # via writeCapture
│   │   ├── request-headers.json    # via writeCapture
│   │   ├── response.sse            # via writeCapture (or response.json)
│   │   └── response-headers.json   # via writeCapture
│   ├── 1/
│   └── …
└── dispatches/
    ├── 0-weather.json              # { "args": …, "result": … }
    ├── 1-time.json
    └── …
```

The `session.json` at the root carries only session-level facts that
describe the session as a whole — `sessionSchemaVersion`, `source`
(provider/model/baseURL), `capturedAt`. There is no catalog of the
contents underneath; ordering and naming come from the filesystem
layout itself, so the metadata cannot go stale.

Exchange directories are byte-identical to the discovery rig's
captures (they're written by the same `writeCapture` function).
Dispatch entries are a single JSON file each because both `args` and
`result` are small JSON values.

### Recording

```ts
import { createRecordingHarness } from "@intx/inference-testing";
import { requireEnv } from "@intx/inference-discovery";

const apiKey = requireEnv("ANTHROPIC_API_KEY");

const harness = createRecordingHarness({
  outputDir: "packages/inference-testing/sessions/my-scenario",
  source: {
    provider: "anthropic",
    model: "claude-sonnet-x",
    baseURL: "https://api.anthropic.com",
  },
  // Hard ceiling — recording stops with a clear error if the reactor
  // tries to make more fetch calls than expected, so a runaway loop
  // can't silently rack up provider charges.
  maxExchanges: 8,
  // Header redaction is plumbed through to the same logic the
  // discovery rig uses for its captures.
  redactRequestHeaders: ["x-api-key", "authorization"],
  redactResponseHeaders: [],
});

// Real handlers run during recording. The harness observes the args
// the reactor produced and the value the handler returned, and writes
// both to `dispatches/<index>-<toolName>.json`.
harness.scenario.onTool("weather", (args) => callRealWeatherAPI(args));

let seq = 0;
for await (const _ev of harness.runInference({
  turns: [
    {
      role: "user",
      content: [{ type: "text", text: "weather?" }],
      timestamp: 0,
    },
  ],
  source: { id, provider: "anthropic", baseURL, apiKey, model },
  nextSeq: () => ++seq,
})) {
  // drain or assert on events
}

// Required to write `session.json`. Wrap recording in try/finally so
// an aborted run still produces a truncated-but-readable capture.
await harness.finalize();
```

`@intx/inference-discovery`'s `assertNotCI` runs at construction by
default — recordings make live network calls and must not run in CI.
The paired `fetch: FetchLike` + `bypassCIGuardForTests: true` test
seam is reserved for this package's own unit tests; production
recording scripts should never pass either flag.

`packages/inference-testing/bin/record-example-sessions.ts` is a small
script that uses the test seam to regenerate the committed example
sessions without provider credentials. Run it with
`bun packages/inference-testing/bin/record-example-sessions.ts`.

### Replay

```ts
import { createReplayHarness, INVARIANTS } from "@intx/inference-testing";

const replay = await createReplayHarness({
  sessionDir: "packages/inference-testing/sessions/my-scenario",
});
try {
  // Production runInference is single-turn; the caller drives the
  // multi-turn loop just as the existing multi-turn-harness tests do.
  // Each runTurn call routes the adapter's fetch through one captured
  // exchange's body-aware matcher; the replay harness enforces that
  // the actual request body canonicalises to the captured body
  // exactly, surfacing SessionReplayMismatchError with a diff on any
  // divergence.
  let conversation = [
    {
      role: "user",
      content: [{ type: "text", text: "weather?" }],
      timestamp: 0,
    },
  ];
  const turn1 = await replay.runTurn({ turns: conversation });

  // Inspect events, validate INVARIANTS, build the next turn's
  // conversation from `turn1Done.data.turn` plus a `tool_result`
  // user turn assembled from `replay.capturedDispatches`. The
  // captured dispatch results are also served from
  // `scenario.onTool` automatically — real tool handlers do NOT
  // run at replay time.
  // …

  replay.assertFullyConsumed();
} finally {
  replay.dispose();
}
```

`tests/inference-testing/session-replay.test.ts` is the canonical
end-to-end replay test. It loads each committed session, drives every
captured turn through `runInference`, asserts the `INVARIANTS` event
shape contract holds for each turn, and verifies the full exchange
and dispatch counts match the capture.

### Captured-dispatch-results design

The replay harness serves captured dispatch results verbatim. Real
tool handlers do **not** run at replay time. This is the locked
design call from INTR-93, and it matters: re-invoking a real handler
risks producing a result that diverges from the captured result,
which causes the next turn's request body to diverge from capture,
which makes the captured response no longer a valid reply — replay
fails out before the orchestration layer has been exercised at all.
Handler correctness is a separate test concern with its own tests;
session replay holds tool I/O constant so that orchestration
regressions show up cleanly.

If you need to exercise a real handler against captured wire (to
surface tool-side regressions), that's a different testing mode with
different trade-offs and is out of scope for the session harness.

### Dependency on `@intx/inference-discovery`

The session harness builds on:

- `writeCapture` — the per-exchange directory layout is byte-identical
  to discovery's, so recording the same conversation through either
  rig produces the same exchange files.
- `assertNotCI` — recording harnesses hard-fail if `CI` is set.
- `requireEnv` / `requireEnvSet` — the env-var loading pattern
  recording scripts should follow.
- The redaction-hook signature — same `readonly string[]` shape.

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

### Per-agent scoping

Multi-agent tests routinely want to assert "agent A made tool call X"
independently of "agent B made tool call Y". The harness supports this
without any special-purpose machinery: each call to
`harness.runInference(...)` returns its own
`AsyncIterable<InferenceEvent>`. Collect events per call into separate
arrays, and every matcher (`expectToolCalls`, `expectToolCall(...).from(events)`)
is automatically scoped to the agent whose events array it sees.

```ts
const harness = setupHarness();

// Route each agent's fetch to its own response stream — either by URL,
// or by body content via `whenRequestBodyMatches` (see below).
harness.scenario.replyOnce("openai", {
  toolCalls: [{ name: "agentATool", args: { value: "from-A" } }],
  predicate: (req) => req.url.includes("/agent-a/"),
});
harness.scenario.replyOnce("openai", {
  toolCalls: [{ name: "agentBTool", args: { value: "from-B" } }],
  predicate: (req) => req.url.includes("/agent-b/"),
});

const eventsA: InferenceEvent[] = [];
const eventsB: InferenceEvent[] = [];

const collectA = (async () => {
  for await (const ev of harness.runInference({
    /* agent A's opts */
  })) {
    eventsA.push(ev);
  }
})();
const collectB = (async () => {
  for await (const ev of harness.runInference({
    /* agent B's opts */
  })) {
    eventsB.push(ev);
  }
})();

await harness.run();
await Promise.all([collectA, collectB]);

// Each assertion sees only the agent's own events.
expectToolCall("agentATool").from(eventsA).toHaveBeenCalledTimes(1);
expectToolCall("agentBTool").from(eventsA).toHaveBeenCalledTimes(0);
expectToolCall("agentBTool").from(eventsB).toHaveBeenCalledTimes(1);
expectToolCall("agentATool").from(eventsB).toHaveBeenCalledTimes(0);
```

The same pattern works when agents share a URL and only the body
distinguishes them — see [body-aware predicates](#body-aware-predicates).
`tests/inference-testing/per-agent-scoping.test.ts` pins this contract.

### Body-aware predicates

When the only distinguishing data between parallel fetches lives in the
request body — for example, multiple agents POSTing to the same
`/chat/completions` endpoint with the same headers and the same `model`
field, distinguishable only by the task id in the seed message —
register matchers via `scenario.whenRequestBodyMatches`. The predicate
receives the buffered body text alongside the `Request`:

```ts
const greetStream = harness.scenario.createStream();
greetStream.enqueueAll(
  wire.completeResponse("openai", {
    toolCalls: [{ name: "writeFile", args: { path: "greet.ts" } }],
  }),
  { startAt: harness.clock.now() + 1 },
);
harness.scenario.whenRequestBodyMatches(
  (body) => body.includes("1a-greet"),
  greetStream,
);

const formatStream = harness.scenario.createStream();
formatStream.enqueueAll(
  wire.completeResponse("openai", {
    toolCalls: [{ name: "writeFile", args: { path: "format.ts" } }],
  }),
  { startAt: harness.clock.now() + 1 },
);
harness.scenario.whenRequestBodyMatches(
  (body) => body.includes("1b-format"),
  formatStream,
);
```

See `tests/inference-testing/per-agent-scoping.test.ts` for a runnable
end-to-end shape that combines body-aware routing with per-agent
assertion scoping.

The harness buffers each fetch's body once before evaluating
body-aware matchers; subsequent body-aware predicates see the cached
text. URL/header-only matchers registered via `whenRequestMatches`
never trigger a body read, so existing tests pay no extra cost.

Scan ordering: sync `whenRequestMatches` predicates evaluate first.
If none bind a fetch and at least one body-aware matcher exists, the
harness buffers the bodies of every still-waiting fetch and runs a
follow-up scan against body-aware matchers in their original
registration order. Ambiguity (two fetches binding to the same
body-aware matcher) is detected over a fully-buffered waiting set, so
`AmbiguousRequestError` carries the same semantics it carries for sync
matchers.

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

A `RequestPredicate` passed to `scenario.whenRequestMatches` (and a
`BodyAwareRequestPredicate` passed to `scenario.whenRequestBodyMatches`)
must be:

- **synchronous** — enforced by the type signature; there is no place
  to `await`. A body-aware predicate receives the body as a string
  parameter, so the predicate itself never reads the body asynchronously.
- **idempotent** — the harness scans the matcher table on every fetch,
  every `whenRequestMatches` / `whenRequestBodyMatches` call, and at
  quiescence checkpoints. A predicate may run any number of times
  against the same request.
- **side-effect-free** — predicates must not mutate harness state, mutate
  closed-over test state, or emit events.
- **independent of harness state** — predicates must not read
  `clock.now()`, `scenario.invokeTool`-managed registries, the matcher
  table, or anything else that changes between scan passes. Reading
  `req.url`, `req.method`, `req.headers`, and (for body-aware predicates)
  the buffered body string passed as the first parameter is fine;
  everything else is a bug class.

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
