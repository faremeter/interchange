import { AmbiguousRequestError, type AmbiguousFetchInfo } from "./errors";
import type { ChunkFiredEvent, SimulatedStream } from "./simulated-stream";
import type { DispatchToolResult, ToolHandler } from "./tool-handler";
import type { Provider } from "./wire/agnostic";

/**
 * Predicate evaluated against wire-event chunks the harness sees being
 * delivered by simulated streams. Used by `scenario.abortAfter` for v1:
 * the harness does not currently observe reactor-side `InferenceEvent`s
 * directly, so wire-event observation is the closest reusable signal
 * available at this layer.
 */
export type WireEventPredicate = (event: ChunkFiredEvent) => boolean;

/**
 * Predicate run against a constructed `Request` to decide whether a matcher
 * applies. The signature is sync-only on purpose: predicates run on every
 * scan pass and must be referentially transparent. Reading mutable state
 * (e.g., `harness.clock.now()`) from a predicate is a documentation-level
 * bug — the type system enforces that predicates cannot await, but it
 * cannot enforce purity.
 */
export type RequestPredicate = (req: Request) => boolean;

/**
 * Options for `scenario.replyOnce`. `text` and `toolCalls` are the response
 * payload (passed through to `wire.completeResponse`); `headUsage` and
 * `tailUsage` are optional usage frames. `predicate` narrows which fetch
 * the matcher routes to (defaults to match-any). `responseOpts` shapes
 * the HTTP envelope of the `Response` the matcher produces.
 */
export type ReplyOnceOpts = {
  readonly text?: string;
  readonly toolCalls?: {
    callId: string;
    name: string;
    argsJSON: string;
  }[];
  readonly headUsage?: {
    input: number;
    output: number;
    cacheRead: number;
    cacheWrite: number;
    thinking: number;
  };
  readonly tailUsage?: {
    input: number;
    output: number;
    cacheRead: number;
    cacheWrite: number;
    thinking: number;
  };
  readonly predicate?: RequestPredicate;
  readonly responseOpts?: WhenRequestMatchesOpts;
};

/**
 * Optional response shape for `whenRequestMatches`. The default behavior is
 * unchanged from before this option existed: `status: 200` with
 * `content-type: text/event-stream`. Callers that need to drive the
 * production HTTP error-classification branches in `runInference` (4xx,
 * 5xx, retry-after parsing, context-overflow detection) supply `status`
 * and an `errorBody` chunk through the matching `SimulatedStream`.
 */
export type WhenRequestMatchesOpts = {
  /** HTTP status code for the `Response`. Defaults to 200. */
  readonly status?: number;
  /**
   * Additional response headers. Merged on top of the harness's default
   * `content-type` (which is `text/event-stream` for 200 and
   * `application/json` for non-2xx). Caller-supplied entries win.
   */
  readonly headers?: Readonly<Record<string, string>>;
};

/**
 * Options for `scenario.stall`. `predicate` narrows which fetch the
 * stalled stream routes to (defaults to match-any). `responseOpts`
 * shapes the HTTP envelope of the `Response` the matcher produces.
 */
export type StallOpts = {
  readonly predicate?: RequestPredicate;
  readonly responseOpts?: WhenRequestMatchesOpts;
};

/**
 * Handle returned by `scenario.stall`. Exposes the underlying
 * `SimulatedStream` (in case a test wants to enqueue something into it
 * later, e.g. to release the stall after the assertions run) plus abort
 * telemetry covering the matched fetch's `AbortSignal`.
 *
 * `aborted` flips to `true` as soon as the matched fetch's signal
 * fires — typically because the inference layer's per-call timeout
 * aborted it, but a caller-supplied `signal` aborting also counts.
 * Reading `aborted` before the matcher fires or before any abort
 * returns `false`. `dispose()` does NOT flip `aborted` (it rejects
 * the underlying fetch with an `Error` rather than aborting its
 * `AbortSignal`); it does, however, resolve `awaitAbort` so tests
 * that awaited it past dispose do not hang the test runner.
 *
 * `awaitAbort` resolves the first time the matched fetch's signal
 * fires, or when `dispose()` runs — whichever happens first. Tests
 * use it when they need to sequence assertions after the abort
 * propagated rather than after `harness.run()` returned.
 */
export type StallHandle = {
  readonly stream: SimulatedStream;
  readonly aborted: boolean;
  readonly awaitAbort: Promise<void>;
};

/**
 * The public scenario seam exposed by the harness. `createStream()` mints a
 * `SimulatedStream` registered with the harness for `dispose()` teardown.
 * `whenRequestMatches(predicate, responseStream, opts?)` registers a
 * single-use matcher (see the TSDoc on the function below). The other
 * methods drive tool-handler orchestration and abort scheduling against
 * the harness's virtual clock.
 *
 * `lastToolDispatch(name)` returns the most-recent result the handler
 * registered for `name` produced when the auto-dispatch path of
 * `harness.runInference` (or an explicit `invokeTool` call) fired. Returns
 * `undefined` if the handler has not yet dispatched a result. Tests assert
 * against this when they want to verify the result the harness piped into
 * a tool dispatch without instrumenting the handler closure itself.
 */
export type Scenario = {
  createStream(): SimulatedStream;
  /**
   * Register a single-use matcher. When a waiting fetch's constructed
   * `Request` makes `predicate` return true, the harness routes that fetch
   * to `responseStream` and marks the matcher consumed. Matchers fire at
   * most once; register N matchers if you need to serve N requests.
   *
   * The scan is non-backtracking: `scanWaitingSet` binds each fetch to the
   * first non-consumed matcher whose predicate accepts it, then never
   * reconsiders that pairing. As a consequence, registering a broad
   * matcher (e.g., `() => true`) before a narrow one can produce
   * `AmbiguousRequestError` when two concurrent fetches both accept the
   * broad matcher even though a valid 1-to-1 assignment exists against the
   * full matcher set. Register the most specific predicate first.
   *
   * `opts.status` and `opts.headers` shape the `Response`'s envelope.
   * Omitting `opts` (or passing `{ status: 200 }`) preserves the original
   * behavior: a 200 response with `content-type: text/event-stream`. A
   * non-2xx `status` defaults `content-type` to `application/json` so error
   * bodies — which `runInference` parses as JSON — round-trip correctly;
   * callers may override with `opts.headers`.
   */
  whenRequestMatches(
    predicate: RequestPredicate,
    responseStream: SimulatedStream,
    opts?: WhenRequestMatchesOpts,
  ): void;
  /**
   * Register a handler for the tool named `name`. On the default path,
   * `harness.runInference` observes `inference.tool_call.end` events from
   * the production reactor and auto-dispatches the registered handler with
   * the parsed arguments — tests need only register the handler. The
   * manual escape hatch is `scenario.invokeTool`, for tests that want to
   * drive dispatch by hand (e.g., dispatch-ordering or error-path
   * assertions). At most one handler may be registered per tool name;
   * re-registering throws.
   *
   * See `ToolHandlerReturn` for the three accepted return shapes (sync,
   * delayed envelope, promise) and the in-flight-quiescence rules tied to
   * the promise shape.
   */
  onTool(name: string, handler: ToolHandler): void;
  /**
   * Manual escape hatch for driving tool-handler orchestration directly.
   * Invokes the previously-registered handler and pipes its result into
   * the supplied `dispatch` callback. Most tests should prefer the
   * default flow through `harness.runInference`, which observes
   * `inference.tool_call.end` events emitted by the production reactor
   * and auto-fires the handler with no test-author plumbing. Reach for
   * `invokeTool` only when the test specifically wants to drive dispatch
   * by hand — for example, to assert dispatch ordering relative to clock
   * advances the test controls, or to exercise an error path that the
   * auto-dispatch wrapper would swallow.
   *
   * `dispatch` is called once per resolved tool result, on the same tick
   * (sync return), at a virtual deadline (delayed envelope), or after
   * promise resolution.
   *
   * Throws synchronously if no handler is registered for `name`. Returns
   * synchronously; the harness's `run()` / `advanceTo()` loop is what
   * awaits any in-flight promise produced by the handler.
   */
  invokeTool(name: string, args: unknown, dispatch: DispatchToolResult): void;
  /**
   * Returns the most-recent result produced by the handler registered for
   * `name`, or `undefined` if no dispatch has fired for that tool yet. The
   * harness records the dispatched result every time a handler fires —
   * whether through `harness.runInference`'s auto-dispatch path or an
   * explicit `invokeTool` call. Tests use this to assert on the resolved
   * tool result without instrumenting the handler closure directly.
   */
  lastToolDispatch(name: string): unknown;
  /**
   * Returns a clone of the most-recently matched `Request`, or `undefined`
   * if no fetch has been routed by a matcher yet. The harness records
   * every routed request so tests can `await req.json()` after the fact
   * to assert on the body shape — the matcher predicate itself is
   * synchronous on purpose (it runs on every scan pass) and so cannot
   * read the body, but `lastRequest` is the post-match seam.
   *
   * The returned `Request` is a clone so the test can consume its body
   * without affecting the original (the production reactor will still
   * read the body via the `Response` returned through the stream).
   */
  lastRequest(): Request | undefined;
  /**
   * Convenience wrapper that creates a stream, builds a complete
   * single-turn response for `provider`, enqueues it at the next safe
   * virtual time (`clock.now() + 1`), and registers a single-use
   * match-any matcher routing the next fetch to it. Returns the stream
   * so tests that need to enqueue additional chunks or capture the
   * handle can still do so.
   *
   * The optional `predicate` narrows the match (defaults to `() => true`).
   * The optional `responseOpts` shapes the `Response` envelope (status,
   * headers) just like `whenRequestMatches`. The optional `whenOpts`
   * passes through to `whenRequestMatches`.
   *
   * Use this for the common "drive one round-trip with this reply" test
   * shape. For richer scenarios (multiple turns, custom chunk timing,
   * tool calls), use `createStream` + `whenRequestMatches` directly.
   */
  replyOnce(provider: Provider, opts: ReplyOnceOpts): SimulatedStream;
  /**
   * Convenience helper for timeout/abort tests. Creates a stream,
   * registers a single-use matcher that routes the next fetch to it,
   * and never enqueues anything — the matched stream parks forever.
   *
   * The returned `StallHandle` exposes the underlying stream (in case
   * the test wants to release the stall by enqueueing chunks later)
   * and live abort telemetry: `aborted` flips true when the matched
   * fetch's `AbortSignal` fires, and `awaitAbort` resolves at the
   * same moment.
   *
   * Quiescence interaction: the harness has no special-case knowledge
   * of stalls. The matched fetch's `Response` is delivered as soon as
   * the matcher fires (the WaitingFetch is settled at that moment);
   * what parks indefinitely is the response body's SSE iterator, not
   * the fetch itself. `checkQuiescence` therefore does not see an
   * unmatched fetch and does not raise `UnmatchedFetchError`. Tests
   * that exercise per-call timeout behaviour should pair `stall` with
   * short `inactivityTimeoutMs` or `totalTimeoutMs` values and
   * `setupHarness({ enableInferenceTimers: true })` so the inference
   * layer's timers fire at virtual time and surface a `"timeout"`
   * `inference.error` before the test's `harness.run()` returns.
   */
  stall(opts?: StallOpts): StallHandle;
  /**
   * Schedules `controller.abort()` to fire at the supplied virtual time.
   * The caller supplies the `AbortController` they want aborted —
   * typically the same one whose `signal` was threaded into a fetch via
   * `inferenceOptions.signal`. The harness does NOT create or own the
   * controller; this keeps the public API explicit (option 1 from the
   * spec) and avoids hidden state shared across scenarios.
   *
   * Throws if `virtualMs` is in the past relative to `clock.now()`; the
   * harness rejects past-time scheduling at the clock level and this
   * surfaces the same error eagerly.
   */
  abortAt(virtualMs: number, controller: AbortController): void;
  /**
   * Registers a reactive abort: whenever a simulated stream owned by
   * this harness delivers a chunk for which `predicate` returns true,
   * the harness calls `controller.abort()` synchronously in the same
   * tick. The matcher fires at most once; subsequent chunks that would
   * match are ignored. v1 observes WIRE EVENTS (chunks served by
   * `SimulatedStream.enqueue`/`enqueueAt`) rather than reactor-side
   * `InferenceEvent`s — the harness sits below the reactor and does not
   * see those today.
   */
  abortAfter(predicate: WireEventPredicate, controller: AbortController): void;
};

/**
 * A registered matcher entry. The table is an ordered list; the first
 * non-consumed entry whose predicate returns true on a given waiting
 * request wins. Each matcher fires at most once — once `consumed` is true
 * it is skipped on every subsequent scan. This rule isn't spelled out
 * explicitly in the locked spec, but it's the only interpretation
 * consistent with "registration-order, first-match-wins" + per-fetch
 * controllers + the `AmbiguousRequestError` case. If a test wants to match
 * the same request twice, it registers two matchers.
 */
export type Matcher = {
  readonly predicate: RequestPredicate;
  readonly responseStream: SimulatedStream;
  /** First two non-anonymous frames of the registration site, if available. */
  readonly source: string | undefined;
  readonly opts: WhenRequestMatchesOpts | undefined;
  consumed: boolean;
};

/**
 * Internal shape of a fetch parked in the waiting set. The `request` field
 * is constructed once when the fetch enters the waiting set and reused
 * across every predicate evaluation, so that any per-`Request` side effect
 * (consuming a body, header normalization) is paid exactly once.
 */
export type WaitingFetch = {
  readonly request: Request;
  readonly signal: AbortSignal | undefined;
  readonly resolve: (response: Response) => void;
  readonly reject: (err: unknown) => void;
  /** Set true once routed or aborted; prevents duplicate settlement. */
  settled: boolean;
};

/**
 * @internal
 *
 * Ordered, append-only registry of `Matcher` entries owned by a single
 * harness. `entries` is exposed so the harness can clear it on `dispose()`
 * without piercing private state; production code should treat the table
 * as opaque and interact only via `register`.
 */
export type MatcherTable = {
  readonly entries: Matcher[];
  register(
    predicate: RequestPredicate,
    responseStream: SimulatedStream,
    source: string | undefined,
    opts: WhenRequestMatchesOpts | undefined,
  ): void;
};

/**
 * @internal
 *
 * Construct an empty `MatcherTable`. Used by `setupHarness`; no external
 * consumer should call this directly.
 */
export function createMatcherTable(): MatcherTable {
  const entries: Matcher[] = [];
  return {
    entries,
    register(
      predicate: RequestPredicate,
      responseStream: SimulatedStream,
      source: string | undefined,
      opts: WhenRequestMatchesOpts | undefined,
    ): void {
      entries.push({
        predicate,
        responseStream,
        source,
        opts,
        consumed: false,
      });
    },
  };
}

/**
 * Walks every parked fetch in arrival order and tries to bind it to the
 * first non-consumed matcher whose predicate accepts its `Request`. The
 * function is idempotent in the sense that it processes the waiting set
 * once per call; the caller is expected to invoke it at each of the three
 * scan triggers (new fetch, new matcher, quiescence approach).
 *
 * If two or more waiting fetches all bind to the same single matcher on
 * one pass, this function throws `AmbiguousRequestError` — a deliberate
 * deviation from "silently route only the first", because routing the
 * first while leaving the others stranded would surface later as a
 * confusing `UnmatchedFetchError`.
 *
 * The `route` callback fulfills a single waiting fetch: it resolves the
 * fetch promise with a `Response` constructed from the matcher's stream
 * and removes the entry from the waiting set. `route` must NOT itself
 * call `scanWaitingSet` — the caller is responsible for re-invoking the
 * scan at the next trigger point.
 */
export function scanWaitingSet(
  waiting: WaitingFetch[],
  table: MatcherTable,
  route: (
    fetch: WaitingFetch,
    stream: SimulatedStream,
    opts: WhenRequestMatchesOpts | undefined,
  ) => void,
): void {
  // Snapshot: matcher -> first waiting fetch that bound to it. If a second
  // fetch tries to bind to the same matcher in this pass, that's ambiguous.
  // We iterate fetches in arrival order to honor first-match-wins on the
  // matcher side AND first-arrival on the fetch side.
  type Binding = { fetch: WaitingFetch; matcher: Matcher };
  const bindings: Binding[] = [];
  // Track which matchers have already been bound during this pass so a
  // second waiting fetch in this pass doesn't re-bind to the same matcher.
  const boundThisPass = new Set<Matcher>();
  // Track conflicts: matcher -> list of fetches that all matched it first.
  const conflicts = new Map<Matcher, WaitingFetch[]>();

  for (const wf of waiting) {
    if (wf.settled) continue;
    let chosen: Matcher | null = null;
    for (const m of table.entries) {
      if (m.consumed) continue;
      if (boundThisPass.has(m)) continue;
      if (m.predicate(wf.request)) {
        chosen = m;
        break;
      }
    }
    if (chosen === null) {
      // Look for an ambiguity: did the first non-consumed matcher (ignoring
      // boundThisPass) also accept this fetch? That's a same-matcher
      // conflict with a prior binding in this pass.
      for (const m of table.entries) {
        if (m.consumed) continue;
        if (!boundThisPass.has(m)) continue;
        if (m.predicate(wf.request)) {
          const list = conflicts.get(m) ?? [];
          if (list.length === 0) {
            // The prior binding fetch is the conflict's first member.
            const priorBinding = bindings.find((b) => b.matcher === m);
            if (priorBinding !== undefined) {
              list.push(priorBinding.fetch);
            }
          }
          list.push(wf);
          conflicts.set(m, list);
          break;
        }
      }
      continue;
    }
    bindings.push({ fetch: wf, matcher: chosen });
    boundThisPass.add(chosen);
  }

  if (conflicts.size > 0) {
    const firstEntry = conflicts.entries().next();
    if (firstEntry.done === true) {
      throw new Error(
        "scanWaitingSet: conflicts.size > 0 but iterator empty (internal bug)",
      );
    }
    const [matcher, fetches] = firstEntry.value;
    const info: AmbiguousFetchInfo[] = fetches.map((f) => ({
      url: f.request.url,
      method: f.request.method,
    }));
    const err = new AmbiguousRequestError(info, matcher.source);
    // Reject every conflicting fetch so awaiters don't hang. Mark `settled`
    // first so a later scan trigger doesn't try to route them.
    for (const wf of fetches) {
      if (wf.settled) continue;
      wf.settled = true;
      wf.reject(err);
    }
    throw err;
  }

  for (const { fetch, matcher } of bindings) {
    if (fetch.settled) continue;
    matcher.consumed = true;
    route(fetch, matcher.responseStream, matcher.opts);
  }
}

/**
 * Best-effort extraction of the call site that registered a matcher, used
 * to enrich `AmbiguousRequestError` messages. We grab one frame above the
 * `whenRequestMatches` call in the captured stack; if the runtime doesn't
 * expose stacks in the expected format, we return `undefined` rather than
 * fabricating data.
 */
export function captureMatcherSource(skipFrames: number): string | undefined {
  const stack = new Error().stack;
  if (stack === undefined) return undefined;
  const lines = stack.split("\n");
  // First line is the Error message ("Error"); subsequent lines are frames.
  // `skipFrames` counts frames inside this package that should be hidden.
  const target = lines[1 + skipFrames];
  if (target === undefined) return undefined;
  return target.trim();
}
