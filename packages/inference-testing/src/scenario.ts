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
 * The request type the harness hands to predicates and returns from
 * `matchedRequests()`. It mirrors Bun's global `Request` and adds a
 * self-returning `clone()` (Bun does not override `clone()`, so a plain
 * `Request.clone()` yields undici's `Request`).
 *
 * Why extend `Bun.__internal.BunRequestOverride` instead of naming
 * `Request`: in this package's compilation undici's `@types/node` global
 * augmentation wins global `Request` resolution, so writing `Request` here
 * would pin the harness's public surface to undici's type — the collision
 * that leaks into Bun-typed consumers. Bun's global `Request` itself
 * extends this same override interface, so naming it directly reproduces
 * the platform type regardless of which global augmentation wins in a
 * given file. This depends on `bun-types`' internal namespace; a bun-types
 * upgrade that restructures it breaks this line at build time.
 */
export interface HarnessRequest extends Bun.__internal.BunRequestOverride {
  clone(): HarnessRequest;
}

/**
 * Predicate run against a constructed `Request` to decide whether a matcher
 * applies. The signature is sync-only on purpose: predicates run on every
 * scan pass and must be referentially transparent. Reading mutable state
 * (e.g., `harness.clock.now()`) from a predicate is a documentation-level
 * bug — the type system enforces that predicates cannot await, but it
 * cannot enforce purity.
 */
export type RequestPredicate = (req: HarnessRequest) => boolean;

/**
 * Predicate variant for `scenario.whenRequestBodyMatches`. Receives the
 * buffered request body as a UTF-8 string plus the original `Request` (for
 * URL/header inspection). The harness buffers the body once per fetch the
 * first time a body-aware matcher could fire; the same buffered text is
 * passed to every body-aware predicate evaluated against that fetch.
 *
 * The same purity contract that governs `RequestPredicate` applies here:
 * sync, idempotent, side-effect-free, and independent of mutable harness
 * state. Reading `bodyText`, `req.url`, `req.method`, and `req.headers` is
 * fine; everything else is a bug class.
 */
export type BodyAwareRequestPredicate = (
  bodyText: string,
  req: HarnessRequest,
) => boolean;

/**
 * Tool-call entry accepted by `ReplyOnceOpts.toolCalls`. Two shapes are
 * permitted:
 *
 * - `{ callId, name, argsJSON }` — the original explicit shape. Use when
 *   the test asserts against an exact `callId` or hand-crafts the
 *   arguments string (e.g., to exercise malformed-JSON paths).
 * - `{ name, args, callId? }` — the friendlier shape. `args` is
 *   `JSON.stringify`'d for you; `callId` is auto-generated if omitted.
 *   Use for the common case where the test only cares about the tool
 *   name and structured arguments.
 *
 * Both shapes may be mixed within the same array.
 *
 * Auto-generated callIds use the prefix `call_auto_` followed by a
 * monotonically-increasing per-harness counter. Tests that pin explicit
 * callIds via the explicit shape should avoid the `call_auto_` prefix
 * to keep their pinned ids distinguishable from auto-generated ones.
 */
export type ReplyOnceToolCall =
  | {
      readonly callId: string;
      readonly name: string;
      readonly argsJSON: string;
    }
  | {
      readonly name: string;
      readonly args: unknown;
      readonly callId?: string;
    };

/**
 * Options for `scenario.replyOnce`. `text` and `toolCalls` are the response
 * payload (passed through to `wire.completeResponse`); `headUsage` and
 * `tailUsage` are optional usage frames. `predicate` narrows which fetch
 * the matcher routes to (defaults to match-any). `responseOpts` shapes
 * the HTTP envelope of the `Response` the matcher produces.
 */
export type ReplyOnceOpts = {
  readonly text?: string;
  readonly toolCalls?: readonly ReplyOnceToolCall[];
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
   * Register a single-use matcher whose predicate sees the request body as
   * a UTF-8 string. Useful when the only field that distinguishes parallel
   * fetches lives in the body (e.g., a task id in the seed message of a
   * multi-agent dispatch run).
   *
   * The body is buffered once per fetch the first time a body-aware
   * matcher could fire against it; subsequent body-aware predicates see
   * the same cached text. URL/header-only predicates registered via
   * `whenRequestMatches` never trigger a body read.
   *
   * Scan ordering: sync `whenRequestMatches` predicates run first. If
   * none bind a fetch and at least one body-aware matcher is registered,
   * the harness buffers the bodies of every still-waiting fetch and then
   * runs a body-aware scan pass. Body-aware matches resolve only after
   * ALL in-flight body buffers complete — `AmbiguousRequestError` for
   * body-aware matchers is detected over a fully-buffered set, not as
   * bodies become available, so the conflict semantics match the sync
   * scan's "single pass over all waiting fetches" model.
   *
   * `opts.status` and `opts.headers` shape the `Response`'s envelope, with
   * the same defaults as `whenRequestMatches`.
   */
  whenRequestBodyMatches(
    predicate: BodyAwareRequestPredicate,
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
   * Returns the full list of matched `Request`s in match order, each as a
   * fresh clone whose body the caller can consume freely. Empty until at
   * least one fetch has been routed by a matcher. Tests use this to assert
   * against the body shape of every request the harness routed during the
   * scenario (e.g., per-agent body assertions across a multi-agent dispatch
   * run) rather than only the most-recent one.
   *
   * Each call returns fresh clones, so consuming one returned Request's
   * body does not affect any other returned Request — neither the
   * siblings returned by the same call nor the Requests returned by
   * later calls. The harness's internal capture list itself is never
   * consumed: every `.json()` / `.text()` invocation runs against a
   * one-shot clone minted at the time `matchedRequests()` was called.
   *
   * To read only the most-recent request, use `matchedRequests().at(-1)`.
   */
  matchedRequests(): HarnessRequest[];
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
 *
 * `bodyAware` discriminates between sync-only predicates (registered via
 * `whenRequestMatches`) and body-aware predicates (registered via
 * `whenRequestBodyMatches`). Sync predicates evaluate in the sync scan
 * pass; body-aware predicates evaluate in a follow-up async scan pass
 * that runs only after every still-waiting fetch's body has been
 * buffered.
 */
export type Matcher =
  | {
      readonly bodyAware: false;
      readonly predicate: RequestPredicate;
      readonly responseStream: SimulatedStream;
      /** First two non-anonymous frames of the registration site, if available. */
      readonly source: string | undefined;
      readonly opts: WhenRequestMatchesOpts | undefined;
      consumed: boolean;
    }
  | {
      readonly bodyAware: true;
      readonly predicate: BodyAwareRequestPredicate;
      readonly responseStream: SimulatedStream;
      readonly source: string | undefined;
      readonly opts: WhenRequestMatchesOpts | undefined;
      consumed: boolean;
    };

/**
 * Internal shape of a fetch parked in the waiting set. The `request` field
 * is constructed once when the fetch enters the waiting set and reused
 * across every predicate evaluation, so that any per-`Request` side effect
 * (header normalization) is paid exactly once.
 *
 * `bodyText` is populated lazily — the first body-aware scan pass triggered
 * against this fetch reads `request.clone().text()` once and caches the
 * result here. Sync-only fetches never trigger a body read; the field
 * stays `undefined` for those.
 */
export type WaitingFetch = {
  readonly request: HarnessRequest;
  readonly signal: AbortSignal | undefined;
  readonly resolve: (response: Response) => void;
  readonly reject: (err: unknown) => void;
  /** Set true once routed or aborted; prevents duplicate settlement. */
  settled: boolean;
  /**
   * Buffered request body, populated lazily by `bufferUnreadBodies`
   * before the body-aware scan pass. `undefined` means "not yet
   * buffered". The harness never overwrites a populated entry; each
   * fetch buffers at most once across its lifetime.
   */
  bodyText: string | undefined;
};

/**
 * @internal
 *
 * Ordered, append-only registry of `Matcher` entries owned by a single
 * harness. `entries` is exposed so the harness can clear it on `dispose()`
 * without piercing private state; production code should treat the table
 * as opaque and interact only via `register` / `registerBodyAware`.
 */
export type MatcherTable = {
  readonly entries: Matcher[];
  register(
    predicate: RequestPredicate,
    responseStream: SimulatedStream,
    source: string | undefined,
    opts: WhenRequestMatchesOpts | undefined,
  ): void;
  registerBodyAware(
    predicate: BodyAwareRequestPredicate,
    responseStream: SimulatedStream,
    source: string | undefined,
    opts: WhenRequestMatchesOpts | undefined,
  ): void;
  /**
   * Returns true when at least one body-aware matcher has ever been
   * registered against this table (consumed or not). Used by the harness
   * to decide whether arriving fetches need their bodies buffered for the
   * body-aware scan pass. Once a body-aware matcher has been registered,
   * the flag stays true for the lifetime of the table — even after the
   * matcher consumes — because the registration tells us tests care
   * about body routing for the run as a whole.
   */
  hasBodyAware(): boolean;
};

/**
 * @internal
 *
 * Construct an empty `MatcherTable`. Used by `setupHarness`; no external
 * consumer should call this directly.
 */
export function createMatcherTable(): MatcherTable {
  const entries: Matcher[] = [];
  let bodyAwareEverRegistered = false;
  return {
    entries,
    register(
      predicate: RequestPredicate,
      responseStream: SimulatedStream,
      source: string | undefined,
      opts: WhenRequestMatchesOpts | undefined,
    ): void {
      entries.push({
        bodyAware: false,
        predicate,
        responseStream,
        source,
        opts,
        consumed: false,
      });
    },
    registerBodyAware(
      predicate: BodyAwareRequestPredicate,
      responseStream: SimulatedStream,
      source: string | undefined,
      opts: WhenRequestMatchesOpts | undefined,
    ): void {
      bodyAwareEverRegistered = true;
      entries.push({
        bodyAware: true,
        predicate,
        responseStream,
        source,
        opts,
        consumed: false,
      });
    },
    hasBodyAware(): boolean {
      return bodyAwareEverRegistered;
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
 *
 * `includeBodyAware` controls which matchers are considered. When false
 * (the sync scan pass), body-aware matchers are skipped entirely — they
 * remain available for a later async pass once bodies are buffered. When
 * true, body-aware matchers are evaluated using the fetch's pre-buffered
 * `bodyText`; a body-aware matcher seen with `bodyText === undefined` is
 * an internal bug (the harness must buffer before calling with
 * `includeBodyAware: true`).
 */
export function scanWaitingSet(
  waiting: WaitingFetch[],
  table: MatcherTable,
  route: (
    fetch: WaitingFetch,
    stream: SimulatedStream,
    opts: WhenRequestMatchesOpts | undefined,
  ) => void,
  includeBodyAware = false,
): void {
  const evaluate = (m: Matcher, wf: WaitingFetch): boolean => {
    if (m.bodyAware) {
      if (!includeBodyAware) return false;
      // Body-aware matchers only fire against fetches whose bodies have
      // already been buffered. A body-aware scan that meets an
      // unbuffered fetch simply treats it as a non-match for this pass;
      // the harness will run another scan after buffering completes.
      //
      // The unbuffered case is expected, not an invariant violation:
      // two body-aware scans can be in flight at the same time, e.g.
      // one queued from a matcher registration and one queued from a
      // fetch arrival. The first scan snapshots `waiting` and starts
      // its `clone().text()` reads; while it is awaiting, a new fetch
      // arrives and pushes onto `waiting`. When the first scan resumes
      // and calls `scanWaitingSet`, it sees the new fetch with
      // `bodyText === undefined`. Returning false here lets the second
      // scan (which buffers the new fetch as part of its own
      // `bufferUnreadBodies` pass) route it on a later pass.
      if (wf.bodyText === undefined) return false;
      return m.predicate(wf.bodyText, wf.request);
    }
    return m.predicate(wf.request);
  };

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
      if (evaluate(m, wf)) {
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
        if (evaluate(m, wf)) {
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
