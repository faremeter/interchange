import { AmbiguousRequestError, type AmbiguousFetchInfo } from "./errors";
import type { ChunkFiredEvent, SimulatedStream } from "./simulated-stream";
import type { DispatchToolResult, ToolHandler } from "./tool-handler";
import type { Provider } from "./wire/agnostic";

/**
 * Predicate on wire-event chunks delivered by simulated streams. Used by
 * `scenario.abortAfter`; the harness sees wire events, not reactor-side
 * `InferenceEvent`s.
 */
export type WireEventPredicate = (event: ChunkFiredEvent) => boolean;

/**
 * The request type handed to predicates and returned by
 * `matchedRequests()`. Mirrors Bun's global `Request` plus a
 * self-returning `clone()` (Bun does not override `clone()`, so plain
 * `Request.clone()` yields undici's `Request`).
 *
 * Extends `Bun.__internal.BunRequestOverride` rather than naming `Request`
 * because undici's global augmentation can win `Request` resolution in
 * this package; naming the override reproduces Bun's platform type
 * regardless of which augmentation wins. Depends on bun-types internals;
 * a restructuring upgrade breaks this line.
 */
export interface HarnessRequest extends Bun.__internal.BunRequestOverride {
  clone(): HarnessRequest;
}

/**
 * Predicate run against a constructed `Request` to decide whether a matcher
 * applies. Sync-only on purpose: predicates run on every scan pass and
 * must be referentially transparent. Reading mutable harness state from a
 * predicate is a bug; the type system cannot enforce purity.
 */
export type RequestPredicate = (req: HarnessRequest) => boolean;

/**
 * Predicate variant for `scenario.whenRequestBodyMatches`. Receives the
 * buffered request body as UTF-8 plus the original `Request`. The body is
 * buffered once per fetch and shared across body-aware predicates.
 *
 * Same purity contract as `RequestPredicate`: sync, idempotent,
 * side-effect-free, independent of mutable harness state.
 */
export type BodyAwareRequestPredicate = (
  bodyText: string,
  req: HarnessRequest,
) => boolean;

/**
 * Tool-call entry accepted by `ReplyOnceOpts.toolCalls`. Two shapes:
 *
 * - `{ callId, name, argsJSON }` — explicit; use when asserting an exact
 *   `callId` or hand-crafting the arguments string.
 * - `{ name, args, callId? }` — friendly; `args` is stringified for you,
 *   `callId` auto-generated if omitted.
 *
 * Both shapes may be mixed in one array. Auto-generated callIds use the
 * reserved `call_auto_` prefix; pinned callIds must avoid it.
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
 * Options for `scenario.replyOnce`. `text`/`toolCalls` are the response
 * payload; `headUsage`/`tailUsage` optional usage frames; `predicate`
 * narrows the matched fetch; `responseOpts` shapes the `Response`
 * envelope.
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
 * Optional response shape for `whenRequestMatches`. Defaults: `status:
 * 200`, `content-type: text/event-stream`. Callers driving the HTTP
 * error-classification branches of `runInference` (4xx, 5xx, retry-after,
 * context-overflow) supply `status` and an `errorBody` chunk through the
 * stream.
 */
export type WhenRequestMatchesOpts = {
  /** HTTP status code for the `Response`. Defaults to 200. */
  readonly status?: number;
  /**
   * Additional response headers, merged over the harness defaults
   * (`text/event-stream` for 200, `application/json` for non-2xx).
   * Caller-supplied entries win.
   */
  readonly headers?: Readonly<Record<string, string>>;
};

/**
 * Options for `scenario.stall`. `predicate` narrows the matched fetch;
 * `responseOpts` shapes the `Response` envelope.
 */
export type StallOpts = {
  readonly predicate?: RequestPredicate;
  readonly responseOpts?: WhenRequestMatchesOpts;
};

/**
 * Handle returned by `scenario.stall`. Exposes the underlying
 * `SimulatedStream` (e.g. to release the stall later) and abort
 * telemetry for the matched fetch's `AbortSignal`.
 *
 * `aborted` flips true when the matched fetch's signal fires; `dispose()`
 * does NOT flip it (dispose rejects the fetch with an `Error`, it does
 * not abort the signal), but does resolve `awaitAbort` so tests awaiting
 * it past dispose do not hang.
 *
 * `awaitAbort` resolves on the first signal fire or on `dispose()`,
 * whichever comes first.
 */
export type StallHandle = {
  readonly stream: SimulatedStream;
  readonly aborted: boolean;
  readonly awaitAbort: Promise<void>;
};

/**
 * The public scenario seam exposed by the harness. `createStream()` mints
 * a `SimulatedStream` tracked for `dispose()` teardown; the other methods
 * drive matchers, tool handlers, and abort scheduling against the virtual
 * clock.
 */
export type Scenario = {
  createStream(): SimulatedStream;
  /**
   * Register a single-use matcher routing the next fetch whose `Request`
   * satisfies `predicate` to `responseStream`. Fires at most once;
   * register N matchers to serve N requests.
   *
   * The scan is non-backtracking: each fetch binds to the first
   * non-consumed accepting matcher and that pairing is never
   * reconsidered. A broad matcher registered before a narrow one can
   * therefore raise `AmbiguousRequestError` when two concurrent fetches
   * both accept the broad matcher. Register the most specific predicate
   * first.
   *
   * `opts` shapes the `Response` envelope: default `status: 200` with
   * `content-type: text/event-stream`; non-2xx defaults `content-type`
   * to `application/json` so error bodies parse.
   */
  whenRequestMatches(
    predicate: RequestPredicate,
    responseStream: SimulatedStream,
    opts?: WhenRequestMatchesOpts,
  ): void;
  /**
   * Single-use matcher whose predicate sees the request body as a UTF-8
   * string. Use when only the body distinguishes parallel fetches (e.g. a
   * task id in a multi-agent dispatch run).
   *
   * The body is buffered once per fetch and shared across body-aware
   * predicates. Sync `whenRequestMatches` predicates run first; if none
   * bind, the harness buffers every still-waiting fetch's body and runs a
   * body-aware scan pass. Ambiguity is detected over the fully-buffered
   * set, matching the sync scan's single-pass model.
   *
   * `opts` shapes the `Response` envelope as in `whenRequestMatches`.
   */
  whenRequestBodyMatches(
    predicate: BodyAwareRequestPredicate,
    responseStream: SimulatedStream,
    opts?: WhenRequestMatchesOpts,
  ): void;
  /**
   * Register a handler for the tool named `name`. The default path
   * auto-dispatches it from `inference.tool_call.end` events; use
   * `invokeTool` to drive dispatch by hand. At most one handler per tool
   * name; re-registering throws. See `ToolHandlerReturn` for the three
   * accepted return shapes.
   */
  onTool(name: string, handler: ToolHandler): void;
  /**
   * Manual escape hatch: invoke the registered handler and pipe its result
   * into `dispatch`. Prefer the default auto-dispatch path through
   * `harness.runInference`; use this for dispatch-ordering or error-path
   * assertions. `dispatch` fires once per resolved result — same tick for
   * sync returns, at a virtual deadline for delayed envelopes, after
   * resolution for promises. Throws if no handler is registered for
   * `name`.
   */
  invokeTool(name: string, args: unknown, dispatch: DispatchToolResult): void;
  /**
   * Most-recent result the handler for `name` dispatched (via
   * auto-dispatch or `invokeTool`), or `undefined` if none has fired.
   */
  lastToolDispatch(name: string): unknown;
  /**
   * All matched `Request`s in match order, each a fresh clone whose body
   * can be consumed freely. Each call re-clones, so consuming one returned
   * body does not affect siblings or later calls. Use
   * `matchedRequests().at(-1)` for only the most recent.
   */
  matchedRequests(): HarnessRequest[];
  /**
   * Convenience wrapper: creates a stream, builds a complete single-turn
   * response for `provider`, enqueues it at `clock.now() + 1`, and
   * registers a match-any single-use matcher. Returns the stream so
   * callers can enqueue more chunks. `opts.predicate` narrows the match,
   * `opts.responseOpts` shapes the envelope. For richer scenarios use
   * `createStream` + `whenRequestMatches`.
   */
  replyOnce(provider: Provider, opts: ReplyOnceOpts): SimulatedStream;
  /**
   * Convenience helper for timeout/abort tests: routes the next fetch to
   * a stream that never delivers bytes. See `StallHandle` for abort
   * telemetry. The fetch itself settles on match — only the body's SSE
   * iterator parks — so quiescence does not raise `UnmatchedFetchError`.
   * Pair with short timeout thresholds and `setupHarness({ enableInferenceTimers: true })`
   * so the inference layer's timers fire at virtual time.
   */
  stall(opts?: StallOpts): StallHandle;
  /**
   * Schedules `controller.abort()` at virtual time `virtualMs`. The
   * caller owns the controller. Throws if `virtualMs` is in the past.
   */
  abortAt(virtualMs: number, controller: AbortController): void;
  /**
   * Registers a reactive abort: when a chunk delivered by a harness
   * stream satisfies `predicate`, calls `controller.abort()` in the same
   * tick. Fires at most once. Observes wire events, not reactor-side
   * `InferenceEvent`s.
   */
  abortAfter(predicate: WireEventPredicate, controller: AbortController): void;
};

/**
 * A registered matcher entry. The table is an ordered list; the first
 * non-consumed entry whose predicate accepts a waiting request wins, at
 * most once per entry — register two matchers to match a request twice.
 *
 * `bodyAware` separates sync predicates (`whenRequestMatches`) from
 * body-aware ones (`whenRequestBodyMatches`), which evaluate in a
 * follow-up scan pass after every waiting fetch's body is buffered.
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
 * A fetch parked in the waiting set. `request` is built once and reused
 * across predicate evaluations. `bodyText` is buffered lazily by the first
 * body-aware scan pass and never overwritten.
 */
export type WaitingFetch = {
  readonly request: HarnessRequest;
  readonly signal: AbortSignal | undefined;
  readonly resolve: (response: Response) => void;
  readonly reject: (err: unknown) => void;
  /** True once routed or aborted; prevents duplicate settlement. */
  settled: boolean;
  /** Buffered request body; `undefined` until the body-aware scan buffers it. */
  bodyText: string | undefined;
};

/**
 * @internal
 *
 * Ordered, append-only registry of `Matcher` entries for one harness.
 * `entries` is exposed so `dispose()` can clear it; treat the rest as
 * opaque.
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
   * True once any body-aware matcher has ever been registered (consumed
   * or not); used to decide whether arriving fetches need body buffering.
   */
  hasBodyAware(): boolean;
};

/**
 * @internal
 *
 * Construct an empty `MatcherTable`. Used by `setupHarness` only.
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
 * Bind each parked fetch, in arrival order, to the first non-consumed
 * matcher whose predicate accepts its `Request`. Processes the waiting set
 * once per call; callers invoke it at each scan trigger (new fetch, new
 * matcher, quiescence).
 *
 * If two or more fetches bind to the same single matcher on one pass,
 * throws `AmbiguousRequestError` — routing only the first would strand
 * the others and surface later as a confusing `UnmatchedFetchError`.
 *
 * `route` settles one fetch with a `Response` from the matcher's stream;
 * it must not itself call `scanWaitingSet`.
 *
 * `includeBodyAware` gates body-aware matchers: false skips them for the
 * sync pass; true evaluates them over the fetch's pre-buffered `bodyText`
 * (`undefined` there is an internal bug).
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
      // Unbuffered fetches are expected here (a concurrent body-aware
      // scan may be mid-buffer; a later scan routes them once buffered),
      // so treat them as non-matches for this pass.
      if (wf.bodyText === undefined) return false;
      return m.predicate(wf.bodyText, wf.request);
    }
    return m.predicate(wf.request);
  };

  // Snapshot the first waiting fetch bound to each matcher this pass; a
  // second fetch binding the same matcher is a conflict. Iterate in
  // arrival order to honor first-match-wins and first-arrival.
  type Binding = { fetch: WaitingFetch; matcher: Matcher };
  const bindings: Binding[] = [];
  // Matchers already bound during this pass.
  const boundThisPass = new Set<Matcher>();
  // Conflicting fetches per matcher.
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
      // Did a matcher already bound this pass also accept this fetch?
      for (const m of table.entries) {
        if (m.consumed) continue;
        if (!boundThisPass.has(m)) continue;
        if (evaluate(m, wf)) {
          const list = conflicts.get(m) ?? [];
          if (list.length === 0) {
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
    // Reject every conflicting fetch so awaiters don't hang.
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
 * Best-effort extraction of the matcher registration call site, used to
 * enrich `AmbiguousRequestError` messages. Returns `undefined` if the
 * stack is not available in the expected format.
 */
export function captureMatcherSource(skipFrames: number): string | undefined {
  const stack = new Error().stack;
  if (stack === undefined) return undefined;
  const lines = stack.split("\n");
  // Line 0 is the Error message; `skipFrames` hides frames inside this
  // package.
  const target = lines[1 + skipFrames];
  if (target === undefined) return undefined;
  return target.trim();
}
