import { AmbiguousRequestError, type AmbiguousFetchInfo } from "./errors";
import type { SimulatedStream } from "./simulated-stream";
import type { DispatchToolResult, ToolHandler } from "./tool-handler";

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
 * The public scenario seam exposed by the harness. `createStream()` mints a
 * `SimulatedStream` registered with the harness for `dispose()` teardown.
 * `whenRequestMatches(predicate, responseStream, opts?)` registers a
 * single-use matcher (see the TSDoc on the function below). The other
 * methods drive tool-handler orchestration and abort scheduling against
 * the harness's virtual clock.
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
   * Register a handler for the tool named `name`. The handler runs when the
   * harness observes a tool call by that name (in v1 this is wired via the
   * test-author-invoked `invokeTool` helper below; future slices will
   * autodetect tool-call frames in served wire bytes). At most one handler
   * may be registered per tool name; re-registering throws.
   *
   * See `ToolHandlerReturn` for the three accepted return shapes (sync,
   * delayed envelope, promise) and the in-flight-quiescence rules tied to
   * the promise shape.
   */
  onTool(name: string, handler: ToolHandler): void;
  /**
   * Invoke a previously-registered tool handler and pipe its result into
   * the supplied `dispatch` callback. Exposed as a v1 hook so test authors
   * can drive the tool-handler orchestration directly until 6c lands the
   * automatic wire-byte autodetect. `dispatch` is called once per resolved
   * tool result, on the same tick (sync return), at a virtual deadline
   * (delayed envelope), or after promise resolution.
   *
   * Throws synchronously if no handler is registered for `name`. Returns
   * synchronously; the harness's `run()` / `advanceTo()` loop is what
   * awaits any in-flight promise produced by the handler.
   */
  invokeTool(name: string, args: unknown, dispatch: DispatchToolResult): void;
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
