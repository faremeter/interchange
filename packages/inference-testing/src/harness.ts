import {
  HarnessId,
  runInference,
  type Dependencies,
  type InferenceHarnessOptions,
} from "@intx/inference";
import type { InferenceEvent } from "@intx/types/runtime";

import {
  ClockWallClockOverrunError,
  createClock,
  type AdvanceOpts,
  type Clock,
  type RunOpts,
} from "./clock";
import {
  UnmatchedFetchError,
  WrongHarnessError,
  type UnmatchedFetchInfo,
} from "./errors";
import {
  captureMatcherSource,
  createMatcherTable,
  scanWaitingSet,
  type ReplyOnceOpts,
  type RequestPredicate,
  type Scenario,
  type WaitingFetch,
  type WhenRequestMatchesOpts,
  type WireEventPredicate,
} from "./scenario";
import { completeResponse, type Provider } from "./wire/agnostic";
import {
  createSimulatedStream,
  toStreamId,
  type ChunkFiredEvent,
  type SimulatedStream,
  type SimulatedStreamHandle,
  type StreamId,
} from "./simulated-stream";
import {
  createToolHandlerRegistry,
  type DispatchToolResult,
  type ToolHandler,
} from "./tool-handler";

/**
 * The deterministic inference test harness returned by `setupHarness`. It
 * bundles the virtual `clock` driving all scheduling, the `deps` to inject
 * into the system under test (`fetch` is stubbed; `HarnessId` is branded so
 * `assertDeps` catches cross-harness contamination), and the `scenario`
 * seam tests use to mint streams, register matchers and tool handlers, and
 * schedule abort behavior. The harness owns disposal of every simulated
 * stream it minted; tests must call `dispose()` (typically in `afterEach`)
 * to suppress bun:test "unclosed ReadableStream" warnings.
 */
export type Harness = {
  readonly clock: Clock;
  readonly deps: Dependencies;
  readonly scenario: Scenario;
  /**
   * Asserts that `candidate` was produced by this harness. Use at test
   * boundaries that take a `Dependencies` from an external source to catch
   * cross-harness contamination (e.g., a test wiring harness A's deps
   * through harness B's reactor).
   *
   * Throws `WrongHarnessError` if `candidate[HarnessId]` does not match this
   * harness's symbol.
   */
  assertDeps(candidate: Dependencies): void;
  /**
   * Delegates to `clock.run()` and then verifies the waiting-fetch set is
   * empty. If any fetch is still parked on a matcher at quiescence, throws
   * `UnmatchedFetchError`. This is the third of the three scan triggers
   * documented in the locked spec.
   */
  run(opts?: RunOpts): Promise<void>;
  /**
   * Delegates to `clock.advanceTo()` and then verifies the waiting-fetch
   * set is empty, throwing `UnmatchedFetchError` if not. Mirrors `run()`'s
   * quiescence check at a bounded virtual deadline.
   */
  advanceTo(virtualMs: number, opts?: AdvanceOpts): Promise<void>;
  /**
   * Cancels every pending (not-yet-fired) scheduled callback for the
   * stream identified by `streamId` and errors that stream's controller
   * with an `AbortError` at `clock.now()`. Used by tool handlers whose
   * own previously-scheduled chunks should NOT land — for example, a
   * handler that detected an upstream failure mid-response and wants to
   * abort before the rest of the body arrives.
   *
   * The seq-ordering correctness argument: pending entries are tagged
   * cancelled BEFORE the abort fires. Whether the clock pops the abort
   * before or after the (cancelled) chunk entries, the chunk callbacks
   * are now no-ops, so the test-visible body never sees them. The abort
   * itself takes effect synchronously when this method returns; the
   * stream's controller is in the errored state immediately.
   *
   * Throws if no stream with the given `streamId` was minted by this
   * harness, or if the stream is already in a terminal state.
   */
  abortBefore(streamId: StreamId): void;
  /**
   * Default driver for production `runInference` through the harness's
   * `deps`. The wrapper:
   *
   * 1. Calls `@intx/inference`'s real `runInference` with `opts`
   *    plus `harness.deps` automatically injected (callers do not — and
   *    must not — pass `deps` themselves).
   * 2. When the underlying iterator emits `inference.tool_call.end`,
   *    looks up the handler registered for that tool name and dispatches
   *    it with the parsed arguments BEFORE yielding the event. The
   *    dispatched result is captured on the harness and is available via
   *    `scenario.lastToolDispatch(name)`. Dispatching before the yield
   *    guarantees the handler fires even if the consumer breaks out of
   *    the `for await` loop on `inference.tool_call.end`.
   * 3. Yields every `InferenceEvent` the underlying iterator emits.
   *
   * If no handler is registered for a tool name observed in an
   * `inference.tool_call.end`, the wrapper throws synchronously from the
   * iterator with a message naming the unregistered tool. This is the
   * defensive-coding choice: a tool call the test did not script for is
   * always a setup bug, never a runtime fallback.
   *
   * Escape hatch: tests that want to drive tool dispatch by hand (for
   * dispatch-ordering or error-path assertions) can call the underlying
   * `runInference` directly with `deps: harness.deps`. That bypasses the
   * auto-dispatch path; the test is then responsible for calling
   * `scenario.invokeTool` itself.
   */
  runInference(
    opts: Omit<InferenceHarnessOptions, "deps">,
  ): AsyncIterable<InferenceEvent>;
  /**
   * Closes every open simulated stream and releases per-fetch resources.
   * Safe to call multiple times; subsequent calls are no-ops. Tests should
   * call `dispose()` in an `afterEach` to prevent bun:test from logging
   * "unclosed ReadableStream" warnings.
   */
  dispose(): void;
};

/**
 * Optional construction overrides for `setupHarness`. Today only an injected
 * `clock` is supported; the field exists primarily for internal tests that
 * want to drive a clock seam they constructed themselves.
 */
export type SetupHarnessOpts = {
  /**
   * Override the clock injected into the harness. Mostly useful for tests
   * inside this package that exercise harness/clock interactions; consumers
   * should let `setupHarness()` create its own clock.
   */
  clock?: Clock;
};

/**
 * Construct a fresh harness. Each call allocates its own clock (unless one
 * is passed via `opts`), its own `HarnessId` symbol, and its own per-fetch
 * waiting set, matcher table, tool-handler registry, and open-stream
 * registry. Nothing is shared across harnesses; tests should construct one
 * harness per `it`/`test` and dispose it in `afterEach`.
 */
export function setupHarness(opts: SetupHarnessOpts = {}): Harness {
  const clock = opts.clock ?? createClock();
  const harnessSymbol = Symbol("HarnessInstance");

  let nextStreamSeq = 0;
  const openStreams = new Set<SimulatedStreamHandle>();
  const waiting: WaitingFetch[] = [];
  const matcherTable = createMatcherTable();
  let disposed = false;

  const streamIdToHandle = new Map<StreamId, SimulatedStreamHandle>();
  const streamToHandle = new WeakMap<SimulatedStream, SimulatedStreamHandle>();

  type AbortAfterRegistration = {
    readonly predicate: WireEventPredicate;
    readonly controller: AbortController;
    fired: boolean;
  };
  const abortAfterRegistrations: AbortAfterRegistration[] = [];

  const handleChunkFired = (event: ChunkFiredEvent): void => {
    if (abortAfterRegistrations.length === 0) return;
    for (const reg of abortAfterRegistrations) {
      if (reg.fired) continue;
      if (!reg.predicate(event)) continue;
      reg.fired = true;
      reg.controller.abort();
    }
  };

  const createStream = (): SimulatedStream => {
    if (disposed) {
      throw new Error(
        "Harness.scenario.createStream: harness has been disposed",
      );
    }
    const streamId = toStreamId(nextStreamSeq++);
    const handleRef: { current: SimulatedStreamHandle | null } = {
      current: null,
    };
    const handle = createSimulatedStream({
      clock,
      streamId,
      onTerminate: () => {
        if (handleRef.current !== null) {
          openStreams.delete(handleRef.current);
        }
      },
      onChunkFired: handleChunkFired,
    });
    handleRef.current = handle;
    openStreams.add(handle);
    streamIdToHandle.set(streamId, handle);
    streamToHandle.set(handle.stream, handle);
    return handle.stream;
  };

  // Most-recently matched request, cloned so the test can consume its
  // body without affecting the original. See `scenario.lastRequest`.
  // Type widened to satisfy the gap between Bun's global Request and
  // the undici Request the fetch stub builds; both have the methods
  // the public surface needs (clone, json, text, headers).
  let lastMatchedRequest:
    | ReturnType<WaitingFetch["request"]["clone"]>
    | undefined;

  const routeWaitingFetch = (
    wf: WaitingFetch,
    stream: SimulatedStream,
    opts: WhenRequestMatchesOpts | undefined,
  ): void => {
    wf.settled = true;
    const idx = waiting.indexOf(wf);
    if (idx >= 0) waiting.splice(idx, 1);
    lastMatchedRequest = wf.request.clone();
    // Per-call abort isolation on the matched stream: once a fetch has
    // been bound to a stream, an abort on its signal must error ONLY
    // that stream's controller. We attach the listener here (the
    // pre-route abort path in `stubFetch` already handled the waiting
    // case) so the listener targets the stream the test reader is about
    // to consume.
    const signal = wf.signal;
    if (signal !== undefined && !signal.aborted) {
      const handle = streamToHandle.get(stream);
      if (handle !== undefined) {
        const onAbort = (): void => {
          if (handle.isClosed()) return;
          handle.cancelPending();
          handle.forceError(new DOMException("aborted", "AbortError"));
        };
        signal.addEventListener("abort", onAbort, { once: true });
      }
    }
    const status = opts?.status ?? 200;
    const defaultContentType =
      status >= 200 && status < 300 ? "text/event-stream" : "application/json";
    const headers: Record<string, string> = {
      "content-type": defaultContentType,
    };
    if (opts?.headers !== undefined) {
      for (const [k, v] of Object.entries(opts.headers)) {
        headers[k] = v;
      }
    }
    wf.resolve(new Response(stream.body, { status, headers }));
  };

  const runScan = (): void => {
    scanWaitingSet(waiting, matcherTable, routeWaitingFetch);
    // Drop any fetches that were settled by scanWaitingSet (ambiguity case).
    for (let i = waiting.length - 1; i >= 0; i--) {
      const entry = waiting[i];
      if (entry !== undefined && entry.settled) {
        waiting.splice(i, 1);
      }
    }
  };

  const whenRequestMatches = (
    predicate: RequestPredicate,
    responseStream: SimulatedStream,
    opts?: WhenRequestMatchesOpts,
  ): void => {
    if (disposed) {
      throw new Error(
        "Harness.scenario.whenRequestMatches: harness has been disposed",
      );
    }
    if (typeof predicate !== "function") {
      throw new Error(
        "Harness.scenario.whenRequestMatches: predicate must be a function",
      );
    }
    if (!streamToHandle.has(responseStream)) {
      throw new Error(
        `Harness.scenario.whenRequestMatches: stream ${String(responseStream.streamId)} was not minted by this harness`,
      );
    }
    // Skip frames: 0 = the Error itself, 1 = captureMatcherSource, 2 = this
    // whenRequestMatches body, 3 = the caller. The captureMatcherSource
    // helper counts frames AFTER the Error message line, so passing `2`
    // here lands on the immediate caller.
    const source = captureMatcherSource(2);
    matcherTable.register(predicate, responseStream, source, opts);
    runScan();
  };

  const inFlightToolHandlers = new Set<Promise<void>>();
  // Collects rejections from in-flight tool handler promises so the
  // quiescence loop can re-throw them deterministically. Without this,
  // a rejection that lands between the `Promise.all` await and the next
  // loop iteration would be lost (the handler was removed from the
  // in-flight set by its own `finally` before Promise.all could observe
  // it).
  const inFlightErrors: unknown[] = [];

  const trackInFlight = (promise: Promise<void>): void => {
    const tracked: Promise<void> = promise.then(
      () => {
        inFlightToolHandlers.delete(tracked);
      },
      (err: unknown) => {
        inFlightToolHandlers.delete(tracked);
        inFlightErrors.push(err);
      },
    );
    inFlightToolHandlers.add(tracked);
  };

  const toolRegistry = createToolHandlerRegistry({ clock, trackInFlight });
  const lastToolDispatchByName = new Map<string, unknown>();

  const recordingDispatch = (
    name: string,
    inner: DispatchToolResult,
  ): DispatchToolResult => {
    return (result: unknown): void => {
      lastToolDispatchByName.set(name, result);
      inner(result);
    };
  };

  const onTool = (name: string, handler: ToolHandler): void => {
    if (disposed) {
      throw new Error("Harness.scenario.onTool: harness has been disposed");
    }
    toolRegistry.register(name, handler);
  };

  const invokeTool = (
    name: string,
    args: unknown,
    dispatch: DispatchToolResult,
  ): void => {
    if (disposed) {
      throw new Error("Harness.scenario.invokeTool: harness has been disposed");
    }
    if (typeof dispatch !== "function") {
      throw new Error(
        "Harness.scenario.invokeTool: dispatch must be a function",
      );
    }
    toolRegistry.invoke(name, args, recordingDispatch(name, dispatch));
  };

  const lastToolDispatch = (name: string): unknown => {
    return lastToolDispatchByName.get(name);
  };

  const abortAt = (virtualMs: number, controller: AbortController): void => {
    if (disposed) {
      throw new Error("Harness.scenario.abortAt: harness has been disposed");
    }
    if (!(controller instanceof AbortController)) {
      throw new Error(
        "Harness.scenario.abortAt: controller must be an AbortController instance",
      );
    }
    clock.schedule(virtualMs, function scenarioAbort() {
      controller.abort();
    });
  };

  const abortAfter = (
    predicate: WireEventPredicate,
    controller: AbortController,
  ): void => {
    if (disposed) {
      throw new Error("Harness.scenario.abortAfter: harness has been disposed");
    }
    if (typeof predicate !== "function") {
      throw new Error(
        "Harness.scenario.abortAfter: predicate must be a function",
      );
    }
    if (!(controller instanceof AbortController)) {
      throw new Error(
        "Harness.scenario.abortAfter: controller must be an AbortController instance",
      );
    }
    abortAfterRegistrations.push({
      predicate,
      controller,
      fired: false,
    });
  };

  const lastRequest = (): Request | undefined =>
    // The clone is produced by the (undici-typed) WaitingFetch.request,
    // but the public Scenario type uses the platform's global Request.
    // The shape is identical; the cast bridges the type-only gap.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- type-only bridge between undici Request and Bun's global Request
    lastMatchedRequest as Request | undefined;

  const replyOnce = (
    provider: Provider,
    opts: ReplyOnceOpts,
  ): SimulatedStream => {
    const stream = createStream();
    const chunks = completeResponse(provider, {
      ...(opts.text !== undefined ? { text: opts.text } : {}),
      ...(opts.toolCalls !== undefined ? { toolCalls: opts.toolCalls } : {}),
      ...(opts.headUsage !== undefined ? { headUsage: opts.headUsage } : {}),
      ...(opts.tailUsage !== undefined ? { tailUsage: opts.tailUsage } : {}),
    });
    // Schedule at the next safe virtual time so callers do not have to
    // compute `clock.now() + N` themselves and so multiple replyOnce
    // calls in the same test never schedule into the past.
    stream.enqueueAll(chunks, { startAt: clock.now() + 1 });
    whenRequestMatches(
      opts.predicate ?? (() => true),
      stream,
      opts.responseOpts,
    );
    return stream;
  };

  const scenario: Scenario = {
    createStream,
    whenRequestMatches,
    onTool,
    invokeTool,
    lastToolDispatch,
    lastRequest,
    replyOnce,
    abortAt,
    abortAfter,
  };

  const buildRequest = (
    input: string | URL | Request,
    init: RequestInit | undefined,
  ): Request => {
    if (input instanceof Request) {
      return init === undefined ? input : new Request(input, init);
    }
    const url = input instanceof URL ? input.toString() : input;
    return new Request(url, init);
  };

  const extractSignal = (
    input: string | URL | Request,
    init: RequestInit | undefined,
  ): AbortSignal | undefined => {
    const fromInit = init?.signal;
    if (fromInit !== undefined && fromInit !== null) return fromInit;
    if (input instanceof Request) return input.signal;
    return undefined;
  };

  const stubFetch = async (
    input: string | URL | Request,
    init?: RequestInit,
  ): Promise<Response> => {
    if (disposed) {
      throw new Error("Harness fetch: harness has been disposed");
    }
    const signal = extractSignal(input, init);
    if (signal?.aborted === true) {
      throw new DOMException("aborted", "AbortError");
    }
    const request = buildRequest(input, init);
    return await new Promise<Response>((resolve, reject) => {
      const entry: WaitingFetch = {
        request,
        signal: signal ?? undefined,
        resolve,
        reject,
        settled: false,
      };
      if (signal !== undefined) {
        const onAbort = (): void => {
          if (entry.settled) return;
          entry.settled = true;
          const idx = waiting.indexOf(entry);
          if (idx >= 0) waiting.splice(idx, 1);
          reject(new DOMException("aborted", "AbortError"));
        };
        signal.addEventListener("abort", onAbort, { once: true });
      }
      waiting.push(entry);
      try {
        runScan();
      } catch (err) {
        // scanWaitingSet rejects conflicting waiting fetches itself before
        // throwing. If this fetch wasn't part of the conflict, it remains
        // in the waiting set unchanged. Propagate the error to whoever
        // triggered the scan (here, the fetch caller) only if this fetch
        // was the conflict's settler. Otherwise swallow the throw — the
        // conflict's victims have already been rejected with the error.
        if (!entry.settled) {
          // We pushed this fetch in the same call and the new arrival
          // triggered the scan that surfaced the ambiguity. Settle this
          // fetch with the error too so the caller's await rejects rather
          // than waiting forever.
          entry.settled = true;
          const idx = waiting.indexOf(entry);
          if (idx >= 0) waiting.splice(idx, 1);
          reject(err);
        }
      }
    });
  };

  const deps: Dependencies = {
    fetch: stubFetch,
    [HarnessId]: harnessSymbol,
  };

  const assertDeps = (candidate: Dependencies): void => {
    const received = candidate[HarnessId];
    if (received !== harnessSymbol) {
      throw new WrongHarnessError(harnessSymbol, received);
    }
  };

  clock.onSyncCallbackError((err) => {
    // A scheduled callback threw synchronously inside `advanceTo`/`run`.
    // Error every still-open simulated stream so the next test does not
    // inherit dangling readers parked on a never-completing `read()`.
    // `forceError` is idempotent; streams already terminated naturally
    // (or errored by the throwing callback itself) are skipped.
    for (const handle of openStreams) {
      handle.forceError(err);
    }
  });

  const collectUnmatched = (): UnmatchedFetchInfo[] => {
    const infos: UnmatchedFetchInfo[] = [];
    for (const wf of waiting) {
      if (wf.settled) continue;
      const headers: Record<string, string> = {};
      wf.request.headers.forEach((value, key) => {
        headers[key] = value;
      });
      infos.push({
        url: wf.request.url,
        method: wf.request.method,
        headers,
      });
    }
    return infos;
  };

  const checkQuiescence = (): void => {
    const unmatched = collectUnmatched();
    if (unmatched.length === 0) return;
    // Settle every unmatched fetch with the same error so awaiters reject
    // rather than hang. The thrown error also surfaces to the test's
    // `await harness.run()` / `await harness.advanceTo(...)` site.
    const err = new UnmatchedFetchError(unmatched);
    for (let i = waiting.length - 1; i >= 0; i--) {
      const wf = waiting[i];
      if (wf === undefined || wf.settled) continue;
      wf.settled = true;
      waiting.splice(i, 1);
      wf.reject(err);
    }
    throw err;
  };

  const DEFAULT_WALL_CLOCK_BUDGET_MS = 250;

  const takeInFlightError = (): unknown => {
    if (inFlightErrors.length === 0) return undefined;
    // Surface the first rejection; additional ones (rare — would require
    // multiple in-flight handlers rejecting in the same tick) are dropped
    // to keep the contract simple. A future slice can extend to
    // AggregateError if real tests need it.
    const [first] = inFlightErrors.splice(0, inFlightErrors.length);
    return first;
  };

  const drainInFlight = async (
    startWall: number,
    wallClockBudgetMs: number,
  ): Promise<void> => {
    // Repeatedly await any in-flight tool handler promises. A handler may
    // schedule additional clock work or register further handlers on
    // resolution, so the run/advanceTo callers re-enter the clock loop
    // after this returns to pick that work up.
    //
    // The drain races each batch against a real-time timer so a handler
    // that's blocked on a real wall-clock timer (e.g., setTimeout(...))
    // surfaces as a ClockWallClockOverrunError instead of hanging the
    // test. This mirrors the budget the clock itself enforces inside
    // `clock.run()`.
    while (inFlightToolHandlers.size > 0) {
      if (wallClockBudgetMs === Infinity) {
        await Promise.all([...inFlightToolHandlers]);
        continue;
      }
      const elapsed = performance.now() - startWall;
      const remaining = wallClockBudgetMs - elapsed;
      if (remaining <= 0) {
        throw new ClockWallClockOverrunError(
          `Harness.run exceeded wall-clock budget of ${String(wallClockBudgetMs)}ms while awaiting in-flight tool handlers (elapsed=${String(elapsed)}ms)`,
          `in-flight tool handlers: ${String(inFlightToolHandlers.size)}`,
        );
      }
      let timer: ReturnType<typeof setTimeout> | null = null;
      const budgetExpired = new Promise<"timeout">((resolve) => {
        timer = setTimeout(() => {
          resolve("timeout");
        }, remaining);
      });
      const allDone = Promise.all([...inFlightToolHandlers]).then(
        () => "done" as const,
      );
      const outcome = await Promise.race([allDone, budgetExpired]);
      if (timer !== null) clearTimeout(timer);
      if (outcome === "timeout") {
        const elapsedNow = performance.now() - startWall;
        throw new ClockWallClockOverrunError(
          `Harness.run exceeded wall-clock budget of ${String(wallClockBudgetMs)}ms while awaiting in-flight tool handlers (elapsed=${String(elapsedNow)}ms)`,
          `in-flight tool handlers: ${String(inFlightToolHandlers.size)}`,
        );
      }
    }
  };

  const clearInFlightState = (): void => {
    inFlightToolHandlers.clear();
    inFlightErrors.length = 0;
  };

  const run = async (runOpts?: RunOpts): Promise<void> => {
    const wallClockBudgetMs =
      runOpts?.wallClockBudgetMs ?? DEFAULT_WALL_CLOCK_BUDGET_MS;
    const startWall = performance.now();
    try {
      for (;;) {
        await clock.run(runOpts);
        const err = takeInFlightError();
        if (err !== undefined) throw err;
        if (inFlightToolHandlers.size === 0) break;
        await drainInFlight(startWall, wallClockBudgetMs);
        const errAfter = takeInFlightError();
        if (errAfter !== undefined) throw errAfter;
        // Loop: handler resolution may have scheduled new heap entries or
        // registered new matchers; let `clock.run()` settle them before we
        // declare quiescence.
      }
    } catch (err) {
      // The harness is intended to be one-shot, but defensively clear any
      // tracked in-flight handlers and queued rejections so a caller that
      // re-uses this harness after a throw does not inherit stale state.
      clearInFlightState();
      throw err;
    }
    checkQuiescence();
  };

  const advanceTo = async (
    virtualMs: number,
    advanceOpts?: AdvanceOpts,
  ): Promise<void> => {
    // advanceTo's wall-clock budget mirrors run()'s default. AdvanceOpts
    // does not expose a wallClockBudgetMs knob today; the in-flight drain
    // uses the default so a stuck handler still surfaces as an overrun.
    const startWall = performance.now();
    try {
      for (;;) {
        await clock.advanceTo(virtualMs, advanceOpts);
        const err = takeInFlightError();
        if (err !== undefined) throw err;
        if (inFlightToolHandlers.size === 0) break;
        await drainInFlight(startWall, DEFAULT_WALL_CLOCK_BUDGET_MS);
        const errAfter = takeInFlightError();
        if (errAfter !== undefined) throw errAfter;
      }
    } catch (err) {
      clearInFlightState();
      throw err;
    }
    checkQuiescence();
  };

  const harnessRunInference = (
    opts: Omit<InferenceHarnessOptions, "deps">,
  ): AsyncIterable<InferenceEvent> => {
    if (disposed) {
      throw new Error("Harness.runInference: harness has been disposed");
    }
    const noopDispatch: DispatchToolResult = () => undefined;
    async function* iterate(): AsyncGenerator<InferenceEvent> {
      const inner = runInference({ ...opts, deps });
      for await (const event of inner) {
        if (event.type === "inference.tool_call.end") {
          const { name, arguments: args } = event.data;
          if (!toolRegistry.has(name)) {
            throw new Error(
              `Harness.runInference: inference.tool_call.end observed for tool "${name}" but no handler was registered via scenario.onTool. Register a handler or drop to the runInference escape hatch and dispatch manually.`,
            );
          }
          toolRegistry.invoke(
            name,
            args,
            recordingDispatch(name, noopDispatch),
          );
        }
        yield event;
      }
    }
    return iterate();
  };

  const abortBefore = (streamId: StreamId): void => {
    if (disposed) {
      throw new Error("Harness.abortBefore: harness has been disposed");
    }
    const handle = streamIdToHandle.get(streamId);
    if (handle === undefined) {
      throw new Error(
        `Harness.abortBefore: no stream with id ${String(streamId)} was minted by this harness`,
      );
    }
    if (handle.isClosed()) {
      throw new Error(
        `Harness.abortBefore: stream ${String(streamId)} is already in a terminal state`,
      );
    }
    // Cancel every pending heap entry for this stream first. The entries
    // remain on the clock heap but the closures now check `cancelled`
    // before doing anything, so when the clock eventually pops them they
    // are no-ops. This is the seq-ordering workaround: we cannot inject
    // a lower seq into the heap from outside the clock, so instead we
    // make every later-seq entry for this stream inert.
    handle.cancelPending();
    handle.forceError(new DOMException("aborted", "AbortError"));
  };

  const dispose = (): void => {
    if (disposed) return;
    disposed = true;
    for (const handle of openStreams) {
      handle.forceClose();
    }
    openStreams.clear();
    streamIdToHandle.clear();
    // Reject any still-waiting fetches so awaiters don't hang after
    // dispose; dispose is a hard teardown, not a quiescence check.
    for (const wf of waiting) {
      if (wf.settled) continue;
      wf.settled = true;
      wf.reject(new Error("Harness fetch: harness has been disposed"));
    }
    waiting.length = 0;
    matcherTable.entries.length = 0;
    inFlightToolHandlers.clear();
    inFlightErrors.length = 0;
    abortAfterRegistrations.length = 0;
    lastToolDispatchByName.clear();
  };

  return {
    clock,
    deps,
    scenario,
    assertDeps,
    run,
    advanceTo,
    runInference: harnessRunInference,
    abortBefore,
    dispose,
  };
}
