import {
  HarnessId,
  runInference,
  type AdapterRegistry,
  type Dependencies,
  type InferenceHarnessOptions,
  type Scheduler,
} from "@intx/inference";
import { createBuiltinRegistry } from "@intx/inference/providers";
import type { CredentialMaterialResolver } from "@intx/types";
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
  type BodyAwareRequestPredicate,
  type HarnessRequest,
  type ReplyOnceOpts,
  type ReplyOnceToolCall,
  type RequestPredicate,
  type Scenario,
  type StallHandle,
  type StallOpts,
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

// Fail-open credential resolver for the test harness. The stubbed `fetch`
// never sends the injected secret anywhere, so a credential sentinel just
// needs SOME material rather than the production fail-closed throw. A test
// asserting a specific injected key supplies its own `readMaterial`.
const DEFAULT_TEST_READ_MATERIAL: CredentialMaterialResolver = () => ({
  secret: "inference-test-secret",
});

/**
 * The deterministic inference test harness returned by `setupHarness`. It
 * bundles the virtual `clock` driving all scheduling, the `deps` to inject
 * into the system under test (`fetch` is stubbed; `HarnessId` is branded so
 * `assertDeps` catches cross-harness contamination), and the `scenario`
 * seam for streams, matchers, tool handlers, and aborts. The harness owns
 * disposal of every stream it minted; call `dispose()` (typically in
 * `afterEach`) to suppress bun:test "unclosed ReadableStream" warnings.
 */
export type Harness = {
  readonly clock: Clock;
  readonly deps: Dependencies;
  readonly scenario: Scenario;
  /**
   * Asserts that `candidate` was produced by this harness. Use at test
   * boundaries that take a `Dependencies` from an external source to catch
   * cross-harness contamination. Throws `WrongHarnessError` on mismatch.
   */
  assertDeps(candidate: Dependencies): void;
  /**
   * Delegates to `clock.run()` and then verifies the waiting-fetch set is
   * empty, throwing `UnmatchedFetchError` otherwise. Third of the three
   * scan triggers.
   */
  run(opts?: RunOpts): Promise<void>;
  /**
   * Delegates to `clock.advanceTo()` and then verifies the waiting-fetch
   * set is empty, throwing `UnmatchedFetchError` otherwise. Mirrors
   * `run()`'s quiescence check at a bounded virtual deadline.
   */
  advanceTo(virtualMs: number, opts?: AdvanceOpts): Promise<void>;
  /**
   * Cancels every pending scheduled callback for the stream identified by
   * `streamId` and errors that stream's controller with an `AbortError` at
   * `clock.now()`. Used by tool handlers whose own previously-scheduled
   * chunks should NOT land. Cancelled entries are tagged before the abort
   * fires, so the test-visible body never sees them; the controller is
   * errored synchronously on return.
   *
   * Throws if no such stream was minted by this harness, or if the stream
   * is already terminal.
   */
  abortBefore(streamId: StreamId): void;
  /**
   * Default driver for production `runInference` through the harness's
   * `deps`. The wrapper injects `deps` (callers must not pass one),
   * auto-dispatches registered tool handlers on `inference.tool_call.end`
   * BEFORE yielding the event (so the handler fires even if the consumer
   * breaks out of the loop), and yields every event. A tool call with no
   * registered handler throws synchronously from the iterator — an
   * unscripted tool call is always a setup bug.
   *
   * Escape hatch: tests driving dispatch by hand call the underlying
   * `runInference` directly with `deps: harness.deps`, bypassing
   * auto-dispatch, and use `scenario.invokeTool` themselves.
   */
  runInference(
    opts: Omit<InferenceHarnessOptions, "deps">,
  ): AsyncIterable<InferenceEvent>;
  /**
   * Closes every open simulated stream and releases per-fetch resources.
   * Safe to call multiple times; later calls are no-ops. Call in
   * `afterEach` to prevent bun:test "unclosed ReadableStream" warnings.
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
   * Override the clock injected into the harness. Mostly for tests inside
   * this package that exercise harness/clock interactions.
   */
  clock?: Clock;
  /**
   * When true, the `Scheduler` exposed via `harness.deps.scheduler` is
   * backed by the virtual clock so production inactivity / total timeouts
   * fire at virtual time. When false (default), the scheduler is a no-op —
   * what almost every test wants (otherwise `harness.run()` would have to
   * advance virtual time through the 600s default total-timeout horizon).
   * Timeout tests set this true and pass explicit short thresholds via
   * `InferenceOptions.inactivityTimeoutMs` / `totalTimeoutMs`.
   */
  enableInferenceTimers?: boolean;
  /**
   * Override the adapter registry exposed via `harness.deps.adapters`.
   * Defaults to `createBuiltinRegistry()` so the harness resolves the same
   * shipped provider set production uses.
   */
  adapters?: AdapterRegistry;
};

/**
 * Construct a fresh harness: its own clock (unless one is passed), its own
 * `HarnessId` symbol, waiting set, matcher table, tool-handler registry,
 * and open-stream registry. Nothing is shared across harnesses; construct
 * one per `it`/`test` and dispose it in `afterEach`.
 */
export function setupHarness(opts: SetupHarnessOpts = {}): Harness {
  const clock = opts.clock ?? createClock();
  const harnessSymbol = Symbol("HarnessInstance");

  let nextStreamSeq = 0;
  const openStreams = new Set<SimulatedStreamHandle>();
  const waiting: WaitingFetch[] = [];
  const matcherTable = createMatcherTable();
  let disposed = false;

  type StallRegistration = {
    readonly stream: SimulatedStream;
    readonly aborted: { value: boolean };
    readonly resolveAwaitAbort: () => void;
  };
  const stallRegistrations: StallRegistration[] = [];

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

  // Capture list for `scenario.matchedRequests()`. Each entry is a
  // clone taken at route time and held purely as a clone-source — never
  // consumed, so `matchedRequests()` can re-clone on every call.
  const matchedRequestsList: HarnessRequest[] = [];

  const routeWaitingFetch = (
    wf: WaitingFetch,
    stream: SimulatedStream,
    opts: WhenRequestMatchesOpts | undefined,
  ): void => {
    wf.settled = true;
    const idx = waiting.indexOf(wf);
    if (idx >= 0) waiting.splice(idx, 1);
    matchedRequestsList.push(wf.request.clone());
    // Per-call abort isolation on the matched stream: once a fetch binds
    // to a stream, an abort on its signal must error ONLY that stream's
    // controller. Attach the listener here (the pre-route abort path in
    // `stubFetch` already handled the waiting case).
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
    // Stall telemetry: if this stream was minted by `scenario.stall`,
    // record when its bound fetch's signal aborts so tests can assert on
    // AbortController propagation directly.
    const stallReg = stallRegistrations.find((r) => r.stream === stream);
    if (stallReg !== undefined) {
      if (signal === undefined) {
        // No signal means no abort can ever fire; `aborted` stays false
        // and `awaitAbort` never resolves.
      } else if (signal.aborted) {
        stallReg.aborted.value = true;
        stallReg.resolveAwaitAbort();
      } else {
        signal.addEventListener(
          "abort",
          () => {
            stallReg.aborted.value = true;
            stallReg.resolveAwaitAbort();
          },
          { once: true },
        );
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

  const sweepSettled = (): void => {
    for (let i = waiting.length - 1; i >= 0; i--) {
      const entry = waiting[i];
      if (entry !== undefined && entry.settled) {
        waiting.splice(i, 1);
      }
    }
  };

  const runScan = (): void => {
    scanWaitingSet(waiting, matcherTable, routeWaitingFetch);
    // Drop any fetches that were settled by scanWaitingSet (ambiguity case).
    sweepSettled();
  };

  // Body-aware scan plumbing: when body-aware matchers exist, the sync
  // scan (above) considers only sync matchers; the harness then buffers
  // the body of every still-waiting fetch (in parallel, via one
  // `clone().text()` per fetch, cached on `WaitingFetch.bodyText`) and
  // runs a second `scanWaitingSet` pass with `includeBodyAware: true`.
  //
  // Buffer-then-scan is deliberate: ambiguous body-aware matches are
  // detected over a fully-buffered set, keeping conflict semantics
  // identical to the sync scan's single-pass model.
  //
  // `run`/`advanceTo` drain in-flight body scans alongside in-flight
  // tool handlers before checking quiescence; a body-aware match can
  // schedule clock work, so the outer loop re-enters `clock.run`.
  const inFlightBodyScans = new Set<Promise<void>>();
  const inFlightScanErrors: unknown[] = [];

  // Best-effort buffering, NOT transactional. When one `clone().text()`
  // throws, the other in-flight reads' writes to `wf.bodyText` still
  // land; the error is routed through `inFlightScanErrors` and re-thrown
  // at the next `run()`/`advanceTo()`. Partially-buffered state can
  // remain on failure — fine for the one-shot harness contract.
  const bufferUnreadBodies = async (): Promise<void> => {
    // Loop until no unbuffered waiting fetches remain: new fetches can
    // arrive (via `stubFetch`) while a body read is in flight, so a
    // single pass could leave them unbuffered for the scan.
    for (;;) {
      const needBuffer = waiting.filter(
        (wf) => !wf.settled && wf.bodyText === undefined,
      );
      if (needBuffer.length === 0) return;
      await Promise.all(
        needBuffer.map(async (wf) => {
          if (wf.settled) return;
          // A concurrent scan may already have buffered this fetch.
          if (wf.bodyText !== undefined) return;
          let text: string;
          try {
            text = await wf.request.clone().text();
          } catch (err) {
            // Aborted or disposed mid-read: the entry is already
            // settled, skip silently. Anything else is a genuine read
            // failure and must surface via `inFlightScanErrors`.
            if (wf.settled) return;
            throw err;
          }
          if (wf.settled) return;
          if (wf.bodyText !== undefined) return;
          wf.bodyText = text;
        }),
      );
    }
  };

  const triggerBodyAwareScan = (): void => {
    if (!matcherTable.hasBodyAware()) return;
    if (disposed) return;
    const scanPromise: Promise<void> = (async () => {
      await bufferUnreadBodies();
      if (disposed) return;
      // Entries may have been settled or removed during the buffer await.
      scanWaitingSet(waiting, matcherTable, routeWaitingFetch, true);
      sweepSettled();
    })()
      .catch((err: unknown) => {
        inFlightScanErrors.push(err);
      })
      .finally(() => {
        inFlightBodyScans.delete(scanPromise);
      });
    inFlightBodyScans.add(scanPromise);
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
    // Skip frames: 0 = the Error itself, 1 = captureMatcherSource, 2 =
    // this whenRequestMatches body, 3 = the caller.
    const source = captureMatcherSource(2);
    matcherTable.register(predicate, responseStream, source, opts);
    runScan();
    // Defensive re-trigger: a pre-existing body-aware matcher would have
    // already routed any accepting fetch under the purity rule, but the
    // re-trigger covers edge cases (consumed flags flipping between
    // scans, future purity relaxation). No-op when no body-aware
    // matchers exist.
    triggerBodyAwareScan();
  };

  const whenRequestBodyMatches = (
    predicate: BodyAwareRequestPredicate,
    responseStream: SimulatedStream,
    opts?: WhenRequestMatchesOpts,
  ): void => {
    if (disposed) {
      throw new Error(
        "Harness.scenario.whenRequestBodyMatches: harness has been disposed",
      );
    }
    if (typeof predicate !== "function") {
      throw new Error(
        "Harness.scenario.whenRequestBodyMatches: predicate must be a function",
      );
    }
    if (!streamToHandle.has(responseStream)) {
      throw new Error(
        `Harness.scenario.whenRequestBodyMatches: stream ${String(responseStream.streamId)} was not minted by this harness`,
      );
    }
    const source = captureMatcherSource(2);
    matcherTable.registerBodyAware(predicate, responseStream, source, opts);
    // Sync scan first: a sync matcher might still bind a fetch (the new
    // body-aware matcher is skipped in the sync pass), and the sweep
    // keeps the waiting set tidy before the async scan reads it.
    runScan();
    triggerBodyAwareScan();
  };

  const inFlightToolHandlers = new Set<Promise<void>>();
  // Collects rejections from in-flight tool handler promises so the
  // quiescence loop can re-throw them deterministically; without this a
  // rejection landing between the `Promise.all` await and the next loop
  // iteration would be lost.
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

  const matchedRequests = (): HarnessRequest[] =>
    // Re-clone every stored Request on each call so the returned objects
    // are fully independent of each other and of later calls. The stored
    // route-time clone is never consumed.
    matchedRequestsList.map((r) => r.clone());

  let nextAutoCallId = 0;
  // The `call_auto_` prefix is reserved for ids the harness mints for
  // the `{ name, args }` shape; pinned explicit callIds must not collide.
  const AUTO_CALL_ID_PREFIX = "call_auto_";
  const normalizeToolCalls = (
    toolCalls: readonly ReplyOnceToolCall[],
  ): { callId: string; name: string; argsJSON: string }[] => {
    return toolCalls.map((tc) => {
      if ("argsJSON" in tc) {
        if (tc.callId.startsWith(AUTO_CALL_ID_PREFIX)) {
          throw new Error(
            `Harness.scenario.replyOnce: callId ${JSON.stringify(tc.callId)} uses the reserved ${JSON.stringify(AUTO_CALL_ID_PREFIX)} prefix; pick a different id for explicit-shape tool calls.`,
          );
        }
        return { callId: tc.callId, name: tc.name, argsJSON: tc.argsJSON };
      }
      if (
        tc.callId !== undefined &&
        tc.callId.startsWith(AUTO_CALL_ID_PREFIX)
      ) {
        throw new Error(
          `Harness.scenario.replyOnce: callId ${JSON.stringify(tc.callId)} uses the reserved ${JSON.stringify(AUTO_CALL_ID_PREFIX)} prefix; pick a different id for pinned tool calls.`,
        );
      }
      const callId =
        tc.callId ?? `${AUTO_CALL_ID_PREFIX}${String(nextAutoCallId++)}`;
      return { callId, name: tc.name, argsJSON: JSON.stringify(tc.args) };
    });
  };

  const replyOnce = (
    provider: Provider,
    opts: ReplyOnceOpts,
  ): SimulatedStream => {
    const stream = createStream();
    const chunks = completeResponse(provider, {
      ...(opts.text !== undefined ? { text: opts.text } : {}),
      ...(opts.toolCalls !== undefined
        ? { toolCalls: normalizeToolCalls(opts.toolCalls) }
        : {}),
      ...(opts.headUsage !== undefined ? { headUsage: opts.headUsage } : {}),
      ...(opts.tailUsage !== undefined ? { tailUsage: opts.tailUsage } : {}),
    });
    // Schedule at `clock.now() + 1` so callers need not compute the next
    // safe virtual time themselves and multiple replyOnce calls never
    // schedule into the past.
    stream.enqueueAll(chunks, { startAt: clock.now() + 1 });
    whenRequestMatches(
      opts.predicate ?? (() => true),
      stream,
      opts.responseOpts,
    );
    return stream;
  };

  const stall = (opts: StallOpts = {}): StallHandle => {
    if (disposed) {
      throw new Error("Harness.scenario.stall: harness has been disposed");
    }
    const stream = createStream();
    const aborted = { value: false };
    let resolveAwaitAbort: () => void = () => {
      throw new Error(
        "Harness.scenario.stall: awaitAbort resolver invoked before Promise constructor ran (internal bug)",
      );
    };
    const awaitAbort = new Promise<void>((resolve) => {
      resolveAwaitAbort = resolve;
    });
    stallRegistrations.push({ stream, aborted, resolveAwaitAbort });
    whenRequestMatches(
      opts.predicate ?? (() => true),
      stream,
      opts.responseOpts,
    );
    return {
      stream,
      get aborted() {
        return aborted.value;
      },
      awaitAbort,
    };
  };

  const scenario: Scenario = {
    createStream,
    whenRequestMatches,
    whenRequestBodyMatches,
    onTool,
    invokeTool,
    lastToolDispatch,
    matchedRequests,
    replyOnce,
    stall,
    abortAt,
    abortAfter,
  };

  const buildRequest = (
    input: string | URL | Request,
    init: RequestInit | undefined,
  ): HarnessRequest => {
    // Single owned bridge between the undici-typed `new Request()` the stub
    // mints and the harness's public `HarnessRequest` (Bun's global shape).
    // Concentrating the cast here keeps the waiting set, predicates, and
    // `matchedRequests()` on one consistent request type downstream.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- type-only bridge between undici Request and the harness's Bun-global-shaped HarnessRequest
    const built = (
      input instanceof Request
        ? init === undefined
          ? input
          : new Request(input, init)
        : new Request(input instanceof URL ? input.toString() : input, init)
    ) as HarnessRequest;
    return built;
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
        bodyText: undefined,
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
        // scanWaitingSet rejects conflicting fetches itself before
        // throwing. If this fetch wasn't the conflict's settler, it
        // remains in the waiting set; swallow the throw. Otherwise
        // settle it with the error too so the caller's await rejects.
        if (!entry.settled) {
          entry.settled = true;
          const idx = waiting.indexOf(entry);
          if (idx >= 0) waiting.splice(idx, 1);
          reject(err);
        }
      }
      // If body-aware matchers are registered and this fetch did not
      // bind on the sync scan, schedule an async body-aware scan. The
      // promise is tracked on `inFlightBodyScans` so `run`/`advanceTo`
      // drain it before checking quiescence.
      if (!entry.settled) {
        triggerBodyAwareScan();
      }
    });
  };

  // Scheduler exposed via deps. With enableInferenceTimers false
  // (default) every `setTimeout` is a no-op — production timers stay
  // inert, which is what almost every test wants; with true, timers
  // are backed by the virtual clock and fire at virtual time.
  const noopCanceller = (): void => {
    /* no scheduled work to cancel */
  };
  // The inert scheduler still reports the virtual clock via `now()` so
  // tests that advance the clock for other purposes read a coherent time.
  const inertScheduler: Scheduler = {
    setTimeout: () => noopCanceller,
    now: () => clock.now(),
  };
  const scheduler: Scheduler =
    opts.enableInferenceTimers === true
      ? {
          setTimeout(callback, delayMs) {
            let cancelled = false;
            clock.schedule(clock.now() + delayMs, () => {
              if (cancelled) return;
              callback();
            });
            return () => {
              cancelled = true;
            };
          },
          now: () => clock.now(),
        }
      : inertScheduler;

  const deps: Dependencies = {
    fetch: stubFetch,
    scheduler,
    adapters: opts.adapters ?? createBuiltinRegistry(),
    [HarnessId]: harnessSymbol,
  };

  const assertDeps = (candidate: Dependencies): void => {
    const received = candidate[HarnessId];
    if (received !== harnessSymbol) {
      throw new WrongHarnessError(harnessSymbol, received);
    }
  };

  clock.onSyncCallbackError((err) => {
    // A scheduled callback threw synchronously; error every still-open
    // stream so the next test does not inherit dangling readers.
    // `forceError` is idempotent.
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
    // rather than hang; the error also surfaces at the `run`/`advanceTo`
    // call site.
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
    // Surface the first rejection; any additional ones are dropped.
    const [first] = inFlightErrors.splice(0, inFlightErrors.length);
    return first;
  };

  const drainInFlightSet = async (
    label: string,
    set: Set<Promise<void>>,
    startWall: number,
    wallClockBudgetMs: number,
  ): Promise<void> => {
    // Await the set until empty; its population may grow during a wait
    // (handlers scheduling more work, scans triggering follow-ups), so
    // re-read `set.size` each pass. Race each batch against a real-time
    // timer so a promise blocked on a real wall-clock timer surfaces as
    // a ClockWallClockOverrunError instead of hanging the test.
    while (set.size > 0) {
      if (wallClockBudgetMs === Infinity) {
        await Promise.all([...set]);
        continue;
      }
      const elapsed = performance.now() - startWall;
      const remaining = wallClockBudgetMs - elapsed;
      if (remaining <= 0) {
        throw new ClockWallClockOverrunError(
          `Harness.run exceeded wall-clock budget of ${String(wallClockBudgetMs)}ms while awaiting ${label} (elapsed=${String(elapsed)}ms)`,
          `${label}: ${String(set.size)}`,
        );
      }
      let timer: ReturnType<typeof setTimeout> | null = null;
      const budgetExpired = new Promise<"timeout">((resolve) => {
        timer = setTimeout(() => {
          resolve("timeout");
        }, remaining);
      });
      const allDone = Promise.all([...set]).then(() => "done" as const);
      const outcome = await Promise.race([allDone, budgetExpired]);
      if (timer !== null) clearTimeout(timer);
      if (outcome === "timeout") {
        const elapsedNow = performance.now() - startWall;
        throw new ClockWallClockOverrunError(
          `Harness.run exceeded wall-clock budget of ${String(wallClockBudgetMs)}ms while awaiting ${label} (elapsed=${String(elapsedNow)}ms)`,
          `${label}: ${String(set.size)}`,
        );
      }
    }
  };

  const drainInFlight = (
    startWall: number,
    wallClockBudgetMs: number,
  ): Promise<void> =>
    drainInFlightSet(
      "in-flight tool handlers",
      inFlightToolHandlers,
      startWall,
      wallClockBudgetMs,
    );

  const drainBodyScans = (
    startWall: number,
    wallClockBudgetMs: number,
  ): Promise<void> =>
    drainInFlightSet(
      "in-flight body-aware scans",
      inFlightBodyScans,
      startWall,
      wallClockBudgetMs,
    );

  const takeBodyScanError = (): unknown => {
    if (inFlightScanErrors.length === 0) return undefined;
    // Surface the first rejection; any additional ones are dropped,
    // mirroring `takeInFlightError`.
    const [first] = inFlightScanErrors.splice(0, inFlightScanErrors.length);
    return first;
  };

  const clearInFlightState = (): void => {
    inFlightToolHandlers.clear();
    inFlightErrors.length = 0;
    inFlightBodyScans.clear();
    inFlightScanErrors.length = 0;
  };

  const drainPendingWork = async (
    startWall: number,
    wallClockBudgetMs: number,
  ): Promise<boolean> => {
    // Drain in-flight tool handlers, then body-aware scans, surfacing
    // the first error from either. Returns false when nothing was
    // drained, at which point the clock is empty and quiescence can be
    // declared. Handlers first: they can register matchers that trigger
    // body scans, so the scans drain after in the same outer iteration.
    let drained = false;
    if (inFlightToolHandlers.size > 0) {
      drained = true;
      await drainInFlight(startWall, wallClockBudgetMs);
      const handlerErr = takeInFlightError();
      if (handlerErr !== undefined) throw handlerErr;
    }
    if (inFlightBodyScans.size > 0) {
      drained = true;
      await drainBodyScans(startWall, wallClockBudgetMs);
      const scanErr = takeBodyScanError();
      if (scanErr !== undefined) throw scanErr;
    }
    return drained;
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
        const scanErr = takeBodyScanError();
        if (scanErr !== undefined) throw scanErr;
        const drained = await drainPendingWork(startWall, wallClockBudgetMs);
        if (!drained) break;
        // Handlers and scans may have scheduled new work; loop so
        // `clock.run()` settles it before quiescence is declared.
      }
    } catch (err) {
      // One-shot harness: clear tracked in-flight work so a re-used
      // harness after a throw does not inherit stale state.
      clearInFlightState();
      throw err;
    }
    checkQuiescence();
  };

  const advanceTo = async (
    virtualMs: number,
    advanceOpts?: AdvanceOpts,
  ): Promise<void> => {
    // Wall-clock budget mirrors run()'s default; AdvanceOpts has no knob.
    const startWall = performance.now();
    try {
      for (;;) {
        await clock.advanceTo(virtualMs, advanceOpts);
        const err = takeInFlightError();
        if (err !== undefined) throw err;
        const scanErr = takeBodyScanError();
        if (scanErr !== undefined) throw scanErr;
        const drained = await drainPendingWork(
          startWall,
          DEFAULT_WALL_CLOCK_BUDGET_MS,
        );
        if (!drained) break;
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
      const inner = runInference({
        ...opts,
        deps,
        readMaterial: opts.readMaterial ?? DEFAULT_TEST_READ_MATERIAL,
      });
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
    // Cancel pending heap entries first: they stay on the heap but turn
    // into no-ops when popped, so the body never sees them. This is the
    // seq-ordering workaround — we cannot inject a lower seq into the
    // heap from outside the clock.
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
    // Reject any still-waiting fetches so awaiters don't hang; dispose
    // is a hard teardown, not a quiescence check.
    for (const wf of waiting) {
      if (wf.settled) continue;
      wf.settled = true;
      wf.reject(new Error("Harness fetch: harness has been disposed"));
    }
    waiting.length = 0;
    matcherTable.entries.length = 0;
    inFlightToolHandlers.clear();
    inFlightErrors.length = 0;
    inFlightBodyScans.clear();
    inFlightScanErrors.length = 0;
    abortAfterRegistrations.length = 0;
    // Resolve still-pending stall awaits so tests awaiting them past
    // dispose do not deadlock the runner.
    for (const reg of stallRegistrations) {
      reg.resolveAwaitAbort();
    }
    stallRegistrations.length = 0;
    lastToolDispatchByName.clear();
    matchedRequestsList.length = 0;
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
