import { HarnessId, type Dependencies } from "@interchange/inference";

import {
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
  type RequestPredicate,
  type Scenario,
  type WaitingFetch,
  type WhenRequestMatchesOpts,
} from "./scenario";
import {
  createSimulatedStream,
  toStreamId,
  type SimulatedStream,
  type SimulatedStreamHandle,
  type StreamId,
} from "./simulated-stream";

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
  const mintedStreams = new WeakSet<SimulatedStream>();

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
    });
    handleRef.current = handle;
    openStreams.add(handle);
    streamIdToHandle.set(streamId, handle);
    mintedStreams.add(handle.stream);
    return handle.stream;
  };

  const routeWaitingFetch = (
    wf: WaitingFetch,
    stream: SimulatedStream,
    opts: WhenRequestMatchesOpts | undefined,
  ): void => {
    wf.settled = true;
    const idx = waiting.indexOf(wf);
    if (idx >= 0) waiting.splice(idx, 1);
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
    if (!mintedStreams.has(responseStream)) {
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

  const scenario: Scenario = {
    createStream,
    whenRequestMatches,
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

  clock.onSyncCallbackError(() => {
    // `forceClose` is idempotent and `dispose()` already tolerates streams
    // that errored synchronously inside a fired callback. Reserved for a
    // future slice if reactor error paths need to attribute errors to
    // specific streams.
    return;
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

  const run = async (runOpts?: RunOpts): Promise<void> => {
    await clock.run(runOpts);
    checkQuiescence();
  };

  const advanceTo = async (
    virtualMs: number,
    advanceOpts?: AdvanceOpts,
  ): Promise<void> => {
    await clock.advanceTo(virtualMs, advanceOpts);
    checkQuiescence();
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
  };

  return {
    clock,
    deps,
    scenario,
    assertDeps,
    run,
    advanceTo,
    dispose,
  };
}
