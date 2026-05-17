import { HarnessId, type Dependencies } from "@interchange/inference";

import { createClock, type Clock } from "./clock";
import { WrongHarnessError } from "./errors";
import {
  createSimulatedStream,
  toStreamId,
  type SimulatedStream,
  type SimulatedStreamHandle,
  type StreamId,
} from "./simulated-stream";

/**
 * Scenario seam exposed by the harness. This slice (1a) gives tests a
 * `createStream()` factory plus a `nextResponse(stream)` arming method —
 * either pre-arms the next fetch with a specific `SimulatedStream`.
 *
 * **This is a placeholder.** Slice 2a replaces this seam with
 * `whenRequestMatches(predicate, responseStream)` — a matcher table the
 * fetch stub walks per-request. The shape below is deliberately minimal so
 * 2a can swap it without churning consumers' imports.
 */
export type HarnessScenario = {
  /**
   * Mint a new `SimulatedStream`. The stream is registered with the harness
   * for `dispose()` teardown automatically. Tests use this to construct a
   * response body, then either drive bytes into it via the stream's own
   * methods or pre-arm it via `nextResponse`.
   */
  createStream(): SimulatedStream;
  /**
   * Pre-arm the next `deps.fetch(...)` call with `stream`. The harness's
   * fetch stub will consume the armed stream FIFO; calling `nextResponse`
   * twice before any fetch arms two streams in order. If a fetch occurs
   * without an armed stream, the fetch stub throws — this slice does no
   * matcher routing.
   */
  nextResponse(stream: SimulatedStream): void;
};

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
  readonly scenario: HarnessScenario;
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
  const armedStreams: SimulatedStream[] = [];
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

  const nextResponse = (stream: SimulatedStream): void => {
    if (disposed) {
      throw new Error(
        "Harness.scenario.nextResponse: harness has been disposed",
      );
    }
    if (!mintedStreams.has(stream)) {
      throw new Error(
        `Harness.scenario.nextResponse: stream ${String(stream.streamId)} was not minted by this harness`,
      );
    }
    armedStreams.push(stream);
  };

  const scenario: HarnessScenario = {
    createStream,
    nextResponse,
  };

  // `runInference` invokes `deps.fetch(url, init)` exactly once per call. The
  // stub returns a `Response` whose body is the FIFO-next armed stream's
  // body. Matcher-table routing arrives in slice 2a; until then, a fetch
  // without an armed stream is a programmer error.
  const stubFetch = async (
    _input: string | URL | Request,
    _init?: RequestInit,
  ): Promise<Response> => {
    if (disposed) {
      throw new Error("Harness fetch: harness has been disposed");
    }
    const next = armedStreams.shift();
    if (next === undefined) {
      throw new Error(
        "Harness fetch: no armed SimulatedStream; call " +
          "scenario.nextResponse(stream) before triggering a fetch " +
          "(matcher-table routing arrives in slice 2a)",
      );
    }
    return new Response(next.body, {
      status: 200,
      headers: { "content-type": "text/event-stream" },
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

  // If a scheduled callback (e.g. `streamEnqueue`/`streamClose`) throws
  // synchronously inside `advanceTo`/`run`, the clock surfaces the error via
  // its `onSyncCallbackError` hook before propagating. Use that to mark the
  // offending stream terminated so `dispose()` does not double-close it.
  clock.onSyncCallbackError(() => {
    // The hook fires per-throwing-callback, but we do not have a back-pointer
    // from the error to the stream. `dispose()` already tolerates already-
    // terminated handles (forceClose is idempotent), so the hook is a no-op
    // for now. Slice 2a/3a may enrich this when reactor error paths arrive.
    return;
  });

  const dispose = (): void => {
    if (disposed) return;
    disposed = true;
    for (const handle of openStreams) {
      handle.forceClose();
    }
    openStreams.clear();
    armedStreams.length = 0;
    streamIdToHandle.clear();
  };

  return {
    clock,
    deps,
    scenario,
    assertDeps,
    dispose,
  };
}
