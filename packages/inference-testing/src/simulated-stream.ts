import type { Clock } from "./clock";

declare const StreamIdBrand: unique symbol;
/**
 * Branded integer identifying a simulated stream within a single harness.
 * The brand keeps `abortBefore` from leaking a plain `number` that callers
 * might confuse with virtual milliseconds or other scalar ids.
 */
export type StreamId = number & { readonly [StreamIdBrand]: true };

/**
 * Mints a branded `StreamId` from a raw integer.
 */
export function toStreamId(n: number): StreamId {
  if (!Number.isInteger(n) || n < 0) {
    throw new Error(
      `toStreamId: id must be a non-negative integer, got ${String(n)}`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- branding a validated integer; the brand is phantom-only
  return n as StreamId;
}

/**
 * A `SimulatedStream` exposes a `ReadableStream<Uint8Array>` `body` that the
 * harness fetch stub hands back as `Response.body`. Tests drive bytes via
 * `enqueue`/`enqueueAt`, terminate with `closeAt`, or fail with `errorAt`;
 * all scheduling is on the harness's virtual `clock`.
 *
 * Each `createSimulatedStream` call allocates its own controller, captured
 * by closure; the harness tracks open streams in a per-harness `Set`.
 */
export type SimulatedStream = {
  readonly streamId: StreamId;
  readonly body: ReadableStream<Uint8Array>;
  enqueue(bytes: Uint8Array): void;
  enqueueAt(virtualMs: number, bytes: Uint8Array): void;
  closeAt(virtualMs: number): void;
  errorAt(virtualMs: number, err: unknown): void;
  /**
   * Schedules a contiguous batch of chunks at evenly-spaced virtual times
   * and, by default, closes the stream one virtual ms after the last
   * chunk — collapsing the common "enqueueAt in a loop then closeAt"
   * pattern into one call.
   *
   * The first chunk lands at `opts.startAt`; each subsequent chunk lands
   * `opts.stepMs` virtual ms later (default 1ms). `opts.autoClose` (default
   * true) closes at last-chunk time plus `stepMs`; pass false to enqueue
   * more chunks afterward and close manually. An empty `chunks` array is a
   * no-op: nothing is scheduled and the stream is not auto-closed.
   */
  enqueueAll(chunks: readonly Uint8Array[], opts: EnqueueAllOpts): void;
};

/**
 * Options for `SimulatedStream.enqueueAll`.
 */
export type EnqueueAllOpts = {
  /** Virtual ms at which the first chunk lands. */
  readonly startAt: number;
  /** Virtual ms gap between successive chunks. Defaults to 1. */
  readonly stepMs?: number;
  /**
   * Whether to close the stream automatically one `stepMs` after the last
   * chunk. Defaults to true; set to false when the test wants to enqueue
   * more chunks afterward and close the stream manually.
   */
  readonly autoClose?: boolean;
};

/**
 * Internal handle the harness uses to manage a `SimulatedStream` beyond its
 * public `enqueue`/`closeAt`/`errorAt` surface. Tests hold the `stream`
 * reference and let the harness route disposal, abort, and pending-callback
 * cancellation through the handle; the per-harness open-stream registry
 * owns its lifecycle.
 */
export type SimulatedStreamHandle = {
  readonly stream: SimulatedStream;
  /**
   * Force-close an open controller and invoke `onTerminate` if the stream
   * has not yet gone terminal. Idempotent. Used by `dispose()` for streams
   * that leaked past test completion.
   */
  forceClose(): void;
  /**
   * Force the controller into the errored state and invoke `onTerminate` if
   * not yet terminal. Used for per-call abort isolation, `abortBefore`, and
   * the `clock.onSyncCallbackError` hook. Idempotent.
   */
  forceError(err: unknown): void;
  /**
   * Cancel every pending scheduled callback for this stream that has not
   * fired. Cancelled callbacks stay on the clock heap but become no-ops
   * when popped. Used by `harness.abortBefore` to suppress already-
   * scheduled chunks ahead of an abort.
   */
  cancelPending(): void;
  /**
   * Whether the controller has been closed/errored (naturally or via
   * `forceClose`/`forceError`).
   */
  isClosed(): boolean;
};

/**
 * Event fired by `SimulatedStream` when a previously-scheduled wire byte
 * chunk lands in the controller. The harness uses this to drive
 * `scenario.abortAfter` matchers.
 */
export type ChunkFiredEvent = {
  readonly streamId: StreamId;
  readonly virtualMs: number;
  readonly bytes: Uint8Array;
};

/**
 * Construction options for `createSimulatedStream`. The harness supplies
 * its own `clock`, the next `streamId`, an `onTerminate` callback that
 * removes the stream from the open-stream set exactly once on the first
 * close/error transition, and an optional `onChunkFired` cross-cut for
 * `scenario.abortAfter` wire-event observation.
 */
export type CreateSimulatedStreamOpts = {
  clock: Clock;
  streamId: StreamId;
  /**
   * Invoked exactly once the first time the stream goes terminal (close or
   * error); the harness removes the handle from its open-stream set.
   */
  onTerminate: () => void;
  /**
   * Invoked synchronously each time a scheduled chunk is delivered to the
   * controller; the harness subscribes here for `scenario.abortAfter`.
   */
  onChunkFired?: (event: ChunkFiredEvent) => void;
};

type PendingEntry = {
  cancelled: boolean;
};

/**
 * Builds a `SimulatedStream` paired with a `SimulatedStreamHandle`. The
 * stream's `ReadableStream` controller is captured by closure in `start()`;
 * before that callback runs (synchronously, per the Streams spec) the
 * scheduled callbacks throw rather than silently dropping bytes. All
 * scheduling routes through the supplied virtual `clock`.
 */
export function createSimulatedStream(
  opts: CreateSimulatedStreamOpts,
): SimulatedStreamHandle {
  const { clock, streamId, onTerminate, onChunkFired } = opts;

  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  let closed = false;

  // Tracks every scheduled enqueue/close/error callback that has not yet
  // fired. `harness.abortBefore` flips each entry's `cancelled` flag so
  // the clock pops them as no-ops. Entries stay on the clock heap; the
  // callback closures check the flag.
  const pending = new Set<PendingEntry>();

  const body = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
    },
  });

  const requireController = (): ReadableStreamDefaultController<Uint8Array> => {
    if (controller === null) {
      throw new Error(
        "SimulatedStream: controller not yet initialized; " +
          "this indicates the ReadableStream start() callback has not " +
          "run, which violates the Streams spec",
      );
    }
    return controller;
  };

  const markTerminated = (): void => {
    if (closed) return;
    closed = true;
    onTerminate();
  };

  const enqueueAt = (virtualMs: number, bytes: Uint8Array): void => {
    if (!(bytes instanceof Uint8Array)) {
      throw new Error("SimulatedStream.enqueueAt: bytes must be a Uint8Array");
    }
    const entry: PendingEntry = { cancelled: false };
    pending.add(entry);
    clock.schedule(virtualMs, function streamEnqueue() {
      pending.delete(entry);
      if (entry.cancelled) return;
      if (closed) {
        throw new Error(
          `SimulatedStream(${String(streamId)}): enqueue after terminal state`,
        );
      }
      requireController().enqueue(bytes);
      clock.notifyActivity();
      if (onChunkFired !== undefined) {
        onChunkFired({ streamId, virtualMs: clock.now(), bytes });
      }
    });
  };

  const enqueue = (bytes: Uint8Array): void => {
    enqueueAt(clock.now(), bytes);
  };

  const closeAt = (virtualMs: number): void => {
    const entry: PendingEntry = { cancelled: false };
    pending.add(entry);
    clock.schedule(virtualMs, function streamClose() {
      pending.delete(entry);
      if (entry.cancelled) return;
      if (closed) {
        throw new Error(
          `SimulatedStream(${String(streamId)}): closeAt after terminal state`,
        );
      }
      requireController().close();
      clock.notifyActivity();
      markTerminated();
    });
  };

  const errorAt = (virtualMs: number, err: unknown): void => {
    const entry: PendingEntry = { cancelled: false };
    pending.add(entry);
    clock.schedule(virtualMs, function streamError() {
      pending.delete(entry);
      if (entry.cancelled) return;
      if (closed) {
        throw new Error(
          `SimulatedStream(${String(streamId)}): errorAt after terminal state`,
        );
      }
      requireController().error(err);
      clock.notifyActivity();
      markTerminated();
    });
  };

  const enqueueAll = (
    chunks: readonly Uint8Array[],
    opts: EnqueueAllOpts,
  ): void => {
    if (chunks.length === 0) return;
    const stepMs = opts.stepMs ?? 1;
    if (!Number.isFinite(stepMs) || stepMs < 0) {
      throw new Error(
        `SimulatedStream.enqueueAll: stepMs must be a non-negative finite number, got ${String(stepMs)}`,
      );
    }
    const autoClose = opts.autoClose ?? true;
    let when = opts.startAt;
    for (const chunk of chunks) {
      enqueueAt(when, chunk);
      when += stepMs;
    }
    if (autoClose) {
      closeAt(when);
    }
  };

  const stream: SimulatedStream = {
    streamId,
    body,
    enqueue,
    enqueueAt,
    closeAt,
    errorAt,
    enqueueAll,
  };

  const forceClose = (): void => {
    if (closed) return;
    const c = controller;
    if (c !== null) {
      c.close();
      clock.notifyActivity();
    }
    markTerminated();
  };

  const forceError = (err: unknown): void => {
    if (closed) return;
    const c = controller;
    if (c !== null) {
      c.error(err);
    }
    markTerminated();
  };

  const cancelPending = (): void => {
    for (const entry of pending) {
      entry.cancelled = true;
    }
    pending.clear();
  };

  const isClosed = (): boolean => closed;

  return {
    stream,
    forceClose,
    forceError,
    cancelPending,
    isClosed,
  };
}
