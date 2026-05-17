import type { Clock } from "./clock";

declare const StreamIdBrand: unique symbol;
/**
 * Branded integer identifying a simulated stream within a single harness.
 * Allocated sequentially by `setupHarness`; the brand keeps the public
 * `abortBefore` signature from leaking a plain `number` that callers might
 * confuse with virtual milliseconds or other scalar identifiers.
 */
export type StreamId = number & { readonly [StreamIdBrand]: true };

/**
 * Mints a branded `StreamId` from a raw integer. The brand exists so that
 * downstream slices (`abortBefore` in particular) can name stream identities
 * in their public signatures without leaking a plain `number`.
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
 * harness fetch stub hands back as `Response.body`. Tests drive bytes into
 * the stream via `enqueue`/`enqueueAt`, terminate it with `closeAt`, or fail
 * it with `errorAt`. All scheduling is in terms of the harness's virtual
 * `clock` — bytes only land when the clock fires.
 *
 * Each call to `createSimulatedStream` allocates its own controller, captured
 * by closure. There is no module-level controller registry; the harness
 * tracks open streams in a per-harness `Set` for `dispose()` teardown.
 */
export type SimulatedStream = {
  readonly streamId: StreamId;
  readonly body: ReadableStream<Uint8Array>;
  enqueue(bytes: Uint8Array): void;
  enqueueAt(virtualMs: number, bytes: Uint8Array): void;
  closeAt(virtualMs: number): void;
  errorAt(virtualMs: number, err: unknown): void;
};

/**
 * Internal handle the harness uses to manage a `SimulatedStream` beyond its
 * public `enqueue`/`closeAt`/`errorAt` surface. Tests do not interact with
 * the handle directly; they hold the `stream` reference and let the harness
 * route disposal, abort, and pending-callback cancellation through the
 * handle. The handle's lifecycle is owned by the per-harness open-stream
 * registry maintained by `setupHarness`.
 */
export type SimulatedStreamHandle = {
  readonly stream: SimulatedStream;
  /**
   * Called by the harness `dispose()` to force-close any controller that
   * is still open. Safe to invoke multiple times; subsequent calls are
   * no-ops. The harness must remove the handle from its open-stream set
   * when `closeAt`/`errorAt` fire naturally so that `dispose()` only
   * touches streams that genuinely leaked past test completion.
   */
  forceClose(): void;
  /**
   * Whether the underlying controller has been closed/errored (either
   * naturally via a fired `closeAt`/`errorAt` or by `forceClose`).
   */
  isClosed(): boolean;
};

/**
 * Construction options for `createSimulatedStream`. The harness supplies
 * its own `clock`, the next `streamId` from its per-harness counter, an
 * `onTerminate` callback that removes the stream from the open-stream set
 * exactly once on the first close/error transition, and an optional
 * `onChunkFired` cross-cut for `scenario.abortAfter` wire-event observation.
 */
export type CreateSimulatedStreamOpts = {
  clock: Clock;
  streamId: StreamId;
  /**
   * Invoked exactly once the first time the stream transitions to a
   * terminal state (close or error). The harness uses this to remove the
   * handle from its open-stream set so `dispose()` does not attempt to
   * close an already-finished controller.
   */
  onTerminate: () => void;
};

/**
 * Builds a `SimulatedStream` paired with a `SimulatedStreamHandle`. The
 * stream's `ReadableStream` controller is captured by closure in `start()`;
 * before that callback runs (synchronously, per the Streams spec) the
 * scheduled callbacks throw rather than silently dropping bytes. All
 * scheduling routes through the supplied virtual `clock`, so chunks land
 * only when the harness drives `clock.run`/`advanceTo`.
 */
export function createSimulatedStream(
  opts: CreateSimulatedStreamOpts,
): SimulatedStreamHandle {
  const { clock, streamId, onTerminate } = opts;

  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  let closed = false;

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
    clock.schedule(virtualMs, function streamEnqueue() {
      if (closed) {
        throw new Error(
          `SimulatedStream(${String(streamId)}): enqueue after terminal state`,
        );
      }
      requireController().enqueue(bytes);
      clock.notifyActivity();
    });
  };

  const enqueue = (bytes: Uint8Array): void => {
    enqueueAt(clock.now(), bytes);
  };

  const closeAt = (virtualMs: number): void => {
    clock.schedule(virtualMs, function streamClose() {
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
    clock.schedule(virtualMs, function streamError() {
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

  const stream: SimulatedStream = {
    streamId,
    body,
    enqueue,
    enqueueAt,
    closeAt,
    errorAt,
  };

  const forceClose = (): void => {
    if (closed) return;
    const c = controller;
    closed = true;
    if (c !== null) {
      c.close();
      clock.notifyActivity();
    }
  };

  const isClosed = (): boolean => closed;

  return { stream, forceClose, isClosed };
}
