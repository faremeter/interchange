export const DEFAULT_SIDECAR_OPERATION_TIMEOUT_MS = 120_000;

export type SidecarReconciliationContext = {
  readonly signal: AbortSignal;
  readonly leaseId: string;
};

export class SidecarOperationTimeoutError extends Error {
  constructor(operation: string, timeoutMs: number) {
    super(`${operation} timed out after ${String(timeoutMs)}ms`);
    this.name = "SidecarOperationTimeoutError";
  }
}

/** Stops waiting on cancellation or an optional deadline, even if work ignores the signal. */
export async function runSidecarOperation<T>(
  operation: string,
  timeoutMs: number | undefined,
  run: (signal: AbortSignal) => Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  if (
    timeoutMs !== undefined &&
    (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0)
  ) {
    throw new Error("Sidecar operation timeout must be a positive integer");
  }
  signal?.throwIfAborted();
  const controller = new AbortController();
  const cancel = () => {
    controller.abort(
      signal?.reason instanceof Error
        ? signal.reason
        : new Error(`${operation} cancelled`),
    );
  };
  signal?.addEventListener("abort", cancel, { once: true });
  let rejectAborted: (() => void) | undefined;
  const aborted = new Promise<never>((_resolve, reject) => {
    rejectAborted = () => {
      reject(controller.signal.reason);
    };
    controller.signal.addEventListener("abort", rejectAborted, { once: true });
  });
  const timer =
    timeoutMs === undefined
      ? undefined
      : setTimeout(() => {
          controller.abort(
            new SidecarOperationTimeoutError(operation, timeoutMs),
          );
        }, timeoutMs);
  try {
    return await Promise.race([
      Promise.resolve().then(() => {
        controller.signal.throwIfAborted();
        return run(controller.signal);
      }),
      aborted,
    ]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", cancel);
    if (rejectAborted !== undefined) {
      controller.signal.removeEventListener("abort", rejectAborted);
    }
  }
}
