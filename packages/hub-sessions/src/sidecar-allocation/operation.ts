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

/**
 * The current connection of a deployment's sidecar did not report the
 * deployment, though its first deploy completed: the sidecar no longer holds
 * it, and the Hub does not deploy an address twice.
 */
export class SidecarDeploymentMissingError extends Error {
  constructor(allocationId: string, generation: number) {
    super(
      `The sidecar of allocation ${allocationId} generation ${String(generation)} no longer holds its deployment`,
    );
    this.name = "SidecarDeploymentMissingError";
  }
}

/**
 * The current connection of a deployment's sidecar reported the deployment
 * stopped though the Hub did not stop it: its workflow child ended itself, or
 * the sidecar could not restore it. The message is the sidecar's error.
 */
export class SidecarDeploymentStoppedError extends Error {
  constructor(error: string) {
    super(error);
    this.name = "SidecarDeploymentStoppedError";
  }
}

/**
 * A first deploy failed before its deploy frame was sent, so nothing ran on
 * the sidecar and the deploy can be tried again.
 */
export class SidecarFirstDeployError extends Error {
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = "SidecarFirstDeployError";
  }
}

/**
 * A deployment its sidecar reported stopped committed history the Hub does
 * not hold yet; the Hub looks again at `retryAt` while it still waits.
 */
export class SidecarDeploymentHistoryPendingError extends Error {
  readonly retryAt: Date;

  constructor(unreceived: string, retryAt: Date) {
    super(`Waiting for history a stopped deployment committed: ${unreceived}`);
    this.name = "SidecarDeploymentHistoryPendingError";
    this.retryAt = retryAt;
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
  // A deadline must never keep the process alive on its own: in-flight work
  // always holds its own handles, and a bare 120s timer would stall teardown
  // and test runners after everything else settled.
  timer?.unref?.();
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
