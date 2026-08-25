/**
 * Run `body`, then always run `cleanup`, without letting a `cleanup`
 * failure mask a `body` failure.
 *
 * When `body` throws, `cleanup` still runs and a `cleanup` failure is
 * handed to `onCleanupErrorAfterBodyError` (to log) and then dropped, so
 * the original `body` error is what propagates. When `body` succeeds, a
 * `cleanup` failure propagates -- there is no primary error to protect, so
 * a failing teardown is the error worth surfacing.
 *
 * This exists so teardown in a `finally` (agent close, warm-cache
 * eviction) can surface its own failure without a `throw` inside a
 * `finally` block, which `no-unsafe-finally` forbids precisely because it
 * silently swallows the in-flight exception -- the masking bug this guards
 * against.
 */
export async function runBodyThenCleanup<T>(
  body: () => Promise<T>,
  cleanup: () => Promise<void>,
  onCleanupErrorAfterBodyError: (cause: unknown) => void,
): Promise<T> {
  let outcome: { ok: true; value: T } | { ok: false; error: unknown };
  try {
    outcome = { ok: true, value: await body() };
  } catch (error) {
    outcome = { ok: false, error };
  }

  try {
    await cleanup();
  } catch (cleanupError) {
    if (outcome.ok) {
      throw cleanupError;
    }
    onCleanupErrorAfterBodyError(cleanupError);
  }

  if (!outcome.ok) {
    throw outcome.error;
  }
  return outcome.value;
}
