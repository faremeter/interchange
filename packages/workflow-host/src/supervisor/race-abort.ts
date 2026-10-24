/**
 * Settle with `wait`, or with `undefined` once `signal` aborts, and drop the
 * abort listener either way. `{ once: true }` removes a listener only when
 * the abort fires, so a loop that races each wake against a long-lived
 * signal would otherwise keep one listener per wake.
 */
export async function raceAbort<T>(
  signal: AbortSignal,
  wait: Promise<T>,
): Promise<T | undefined> {
  if (signal.aborted) return undefined;
  const aborted = Promise.withResolvers<undefined>();
  const onAbort = (): void => {
    aborted.resolve(undefined);
  };
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    return await Promise.race([wait, aborted.promise]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}
