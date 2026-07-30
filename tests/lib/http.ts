// HTTP readiness polling shared by the hub-subprocess harness and the
// dev orchestrator.
//
// A connection failure or a 5xx means "still starting"; any routable
// response (200, 401, 404) proves the listener is up. The last probe
// failure rides along in the timeout error so a hung start stays
// diagnosable.

/**
 * Poll `url` until it responds with a status below 500 or `timeoutMs`
 * elapses. `intervalMs` sets the probe cadence.
 */
export async function waitForHTTP(
  url: string,
  timeoutMs: number,
  intervalMs = 500,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: Error | null = null;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.status < 500) return;
      lastErr = new Error(`server returned status ${res.status}`);
    } catch (e) {
      lastErr = e instanceof Error ? e : new Error(String(e));
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(
    `${url} did not become ready within ${timeoutMs}ms (last: ${lastErr?.message ?? "no probe"})`,
  );
}
