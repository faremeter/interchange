/**
 * Stop a `Bun.serve` server, bounding the wait so teardown cannot hang.
 * A server-initiated WebSocket close through Hono does not always fire
 * the server-side `onClose`, so Bun can count the dropped connection as
 * live and `server.stop()` waits forever for it to drain; this bound
 * still gives normal shutdowns time to complete.
 */
export async function stopServerBounded(
  server: ReturnType<typeof Bun.serve>,
): Promise<void> {
  const STOP_TIMEOUT_MS = 1_000;
  await Promise.race([
    server.stop(true),
    new Promise<void>((resolve) => setTimeout(resolve, STOP_TIMEOUT_MS)),
  ]);
}
