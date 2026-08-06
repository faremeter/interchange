/**
 * Stop a `Bun.serve` server, bounding the wait so teardown cannot hang.
 *
 * A server-initiated WebSocket close through Hono does not always fire the
 * server-side `onClose`, so Bun can keep counting the dropped connection as
 * live. Both forms of `server.stop()` then wait forever for that phantom
 * connection to drain. Test processes already own and close their tracked
 * handles, so this bound prevents the runtime bookkeeping bug from wedging
 * teardown while still giving normal shutdowns time to complete.
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
