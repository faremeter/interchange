// Waiter over a reactor's emitted-event stream. A reactor run ends with
// `reactor.done` (including on the fatal error path), so tests wait for
// that event rather than an interval; CONVENTIONS.md names this shape
// as the one to copy.

/**
 * Resolve once the stream emits `reactor.done`; throw if the stream
 * ends without it so the caller fails on the real cause. Typed
 * structurally so callers need not import the reactor's event union.
 */
export async function waitForReactorDone(
  stream: AsyncIterable<{ type: string }>,
): Promise<void> {
  for await (const event of stream) {
    if (event.type === "reactor.done") return;
  }
  throw new Error("reactor event stream ended before reactor.done");
}
