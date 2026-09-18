// Waiter over a reactor's emitted-event stream.
//
// A reactor run ends with a `reactor.done` event, including on the fatal
// error path, so a test that needs the run finished waits for that event
// rather than for an interval. CONVENTIONS.md names this shape as the one to
// copy; it lives here so it is imported rather than copied again.

/**
 * Resolve once the stream emits `reactor.done`.
 *
 * Throws if the stream ends without it: returning would resolve as though the
 * run had completed, and the caller would then fail on a later assertion
 * instead of on the real cause.
 *
 * Typed structurally so a caller need not import the reactor's event union to
 * await its terminal event.
 */
export async function waitForReactorDone(
  stream: AsyncIterable<{ type: string }>,
): Promise<void> {
  for await (const event of stream) {
    if (event.type === "reactor.done") return;
  }
  throw new Error("reactor event stream ended before reactor.done");
}
