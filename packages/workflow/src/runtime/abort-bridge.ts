// Bridging an outer abort signal into a local reaction.
//
// A bridge built only from the abort edge misses a signal that already fired,
// and every bridge here is built after an await, so the window is real. Check
// the level, not just the event. Consolidated here so the next bridge is a
// call, not a fresh chance to get it wrong.

/**
 * Run `onAbort` when `outer` aborts, including when it already has. Returns a
 * detach function; the listener is registered `once`.
 */
export function bridgeAbort(
  outer: AbortSignal,
  onAbort: () => void,
): () => void {
  if (outer.aborted) {
    onAbort();
    return () => {
      /* nothing was attached */
    };
  }
  outer.addEventListener("abort", onAbort, { once: true });
  return () => {
    outer.removeEventListener("abort", onAbort);
  };
}
