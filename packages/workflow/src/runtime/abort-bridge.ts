// Bridging an outer abort signal into a local reaction.
//
// An `AbortSignal` carries both an edge (the `abort` event) and a level
// (`.aborted`). A bridge built only from the edge misses a signal that already
// fired, and the runtime is full of places where it can have: every one of
// these bridges is built after an await, usually a durable commit or flush, so
// the window between the caller's decision to abort and the bridge existing is
// real rather than theoretical.
//
// The failure is quiet. Nothing throws; the local controller simply never
// aborts. Four of these bridges were written without the level check: three
// left something waiting forever, and the fourth let an action handler run on
// behalf of a cancelled run, which completes rather than hangs but spends the
// effect anyway.
//
// Consolidated here so the next bridge is a call rather than a fresh chance to
// get it wrong. Three sites stay hand-written and are not this shape: one also
// consults drain and returns early on it, and two throw rather than react.

/**
 * Run `onAbort` when `outer` aborts, including when it already has.
 *
 * Returns a detach function for callers whose reaction outlives the thing it
 * guards. Callers that hold the bridge for the lifetime of the surrounding
 * operation can ignore it -- the listener is registered `once`.
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
