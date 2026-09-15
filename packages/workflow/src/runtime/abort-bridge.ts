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
// aborts. Five bridges were written without the level check. Two left
// something waiting forever: `parkOnSignalResult` awaited a signal nobody
// would send, and the local child spawner left the child uncancelled while
// its parent awaited a terminal that never came. Two let work proceed for a
// cancelled run: `runStep` handed the invoker a signal that could never fire,
// so an agent send spent inference until the step's timeout, and
// `createDefaultActionInvoker` ran a handler whose effect nothing downstream
// can undo. `runAction` is the fifth and joins those two -- the signal it
// builds is what the action invoker's entry refusal reads, so an edge-only
// bridge there hides the abort from the handler and defeats that refusal
// with it.
//
// Consolidated here so the next bridge is a call rather than a fresh chance to
// get it wrong. Four sites stay hand-written and are not this shape:
// `createStepAbort` also consults drain and returns early on it, and
// `waitForTimer`, `createInMemorySpawnChild` and `createDefaultActionInvoker`
// throw rather than react. The spawner's listener is the one outer-abort
// bridge here still built from the edge alone; it is safe because nothing
// awaits between its entry throw and the registration, so the window a level
// check closes elsewhere never opens there. Its exclusion above names that
// entry throw, not the listener.

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
