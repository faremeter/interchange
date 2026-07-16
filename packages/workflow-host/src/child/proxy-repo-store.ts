// Child-side proxy `RepoStore` for the workflow-run repo.
//
// Wraps a bare read-only substrate handle (the child constructs one
// against the shared on-disk data dir for `getRepoDir` + other read
// paths) and intercepts `writeTreePreservingPrefix` so the write is
// proxied over the control IPC into the supervisor's substrate. The
// supervisor is the sole writer of the workflow-run repo's ref; the
// child has no write authority and would race the supervisor's
// claim-check writes if it opened its own.
//
// Subscription fan-out: `RepoStore.subscribe` is a local-process
// in-memory subscriber pattern; the bare-store's `subscribe` only
// fires when a write lands on THAT particular substrate instance. The
// supervisor's writes against its own substrate do not reach the
// child's bare-store subscribers. The proxy therefore maintains its
// own per-ref subscriber list and synthesizes a `ref.updated` event to
// every subscriber after each successful proxied write -- the on-disk
// repo already carries the new commit (the supervisor's substrate
// commits before responding to the IPC), so subscribers that follow
// up with a tree read (via `subscribeKind`'s `getRepoDir` +
// `readBlobAtCommit` path) see the prospective tree's bytes.
//
// Methods that mutate state via paths other than
// `writeTreePreservingPrefix` (`initRepo`, `writeTree`, `receivePack`)
// throw on call. The workflow-host runtime body and adapters do not
// invoke these against the workflow-run repo proxy today; a future
// caller that tries to surfaces a structured failure rather than a
// silent disk write that would corrupt the single-writer invariant.

import type {
  InitRepoOpts,
  Principal,
  RepoId,
  RepoStore,
  WriteResult,
} from "@intx/hub-sessions/substrate";

import type { ChildSubstrateWriteBridge } from "./substrate-write-bridge";

type WriteTreeArgs = Parameters<RepoStore["writeTree"]>[3];
type WriteTreePreservingPrefixArgs = Parameters<
  RepoStore["writeTreePreservingPrefix"]
>[3];
type WriteTreeDeltaArgs = Parameters<RepoStore["writeTreeDelta"]>[3];

type SubscribeOpts = Parameters<RepoStore["subscribe"]>[3];

export interface CreateProxyWorkflowRunRepoStoreOpts {
  /**
   * Bare substrate handle the child opens against the shared on-disk
   * data dir. Used for the read-only methods that consult the
   * substrate's local state -- `getRepoDir` (path computation, no
   * I/O), `resolveRef`, `listRefs`, `resolveHead`, `openCommittedReads`,
   * `createPack`. The bare store is never used as a writer here; its
   * `writeTreePreservingPrefix` / `writeTree` / `receivePack` are not
   * reachable through this proxy.
   */
  bareStore: RepoStore;
  /**
   * Substrate-write bridge that forwards proxied writes over the
   * control IPC into the supervisor.
   */
  bridge: ChildSubstrateWriteBridge;
  /**
   * Workflow-run repo id this proxy services. Used as a guard: writes
   * targeting any other repo id surface a structured failure so a
   * stray call against a non-workflow-run repo does not silently land
   * in the wrong substrate.
   */
  workflowRunRepoId: RepoId;
}

type SubscriberState = {
  buffer: { seq: number; event: unknown }[];
  waiter:
    | ((value: IteratorResult<{ seq: number; event: unknown }>) => void)
    | null;
  closed: boolean;
  signal: AbortSignal;
};

export function createProxyWorkflowRunRepoStore(
  opts: CreateProxyWorkflowRunRepoStoreOpts,
): RepoStore {
  const { bareStore, bridge, workflowRunRepoId } = opts;

  // Per-ref subscriber lists. The proxy fans `ref.updated` events to
  // every subscriber on the ref after a successful proxied write so
  // a `subscribeKind` loop watching this ref (the signal channel's
  // `awaitNext` is the canonical consumer) wakes up.
  const subscribers = new Map<string, Set<SubscriberState>>();

  // Per-ref last-seen sha so the synthesized `ref.updated` carries the
  // matching `oldSha`. Subscriber semantics in `subscribeKind` use the
  // oldSha to enumerate commits added on top of the prior tip.
  const lastSha = new Map<string, string | null>();

  function refKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }

  function notifyRefUpdate(
    repoId: RepoId,
    ref: string,
    oldSha: string | null,
    newSha: string,
  ): void {
    const key = refKey(repoId, ref);
    const set = subscribers.get(key);
    if (set === undefined) return;
    const event = {
      type: "ref.updated" as const,
      ref,
      oldSha,
      newSha,
    };
    // The substrate's subscribe contract emits one entry per commit
    // with the seq value derived at commit time. The proxy does not
    // know the canonical seq the substrate assigned (the substrate
    // computes it from the ref's history); the field is informational
    // for the subscribe iterator's consumers. `subscribeKind` does
    // not consult the value -- it walks the commit tree from `oldSha`
    // to `newSha` itself -- so any monotonic value preserves the
    // downstream contract. Use a per-ref monotonic counter.
    const seq = (lastRefSeq.get(key) ?? 0) + 1;
    lastRefSeq.set(key, seq);
    for (const sub of set) {
      if (sub.closed) continue;
      const entry = { seq, event };
      if (sub.waiter !== null) {
        const w = sub.waiter;
        sub.waiter = null;
        w({ value: entry, done: false });
        continue;
      }
      sub.buffer.push(entry);
    }
  }

  const lastRefSeq = new Map<string, number>();

  return {
    initRepo: (_repoId: RepoId, _initOpts?: InitRepoOpts): Promise<void> => {
      throw new Error(
        "workflow-child proxy substrate: initRepo is not supported (writes are proxied to the supervisor)",
      );
    },
    writeTree: (
      _principal: Principal,
      _repoId: RepoId,
      _ref: string,
      _content: WriteTreeArgs,
    ): Promise<WriteResult> => {
      throw new Error(
        "workflow-child proxy substrate: writeTree is not supported (writes are proxied to the supervisor)",
      );
    },
    receivePack: (
      _principal: Principal,
      _repoId: RepoId,
      _ref: string,
      _pack: Uint8Array,
      _commitSha: string,
      _expectedOldSha: string | null,
    ): Promise<void> => {
      throw new Error(
        "workflow-child proxy substrate: receivePack is not supported (writes are proxied to the supervisor)",
      );
    },
    writeTreeDelta: (
      _principal: Principal,
      _repoId: RepoId,
      _ref: string,
      _args: WriteTreeDeltaArgs,
    ): Promise<WriteResult> => {
      throw new Error(
        "workflow-child proxy substrate: writeTreeDelta is not supported (claim-check writes run supervisor-side)",
      );
    },
    async writeTreePreservingPrefix(
      _principal: Principal,
      repoId: RepoId,
      ref: string,
      args: WriteTreePreservingPrefixArgs,
    ): Promise<WriteResult> {
      if (
        repoId.kind !== workflowRunRepoId.kind ||
        repoId.id !== workflowRunRepoId.id
      ) {
        throw new Error(
          `workflow-child proxy substrate: writeTreePreservingPrefix targeting ${repoId.kind}/${repoId.id} is not supported (proxy services ${workflowRunRepoId.kind}/${workflowRunRepoId.id})`,
        );
      }
      const key = refKey(repoId, ref);
      const priorSha = lastSha.has(key)
        ? (lastSha.get(key) ?? null)
        : await bareStore.resolveRef(_principal, repoId, ref);
      const result = await bridge.submit({
        repoId: { kind: repoId.kind, id: repoId.id },
        ref,
        preservePrefix: args.preservePrefix,
        message: args.message,
        merge: args.merge,
      });
      lastSha.set(key, result.commitSha);
      notifyRefUpdate(repoId, ref, priorSha, result.commitSha);
      // The terminal signal is consumed supervisor-side (where the real
      // substrate write happens); the child-proxied result carries only
      // the commit, so report no terminal runs to the runtime body.
      return { commitSha: result.commitSha, newlyTerminalRuns: [] };
    },
    createPack: bareStore.createPack.bind(bareStore),
    commitPackedTip: bareStore.commitPackedTip.bind(bareStore),
    resolveRef: bareStore.resolveRef.bind(bareStore),
    listRefs: bareStore.listRefs.bind(bareStore),
    resolveHead: bareStore.resolveHead.bind(bareStore),
    getRepoDir: bareStore.getRepoDir.bind(bareStore),
    openCommittedReads: bareStore.openCommittedReads.bind(bareStore),
    subscribe(
      _principal: Principal,
      repoId: RepoId,
      ref: string,
      subOpts: SubscribeOpts,
    ): AsyncIterableIterator<{ seq: number; event: unknown }> {
      // Synthesizing the subscribe surface in the proxy: the bare
      // store's `subscribe` would only fire from its own writes, but
      // the writes for this ref happen in the supervisor's address
      // space. Subscribers attached here receive events whenever the
      // proxy's `writeTreePreservingPrefix` returns successfully.
      //
      // `from: { seq }` replay against historical commits is not
      // emitted here today: the runtime body's signal-channel and
      // similar consumers attach with `from: "head"` so only events
      // committed after subscription fire. A `from: { seq }` caller
      // (a resume path that wants to replay) would currently miss
      // historical commits -- the bare store's `subscribe` is the
      // path that supports replay today, and resume code can call it
      // directly if needed.
      const key = refKey(repoId, ref);
      let set = subscribers.get(key);
      if (set === undefined) {
        set = new Set<SubscriberState>();
        subscribers.set(key, set);
      }
      const sub: SubscriberState = {
        buffer: [],
        waiter: null,
        closed: false,
        signal: subOpts.signal,
      };
      set.add(sub);
      const cleanup = (): void => {
        sub.closed = true;
        const current = subscribers.get(key);
        if (current !== undefined) {
          current.delete(sub);
          if (current.size === 0) subscribers.delete(key);
        }
        if (sub.waiter !== null) {
          const w = sub.waiter;
          sub.waiter = null;
          w({ value: undefined, done: true });
        }
      };
      const onAbort = (): void => {
        cleanup();
      };
      if (subOpts.signal.aborted) {
        cleanup();
      } else {
        subOpts.signal.addEventListener("abort", onAbort, { once: true });
      }
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        next(): Promise<IteratorResult<{ seq: number; event: unknown }>> {
          if (sub.closed) {
            return Promise.resolve({ value: undefined, done: true });
          }
          if (sub.buffer.length > 0) {
            const next = sub.buffer.shift();
            if (next === undefined) {
              return Promise.resolve({ value: undefined, done: true });
            }
            return Promise.resolve({ value: next, done: false });
          }
          return new Promise((resolve) => {
            sub.waiter = resolve;
          });
        },
        return(): Promise<IteratorResult<{ seq: number; event: unknown }>> {
          cleanup();
          return Promise.resolve({
            value: undefined,
            done: true,
          });
        },
      };
    },
  };
}
