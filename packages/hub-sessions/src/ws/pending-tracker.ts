// Generic pending-round-trip tracker for hub→sidecar requests.
//
// `sidecar-handler.ts` runs five request/response round-trips over the same
// websocket (session requests, agent deploys, pack transfers, undeploys,
// workflow probes). Each used to hand-roll the same lifecycle: register an
// entry in a Map keyed by request/transfer/address, arm a timeout that
// settles the entry, settle it early when the matching reply frame arrives,
// and sweep every entry owned by a connection when that connection drops.
// The sites differ only in the key, the resolved value, and the per-site
// cleanup their resolve/reject closures capture — the map + timer lifecycle
// is identical. `PendingTracker` owns that lifecycle so timeouts and
// disconnect sweeps behave uniformly and a fix to either lands in one place.

// Minimal handle so the router doesn't depend on a specific WebSocket impl.
export type WsHandle = {
  send(data: string): void;
  close(): void;
};

/**
 * Arms a one-shot timeout and returns the canceller for it.
 *
 * The tracker's timeouts are the only thing about it that depends on time
 * passing, and a caller that wants them settled on demand rather than by
 * waiting has nowhere to reach in: the global timer is not addressable from
 * outside. Injecting the arming function makes the timeout observable and
 * cancellable by whoever owns the tracker, so a test can fire a timeout, or
 * assert one was disarmed, without a real duration elapsing.
 *
 * Returning the canceller rather than a handle keeps the timer's identity
 * private to whichever implementation armed it.
 */
export type ScheduleTimeout = (handler: () => void, ms: number) => () => void;

const scheduleOnGlobalTimer: ScheduleTimeout = (handler, ms) => {
  const timer = setTimeout(handler, ms);
  return () => {
    clearTimeout(timer);
  };
};

export type PendingEntry<Key, Value, Meta> = {
  key: Key;
  ws: WsHandle;
  /**
   * Opaque per-entry payload for settle-time ownership checks. The pack
   * tracker stores the send-site `{ agentAddress, repoId }` so an ack/reject
   * is honored only when it comes from the connection that owns the transfer
   * for the same repo; the other trackers pass `undefined`.
   */
  meta: Meta;
  resolve(value: Value): void;
  reject(error: string): void;
  /**
   * Disarms this entry's timeout.
   *
   * Runs at most once per entry, which is why it need not be idempotent and
   * `ScheduleTimeout` does not ask its implementations for that: each of the
   * four settle paths cancels and drops the entry from the map in one
   * synchronous block, and a timeout that fires drops the entry itself, so
   * nothing can reach the entry to cancel it a second time.
   */
  cancelTimeout(): void;
};

/**
 * A tracked entry plus the identity its own timeout handler recognizes it
 * by. Keys are reused across round-trips, so "is the key occupied" cannot
 * tell a live successor apart from the entry a given timeout belongs to;
 * `token` can, because it is allocated per `register` call. It is internal
 * to the tracker: `get` hands callers the `PendingEntry` view.
 */
type TrackedEntry<Key, Value, Meta> = PendingEntry<Key, Value, Meta> & {
  readonly token: symbol;
};

export class PendingTracker<Key, Value = void, Meta = undefined> {
  private readonly entries = new Map<Key, TrackedEntry<Key, Value, Meta>>();

  /**
   * `schedule` defaults to the global timer, which is what every production
   * caller wants; it is injectable so a caller can drive the timeouts.
   */
  constructor(
    private readonly schedule: ScheduleTimeout = scheduleOnGlobalTimer,
  ) {}

  /**
   * Register a pending round-trip and arm its timeout. The entry is stored
   * before the caller sends its frame, so a synchronous reply (loopback
   * transports, tests) settles it. When `timeoutMs` elapses the entry is
   * dropped and `reject` is invoked with `timeoutMessage` — the same
   * rejection path an error reply frame uses, so per-site cleanup (routing
   * rollback, address bookkeeping) runs exactly once either way.
   *
   * `meta` is the opaque per-entry payload settle-time ownership checks read
   * off `get`; pass `undefined` when the round-trip carries none.
   */
  register(
    key: Key,
    ws: WsHandle,
    options: {
      timeoutMs: number;
      timeoutMessage: string;
      resolve(value: Value): void;
      reject(error: string): void;
    },
    meta: Meta,
  ): void {
    // Identifies the entry this call is about to register, so the handler
    // below can recognize it. Allocated before `schedule`, because the
    // handler closes over it and the entry itself does not exist yet.
    const token = Symbol("pending-entry");
    const cancelTimeout = this.schedule(() => {
      // Only the entry this call registered may be timed out. The global
      // timer never fires a cancelled timeout, but `schedule` is injectable
      // now and a caller's implementation is not owed that guarantee -- so
      // the handler checks rather than trusting cancellation to be perfect.
      // The check compares `token` rather than asking whether the key is
      // occupied: keys are reused (`pendingDeploys` is keyed by agent
      // address), so a stale fire can find the key held by a live successor.
      // Treating that as its own entry would drop the successor without
      // settling it and run this already-settled entry's per-site cleanup a
      // second time.
      const current = this.entries.get(key);
      if (current === undefined || current.token !== token) return;
      this.entries.delete(key);
      options.reject(options.timeoutMessage);
    }, options.timeoutMs);
    this.entries.set(key, {
      key,
      ws,
      meta,
      resolve: options.resolve,
      reject: options.reject,
      cancelTimeout,
      token,
    });
  }

  has(key: Key): boolean {
    return this.entries.has(key);
  }

  get(key: Key): PendingEntry<Key, Value, Meta> | undefined {
    return this.entries.get(key);
  }

  /**
   * Settle a pending entry as resolved. No-op when the entry is already
   * gone (timed out or swept on disconnect).
   */
  resolve(key: Key, value: Value): boolean {
    const entry = this.entries.get(key);
    if (entry === undefined) return false;
    entry.cancelTimeout();
    this.entries.delete(key);
    entry.resolve(value);
    return true;
  }

  /**
   * Settle a pending entry as rejected. No-op when the entry is already
   * gone (timed out or swept on disconnect).
   */
  reject(key: Key, error: string): boolean {
    const entry = this.entries.get(key);
    if (entry === undefined) return false;
    entry.cancelTimeout();
    this.entries.delete(key);
    entry.reject(error);
    return true;
  }

  /**
   * Drop a pending entry without settling it. The caller rejects its own
   * promise directly — used when a frame provably never reached the wire, so
   * the failure must not take the normal rejection path (the deploy
   * "failed to send" path, which must report `frameSent: false` and must not
   * let the armed timer fire later and double-reject).
   */
  delete(key: Key): void {
    const entry = this.entries.get(key);
    if (entry === undefined) return;
    entry.cancelTimeout();
    this.entries.delete(key);
  }

  /**
   * Reject every pending entry owned by `ws` — the disconnect sweep. Each
   * entry's reject closure runs its own per-site cleanup (address
   * bookkeeping), identical to a frame-error rejection.
   */
  rejectAllForWs(ws: WsHandle, error: string): void {
    for (const [key, entry] of this.entries) {
      if (entry.ws !== ws) continue;
      entry.cancelTimeout();
      this.entries.delete(key);
      entry.reject(error);
    }
  }
}
