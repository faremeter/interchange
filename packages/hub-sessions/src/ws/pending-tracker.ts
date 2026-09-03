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
  timer: ReturnType<typeof setTimeout>;
};

export class PendingTracker<Key, Value = void, Meta = undefined> {
  private readonly entries = new Map<Key, PendingEntry<Key, Value, Meta>>();

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
    const timer = setTimeout(() => {
      this.entries.delete(key);
      options.reject(options.timeoutMessage);
    }, options.timeoutMs);
    this.entries.set(key, {
      key,
      ws,
      meta,
      resolve: options.resolve,
      reject: options.reject,
      timer,
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
    clearTimeout(entry.timer);
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
    clearTimeout(entry.timer);
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
    clearTimeout(entry.timer);
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
      clearTimeout(entry.timer);
      this.entries.delete(key);
      entry.reject(error);
    }
  }
}
