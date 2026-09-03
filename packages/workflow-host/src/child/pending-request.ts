// Generic pending-request core shared by the three child-side control-IPC
// bridges (substrate-write, outbound-mail, mailbox-mutation).
//
// Every bridge runs the same request/response round-trip over the control
// channel: `submit` mints a `requestId`, registers a pending awaiter keyed
// by that id, emits a request frame upstream, and resolves/rejects the
// awaiter when the supervisor's matching response frame lands. The three
// bridges differ only in the request payload they send, the value they
// resolve, the label their error messages carry, and (for the
// substrate-write bridge) an intermediate merge round-trip that must peek
// at the pending entry without settling it.
//
// This core owns that lifecycle in one place so the per-bridge files
// contain only their wire-specific parts. It never touches the control
// channel itself: each bridge performs its own `upstreamSender.send` so
// the frame payloads stay byte-identical to what the supervisor's
// `ControlPayload` validator expects.

type PendingEntry<Value, Meta> = {
  meta: Meta;
  resolve: (value: Value) => void;
  reject: (err: Error) => void;
};

/**
 * A live pending entry as returned by {@link PendingRequestCore.get} and
 * {@link PendingRequestCore.settle}. `meta` is the per-bridge payload the
 * entry was registered with (e.g. the substrate-write request whose merge
 * closure the merge round-trip invokes).
 */
export type PendingEntryHandle<Value, Meta> = {
  meta: Meta;
  resolve: (value: Value) => void;
  reject: (err: Error) => void;
};

/**
 * Options for {@link createPendingRequestCore}. `label` prefixes every
 * error the core builds (`<label>: upstream send failed for requestId
 * <id>: ...`, `<label> (requestId=<id>) rejected by supervisor: ...`,
 * `<label> (requestId=<id>) cancelled: ...`, `<label>: no pending entry
 * for requestId <id>`), so each bridge's observable error strings stay
 * its own. `allocatorPrefix` seeds the default requestId allocator
 * (`<prefix>-<counter>-<rand>`); tests inject `allocateRequestId` for a
 * deterministic id.
 */
export type PendingRequestCoreOptions = {
  label: string;
  allocatorPrefix: string;
  /** `undefined` is accepted explicitly so callers can forward an optional option verbatim. */
  allocateRequestId?: (() => string) | undefined;
};

/**
 * The pending-request lifecycle surface a bridge composes into its own
 * `submit` / `handle*` / `cancelAll` methods.
 */
export type PendingRequestCore<Value, Meta = undefined> = {
  /** Number of registered-but-unsettled requests. */
  readonly pendingCount: number;
  /**
   * Mint a requestId, register an awaiter keyed by it, and return the id
   * plus the promise the bridge returns from its `submit`. The entry
   * stays pending until `settle` or `cancelAll` acts on it, or `discard`
   * removes it without settling (the upstream-send-failure path).
   */
  register(meta: Meta): { requestId: string; promise: Promise<Value> };
  /**
   * Remove a pending entry without settling its promise. Used when the
   * upstream send itself fails, so the abandoned awaiter does not leak.
   */
  discard(requestId: string): void;
  /**
   * Look up a pending entry WITHOUT removing it. Used by the
   * substrate-write bridge's merge round-trip, which must reach the
   * entry's merge closure while the entry stays alive for the terminal
   * write response.
   */
  get(requestId: string): PendingEntryHandle<Value, Meta> | undefined;
  /**
   * Look up a pending entry and remove it. Used by the response-frame
   * handlers: the entry is gone before its promise resolves/rejects, so
   * a later stale response or `cancelAll` finds nothing to act on.
   */
  settle(requestId: string): PendingEntryHandle<Value, Meta> | undefined;
  /**
   * Reject every pending entry with `<label> (requestId=<id>) cancelled:
   * <reason>` and clear the map. The control loop invokes this on any
   * exit path so no awaiter leaks when the supervisor has torn the IPC
   * down.
   */
  cancelAll(reason: string): void;
  /** `<label>: upstream send failed for requestId <id>: <cause message>` with `{ cause }`. */
  sendFailedError(requestId: string, cause: unknown): Error;
  /** `<label> (requestId=<id>) rejected by supervisor: <reason>`. */
  rejectedError(requestId: string, reason: string): Error;
  /** `<label>: no pending entry for requestId <id>` (substrate merge failure reply). */
  noPendingError(requestId: string): Error;
};

/**
 * Construct a pending-request core for one bridge. Pending entries live in
 * a map keyed by `requestId`; the bridge's response handler settles the
 * awaiter when the matching response frame lands.
 */
export function createPendingRequestCore<Value, Meta = undefined>(
  opts: PendingRequestCoreOptions,
): PendingRequestCore<Value, Meta> {
  const pending = new Map<string, PendingEntry<Value, Meta>>();
  const allocate =
    opts.allocateRequestId ?? defaultRequestIdAllocator(opts.allocatorPrefix);

  return {
    get pendingCount() {
      return pending.size;
    },
    register(meta: Meta): { requestId: string; promise: Promise<Value> } {
      const requestId = allocate();
      const promise = new Promise<Value>((resolve, reject) => {
        pending.set(requestId, { meta, resolve, reject });
      });
      return { requestId, promise };
    },
    discard(requestId: string): void {
      pending.delete(requestId);
    },
    get(requestId: string): PendingEntryHandle<Value, Meta> | undefined {
      return pending.get(requestId);
    },
    settle(requestId: string): PendingEntryHandle<Value, Meta> | undefined {
      const entry = pending.get(requestId);
      if (entry !== undefined) pending.delete(requestId);
      return entry;
    },
    cancelAll(reason: string): void {
      for (const [requestId, entry] of pending) {
        entry.reject(
          new Error(
            `${opts.label} (requestId=${requestId}) cancelled: ${reason}`,
          ),
        );
      }
      pending.clear();
    },
    sendFailedError(requestId: string, cause: unknown): Error {
      const reason = cause instanceof Error ? cause.message : String(cause);
      return new Error(
        `${opts.label}: upstream send failed for requestId ${requestId}: ${reason}`,
        { cause },
      );
    },
    rejectedError(requestId: string, reason: string): Error {
      return new Error(
        `${opts.label} (requestId=${requestId}) rejected by supervisor: ${reason}`,
      );
    },
    noPendingError(requestId: string): Error {
      return new Error(
        `${opts.label}: no pending entry for requestId ${requestId}`,
      );
    },
  };
}

/**
 * Default `requestId` allocator: a per-instance monotonic counter plus a
 * random suffix, so ids are unique across bridge instances without any
 * cross-instance coordination. The `prefix` names the owning bridge
 * (`sw-`, `om-`, `mm-`) for triage in supervisor logs.
 */
export function defaultRequestIdAllocator(prefix: string): () => string {
  let counter = 0;
  return () => {
    counter += 1;
    const rand = Math.random().toString(36).slice(2, 10);
    return `${prefix}-${String(counter)}-${rand}`;
  };
}
