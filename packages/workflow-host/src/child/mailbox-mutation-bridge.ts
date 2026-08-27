// Child-side mailbox-mutation bridge (INBOUND half of mailbox ownership,
// §3b).
//
// The supervisor is the sole mail owner: it holds the long-lived
// substrate mailbox store and is the only writer to the workflow-run
// ref. A step agent reads its INBOX locally (the supervisor-backed
// transport's read surface opens fresh committed snapshots), but every
// MUTATION of the mailbox -- flag writes (`\Seen`, `\Deleted`, ...) and
// `expunge` -- routes up to the supervisor through this bridge rather
// than being flushed from the child. A second writer flushing the same
// ref from the child would race the supervisor's in-memory mirror and
// break uid / modseq monotonicity, so the child never writes the
// mailbox directly.
//
// Lifecycle of one mutation:
//
//   1. The agent's mail tool (flag or expunge) calls the
//      supervisor-backed transport's `setFlags` / `clearFlags` /
//      `expunge`. The transport calls `bridge.submit(mutation)`.
//   2. `submit` mints a `requestId`, registers a pending awaiter, and
//      emits `mailbox.mutate.request` upstream carrying the op and its
//      operands.
//   3. The supervisor applies the op to its owned mailbox store,
//      flushes, and replies with `mailbox.mutate.response`. The reply is
//      sent only after the flush, so the child's next committed read
//      observes the mutation (the same flush-before-signal ordering
//      `mailbox.notify` relies on).
//   4. The bridge resolves / rejects the pending awaiter; the
//      transport method returns to the mail tool. A supervisor-side
//      failure (unknown uid, substrate fault) surfaces as a rejection so
//      the agent's mail-tool call fails loudly rather than silently
//      dropping the mutation.

import { getLogger } from "@intx/log";

import type {
  ControlChannelSender,
  ControlPayload,
} from "../ipc/control-channel";

const logger = getLogger(["workflow-host", "child", "mailbox-mutation-bridge"]);

/**
 * A mailbox mutation the child asks the supervisor to apply. A flag
 * mutation carries the target `uid` and the `flags` to add or remove; an
 * `expunge` sweeps every `\Deleted` message in the mailbox and so carries
 * neither.
 */
export type MailboxMutation =
  | {
      runId: string;
      mailbox: string;
      op: "addFlags" | "removeFlags";
      uid: number;
      flags: string[];
    }
  | {
      runId: string;
      mailbox: string;
      op: "expunge";
    };

/**
 * The supervisor's applied-mutation result. `expungedUids` is present
 * only for an `expunge` and lists the uids the sweep removed, so the
 * agent tool can report how many messages it consumed.
 */
export type MailboxMutationResult = {
  expungedUids?: number[];
};

/**
 * Bridge surface the child's supervisor-backed transport reaches into.
 * `submit` sends a `mailbox.mutate.request` upstream and resolves once
 * the supervisor's matching `mailbox.mutate.response` lands.
 * `handleResult` is the receiver-side entry point the child's control
 * loop invokes when the downstream `mailbox.mutate.response` frame
 * arrives. `cancelAll` is the cleanup hook the control loop invokes on
 * any exit path so a pending mutation does not leak an awaiter when the
 * supervisor has torn the IPC down.
 */
export interface ChildMailboxMutationBridge {
  submit(mutation: MailboxMutation): Promise<MailboxMutationResult>;
  handleResult(
    data: Extract<ControlPayload, { type: "mailbox.mutate.response" }>["data"],
  ): void;
  cancelAll(reason: string): void;
  readonly pendingCount: number;
}

export interface CreateChildMailboxMutationBridgeOpts {
  upstreamSender: ControlChannelSender;
  /**
   * Optional `requestId` allocator. Production wires a per-instance
   * monotonic counter plus a random suffix; tests inject a
   * deterministic factory so the upstream frame's `requestId` is
   * predictable.
   */
  allocateRequestId?: () => string;
}

type PendingEntry = {
  resolve: (value: MailboxMutationResult) => void;
  reject: (err: Error) => void;
};

/**
 * Construct the child-side mailbox-mutation bridge. Pending mutations
 * live in a map keyed by `requestId`; the bridge resolves the awaiter
 * when the supervisor's matching `mailbox.mutate.response` lands.
 */
export function createChildMailboxMutationBridge(
  opts: CreateChildMailboxMutationBridgeOpts,
): ChildMailboxMutationBridge {
  const pending = new Map<string, PendingEntry>();
  const allocate = opts.allocateRequestId ?? defaultRequestIdAllocator();

  return {
    get pendingCount() {
      return pending.size;
    },
    async submit(mutation: MailboxMutation): Promise<MailboxMutationResult> {
      const requestId = allocate();
      const resultPromise = new Promise<MailboxMutationResult>(
        (resolve, reject) => {
          pending.set(requestId, { resolve, reject });
        },
      );
      const data =
        mutation.op === "expunge"
          ? {
              requestId,
              runId: mutation.runId,
              mailbox: mutation.mailbox,
              op: mutation.op,
            }
          : {
              requestId,
              runId: mutation.runId,
              mailbox: mutation.mailbox,
              op: mutation.op,
              uid: mutation.uid,
              flags: mutation.flags,
            };
      try {
        await opts.upstreamSender.send({
          type: "mailbox.mutate.request",
          data,
        });
      } catch (cause) {
        pending.delete(requestId);
        const reason = cause instanceof Error ? cause.message : String(cause);
        throw new Error(
          `workflow-child mailbox mutation: upstream send failed for requestId ${requestId}: ${reason}`,
          { cause },
        );
      }
      return resultPromise;
    },
    handleResult(data) {
      const entry = pending.get(data.requestId);
      if (entry === undefined) {
        logger.warn`mailbox.mutate.response landed with no pending entry; requestId=${data.requestId} dropped`;
        return;
      }
      pending.delete(data.requestId);
      if (data.result.ok) {
        const result: MailboxMutationResult = {};
        if (data.result.expungedUids !== undefined) {
          result.expungedUids = data.result.expungedUids;
        }
        entry.resolve(result);
        return;
      }
      entry.reject(
        new Error(
          `workflow-child mailbox mutation (requestId=${data.requestId}) rejected by supervisor: ${data.result.reason}`,
        ),
      );
    },
    cancelAll(reason: string) {
      for (const [requestId, entry] of pending) {
        entry.reject(
          new Error(
            `workflow-child mailbox mutation (requestId=${requestId}) cancelled: ${reason}`,
          ),
        );
      }
      pending.clear();
    },
  };
}

function defaultRequestIdAllocator(): () => string {
  let counter = 0;
  return () => {
    counter += 1;
    const rand = Math.random().toString(36).slice(2, 10);
    return `mm-${String(counter)}-${rand}`;
  };
}
