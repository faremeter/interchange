// Pack-send protocol owner.
//
// Mints transferIds, chunks the pack, emits the
// `repo.pack.push` / `repo.pack.done` frame sequence, and resolves a
// Promise when the matching `repo.pack.ack` arrives (or rejects on
// `repo.pack.reject`). Both the existing hub-link agent-state push
// path and the sidecar-side workflow-run push hook consume this
// shape; the protocol logic lives once.

import type {
  PackAckFrame,
  PackDoneFrame,
  PackPushFrame,
  PackRejectFrame,
  RepoId,
} from "@intx/types/sidecar";

import { chunkPack } from "./chunker";

export type PackSendFrame = PackPushFrame | PackDoneFrame;

export type PackSendOpts = {
  agentAddress: string;
  repoId: RepoId;
  /**
   * Caller-supplied transfer id. Must be unique across the lifetime of
   * this sender; the sender does not re-mint on collision. Hub-link
   * uses an incoming `sync.request.transferId` for state-pack pushes;
   * the workflow-run client mints fresh ids per push.
   */
  transferId: string;
  pack: Uint8Array;
  /** Workflow-run ref or deploy ref the receiver should advance. */
  ref: string;
  /** Commit SHA the receiver should pin the ref to after apply. */
  commitSha: string;
};

export type PackSender = {
  /**
   * Stream the pack as a sequence of `repo.pack.push` frames followed
   * by a `repo.pack.done`. Resolves on the matching `repo.pack.ack`;
   * rejects on `repo.pack.reject` carrying the reason or on
   * `cancelAll`. The caller must route inbound ack/reject frames
   * through `handleAck` / `handleReject` so the Promise resolves.
   */
  send(opts: PackSendOpts): Promise<void>;
  /**
   * Resolve the pending transfer matched by `frame.transferId`.
   * Returns `true` when a transfer was matched (and the Promise
   * resolved), `false` when no transfer is pending under that id.
   * Callers that share a single sender across multiple pack flows use
   * the boolean to dispatch unknown ids to a different handler.
   */
  handleAck(frame: PackAckFrame): boolean;
  /**
   * Reject the pending transfer matched by `frame.transferId`. Returns
   * the same shape as `handleAck`.
   */
  handleReject(frame: PackRejectFrame): boolean;
  /**
   * Reject every in-flight transfer with the supplied reason. Used by
   * hub-link's `open` handler to fail any transfers that did not
   * complete before the connection cycle.
   */
  cancelAll(reason: string): void;
};

export type PackSenderDeps = {
  /**
   * Frame-send sink. The caller routes the frame onto the wire; the
   * sender does not own WebSocket access.
   */
  sendFrame: (frame: PackSendFrame) => void;
};

type PendingTransfer = {
  resolve: () => void;
  reject: (err: Error) => void;
};

export function createPackSender(deps: PackSenderDeps): PackSender {
  const pending = new Map<string, PendingTransfer>();

  function send(opts: PackSendOpts): Promise<void> {
    const { agentAddress, repoId, transferId, pack, ref, commitSha } = opts;
    if (pending.has(transferId)) {
      return Promise.reject(
        new Error(`pack sender: transferId ${transferId} is already in flight`),
      );
    }
    return new Promise<void>((resolve, reject) => {
      pending.set(transferId, { resolve, reject });
      try {
        for (const chunk of chunkPack(pack)) {
          deps.sendFrame({
            type: "repo.pack.push",
            agentAddress,
            repoId,
            transferId,
            seq: chunk.seq,
            data: chunk.data,
          });
        }
        deps.sendFrame({
          type: "repo.pack.done",
          agentAddress,
          repoId,
          transferId,
          ref,
          commitSha,
        });
      } catch (cause) {
        // A synchronous throw out of `sendFrame` (closed transport,
        // serializer error) would otherwise leave the pending entry
        // in flight forever; the next `send` for this transferId would
        // then reject at the "already in flight" guard above. Clean
        // the entry up at the boundary that owns it.
        pending.delete(transferId);
        reject(
          cause instanceof Error
            ? cause
            : new Error(`pack sender: sendFrame threw: ${String(cause)}`),
        );
      }
    });
  }

  function handleAck(frame: PackAckFrame): boolean {
    const entry = pending.get(frame.transferId);
    if (entry === undefined) return false;
    pending.delete(frame.transferId);
    entry.resolve();
    return true;
  }

  function handleReject(frame: PackRejectFrame): boolean {
    const entry = pending.get(frame.transferId);
    if (entry === undefined) return false;
    pending.delete(frame.transferId);
    entry.reject(
      new Error(
        `pack rejected by receiver (transferId=${frame.transferId} reason=${frame.reason})`,
      ),
    );
    return true;
  }

  function cancelAll(reason: string): void {
    for (const [id, entry] of pending) {
      pending.delete(id);
      entry.reject(new Error(reason));
    }
  }

  return { send, handleAck, handleReject, cancelAll };
}
