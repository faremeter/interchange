// Pack-send protocol owner. Chunks the pack, emits the
// `repo.pack.push` / `repo.pack.done` frames, and resolves on the
// matching `repo.pack.ack` (or rejects on `repo.pack.reject`).

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
  /** Caller-supplied transfer id; must be unique across this sender. */
  transferId: string;
  pack: Uint8Array;
  /** Workflow-run ref or deploy ref the receiver should advance. */
  ref: string;
  /** Commit SHA the receiver should pin the ref to after apply. */
  commitSha: string;
};

export type PackSender = {
  /** Emit `repo.pack.push` frames then `repo.pack.done`; resolves on the matching ack. */
  send(opts: PackSendOpts): Promise<void>;
  /** Resolve the pending transfer for `frame.transferId`; `false` when none is pending. */
  handleAck(frame: PackAckFrame): boolean;
  /** Reject the pending transfer for `frame.transferId`; `false` when none is pending. */
  handleReject(frame: PackRejectFrame): boolean;
  /** Reject every in-flight transfer with `reason`. */
  cancelAll(reason: string): void;
};

export type PackSenderDeps = {
  /** Frame-send sink; the sender does not own the WebSocket. */
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
        // A throw from `sendFrame` must not leave the entry pending;
        // clean it up so a retry under the same transferId is admitted.
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
