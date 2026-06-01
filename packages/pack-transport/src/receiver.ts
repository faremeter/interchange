// Pack transfer chunk accumulator.
//
// Manages in-flight pack transfers arriving over the WebSocket. Each transfer
// is identified by a transferId and consists of ordered repo.pack.push
// frames followed by a repo.pack.done. The receiver validates seq continuity
// and rejects concurrent transfers for the same agent.

import { createHash } from "node:crypto";
import type {
  PackPushFrame,
  PackDoneFrame,
  PackRejectReason,
} from "@intx/types/sidecar";

type InFlightTransfer = {
  agentAddress: string;
  chunks: Uint8Array[];
  nextSeq: number;
};

export type PackReceiver = {
  handlePush(frame: PackPushFrame): PackRejectReason | null;
  handleDone(frame: PackDoneFrame): {
    pack: Uint8Array;
    ref: string;
    commitSha: string;
    /**
     * Hex-encoded sha256 of the assembled pack bytes. Computed
     * symmetrically on both producer (hub) and consumer (sidecar)
     * sides so the manifest row's `asset_pack_sha` reflects the bytes
     * that actually crossed the wire.
     */
    assetPackSha: string;
  } | null;
  hasTransfer(transferId: string): boolean;
  cancel(transferId: string): void;
  cancelByAgent(agentAddress: string): void;
  reset(): void;
};

export function createPackReceiver(): PackReceiver {
  const transfers = new Map<string, InFlightTransfer>();
  // Track which agents have active transfers to reject concurrent ones.
  const agentTransfers = new Map<string, string>();

  function handlePush(frame: PackPushFrame): PackRejectReason | null {
    const existing = agentTransfers.get(frame.agentAddress);
    if (existing !== undefined && existing !== frame.transferId) {
      return "conflict";
    }

    let transfer = transfers.get(frame.transferId);
    if (transfer === undefined) {
      transfer = {
        agentAddress: frame.agentAddress,
        chunks: [],
        nextSeq: 0,
      };
      transfers.set(frame.transferId, transfer);
      agentTransfers.set(frame.agentAddress, frame.transferId);
    } else if (transfer.agentAddress !== frame.agentAddress) {
      return "corrupt";
    }

    if (frame.seq !== transfer.nextSeq) {
      cleanup(frame.transferId, frame.agentAddress);
      return "corrupt";
    }

    const chunk = Buffer.from(frame.data, "base64");
    transfer.chunks.push(new Uint8Array(chunk));
    transfer.nextSeq++;
    return null;
  }

  function handleDone(frame: PackDoneFrame): {
    pack: Uint8Array;
    ref: string;
    commitSha: string;
    assetPackSha: string;
  } | null {
    const transfer = transfers.get(frame.transferId);
    if (transfer === undefined) {
      return null;
    }
    if (transfer.agentAddress !== frame.agentAddress) {
      return null;
    }

    const totalLength = transfer.chunks.reduce((s, c) => s + c.length, 0);
    const pack = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of transfer.chunks) {
      pack.set(chunk, offset);
      offset += chunk.length;
    }

    const assetPackSha = createHash("sha256").update(pack).digest("hex");

    cleanup(frame.transferId, transfer.agentAddress);
    return {
      pack,
      ref: frame.ref,
      commitSha: frame.commitSha,
      assetPackSha,
    };
  }

  function hasTransfer(transferId: string): boolean {
    return transfers.has(transferId);
  }

  function cancel(transferId: string): void {
    const transfer = transfers.get(transferId);
    if (transfer !== undefined) {
      cleanup(transferId, transfer.agentAddress);
    }
  }

  function cancelByAgent(agentAddress: string): void {
    const transferId = agentTransfers.get(agentAddress);
    if (transferId !== undefined) {
      cleanup(transferId, agentAddress);
    }
  }

  function cleanup(transferId: string, agentAddress: string): void {
    transfers.delete(transferId);
    if (agentTransfers.get(agentAddress) === transferId) {
      agentTransfers.delete(agentAddress);
    }
  }

  function reset(): void {
    transfers.clear();
    agentTransfers.clear();
  }

  return { handlePush, handleDone, hasTransfer, cancel, cancelByAgent, reset };
}
