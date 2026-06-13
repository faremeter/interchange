// Control channel: NDJSON over stdio, Ed25519-signed per direction.
//
// Two Ed25519 keypairs flow per spawn:
//   - Supervisor's keypair. The supervisor holds the private half and
//     signs every downstream (supervisor->child) frame. The matching
//     public half is passed to the child in spawn-time env
//     (`HOST_PUBKEY`) and the child verifies downstream frames
//     against it. The supervisor's PRIVATE KEY NEVER LEAVES THE
//     SUPERVISOR'S ADDRESS SPACE.
//   - Child's keypair. The child mints it at startup, holds the
//     private half in its own address space, and signs every upstream
//     (child->supervisor) frame. The matching public half rides as
//     `childPublicKey` on the upstream `ready` frame's payload; the
//     supervisor extracts it on receive and uses it to verify
//     subsequent upstream frames. The CHILD'S PRIVATE KEY NEVER
//     LEAVES THE CHILD'S ADDRESS SPACE.
//
// Upstream `ready` bootstraps the supervisor's view of the child's
// public key. The supervisor's receiver opens in bootstrap mode:
// the first frame's envelope is parsed structurally so the
// supervisor can extract `childPublicKey` from the payload, then the
// signature is verified against that key. Subsequent upstream frames
// verify against the same key. A child-signed frame whose claimed
// `childPublicKey` does not match the bootstrap value (or any
// non-`ready` first frame) crashes the receiver.
//
// Wire format: one signed envelope per line. Each line is the JSON
// serialization of `{ envelope: { seq, channelId, payload }, sig:
// <hex Ed25519> }`. The signature covers the canonical bytes of the
// envelope sub-object (see `envelope.ts`).
//
// Payload union: every legal payload's discriminator lives in this
// module. The mixing-the-two-channels failure mode -- a "control"
// frame whose payload is structurally an InferenceEvent -- is
// prevented at the type level by the disjoint discriminated union.

import { type } from "arktype";

import {
  decodeEnvelope,
  encodeEnvelope,
  FrameEnvelope,
  hexDecode,
  hexEncode,
  SignedEnvelope,
} from "./envelope";
import { signEd25519, verifyEd25519 } from "./crypto";

/**
 * Wire-shape of one per-step credentials entry the supervisor pushes
 * inside a `grants-updated` frame. Mirrors `CredentialsSnapshotStep`
 * in `supervisor/credentials.ts` -- duplicated here as an arktype
 * validator so the control-channel module stays free of a
 * compile-time import on the supervisor module (the IPC module sits
 * underneath the supervisor and child modules in the dependency
 * graph). The contentHash pins the per-step grants so the child can
 * detect a stale push and ignore an out-of-order one.
 */
export const CredentialsSnapshotStepPayload = type({
  stepId: "string",
  address: "string",
  grants: "unknown[]",
  contentHash: "string",
});

export const CredentialsSnapshotPayload = type({
  steps: CredentialsSnapshotStepPayload.array(),
});

/**
 * Discriminated union of every control-channel payload kind. The
 * `type` discriminator namespaces the control-plane vocabulary so a
 * future addition (e.g. `connector-bind`) lands by extending this
 * union and not by widening the envelope shape. Inference events
 * NEVER appear here; they ride the event channel.
 */
export const ControlPayload = type(
  {
    type: "'trigger.fire'",
    data: {
      runId: "string",
      messageId: "string",
      receivedAt: "number",
    },
  },
  "|",
  {
    type: "'signal.deliver'",
    data: {
      runId: "string",
      signalName: "string",
      signalId: "string",
      payload: "unknown",
    },
  },
)
  .or({
    type: "'drain'",
    data: {
      deadlineMs: "number",
    },
  })
  .or({
    type: "'shutdown'",
    data: {
      reason: "string",
    },
  })
  .or({
    type: "'grants-updated'",
    data: {
      /**
       * Full credentialsSnapshot the supervisor assembled. The child
       * replaces its in-memory snapshot wholesale on receive so the
       * authorize closure binds to the new per-step grants on the
       * next step invocation. Carried inline rather than by reference
       * because the snapshot is per-step grants payload -- the
       * supervisor is the only producer and the child is the only
       * consumer, so the substrate round-trip would just add latency.
       */
      snapshot: CredentialsSnapshotPayload,
      /**
       * Per-step content hashes the supervisor expects the snapshot
       * to pin to. Surfaced separately so receivers can cheap-compare
       * a push against the snapshot they already have without rehashing
       * each step's grants. Optional; the receiver does not require it
       * but uses it for the staleness cross-check when present.
       */
      "stepHashes?": "Record<string, string>",
    },
  })
  .or({
    type: "'sources-updated'",
    data: {
      "sourceHashes?": "Record<string, string>",
    },
  })
  .or({
    type: "'ready'",
    data: {
      childPid: "number",
      /**
       * Hex-encoded Ed25519 public key the child minted at startup.
       * The supervisor extracts this on receive and uses it to verify
       * every subsequent upstream control frame's signature. The
       * child's private key never leaves the child's address space.
       */
      childPublicKey: "string",
    },
  })
  .or({
    // Child-initiated request to recycle the workflow-process. The
    // child emits this when its own self-check decides it needs to be
    // recycled (an internal consistency error it can't recover from,
    // a watchdog tripping); the supervisor receives it on its
    // upstream control-channel reader and funnels it into the same
    // `triggerRecycle` code path the operator and policy origins use.
    // The `reason` rides verbatim into the supervisor's reason field;
    // the supervisor does not interpret it beyond logging and
    // attaching it to the recycle attempt.
    type: "'recycle.request'",
    data: {
      reason: "string",
    },
  })
  .or({
    // Child-initiated workflow-run pack push request. The child cannot
    // hold its own hub WebSocket because the hub-link surface binds to
    // sidecar-main-process state (the deploy router, the session
    // manager); the child therefore routes pack pushes back over the
    // existing control IPC and the supervisor forwards via the
    // host's `HubLink.pushWorkflowRunPack`. `pushId` correlates the
    // matching upstream-supervisor `pack.push.response` so the child's
    // pending-id map can resolve the awaiter. `packBase64` carries the
    // pack bytes verbatim; the wire form is one NDJSON line per IPC
    // frame, which Bun's pipes carry without a line-size cap below the
    // pack sizes a workflow-run commit produces. A future variant that
    // needs chunking can extend the union with `pack.push.chunk` and
    // `pack.push.commit` without disturbing existing consumers.
    type: "'pack.push.request'",
    data: {
      pushId: "string > 0",
      agentAddress: "string > 0",
      repoId: {
        kind: "string",
        id: "string > 0",
      },
      ref: "string > 0",
      commitSha: "string > 0",
      packBase64: "string > 0",
    },
  })
  .or({
    // Supervisor's reply to a child's `pack.push.request`. The
    // `pushId` echoes the child's allocated correlation id so the
    // child's pending-id map can resolve the awaiter. The `result`
    // variant is a discriminated union so the typed handler at the
    // child knows whether to resolve or reject. A failed push from
    // the host's `HubLink.pushWorkflowRunPack` is surfaced as `{ ok:
    // false, reason }`; the child's sink rejects with the reason, and
    // the wrap's caller (the workflow-run commit path) surfaces it
    // to the runtime body per defensive-coding -- the supervisor never
    // swallows a hub-side rejection.
    type: "'pack.push.response'",
    data: {
      pushId: "string > 0",
      result: type(
        {
          ok: "true",
        },
        "|",
        {
          ok: "false",
          reason: "string > 0",
        },
      ),
    },
  });

export type ControlPayload = typeof ControlPayload.infer;

export interface NdjsonWriter {
  write(line: string): Promise<void> | void;
}

export interface NdjsonReader {
  read(): AsyncIterableIterator<string>;
}

export interface ControlChannelSenderOpts {
  privateKeySeed: Uint8Array;
  channelId: string;
  writer: NdjsonWriter;
}

export interface ControlChannelSender {
  send(payload: ControlPayload): Promise<void>;
  readonly seq: number;
}

/**
 * Construct the supervisor-side control-channel sender. The
 * supervisor's Ed25519 seed lives in closure. The matching public
 * key flows to the child through spawn-time env -- never the seed.
 */
export function createControlChannelSender(
  opts: ControlChannelSenderOpts,
): ControlChannelSender {
  let seq = 0;
  return {
    get seq() {
      return seq;
    },
    async send(payload: ControlPayload) {
      seq += 1;
      const envelope: FrameEnvelope = {
        seq,
        channelId: opts.channelId,
        payload,
      };
      const envelopeBytes = encodeEnvelope(envelope);
      const sig = signEd25519(envelopeBytes, opts.privateKeySeed);
      const signed: SignedEnvelope = {
        envelope,
        sig: hexEncode(sig),
      };
      await opts.writer.write(JSON.stringify(signed) + "\n");
    },
  };
}

export interface ControlChannelReceiverOpts {
  /**
   * Public key used to verify every inbound frame. When `Uint8Array`
   * the value is fixed at construction time (the child's downstream
   * receiver uses the supervisor's pubkey from `HOST_PUBKEY`). When
   * `{ bootstrapFromReady: true }` the receiver opens in
   * bootstrap mode: the first frame must be `ready` and must carry
   * a `childPublicKey` hex-encoded Ed25519 public key in its payload.
   * The receiver extracts the key, verifies the first frame's
   * signature against it, then continues verifying subsequent frames
   * against the same key. The supervisor's upstream receiver opens
   * in bootstrap mode so the child can publish its own public key
   * over the wire without the supervisor ever holding the matching
   * private half.
   */
  publicKey: Uint8Array | { bootstrapFromReady: true };
  channelId: string;
  reader: NdjsonReader;
  /**
   * Invoked when any invariant is violated: signature failure,
   * channelId mismatch, non-monotonic seq, malformed payload. The
   * receiver's contract is to crash on any such violation. The
   * caller wires this to a process-exit path; tests inject a
   * recorder to assert on the failure mode.
   */
  onCrash: (reason: string) => void;
}

/**
 * Construct the child-side control-channel receiver. Yields one
 * verified, in-order `ControlPayload` per call. Any frame that
 * fails verification, carries a non-current channelId, or arrives
 * out of order calls `onCrash` and ends the iterator.
 */
export async function* receiveControlChannel(
  opts: ControlChannelReceiverOpts,
): AsyncGenerator<ControlPayload, void, void> {
  let highestSeq = 0;
  let activePublicKey: Uint8Array | null =
    opts.publicKey instanceof Uint8Array ? opts.publicKey : null;
  const bootstrapping = activePublicKey === null;
  for await (const line of opts.reader.read()) {
    if (line.length === 0) continue;

    let raw: unknown;
    try {
      raw = JSON.parse(line);
    } catch (cause) {
      opts.onCrash(
        `control channel received non-JSON line: ${errorMessage(cause)}`,
      );
      return;
    }

    const signed = SignedEnvelope(raw);
    if (signed instanceof type.errors) {
      opts.onCrash(
        `control channel envelope failed validation: ${signed.summary}`,
      );
      return;
    }

    let envelopeBytes: Uint8Array;
    try {
      envelopeBytes = encodeEnvelope(signed.envelope);
    } catch (cause) {
      opts.onCrash(
        `control channel envelope re-encode failed: ${errorMessage(cause)}`,
      );
      return;
    }

    let sigBytes: Uint8Array;
    try {
      sigBytes = hexDecode(signed.sig);
    } catch (cause) {
      opts.onCrash(
        `control channel signature decode failed: ${errorMessage(cause)}`,
      );
      return;
    }

    if (activePublicKey === null) {
      // Bootstrap mode: the first frame must be `ready`. Extract the
      // child's public key from the payload, then verify the
      // first frame's signature against it. The receiver crashes if
      // the payload is not a `ready` frame or carries a malformed
      // `childPublicKey`.
      const candidate = ControlPayload(signed.envelope.payload);
      if (candidate instanceof type.errors) {
        opts.onCrash(
          `control channel bootstrap payload failed validation: ${candidate.summary}`,
        );
        return;
      }
      if (candidate.type !== "ready") {
        opts.onCrash(
          `control channel bootstrap expected a ready frame, got ${candidate.type}`,
        );
        return;
      }
      let bootstrapKey: Uint8Array;
      try {
        bootstrapKey = hexDecode(candidate.data.childPublicKey);
      } catch (cause) {
        opts.onCrash(
          `control channel bootstrap childPublicKey decode failed: ${errorMessage(cause)}`,
        );
        return;
      }
      activePublicKey = bootstrapKey;
    }

    const ok = verifyEd25519(envelopeBytes, sigBytes, activePublicKey);
    if (!ok) {
      opts.onCrash(
        `control channel signature did not verify (seq=${String(signed.envelope.seq)}, channelId=${signed.envelope.channelId}${bootstrapping ? "; bootstrap" : ""})`,
      );
      return;
    }

    if (signed.envelope.channelId !== opts.channelId) {
      opts.onCrash(
        `control channel channelId mismatch: expected ${opts.channelId}, got ${signed.envelope.channelId} at seq=${String(signed.envelope.seq)}`,
      );
      return;
    }

    if (signed.envelope.seq <= highestSeq) {
      opts.onCrash(
        `control channel out-of-order seq: expected > ${String(highestSeq)}, got ${String(signed.envelope.seq)}`,
      );
      return;
    }
    if (signed.envelope.seq !== highestSeq + 1) {
      opts.onCrash(
        `control channel seq gap: expected ${String(highestSeq + 1)}, got ${String(signed.envelope.seq)}`,
      );
      return;
    }
    highestSeq = signed.envelope.seq;

    const payload = ControlPayload(signed.envelope.payload);
    if (payload instanceof type.errors) {
      opts.onCrash(
        `control channel payload failed validation: ${payload.summary}`,
      );
      return;
    }

    yield payload;
  }
}

function errorMessage(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  return String(cause);
}

/**
 * Re-export the envelope decoder for callers that need to inspect
 * a control frame's envelope without going through the receiver
 * iterator (testing harnesses that fuzz the wire format).
 */
export { decodeEnvelope };
