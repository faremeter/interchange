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

import { hexDecode, hexEncode, SignalKind } from "@intx/types";
import {
  BoundedApprovalSnapshot,
  InferenceSource,
  InterchangeType,
} from "@intx/types/runtime";

import {
  decodeEnvelope,
  encodeEnvelope,
  FrameEnvelope,
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
 * Wire shape of a `sources-updated` frame's `data`: the full ordered
 * inference-source failover chain plus the default source id. Carried
 * inline like the grants snapshot -- a single-producer, single-consumer
 * supervisor->child push, so a substrate round-trip would only add
 * latency. No per-source hash rides along; a source list is flat, with no
 * per-item pin for a receiver to cross-check.
 *
 * The `narrow` pins two frame-structural invariants at this boundary so
 * every consumer can trust them without re-checking: source ids are
 * unique, and the first element is the default source. The head-is-default
 * rule is what keeps the two rotation paths in agreement -- a warm agent's
 * `setSources` activates the matched default index, while a cold rebuild
 * pins element 0 -- so they pick the same active source only when the
 * default is the head.
 */
export const SourcesUpdatedData = type({
  sources: InferenceSource.array().atLeastLength(1),
  defaultSource: "string > 0",
}).narrow((data, ctx) => {
  const seen = new Set<string>();
  for (const source of data.sources) {
    if (seen.has(source.id)) {
      return ctx.mustBe(
        `a source list with unique ids; "${source.id}" appears more than once`,
      );
    }
    seen.add(source.id);
  }
  const head = data.sources[0];
  if (head === undefined || head.id !== data.defaultSource) {
    return ctx.mustBe(
      "a source list whose first element is the default source",
    );
  }
  return true;
});

/**
 * Wire projection of an attachment on an outbound mail message. The
 * runtime `MessageAttachment.data` is raw bytes; the NDJSON control
 * channel is text, so the bytes ride base64-encoded under `dataBase64`.
 * The child encodes on send; the supervisor decodes before handing the
 * `OutboundMessage` to the host transport.
 */
export const OutboundAttachmentPayload = type({
  name: "string",
  contentType: "string",
  dataBase64: "string",
});

/**
 * Wire projection of `@intx/types/runtime`'s `OutboundMessage`. Mirrors
 * that type field-for-field with two adjustments for the NDJSON wire:
 * attachment bytes are base64 strings (see `OutboundAttachmentPayload`),
 * and every optional field is spelled with the `"?"` suffix so an
 * absent field round-trips as absent rather than `null`. The supervisor
 * reconstructs the runtime `OutboundMessage` from this shape before
 * invoking `MailBusBindings.sendOutbound`.
 *
 * Duplicated here as an arktype validator (rather than importing the
 * TypeScript `OutboundMessage` type) so the IPC module validates the
 * child-supplied payload at the wire boundary -- the child is a separate
 * process and its frames are untrusted input the receiver must parse.
 */
export const OutboundMessagePayload = type({
  to: "string | string[]",
  "cc?": "string | string[]",
  "subject?": "string",
  type: InterchangeType,
  "content?": "string",
  "payload?": "Record<string, unknown>",
  "summary?": "string",
  "attachments?": OutboundAttachmentPayload.array(),
  "inReplyTo?": "string",
  "correlationId?": "string",
  "sessionId?": "string",
  "tenantId?": "string",
});

export type OutboundMessagePayload = typeof OutboundMessagePayload.infer;

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
    data: SourcesUpdatedData,
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
    // Child-initiated `writeTreePreservingPrefix` request. The child
    // does not hold a substrate write authority for the workflow-run
    // repo (single-writer at the ref tip belongs to the supervisor);
    // its workflow-run substrate proxy forwards every write through
    // this frame. The supervisor receives the request, runs its own
    // wrapped `writeTreePreservingPrefix`, and reaches back to the
    // child for the merge bytes via `substrate.merge.request` so the
    // child's merge closure (which knows about seq computation,
    // duplicate detection, etc.) keeps producing the prospective tree.
    // The supervisor resolves the child's awaiter with
    // `substrate.write.response`.
    type: "'substrate.write.request'",
    data: {
      requestId: "string > 0",
      repoId: {
        kind: "string",
        id: "string > 0",
      },
      ref: "string > 0",
      preservePrefix: "string > 0",
      message: "string > 0",
    },
  })
  .or({
    // Supervisor-initiated request for the child's merge bytes. Fired
    // from inside the supervisor's `writeTreePreservingPrefix` merge
    // callback while the per-repo lock is held; the child receives the
    // existing prefix entries (base64-encoded bytes), invokes its merge
    // closure, and replies with the prospective tree on
    // `substrate.merge.response`. Carrying the entries inline preserves
    // the lock window: the supervisor blocks inside the merge callback
    // until the response lands.
    type: "'substrate.merge.request'",
    data: {
      requestId: "string > 0",
      existing: type({
        path: "string > 0",
        contentBase64: "string",
      }).array(),
    },
  })
  .or({
    // Child's merge result. `requestId` correlates with the
    // `substrate.write.request` that started the write; the supervisor
    // resumes its merge callback with the supplied entries (or
    // propagates the structured failure).
    type: "'substrate.merge.response'",
    data: {
      requestId: "string > 0",
      result: type(
        {
          ok: "true",
          files: type({
            path: "string > 0",
            contentBase64: "string",
          }).array(),
        },
        "|",
        {
          ok: "false",
          reason: "string > 0",
        },
      ),
    },
  })
  .or({
    // Supervisor's terminal reply to a child's `substrate.write.request`.
    // The `requestId` echoes the child's allocated correlation id so
    // the child's pending-id map resolves the awaiter. A successful
    // write surfaces `commitSha`; the child's proxy returns that to its
    // caller. A failed write (substrate rejection, validatePush
    // violation, the supervisor's pack-push wrap's downstream
    // `HubLink.pushWorkflowRunPack` rejection) surfaces a structured
    // `{ ok: false, reason }` the child's proxy rethrows.
    type: "'substrate.write.response'",
    data: {
      requestId: "string > 0",
      result: type(
        {
          ok: "true",
          commitSha: "string > 0",
        },
        "|",
        {
          ok: "false",
          reason: "string > 0",
        },
      ),
    },
  })
  .or({
    // Child-initiated outbound-mail request (OUTBOUND half of mailbox
    // ownership, §3a). The workflow-process child never holds the
    // agent's signing key and never calls `transport.send` itself. When
    // a step agent produces a reply or invokes a mail-send tool, the
    // child forwards the structured outbound message plus the sender
    // (agent) address up over the control channel; the supervisor
    // performs the actual signed send through the host's real transport
    // (`MailBusBindings.sendOutbound`), which signs with the sender's
    // `CryptoProvider` exactly as the in-process path does. The
    // supervisor is the sole mail owner and the only process that can
    // emit signed mail on the agent's behalf.
    //
    // `requestId` correlates the supervisor's `outbound.result` reply so
    // the child's mail-tool `send()` resolves with the real
    // `SendReceipt` (or rejects with the supervisor's structured
    // failure). The message is carried as a JSON-projected
    // `OutboundMessage`; attachment bytes ride base64-encoded so the
    // NDJSON wire stays text-safe.
    type: "'outbound.message'",
    data: {
      requestId: "string > 0",
      senderAddress: "string > 0",
      "mailbox?": "string",
      message: OutboundMessagePayload,
    },
  })
  .or({
    // Supervisor's terminal reply to a child's `outbound.message`. The
    // `requestId` echoes the child's correlation id so the child's
    // pending mail-tool awaiter resolves. A successful send surfaces the
    // `SendReceipt` (messageId + status); a failed send (unregistered
    // sender, signing failure, transport rejection) surfaces a
    // structured `{ ok: false, reason }` the child's transport rethrows
    // so the mail-tool call fails loudly rather than dropping the send.
    type: "'outbound.result'",
    data: {
      requestId: "string > 0",
      result: type(
        {
          ok: "true",
          messageId: "string > 0",
          status: "'delivered' | 'queued'",
        },
        "|",
        {
          ok: "false",
          reason: "string > 0",
        },
      ),
    },
  })
  .or({
    // Child-initiated terminal-run notification. The workflow-process
    // child emits this when one of its runs reaches a terminal phase
    // (`RunCompleted`, `RunFailed`, `RunCancelled`) so the supervisor's
    // dispatch loop and drain accumulators can settle without re-reading
    // the workflow-run substrate from the supervisor process. The child
    // commits the terminal event to its own substrate through the
    // workflow-run pack-push pipeline; this frame is the peer-channel
    // notification that mirrors the commit so the supervisor's
    // in-process consumers do not have to round-trip the substrate.
    //
    // The `seq` mirrors the on-disk EventBase.seq the child assigned at
    // commit time. The supervisor does not authoritatively verify the
    // commit landed -- the pack-push response covers that contract --
    // but the field is carried so a downstream consumer can correlate
    // the notification with the substrate blob.
    type: "'terminal.event'",
    data: {
      runId: "string > 0",
      seq: "number >= 0",
      kind: "'RunCompleted' | 'RunFailed' | 'RunCancelled'",
      at: "string > 0",
      "error?": {
        message: "string",
      },
    },
  })
  .or({
    // Child-initiated control-plane suspension notification. The
    // workflow-process child emits this when a workflow agent step parks
    // on a reserved `signalName(correlationId)` channel (`env.onPark`),
    // so the supervisor can register the correlation out-of-band before
    // the parked run can be resumed. The supervisor stamps the
    // deployment identity it owns (`deploymentId` + `agentAddress`) and
    // forwards a `signal.correlation.register` frame to the hub, which
    // co-writes the run's routing + approval rows. Mirrors
    // `terminal.event`: a peer-channel notification the supervisor fans
    // out, distinct from the substrate commit the run also produces.
    //
    // `signalName` is deliberately NOT carried: it is a pure function of
    // `correlationId` (`signalName(correlationId)`), recomputed by every
    // consumer that needs it, so the two cannot drift.
    type: "'park.notify'",
    data: {
      runId: "string > 0",
      correlationId: "string > 0",
      kind: SignalKind,
      // Approver-facing snapshot of the parked tool call, size-capped at this
      // process boundary. Optional: only an ask-rail suspension carries one.
      "snapshot?": BoundedApprovalSnapshot,
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
  // Serialize sends. Signing is async, so without a lock two concurrent
  // callers could each assign seq, suspend on `signEd25519`, and resume in
  // signature-resolution order — writing frames out of seq order, which the
  // receiver rejects as a gap and crashes the channel. The promise chain
  // makes each send await the previous send's completion before it assigns
  // seq, signs, and writes, keeping that critical section atomic.
  let tail: Promise<void> = Promise.resolve();
  return {
    get seq() {
      return seq;
    },
    send(payload: ControlPayload): Promise<void> {
      const previous = tail;
      let release: () => void = () => undefined;
      tail = new Promise<void>((resolve) => {
        release = resolve;
      });
      return (async () => {
        await previous;
        try {
          seq += 1;
          const envelope: FrameEnvelope = {
            seq,
            channelId: opts.channelId,
            payload,
          };
          const envelopeBytes = encodeEnvelope(envelope);
          const sig = await signEd25519(envelopeBytes, opts.privateKeySeed);
          const signed: SignedEnvelope = {
            envelope,
            sig: hexEncode(sig),
          };
          await opts.writer.write(JSON.stringify(signed) + "\n");
        } finally {
          release();
        }
      })();
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

    const ok = await verifyEd25519(envelopeBytes, sigBytes, activePublicKey);
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
