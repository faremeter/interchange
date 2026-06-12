// =============================================================
// THREAT MODEL -- workflow-process supervisor/child IPC
// =============================================================
//
// The workflow-process is a Bun child the supervisor spawns per
// active deployment. That child runs user-supplied workflow code:
// tools the operator deployed, director prompts the operator wrote,
// agent harnesses operating against external APIs. The supervisor
// lives in-sidecar, owns the mail-bus identity for the deployment's
// addresses, and holds the Ed25519 signing key used to commit
// authoritative records (`CancelRequested`, drain signatures, audit
// frames) on the deployment's behalf.
//
// The IPC channels between the supervisor and the child are the
// airlock between an authoritative-but-trusted process and a
// potentially-compromised one. Per-frame crypto is what makes the
// airlock seal: any frame the child receives whose signature does
// not verify under the supervisor's Ed25519 public key is dropped
// (and the receiver crashes); any frame the supervisor receives
// whose HMAC does not verify is dropped (and the receiver crashes).
//
// Design pieces, with the failure mode each one defends against:
//
// 1. Asymmetric crypto on the control channel, two keypairs per spawn.
//    Both directions of the control channel carry Ed25519-signed
//    frames; each direction uses its own keypair so neither end
//    holds the private half of the keypair its peer signs with.
//
//    Supervisor's keypair (downstream: supervisor -> child).
//    The supervisor mints the keypair at spawn time, signs every
//    downstream control frame (trigger.fire, signal.deliver, drain,
//    recycle, shutdown, grants-updated, sources-updated) with the
//    private half, and passes the corresponding 32-byte public half
//    to the child in spawn-time env (`HOST_PUBKEY`). The
//    SUPERVISOR'S PRIVATE KEY NEVER LEAVES THE SUPERVISOR'S ADDRESS
//    SPACE. The spawn-time env carries pubkey + HMAC key + channelId
//    and NEVER the supervisor's private key. A leak via fork() memory
//    copy is impossible because the supervisor uses `spawn` with an
//    explicitly constructed `env` object -- there is no inheritance
//    of the supervisor's process env into the child by reference.
//    A leak via accidental `process.env` propagation is prevented by
//    constructing the child env as a fresh object containing only
//    the documented variables (no `...process.env` spread). A leak
//    via serialization is prevented by the supervisor never placing
//    the private key in any IPC payload, log line, or audit-log
//    frame -- it lives only as a 32-byte Uint8Array held in closure
//    by the signing-key callback.
//
//    Child's keypair (upstream: child -> supervisor).
//    The child mints its own Ed25519 keypair at startup, signs every
//    upstream control frame with the private half, and publishes the
//    matching public half in the payload of the upstream `ready`
//    frame as `childPublicKey` (hex-encoded). The supervisor's
//    upstream receiver opens in bootstrap mode: it parses the first
//    frame's envelope structurally, extracts `childPublicKey`,
//    verifies the `ready` frame's signature against it, and uses the
//    same key to verify every subsequent upstream frame. The CHILD'S
//    PRIVATE KEY NEVER LEAVES THE CHILD'S ADDRESS SPACE. A
//    compromised child cannot forge a frame the supervisor accepts
//    under a different key: the bootstrap pins the verification key
//    to the value the first `ready` frame published, and a frame
//    that claims a different sender is one whose signature the
//    receiver cannot verify.
//
// 2. HMAC-SHA256 on the event channel.
//    Symmetric authentication is correct for the high-rate path
//    (InferenceEvents stream at the reactor's emit cadence, including
//    `message.run.started` / `message.run.ended` brackets). Both
//    sides hold the same 32-byte key; both sides authenticate every
//    frame. HMAC is roughly two orders of magnitude cheaper per byte
//    than Ed25519, which is what makes per-frame authentication
//    affordable at high rate. The cost saving holds across the full
//    InferenceEvent volume profile (delta events at token rate).
//
// 3. ChannelId rotation on every spawn AND every recycle.
//    The channelId is 16 bytes from `crypto.randomBytes`, hex-encoded
//    (interface-decisions Bonus 2). The supervisor mints it at every
//    spawn, places it in spawn-time env (`IPC_CHANNEL_ID`), and mints
//    a fresh one at every recycle. Receivers track the current
//    channelId; any frame carrying a non-current channelId is a
//    signal that the wire is still attached to a predecessor
//    workflow-process (a recycled child's leftover state in some
//    socket the supervisor failed to fully close, or an attacker who
//    captured a frame from a previous spawn and is replaying it
//    against the current one). The receiver crashes loudly rather
//    than processing the stale frame. Crash-on-mismatch is the only
//    honest response: a "stale frame" is by construction either a
//    replay or a programming bug, and silently dropping it would
//    paper over both.
//
// 4. Monotonic seq per channelId, crash on out-of-order.
//    Every frame within a channelId's lifetime carries a strictly-
//    increasing seq. The sender maintains a counter; the receiver
//    tracks the highest seq seen and requires the next frame's seq
//    to equal `highestSeq + 1` exactly (gap = drop, repeat = replay,
//    decrease = replay). Any seq violation crashes the receiver.
//    The combination of channelId rotation and monotonic seq
//    prevents an attacker who captured a previous channelId's frame
//    stream from replaying it against the current channel: the
//    captured frames carry the wrong channelId. Within the current
//    channelId, the monotonic-seq check prevents replay of an
//    earlier frame in the same channel.
//
// 5. Crash-on-overrun on the event channel.
//    The supervisor buffers in userspace with a bound (default 1024
//    frames). On overrun, the supervisor logs the saturation and the
//    workflow-process kills itself. The audit chain is built from
//    forwarded InferenceEvents; a silent drop of a single event
//    breaks the chain in a way no downstream consumer can detect.
//    The choice is between two unrecoverable states: (a) corrupt
//    audit chain that pretends to be correct, or (b) crash that
//    advertises itself. Crash wins.
//
// 6. Clean control-vs-event boundary.
//    The control channel carries `credentialsSnapshot` updates,
//    drain, recycle, grants-updated, ready, shutdown -- shapes whose
//    authority is the supervisor's identity and whose rate is low.
//    The event channel carries InferenceEvents and the bracket
//    events -- shapes whose authority is the deployment's identity
//    (which the supervisor and child share via the HMAC key) and
//    whose rate is high. The two channels' typed payload unions are
//    DISJOINT BY CONSTRUCTION (the discriminated `ControlPayload`
//    union in `control-channel.ts` does not overlap the
//    `EventPayload` union in `event-channel.ts`). A control payload
//    that included an inference-event shape would defeat the split;
//    an inference-event payload that included a `drain` or `recycle`
//    discriminator would let a compromised child issue control-plane
//    commands the supervisor honors. Neither is possible at the
//    type level.
//
// 7. Supervisor-minted channelId.
//    The supervisor is the single source of truth for channelId.
//    The child does NOT generate its own and never proposes a value.
//    A protocol that let the child propose a channelId would let a
//    compromised child negotiate a channelId an attacker had
//    pre-captured frames for. The supervisor mints, the child reads
//    from env, the supervisor enforces.
//
// =============================================================

export {
  ControlPayload,
  CredentialsSnapshotPayload,
  CredentialsSnapshotStepPayload,
  createControlChannelSender,
  receiveControlChannel,
  type ControlChannelSender,
  type ControlChannelSenderOpts,
  type ControlChannelReceiverOpts,
  type NdjsonReader,
  type NdjsonWriter,
} from "./control-channel";

export {
  DEFAULT_EVENT_BUFFER_LIMIT,
  EventPayload,
  createEventChannelSender,
  receiveEventChannel,
  type EventChannelSender,
  type EventChannelSenderOpts,
  type EventChannelReceiverOpts,
  type FrameReader,
  type FrameWriter,
} from "./event-channel";

export {
  FrameEnvelope,
  MacedEnvelope,
  SignedEnvelope,
  decodeEnvelope,
  encodeEnvelope,
  hexDecode,
  hexEncode,
} from "./envelope";

export {
  IPC_CRYPTO,
  generateChannelId,
  generateHmacKey,
  signEd25519,
  signHmac,
  verifyEd25519,
  verifyHmac,
} from "./crypto";
