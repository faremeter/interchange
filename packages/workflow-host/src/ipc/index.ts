// Threat model -- workflow-process supervisor/child IPC.
//
// The workflow-process is a Bun child the supervisor spawns per active
// deployment. That child runs user-supplied workflow code; the supervisor
// lives in-sidecar, owns the deployment's mail-bus identity, and holds the
// Ed25519 signing key that commits authoritative records. The IPC channels
// between them are the airlock between an authoritative-but-trusted process
// and a potentially-compromised one: any frame the child receives whose
// signature does not verify under the supervisor's Ed25519 public key is
// dropped (and the receiver crashes); any frame the supervisor receives
// whose HMAC does not verify is dropped (and the receiver crashes). Design
// pieces, with the failure mode each defends against:
//
// 1. Asymmetric crypto on the control channel, two keypairs per spawn.
//    Each direction signs with its own keypair so neither end holds the
//    private half of the keypair its peer signs with. Downstream: the
//    supervisor mints the keypair at spawn, signs every frame with the
//    private half, and passes the public half in spawn-time env
//    (`HOST_PUBKEY`). The private key never leaves the supervisor's address
//    space: the child env is an explicitly constructed object with only the
//    documented variables (no `...process.env` spread), and the key is
//    never placed in any IPC payload, log line, or audit frame. Upstream:
//    the child mints its own keypair at startup and publishes the public
//    half on the `ready` frame's `childPublicKey`; the supervisor's
//    bootstrap mode parses the first frame's envelope structurally,
//    extracts the key, verifies the `ready` signature against it, and
//    verifies every subsequent frame against the same key -- so a
//    compromised child cannot forge a frame the supervisor accepts under a
//    different key. The child's private key never leaves the child's
//    address space.
//
// 2. HMAC-SHA256 on the event channel. Symmetric authentication is correct
//    for the high-rate path (InferenceEvents at the reactor's emit
//    cadence). Both sides hold the same 32-byte key; HMAC is roughly two
//    orders of magnitude cheaper per byte than Ed25519, which is what makes
//    per-frame authentication affordable at high rate.
//
// 3. ChannelId rotation on every spawn AND every recycle. The supervisor
//    mints the channelId (16 random bytes, hex-encoded), places it in
//    spawn-time env (`IPC_CHANNEL_ID`), and mints a fresh one at every
//    recycle. Any frame carrying a non-current channelId is either a
//    recycled child's leftover state or a replayed frame from a previous
//    spawn; the receiver crashes loudly rather than processing it.
//    Crash-on-mismatch is the only honest response: a "stale frame" is by
//    construction either a replay or a programming bug.
//
// 4. Monotonic seq per channelId, crash on out-of-order. The receiver
//    requires each frame's seq to equal `highestSeq + 1` exactly (gap =
//    drop, repeat or decrease = replay). Combined with channelId rotation
//    this blocks replay of a captured frame stream against the current
//    channel (wrong channelId) and replay of an earlier frame within it
//    (seq check).
//
// 5. Crash-on-overrun on the event channel. The supervisor buffers in
//    userspace with a bound (default 1024 frames). On overrun it logs the
//    saturation and the child kills itself: a silent drop of a single
//    forwarded InferenceEvent breaks the audit chain in a way no downstream
//    consumer can detect, so the choice is between a corrupt audit chain
//    that pretends to be correct and a crash that advertises itself. Crash
//    wins.
//
// 6. Clean control-vs-event boundary. The control channel carries low-rate,
//    supervisor-authority shapes (credentials, drain, recycle, ready,
//    shutdown); the event channel carries high-rate, deployment-authority
//    shapes (InferenceEvents and brackets, authenticated by the shared HMAC
//    key). The two typed payload unions are disjoint by construction: a
//    control payload shaped like an inference event would defeat the split,
//    and an event payload shaped like `drain`/`recycle` would let a
//    compromised child issue control-plane commands. Neither is possible at
//    the type level.
//
// 7. Supervisor-minted channelId. The supervisor is the single source of
//    truth; the child never proposes a value, so a compromised child cannot
//    negotiate a channelId an attacker had pre-captured frames for. The
//    supervisor mints, the child reads from env, the supervisor enforces.

export {
  ControlPayload,
  MailboxNotifyHeaders,
  OutboundAttachmentPayload,
  OutboundMessagePayload,
  SourcesUpdatedData,
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
