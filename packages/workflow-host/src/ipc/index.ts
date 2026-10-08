// THREAT MODEL -- workflow-process supervisor/child IPC.
//
// The workflow-process is a Bun child the supervisor spawns per active
// deployment; the child runs user-supplied workflow code, the supervisor
// lives in-sidecar, owns the deployment's mail-bus identity, and holds the
// Ed25519 key that signs authoritative records. The IPC channels between
// them are the airlock between a trusted process and a potentially
// compromised one: a frame whose per-frame authentication fails is
// dropped and the receiver crashes. Design pieces, with the failure mode
// each defends against:
//
// 1. Asymmetric crypto on the control channel, two keypairs per spawn.
//    Each direction signs with its own keypair, so neither end holds the
//    private half its peer signs with; the PRIVATE KEY NEVER LEAVES the
//    signer's address space. Downstream: the supervisor mints at spawn,
//    signs every frame, and passes the public half via spawn-time env
//    (`HOST_PUBKEY`) -- an explicitly constructed object (no
//    `...process.env` spread), and the key never rides a payload, log
//    line, or audit frame. Upstream: the child mints at startup and
//    publishes the public half on the `ready` frame's `childPublicKey`;
//    the supervisor's bootstrap mode parses the first frame structurally,
//    verifies `ready` against that key, then verifies every later frame
//    against the same key, so a compromised child cannot forge under a
//    different key.
//
// 2. HMAC-SHA256 on the event channel. Both sides hold the same 32-byte
//    key, and HMAC is roughly two orders of magnitude cheaper per byte
//    than Ed25519 -- what makes per-frame authentication affordable at
//    the reactor's emit cadence.
//
// 3. ChannelId rotation on every spawn and every recycle. The supervisor
//    mints the channelId (16 random bytes, hex) into spawn-time env
//    (`IPC_CHANNEL_ID`), fresh at each recycle; a frame with a
//    non-current channelId is a recycled child's leftover or a replayed
//    frame from a previous spawn, and the receiver crashes loudly -- the
//    only honest response to a frame that is by construction a replay or
//    a bug.
//
// 4. Monotonic seq per channelId, crash on out-of-order. The receiver
//    requires seq == `highestSeq + 1` exactly (gap = drop, repeat or
//    decrease = replay); with channelId rotation this blocks replay of a
//    captured stream (wrong channelId) and replay within it (seq check).
//
// 5. Crash-on-overrun on the event channel. The supervisor buffers in
//    userspace (default 1024 frames); on overrun it logs and the child
//    kills itself, because a silent drop of one forwarded InferenceEvent
//    breaks the audit chain undetectably -- a crash that advertises
//    itself beats a corrupt chain that pretends to be correct.
//
// 6. Clean control-vs-event boundary. The control channel carries
//    low-rate, supervisor-authority shapes (credentials, drain, recycle,
//    ready, shutdown); the event channel carries high-rate,
//    deployment-authority shapes (InferenceEvents and brackets, under the
//    shared HMAC key). The two payload unions are disjoint by
//    construction: a control-shaped event or an event-shaped
//    `drain`/`recycle` (a compromised child issuing control commands) is
//    impossible at the type level.
//
// 7. Supervisor-minted channelId. The child never proposes a value, so a
//    compromised child cannot negotiate a channelId an attacker already
//    captured frames for; the supervisor mints, the child reads from
//    env, the supervisor enforces.

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
