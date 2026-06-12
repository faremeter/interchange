// Spawn-time env parser for the workflow-process child.
//
// The supervisor's spawn path constructs a fresh env object carrying
// only the IPC trust anchors plus a tightly-scoped set of deployment
// identifiers. The binary parses `process.env` once at start and hands
// the validated struct to `runWorkflowChild`. The struct shape is the
// only env-shaped surface the runtime body sees; everything else flows
// through IPC frames.
//
// The IPC trust anchors carried here are public-half values: the
// supervisor's Ed25519 PUBLIC key (`HOST_PUBKEY`) plus the shared HMAC
// key (`IPC_HMAC_KEY`) the supervisor minted at spawn time. The
// supervisor's Ed25519 PRIVATE key never appears in env per the IPC
// threat model; the child verifies but never signs control frames.

import { type } from "arktype";

import { hexDecode, IPC_CRYPTO } from "../ipc/index";

/**
 * Required env keys carried by the supervisor at spawn time. The
 * validator surface is intentionally narrow: every key documented at
 * the supervisor's `spawn(opts)` method is represented here, and
 * anything the supervisor did not place in the env causes a targeted
 * failure rather than a silent fallback.
 */
const SpawnTimeEnvShape = type({
  IPC_CHANNEL_ID: "string > 0",
  IPC_HMAC_KEY: "string > 0",
  HOST_PUBKEY: "string > 0",
  DEPLOYMENT_ID: "string > 0",
  DEFINITION_HASH: "string > 0",
  MAILBOX_ADDRESS: "string > 0",
}).onUndeclaredKey("ignore");

/**
 * Parsed and validated spawn-time env. The hex-encoded trust anchors
 * decode to their raw byte representations so the IPC channel
 * constructors can consume them without re-validating the hex shape.
 */
export interface SpawnTimeEnv {
  /** Channel identifier minted by the supervisor for this spawn. */
  channelId: string;
  /** 32-byte shared HMAC key for the event channel. */
  hmacKey: Uint8Array;
  /** Supervisor's 32-byte Ed25519 public key for control-frame verification. */
  hostPublicKey: Uint8Array;
  /** Deployment identity the supervisor manages. */
  deploymentId: string;
  /** Content hash of the deployed `WorkflowDefinition`. */
  definitionHash: string;
  /** Mail address the deployment registered on the bus. */
  mailboxAddress: string;
}

/**
 * Parse and validate `process.env`-shaped input into the typed
 * `SpawnTimeEnv` struct. Any missing key, malformed hex, or off-size
 * byte payload throws so the binary aborts before opening IPC.
 *
 * The validator runs at the boundary; downstream consumers trust the
 * parsed struct without re-checking.
 */
export function parseSpawnTimeEnv(
  rawEnv: Record<string, string | undefined>,
): SpawnTimeEnv {
  const present: Record<string, string> = {};
  for (const [key, value] of Object.entries(rawEnv)) {
    if (value !== undefined) present[key] = value;
  }
  const validated = SpawnTimeEnvShape(present);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow-child spawn-time env failed validation: ${validated.summary}`,
    );
  }
  const hmacKey = hexDecode(validated.IPC_HMAC_KEY);
  if (hmacKey.length !== IPC_CRYPTO.HMAC_KEY_BYTES) {
    throw new Error(
      `workflow-child IPC_HMAC_KEY must decode to ${String(IPC_CRYPTO.HMAC_KEY_BYTES)} bytes; got ${String(hmacKey.length)}`,
    );
  }
  const hostPublicKey = hexDecode(validated.HOST_PUBKEY);
  if (hostPublicKey.length !== IPC_CRYPTO.ED25519_KEY_BYTES) {
    throw new Error(
      `workflow-child HOST_PUBKEY must decode to ${String(IPC_CRYPTO.ED25519_KEY_BYTES)} bytes; got ${String(hostPublicKey.length)}`,
    );
  }
  // The channelId is supervisor-minted and the receiver compares it
  // byte-for-byte against incoming frames. Hex-decoding here would
  // surface a malformed value but the IPC primitives expect the
  // hex-encoded string form, so we only verify the encoded length
  // matches the documented channelId byte width.
  const expectedChannelIdHex = IPC_CRYPTO.CHANNEL_ID_BYTES * 2;
  if (validated.IPC_CHANNEL_ID.length !== expectedChannelIdHex) {
    throw new Error(
      `workflow-child IPC_CHANNEL_ID must be ${String(expectedChannelIdHex)} hex chars; got ${String(validated.IPC_CHANNEL_ID.length)}`,
    );
  }
  return {
    channelId: validated.IPC_CHANNEL_ID,
    hmacKey,
    hostPublicKey,
    deploymentId: validated.DEPLOYMENT_ID,
    definitionHash: validated.DEFINITION_HASH,
    mailboxAddress: validated.MAILBOX_ADDRESS,
  };
}
