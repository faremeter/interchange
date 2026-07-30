// Supervisor-side producer of the workflow-process child's spawn-time env.
//
// Both the initial spawn and every recycle respawn route the env through
// this single builder, so the required-key contract the child-side parser
// (`parseSpawnTimeEnv`) enforces has exactly one producer. A divergence
// between the two paths -- the recycle env once omitted `STEP_COUNT` and
// broke every recycle -- is no longer expressible: the required keys are
// built as an exactly-typed record, so omitting one is a compile error.

import { hexEncode } from "@intx/types";

import type { RequiredSpawnEnvKey } from "../child/env-bootstrap";

export interface ChildSpawnEnvParts {
  /**
   * The deployment's stable substrate env (DATA_DIR, the adapter manifest,
   * and so on), frozen for the deployment's lifetime. Layered UNDER the
   * dynamic env fragment (so a host revision wins) and the per-spawn anchors
   * (so a required key is never shadowed).
   */
  substrateEnv: Record<string, string>;
  /**
   * Host-supplied dynamic env fragment, recomputed for every spawn and
   * respawn. Its keys layer OVER `substrateEnv` (so a value the host revised
   * between spawns wins) and UNDER the required anchors. Returns `{}` when
   * the host has no dynamic entries.
   */
  dynamicSpawnEnv: () => Record<string, string>;
  /** Supervisor-minted IPC channel id for this spawn. */
  channelId: string;
  /** Shared HMAC key for the event channel, minted for this spawn. */
  hmacKey: Uint8Array;
  /** Supervisor's Ed25519 public key for this spawn's control channel. */
  hostPublicKey: Uint8Array;
  /** Deployment identity the supervisor manages. */
  deploymentId: string;
  /** Mail address the deployment registered on the bus. */
  deploymentMailAddress: string;
  /** Step count of the deployed workflow (`stepOrder.length`). */
  stepCount: number;
  /** Content hash of the deployed workflow definition. */
  definitionHash: string;
  /** Whether this deployment's agent is warm-kept across messages. */
  warmKeep: boolean;
}

/**
 * Build the spawn-time env for a workflow-process child. The single
 * producer of what `parseSpawnTimeEnv` consumes; the initial spawn and
 * every recycle respawn both call it, so neither can drift from the
 * required-key contract.
 */
export function buildChildSpawnEnv(
  parts: ChildSpawnEnvParts,
): Record<string, string> {
  // Exactly-typed so omitting a required key fails the type-check rather
  // than surfacing as a child env-parse abort in production.
  const required: Record<RequiredSpawnEnvKey, string> = {
    IPC_CHANNEL_ID: parts.channelId,
    IPC_HMAC_KEY: hexEncode(parts.hmacKey),
    HOST_PUBKEY: hexEncode(parts.hostPublicKey),
    DEPLOYMENT_ID: parts.deploymentId,
    DEFINITION_HASH: parts.definitionHash,
    MAILBOX_ADDRESS: parts.deploymentMailAddress,
    STEP_COUNT: String(parts.stepCount),
  };
  return {
    ...parts.substrateEnv,
    // Host-revised entries win over the frozen substrate env; the required
    // anchors below still win over everything.
    ...parts.dynamicSpawnEnv(),
    ...required,
    WARM_KEEP: parts.warmKeep ? "true" : "false",
  };
}
