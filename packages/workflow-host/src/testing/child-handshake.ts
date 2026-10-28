// A fake workflow child's side of the startup handshake.
//
// A real child reports the definition it verified before the supervisor sends
// it anything, then sends `ready` once it has applied its authority. A test
// that drives a supervisor without spawning a child sends the report through
// this helper and spawns with the matching hash.

import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";

import type { ControlChannelSender } from "../ipc/control-sender";

/** The definition a fake child reports unless a test passes its own. */
export const TEST_DEFINITION: WorkflowProjectionDefinition = {
  id: "test-workflow",
  triggers: [],
  stepOrder: [],
  steps: {},
};

/** The approved hash of `TEST_DEFINITION`: the `definitionHash` to spawn with. */
export const TEST_DEFINITION_HASH =
  await computeWireDefinitionHash(TEST_DEFINITION);

/**
 * Send a child's `definition` frame. It must be the first frame on the
 * sender, since the supervisor learns the child's verifying key from it.
 */
export function sendDefinition(
  sender: ControlChannelSender,
  opts: {
    childPid: number;
    /** Hex-encoded Ed25519 public key that signs the child's frames. */
    childPublicKey: string;
    projection?: WorkflowProjectionDefinition;
  },
): Promise<void> {
  return sender.send({
    type: "definition",
    data: {
      childPid: opts.childPid,
      childPublicKey: opts.childPublicKey,
      result: { ok: true, projection: opts.projection ?? TEST_DEFINITION },
    },
  });
}
