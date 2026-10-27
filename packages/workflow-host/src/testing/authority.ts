// Sending a new workflow child its first control frame.
//
// A child reads the supervisor's `authority.set` before it resumes any run,
// so a test that drives a child over the real channel code sends it first.

import type { AuthoritySetData } from "../ipc/control-payloads";
import type { ControlChannelSender } from "../ipc/control-sender";

/**
 * Send `authority.set`: the run's grants snapshot and credential material.
 * An omitted field is sent as null (no grants, no material), so the helper
 * never sends the form that keeps the child's current snapshot.
 */
export function sendAuthoritySet(
  sender: ControlChannelSender,
  data: Partial<AuthoritySetData> = {},
): Promise<void> {
  return sender.send({
    type: "authority.set",
    data: { snapshot: data.snapshot ?? null, delivery: data.delivery ?? null },
  });
}
