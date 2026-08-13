// Canonical runId derivation for a workflow deployment's top-level run.
//
// A workflow deployment has ONE addressable top-level run, whose stable runId
// is the local part of the deployment's mail address -- the `<runId>` in
// `<runId>@<domain>`. The supervisor's dispatch loop keys its per-run state,
// its grants barrier, and its terminal wait on this id. Every producer of a
// run's grants -- the hub-api trigger route and the sidecar's mail-deliver
// path -- must stage those grants under the SAME id, or they land under a run
// id the supervisor never looks up and the run fails closed on its
// `onRunStart` barrier.
//
// This module is the single source of truth those producers import, so
// their derivations cannot diverge. It exists to end the divergence that
// let the mail's Message-ID (a per-message identifier) masquerade as the
// runId: the runId is a property of the deployment, not of the individual
// trigger occurrence. Internal section/body runs still receive their own
// synthetic run ids and are not externally addressable.

import { parseRunAddress } from "./agent-address";

/**
 * The stable runId for a workflow deployment's one addressable top-level run:
 * the local part of its mail address, before the `@`. Callers hold the
 * deployment mail address in different forms -- a routing recipient, a
 * supervisor binding, a route-derived address -- and route it through this one
 * function so the runId contract is stated in exactly one place.
 *
 * Delegates to `parseRunAddress` so a single function owns the `@`-split: the
 * runId is the parsed local part. A malformed address (no `run_` marker, no
 * `@`, or an empty domain) is a caller bug, not a value to key state under, so
 * this throws rather than returning a fabricated id that would land run state
 * under an id the supervisor never looks up.
 */
export function deriveWorkflowRunId(address: string): string {
  const parsed = parseRunAddress(address);
  if (parsed === null) {
    throw new Error(`Invalid run address: ${JSON.stringify(address)}`);
  }
  return parsed.runId;
}
