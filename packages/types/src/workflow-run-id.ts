// Canonical runId for a workflow deployment's one top-level run: the local
// part of its mail address (`<runId>@<domain>`). The supervisor keys
// per-run state, the grants barrier, and the terminal wait on this id, so
// every grants producer must stage under the SAME id or the run fails
// closed on its `onRunStart` barrier. Single source of truth the producers
// import, so their derivations cannot diverge.

import { parseRunAddress } from "./agent-address";

/**
 * The stable runId for a workflow deployment's one top-level run: the
 * local part of its mail address, before the `@`. Every producer routes
 * the address through this one function so the runId contract lives in
 * one place.
 *
 * Delegates to `parseRunAddress` for the `@`-split. A malformed address
 * (no `run_` marker, no `@`, or an empty domain) is a caller bug, so this
 * throws rather than fabricating an id the supervisor never looks up.
 */
export function deriveWorkflowRunId(address: string): string {
  const parsed = parseRunAddress(address);
  if (parsed === null) {
    throw new Error(`Invalid run address: ${JSON.stringify(address)}`);
  }
  return parsed.runId;
}
