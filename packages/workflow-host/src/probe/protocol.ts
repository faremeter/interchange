import { type } from "arktype";

import type { DirectorRegistry, ToolDeclaration } from "@intx/agent";
import type { GrantEffect } from "@intx/types";
import { GrantWalkSnapshot } from "@intx/types/grant-snapshot";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { WorkflowDefinition } from "@intx/workflow/definition";

/**
 * Capability walk the probe child runs over the evaluated definition.
 * The process that boots the probe passes the real walk. This module
 * does not import it.
 */
export type WalkCapabilities = (
  workflow: WorkflowDefinition,
  registry: DirectorRegistry,
  pluginDefs: ReadonlyMap<string, readonly ToolDeclaration[]>,
) => {
  readonly perStep: ReadonlyMap<
    string,
    {
      readonly grants: readonly string[];
      readonly grantEffects: ReadonlyMap<string, GrantEffect>;
    }
  >;
  readonly unresolvedDirectors: readonly string[];
};

// ---------------------------------------------------------------------------
// Result payload wire (child -> host)
// ---------------------------------------------------------------------------

/**
 * The child's single result payload, carried inside the HMAC-signed
 * envelope. `ok: true` ships the inert projection, the advisory grant
 * set, the un-flattened grant walk snapshot, and the wire hash; `ok:
 * false` ships the failure reason (eval throw, malformed code) so the
 * host can reject the probe with a meaningful message rather than a bare
 * "child exited" surface.
 */
export const ProbeResultPayload = type({
  ok: "true",
  projection: "unknown",
  grants: "string[]",
  grantWalkSnapshot: GrantWalkSnapshot,
  wireHash: "string > 0",
}).or({
  ok: "false",
  error: "string",
});
export type ProbeResultPayload = typeof ProbeResultPayload.infer;

/**
 * The inert answer a probe execution produces: the workflow's inert
 * needs-surface projection, the advisory grant set derived from it, the
 * un-flattened grant walk snapshot the set is derived from, and the
 * projection's content hash. Structurally the `WorkflowProbeResult` the
 * hub-agent probe seam consumes.
 *
 * `grantWalkSnapshot` carries the per-step grant declarations (grant
 * strings plus each step's tool-grant `grantEffects` map) and the
 * definition's full `grantRequirements` -- the grouping and effect data
 * the flattened `grants` union discards.
 */
export interface WorkflowProbeResult {
  readonly projection: WorkflowProjectionDefinition;
  readonly grants: string[];
  readonly grantWalkSnapshot: GrantWalkSnapshot;
  readonly wireHash: string;
}
