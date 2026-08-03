import { type } from "arktype";

/**
 * Requires a workflow to use a sidecar that is not shared with unrelated
 * workflows or ordinary work while its allocation is active.
 */
export const SidecarPlacementRequirement = type({
  sharing: "'exclusive'",
  "reuse?": "'never' | 'same-deployment'",
});
export type SidecarPlacementRequirement =
  typeof SidecarPlacementRequirement.infer;
