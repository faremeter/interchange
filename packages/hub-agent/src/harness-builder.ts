// HarnessBuilder seam.
//
// The package declares the shape of the host's source-admission check;
// the host (apps/sidecar today, any custom sidecar tomorrow) supplies
// the concrete implementation. This keeps @intx/hub-agent free of
// dependencies on the concrete inference packages the check consults.

import type { InferenceSource } from "@intx/types/runtime";

export type HarnessBuilder = {
  /**
   * Throws if the supplied source cannot be built by this host. The
   * sidecar deploy router calls this to admit a step's pinned inference
   * source before spawning, so the operator sees rejection on the
   * control plane rather than during the next inference call.
   */
  canBuildSource(source: InferenceSource): void;
};
