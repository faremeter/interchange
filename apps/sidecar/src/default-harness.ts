// Default HarnessBuilder for the sidecar app.
//
// Implements the HarnessBuilder source-admission seam declared by
// @intx/hub-agent using the sidecar app's adapter registry. The package
// never sees the concrete inference dependencies the check consults.

import { type AdapterRegistry } from "@intx/inference";
import type { HarnessBuilder } from "@intx/hub-agent";
import type { InferenceSource } from "@intx/types/runtime";

export interface DefaultHarnessBuilderConfig {
  /**
   * Adapter registry resolved once at the boot edge (built-ins merged
   * with any operator-configured custom adapters). It backs the
   * `canBuildSource` membership check the deploy router calls to admit a
   * step's pinned inference source before spawning.
   */
  readonly adapters: AdapterRegistry;
}

export function createDefaultHarnessBuilder(
  config: DefaultHarnessBuilderConfig,
): HarnessBuilder {
  return {
    canBuildSource(source: InferenceSource): void {
      if (!config.adapters.has(source.provider)) {
        throw new Error(
          `Source provider "${source.provider}" is not registered`,
        );
      }
    },
  };
}
