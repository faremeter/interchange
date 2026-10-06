// Process edge for the tool-package materialization boundary.
//
// `parseToolRegistries` and `hostPlatform` in `@intx/tool-packaging`
// own the `SIDECAR_TOOL_REGISTRIES` rules and the npm `os`/`cpu`
// allowlists. This module reads the process and forwards one input
// each. Boot, the deploy router, and the closure materializer call
// these names.

import {
  hostPlatform,
  parseToolRegistries,
  type HostPlatform,
  type RegistryConfig,
} from "@intx/tool-packaging";

export function readRegistries(): ReadonlyMap<string, RegistryConfig> {
  return parseToolRegistries(process.env["SIDECAR_TOOL_REGISTRIES"]);
}

export function resolveHostPlatform(): HostPlatform {
  return hostPlatform(process.platform, process.arch);
}
