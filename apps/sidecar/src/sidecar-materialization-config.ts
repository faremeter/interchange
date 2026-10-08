// Process edge for the tool-package materialization boundary. This module
// reads the process and forwards one input each; `parseToolRegistries` and
// `hostPlatform` in `@intx/tool-packaging` own the rules. Boot, the deploy
// router, and the closure materializer call these names.

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
