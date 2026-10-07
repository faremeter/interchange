// Harness-side factory for the RuntimeCapabilities that tool packages
// consume. The wrapper exists so callers (sidecar, alternate runtimes)
// pass a config object keyed by domain (`transport`) and the harness
// owns the translation to RuntimeCapabilityMap keys
// (`mail.transport`). New capabilities evolve callers' shapes through
// this wrapper, not at the call site.

import {
  createRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";
import type { MessageTransport } from "@intx/types/runtime";

export interface HarnessRuntimeCapabilitiesOptions {
  transport: MessageTransport;
}

export function createHarnessRuntimeCapabilities(
  opts: HarnessRuntimeCapabilitiesOptions,
): RuntimeCapabilities {
  return createRuntimeCapabilities({ "mail.transport": opts.transport });
}
