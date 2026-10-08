// Harness-side factory for the RuntimeCapabilities tool packages
// consume. Callers pass a config keyed by domain (`transport`); the
// harness translates to RuntimeCapabilityMap keys (`mail.transport`),
// so new capabilities evolve callers' shapes through this wrapper, not
// at the call site.

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
