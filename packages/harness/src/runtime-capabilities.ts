// Harness-side factory for the RuntimeCapabilities that tool packages
// consume. The wrapper exists so callers (sidecar, alternate runtimes)
// pass a config object keyed by domain (`transport`) and the harness
// owns the translation to RuntimeCapabilityMap keys (`mail.transport`).
// When new capabilities are added, callers' shapes evolve through this
// wrapper, not at the call site.

import {
  createRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";
import type { MessageTransport } from "@intx/types/runtime";
import type { MailSigner } from "@intx/types/mail-signer";

export interface HarnessRuntimeCapabilitiesOptions {
  transport: MessageTransport;
  signer: MailSigner;
}

export function createHarnessRuntimeCapabilities(
  opts: HarnessRuntimeCapabilitiesOptions,
): RuntimeCapabilities {
  return createRuntimeCapabilities({
    "mail.transport": opts.transport,
    "mail.signer": opts.signer,
  });
}
