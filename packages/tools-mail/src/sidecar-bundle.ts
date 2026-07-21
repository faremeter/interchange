// Sidecar-bundle entry for `@intx/tools-mail` — the convention-compliant
// factory the tool-package loader invokes.
//
// The factory declares the env keys it touches (`transport`, `address`)
// via `defineTool`'s `requires`. The host populates those slots in the
// per-instance env; the factory wraps the existing `createMailTools`
// implementation.

import { defineTool, type BaseEnv } from "@intx/agent";
import type { MessageTransport } from "@intx/types/runtime";
import { createRuntimeCapabilities } from "@intx/types/runtime-capabilities";

import { createMailTools } from "./index";
import { TOOL_DEFINITIONS } from "./definitions";

/**
 * Env contract for the mail tool bundle. Extends `BaseEnv` with the
 * harness-level fields the mail tools depend on at handler-init time.
 * Compatible by structure with `@intx/harness`'s `MailEnv` so a host
 * that already provides that env can pass it straight through.
 */
export interface MailToolEnv extends BaseEnv {
  transport: MessageTransport;
  address: string;
}

/**
 * Named export the loader picks up. The id is package-namespaced per
 * the convention; the model-facing tool names are synthesized by the
 * loader as `@intx/tools-mail/sidecar-bundle:<def.name>`.
 */
export const mail = defineTool<MailToolEnv>({
  id: "@intx/tools-mail/sidecar-bundle",
  requires: ["transport", "address"],
  definitions: TOOL_DEFINITIONS.map((def) => ({ name: def.name })),
  factory: (env) => {
    const capabilities = createRuntimeCapabilities({
      "mail.transport": env.transport,
    });
    const tools = createMailTools({ capabilities });
    return {
      definitions: tools.definitions,
      run: (call, signal) => tools.run(call, signal),
      dispose: () => tools.dispose(),
    };
  },
});
