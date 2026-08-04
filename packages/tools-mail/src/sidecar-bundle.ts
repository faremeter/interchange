// Sidecar-bundle entry for `@intx/tools-mail` — the convention-compliant
// factory the tool-package loader invokes.
//
// The bundle consumes the host-assembled runtime capabilities rather than
// building its own. The host (the sidecar's step-env builder) owns the
// `RuntimeCapabilities` and puts it on `env.capabilities`; this factory
// resolves `mail.transport` from it through `createMailTools` instead of
// re-wrapping a raw transport it was handed separately. The env keys it
// touches (`capabilities`, `address`) are declared in `requires`.

import { defineTool, type BaseEnv } from "@intx/agent";
import type { RuntimeCapabilities } from "@intx/types/runtime-capabilities";

import { createMailTools } from "./index";
import { TOOL_DEFINITIONS } from "./definitions";

/**
 * Env contract for the mail tool bundle. Extends `BaseEnv` with the
 * host-assembled `capabilities` -- from which the mail tools resolve
 * `mail.transport` -- and the agent `address`.
 */
export interface MailToolEnv extends BaseEnv {
  capabilities: RuntimeCapabilities;
  address: string;
}

/**
 * Named export the loader picks up. The id is package-namespaced per
 * the convention; the model-facing tool names are synthesized by the
 * loader as `@intx/tools-mail/sidecar-bundle:<def.name>`.
 */
export const mail = defineTool<MailToolEnv>({
  id: "@intx/tools-mail/sidecar-bundle",
  requires: ["capabilities", "address"],
  definitions: TOOL_DEFINITIONS.map((def) => ({ name: def.name })),
  factory: (env) => {
    const tools = createMailTools({ capabilities: env.capabilities });
    return {
      definitions: tools.definitions,
      run: (call, signal) => tools.run(call, signal),
      dispose: () => tools.dispose(),
    };
  },
});
