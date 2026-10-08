// Sidecar-bundle entry for `@intx/tools-mail` -- the convention-compliant
// factory the tool-package loader invokes. The bundle consumes the
// host-assembled runtime capabilities from `env.capabilities` and resolves
// `mail.transport` through `createMailTools`; the env keys it touches are
// declared in `requires`.

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
 * the convention; the loader synthesizes model-facing tool names from
 * it as `@intx/tools-mail/sidecar-bundle:<def.name>`.
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
