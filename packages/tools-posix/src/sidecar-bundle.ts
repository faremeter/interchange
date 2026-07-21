// Sidecar-bundle entry for `@intx/tools-posix` — the convention-compliant
// factory the tool-package loader invokes.
//
// The factory uses `BaseEnv` fields (`workdir`, `storage`) and the
// optional `plugins` slot. Plugins are filtered by shape: any element
// of `env.plugins` that has a `tools` array, a `middleware` function,
// or a `dispose` function is treated as a `ToolPlugin` and handed to
// `createPosixTools`. This is how LSP (a plugin factory) plugs into
// posix without posix needing to know about LSP by name.

import { defineTool, isToolPluginInstance } from "@intx/agent";
import { createBlobReader } from "@intx/types/runtime";

import { createPosixTools } from "./index";
import type { ToolPlugin } from "./plugin";
import { TOOL_DEFINITIONS } from "./registry";

function isToolPlugin(value: unknown): value is ToolPlugin {
  // Require the `kind: "tool-plugin"` marker minted by definePlugin
  // before any shape check. A foreign object that happens to expose
  // `tools`, `middleware`, or `dispose` would have been
  // mis-identified by the previous duck-typing-only check; the
  // marker eliminates that collision risk.
  if (!isToolPluginInstance(value)) return false;
  const hasTools = "tools" in value && Array.isArray(value["tools"]);
  const hasMiddleware =
    "middleware" in value && typeof value["middleware"] === "function";
  const hasDispose =
    "dispose" in value && typeof value["dispose"] === "function";
  return hasTools || hasMiddleware || hasDispose;
}

/**
 * Named export the loader picks up. The id is package-namespaced per
 * the convention.
 */
export const posix = defineTool({
  id: "@intx/tools-posix/sidecar-bundle",
  definitions: TOOL_DEFINITIONS.map((def) => ({ name: def.name })),
  factory: (env) => {
    const blobReader = createBlobReader(env.storage);
    const plugins = (env.plugins ?? []).filter(isToolPlugin);
    const tools = createPosixTools({
      cwd: env.workdir,
      blobReader,
      plugins,
    });
    return {
      definitions: tools.definitions,
      run: (call, signal) => tools.run(call, signal),
      dispose: () => tools.dispose(),
    };
  },
});
