// Sidecar-bundle entry for `@intx/tools-posix` — the convention-compliant
// factory the tool-package loader invokes.
//
// The factory reads the working tree it operates on from `env.toolCwd`
// and the blob store from the `BaseEnv` `storage` field, plus the
// optional `plugins` slot. Plugins are filtered by shape: any element
// of `env.plugins` that has a `tools` array, a `middleware` function,
// or a `dispose` function is treated as a `ToolPlugin` and handed to
// `createPosixTools`. This is how LSP (a plugin factory) plugs into
// posix without posix needing to know about LSP by name.

import { defineTool, isToolPluginInstance, type BaseEnv } from "@intx/agent";
import { createBlobReader } from "@intx/types/runtime";

import { createPosixTools } from "./index";
import type { ToolPlugin } from "./plugin";
import { GATED_TOOL_NAMES, TOOL_DEFINITIONS } from "./registry";

/**
 * Env contract for the posix sidecar bundle. `toolCwd` is the working
 * tree the posix filesystem tools operate on: read, write, edit, shell,
 * search, and grep resolve relative paths against it.
 *
 * It is independent of the `BaseEnv` `workdir` lock and storage
 * boundary. Two agents may share one `toolCwd` while holding distinct
 * `workdir` values; the posix tools apply no lock to `toolCwd`, so
 * concurrent writes to a shared tree are the caller's corruption risk
 * to own.
 */
export interface PosixToolEnv extends BaseEnv {
  toolCwd: string;
}

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
export const posix = defineTool<PosixToolEnv>({
  id: "@intx/tools-posix/sidecar-bundle",
  requires: ["toolCwd"],
  definitions: TOOL_DEFINITIONS.map((def) => ({
    name: def.name,
    ...(GATED_TOOL_NAMES.has(def.name) ? { approval: "ask" as const } : {}),
  })),
  factory: (env) => {
    const blobReader = createBlobReader(env.storage);
    const plugins = (env.plugins ?? []).filter(isToolPlugin);
    const tools = createPosixTools({
      cwd: env.toolCwd,
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
