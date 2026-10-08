// Sidecar-bundle entry for `@intx/tools-posix`: the loader-invoked factory.
// `env.toolCwd` is the working tree, `env.storage` the blob store, and
// `env.plugins` is filtered to ToolPlugin-shaped values.

import { defineTool, isToolPluginInstance, type BaseEnv } from "@intx/agent";
import { createBlobReader } from "@intx/types/runtime";

import { createPosixTools } from "./index";
import type { ToolPlugin } from "./plugin";
import { GATED_TOOL_NAMES, TOOL_DEFINITIONS } from "./registry";

/**
 * Env contract: `toolCwd` is the working tree the posix tools resolve
 * relative paths against, independent of `BaseEnv` `workdir`. The posix
 * tools apply no lock to `toolCwd`; callers own the concurrency risk.
 */
export interface PosixToolEnv extends BaseEnv {
  toolCwd: string;
}

function isToolPlugin(value: unknown): value is ToolPlugin {
  // The `kind: "tool-plugin"` marker (minted by definePlugin) prevents
  // mis-identifying foreign objects that happen to expose those fields.
  if (!isToolPluginInstance(value)) return false;
  const hasTools = "tools" in value && Array.isArray(value["tools"]);
  const hasMiddleware =
    "middleware" in value && typeof value["middleware"] === "function";
  const hasDispose =
    "dispose" in value && typeof value["dispose"] === "function";
  return hasTools || hasMiddleware || hasDispose;
}

/** Named export the loader picks up; id is package-namespaced. */
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
