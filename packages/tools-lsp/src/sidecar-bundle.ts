// Sidecar-bundle entry for `@intx/tools-lsp`: a plugin-shaped factory the
// tool-package loader threads into `env.plugins` before tool factories run.
// LSP is not a self-contained tool runner: it contributes one standalone
// tool plus middleware that decorates posix's edit tools with
// diagnostics-after-edit. The `ToolPlugin` it produces is what
// `@intx/tools-posix`'s sidecar-bundle threads into `createPosixTools`.

import { definePlugin } from "@intx/agent";
import type { PosixToolEnv } from "@intx/tools-posix/sidecar-bundle";

import { createLSPPlugin, LSP_TOOL_DEFINITION } from "./index";

/**
 * Named export the loader picks up. The factory returns a `ToolPlugin` for
 * posix's bundle, so the LSP tool surfaces under posix's namespace. `dispose`
 * chains through to `lsp.dispose()` (see index.ts), which terminates the LSP
 * subprocess; the harness calls it on partial-success teardown and on
 * regular shutdown.
 */
export const lsp = definePlugin({
  id: "@intx/tools-lsp/sidecar-bundle",
  // Declare the standalone tool this plugin contributes so the deploy-time
  // capability walk can authorize it without instantiating the plugin (which
  // would start a language-server subprocess). The name matches what posix's
  // bundle registers from `env.plugins`, so the walked `tool:lsp` grant
  // matches the reactor's `tool:<call.name>` query. Not approval-gated, so
  // no `ask` mark.
  requires: ["toolCwd"],
  definitions: [{ name: LSP_TOOL_DEFINITION.name }],
  // Posix env: LSP contributes into posix's bundle and operates on the same
  // working tree, so it reads `toolCwd`, not the lock-boundary `workdir`.
  factory: (env: PosixToolEnv) => createLSPPlugin({ cwd: env.toolCwd }),
});
