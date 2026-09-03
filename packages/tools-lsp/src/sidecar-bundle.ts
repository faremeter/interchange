// Sidecar-bundle entry for `@intx/tools-lsp` — a plugin-shaped factory
// the tool-package loader picks up and threads into `env.plugins`
// before tool factories run.
//
// LSP is not a self-contained tool runner: it contributes one
// standalone tool (`lsp_diagnostics` and friends) plus middleware
// that decorates posix's edit tools with diagnostics-after-edit. The
// `ToolPlugin` shape it produces is what `@intx/tools-posix`'s
// sidecar-bundle reads from `env.plugins` and threads into
// `createPosixTools({ plugins })`.

import { definePlugin } from "@intx/agent";
import type { PosixToolEnv } from "@intx/tools-posix/sidecar-bundle";

import { createLSPPlugin, LSP_TOOL_DEFINITION } from "./index";

/**
 * Named export the loader picks up. The factory returns a
 * `ToolPlugin` (from `@intx/tools-posix`). Posix's sidecar-bundle
 * filters `env.plugins` for entries that look like ToolPlugins and
 * hands them to `createPosixTools`. The LSP tool surfaces to the
 * model under posix's namespace because the plugin contributes it to
 * posix's bundle.
 *
 * `createLSPPlugin` returns a ToolPlugin whose `dispose` chains
 * through to `lsp.dispose()` (see packages/tools-lsp/src/index.ts),
 * which terminates the LSP subprocess. The default harness's
 * plugin-construction rollback loop calls that `dispose` on
 * partial-success teardown, and the harness's regular shutdown
 * path calls it through the bundle's own dispose chain.
 */
export const lsp = definePlugin({
  id: "@intx/tools-lsp/sidecar-bundle",
  // Declare the standalone tool this plugin contributes so the deploy-time
  // capability walk can authorize it without instantiating the plugin (which
  // would start a language-server subprocess). The runtime tool name is the
  // bare `LSP_TOOL_DEFINITION.name` ("lsp") -- the same name posix's bundle
  // registers it under from `env.plugins` -- so the walked `tool:lsp` grant
  // matches the reactor's `tool:<call.name>` query. The tool is not
  // approval-gated, so it carries no `ask` mark.
  requires: ["toolCwd"],
  definitions: [{ name: LSP_TOOL_DEFINITION.name }],
  // The env type is posix's: LSP contributes into posix's bundle via
  // `env.plugins` and operates on the same working tree, so it reads the
  // same `toolCwd` the posix tools scope to, not the lock-boundary
  // `workdir`.
  factory: (env: PosixToolEnv) => createLSPPlugin({ cwd: env.toolCwd }),
});
