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

import { createLSPPlugin } from "./index";

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
  factory: (env) => createLSPPlugin({ cwd: env.workdir }),
});
