# @intx/tools-lsp

LSP-backed tool plugin for the agent harness. Spawns language servers
in the agent's working directory (over JSON-RPC via
`vscode-jsonrpc`), exposes them as a single `lsp` tool the agent can
call, and installs middleware that ensures `write` and `edit` actions
on `@intx/tools-posix` keep the LSP's in-memory document state in
sync with what the agent wrote to disk.

Composes with `@intx/tools-posix`. `apps/sidecar` and the
`coding-agent` example consume both together.

`createLSPPlugin` returns a `ToolPlugin`. It is installed by
passing it to `createPosixTools` as a `plugins[]` entry; the posix
tool runner mounts the `lsp` tool and threads the LSP middleware
into its `write` and `edit` paths.

```ts
import { createPosixTools } from "@intx/tools-posix";
import { createLSPPlugin } from "@intx/tools-lsp";

const cwd = "/path/to/workspace";

const tools = createPosixTools({
  cwd,
  plugins: [createLSPPlugin({ cwd, minSeverity: 2 })],
});
```
