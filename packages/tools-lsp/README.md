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

TypeScript support runs `typescript-language-server`, found in the
project's `node_modules/.bin` or on `PATH`, on the `tsserver` script
of the workspace's TypeScript. TypeScript 7 ships no such script, so a
TypeScript 7 workspace gets no server unless `tsserverPath` names one
from an older install, such as an npm alias
(`"typescript5": "npm:typescript@5.9.3"`):

```ts
createLSPPlugin({
  cwd,
  tsserverPath: `${cwd}/node_modules/typescript5/lib/tsserver.js`,
});
```

Relative `tsserverPath` values resolve from the caller's current directory
when the plugin is created, even when `cwd` points to another workspace.
