# coding-agent example

A reference consumer of `@intx/agent`. Wires the agent against
`@intx/tools-posix` (read/write/edit files, run shell, grep, search)
and `@intx/tools-lsp` (language-server diagnostics) so the model can
read files, write files, and reason about the codebase it is operating on.

This example exists to demonstrate the public surface of `@intx/agent`
end-to-end. Treat it as documentation that compiles.

## What it shows

- Constructing an `AgentDefinition` via `defineAgent` and an env with an
  isogit-backed `ContextStore` at a real `contextDir`, then
  instantiating via `createAgent(def, env)`.
- Flowing the working directory through env-DI: `env.toolCwd` (the tree
  the model reads and edits, from `--cwd`) is distinct from `env.workdir`
  (the isogit lock and storage boundary, at `contextDir`). The shipped
  `@intx/tools-posix` sidecar bundle and the `@intx/tools-lsp` plugin both
  read `toolCwd` through their `requires` declarations, and the LSP plugin
  is composed onto the env via `env.plugins`. Disposal stays caller-owned;
  the example captures the tool bundle's disposer and `close()` awaits it
  after the agent closes.
- A single-shot `send()` that returns the model's reply.
- Persistent history: re-running the example against the same `contextDir`
  picks up where the previous run left off — this is the resume-from-crash
  story. There is nothing to opt into; the agent commits each cycle to git
  via `@intx/storage-isogit` and `history()` projects from there.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
bun run start "list the markdown files in the repo root"
```

By default the example stores conversation state under
`<repo-root>/tmp/coding-agent/context/`. The directory is gitignored by the
repository-wide `tmp/` rule. Delete it to start a fresh conversation:

```bash
rm -rf ../../tmp/coding-agent
```

The default working directory for tool calls is the repository root; pass
`--cwd <path>` to change it.

## Inference source

The example targets Anthropic out of the box. To use a different provider,
construct the agent yourself with the appropriate `InferenceSource` and call
`createCodingAgent` from `./src/agent.ts` directly — `createCodingAgent`
accepts a `sourceOverride` that bypasses the Anthropic default.
