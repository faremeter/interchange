# coding-agent example

A reference consumer of `@intx/agent`. Wires the agent against
`@intx/tools-posix` (read/write/edit files, run shell, grep, search)
and `@intx/tools-lsp` (language-server diagnostics) so the model can
read files, write files, and reason about the codebase it is operating on.

This example exists to demonstrate the public surface of `@intx/agent`
end-to-end. Treat it as documentation that compiles.

## What it shows

- Constructing an agent via `createAgent` with a real `contextDir`.
- Bridging an existing `ToolRunner` (the bundle returned by
  `createPosixTools` with the LSP plugin attached) into the agent via
  `fromToolRunner`.
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

## Provider

The example targets Anthropic out of the box. To use a different provider,
construct the agent yourself with the appropriate `ProviderConfig` and call
`createCodingAgent` from `./src/agent.ts` directly — `createCodingAgent`
accepts a `providerOverride` that bypasses the Anthropic default.
