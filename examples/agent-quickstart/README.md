# agent-quickstart

The smallest runnable `@intx/agent` program. Construct, send a
prompt, print the reply, close. Nothing else.

This example exists to answer "what is the minimum amount of code I
need to talk to an agent?" — read the body of
[`src/cli.ts`](./src/cli.ts) and you have it. Everything around the
`main()` body is plumbing the integration test uses to swap a stub
inference source in for the real Anthropic API; production callers do
not see that machinery.

## What it shows

- Constructing an `AgentDefinition` via `defineAgent` with `id`,
  `systemPrompt`, `tools`, `capabilities`, and `inference`.
- Building a `BaseEnv` with the active source, an isogit `ContextStore`,
  a workdir, a no-op audit store, a permissive authorize, and the
  built-in director registry.
- Instantiating via `createAgent(def, env)`.
- A single round trip through `agent.send(prompt)`.
- Tearing the agent down with `agent.close()` so the singleton-per-
  `workdir` lock is released cleanly.

Notably absent: tools, streaming, structured payloads, multi-turn
state. Those each have their own example in this directory.

## Running

Against the real Anthropic API:

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-quickstart
bun run start "name three planets"
```

The reply is written to stdout; the agent's audit and history land in
`<repo-root>/tmp/agent-quickstart/context/`. That directory is
gitignored by the repo-wide `tmp/` rule. Re-running the script picks
up the previous conversation (see
[`examples/agent-resume`](../agent-resume/README.md) for what that
implies); delete the directory if you want a fresh start:

```bash
rm -rf ../../tmp/agent-quickstart
```

Without `ANTHROPIC_API_KEY` set the example prints a short message
explaining what to set and exits non-zero. That message comes from
`@intx/example-agent-common`'s `resolveAgentSource` helper, which
every agent-\* example shares.

## Walkthrough

The `main()` function in `src/cli.ts` does five things in order:

1. **Parse arguments.** The prompt is the rest of the command line
   joined by spaces; an empty prompt prints a one-line usage message
   and returns exit code 1.
2. **Resolve the inference source.** `resolveAgentSource` wraps the
   lower-level `resolveSource` (which reads `ANTHROPIC_API_KEY` from
   `env`, defaults the model to `claude-sonnet-4-6`, and synthesizes an
   `id` of `${provider}:${model}`); on incomplete env it writes a
   friendly help message to stderr and returns `null`, otherwise it
   returns the resolved `InferenceSource`. Tests bypass env resolution
   by supplying `sourceOverride`.
3. **Construct the agent.** `contextDir` is the only piece of state
   the example owns; the isogit-backed context store materialises
   inside it on first use. `tools: []` keeps the surface honest — a
   plain conversation has nothing to call.
4. **Send the prompt.** `agent.send()` resolves once the reactor
   reaches `connector.reply`, returning both the text and the full
   `ConversationTurn` that produced it.
5. **Close.** `agent.close()` aborts the reactor, drains the send
   queue, waits for any in-flight commits to flush, and releases the
   contextDir singleton lock so the next process can open the same
   directory.

`agent.send()` and `agent.close()` are the two methods you have to
know. Every other surface on `Agent` (streaming, history, readAt,
setSource, deliver) is layered on top and demonstrated in a
dedicated example.

## Why so short?

The size of the smallest useful program is a feature of the package.
If `createAgent` needed eighty lines of setup before you could say
hello, nobody would reach for it for a quick experiment. The body of
`main()` fits on a single screen — argument parsing, provider
resolution, agent construction, one send, close — with no scaffolding
beyond what the surrounding example test seam (stdout / stderr /
`sourceOverride` / `deps` injection) demands.

## Next

- [`agent-resume`](../agent-resume/README.md) — run twice on the same
  `contextDir` and see the conversation pick up where it left off.
- [`agent-rewind`](../agent-rewind/README.md) — walk the conversation
  history and root a new agent at an older state.
- [`coding-agent`](../coding-agent/README.md) — the full-fat reference
  consumer, wired against the posix and LSP tool packages.
