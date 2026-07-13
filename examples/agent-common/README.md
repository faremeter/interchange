# example-agent-common

Shared helpers for the `agent-*` example packages.

This package is **not itself an example** — it is the place where every
agent-\* example pulls in identical boilerplate so the examples
themselves stay focused on the feature they demonstrate.

## What's here

- `resolveSource({ env, sourceOverride, exampleName })` returns an
  `InferenceSource` derived from `ANTHROPIC_API_KEY` (with a default
  model, base URL, and synthesized `id`), or returns a
  `{ ok: false, help }` object whose `help` string is a friendly
  multi-line message the caller writes to stderr before exiting
  non-zero. Tests bypass env resolution entirely by passing
  `sourceOverride` directly.
- `resolveAgentSource(opts, env, exampleName, stderr)` wraps
  `resolveSource` for the common single-source case: it writes the
  help message to `stderr` and returns `null` when the env is
  incomplete, otherwise returns the resolved `InferenceSource`.
- `openExampleAgent(opts, spec)` constructs the `@intx/agent` `Agent`
  every runnable example uses. It builds an `AgentDefinition` that
  wraps the example's tools as one bundle factory and an `AgentEnv`
  carrying the active source (the entry whose `id` matches
  `defaultSource`), an isogit-backed context store at the example's
  context dir that also serves as the audit store, a permissive
  authorize, and the default director registry.

If you are looking for a runnable agent example, start with
[`examples/agent-quickstart`](../agent-quickstart/README.md).
