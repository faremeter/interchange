# Examples

Runnable reference consumers of the public packages in this
repository. Each example is a workspace member with its own
`package.json` and is gated by `make all` (lint, type-check, test)
just like packages and apps — they are not throwaway scratch code,
and they are expected to keep working as the packages they consume
evolve.

Every example here targets `@intx/agent`. Start with
[`agent-quickstart`](./agent-quickstart/README.md); the rest layer
one concept on top of that baseline.

## Where to start

- [`agent-quickstart`](./agent-quickstart/README.md) — the smallest
  runnable agent. Construct, send a prompt, print the reply, close.
  Read this first.

## State and time

The agent's `contextDir` is a real git repository. These examples
show what that buys you.

- [`agent-resume`](./agent-resume/README.md) — re-run against the
  same `contextDir` and the previous conversation is already there.
  The resume-from-crash story.
- [`agent-rewind`](./agent-rewind/README.md) — clone the
  `contextDir`, roll `HEAD` back to an older commit, and open an
  agent at that earlier state. Branchable history.
- [`agent-audit-log`](./agent-audit-log/README.md) — walk the commit
  log directly with `isomorphic-git`. The audit record is not a
  separate artifact; it is the git tree.

## Tool I/O shapes

How tool calls and inbound messages move data through the agent
without overwhelming the conversation or the model.

- [`agent-blob-spill`](./agent-blob-spill/README.md) — the default
  size-cap transform writes oversized tool output to a blob and
  rewrites the `tool_result` to a `tool-output://` URI.
- [`agent-rich-tool`](./agent-rich-tool/README.md) — a tool returns
  a `pendingMarker` to open a correlation gate, then resolves later
  when an inbound message arrives with the matching id.
- [`agent-structured-payload`](./agent-structured-payload/README.md) —
  deliver a typed `InterchangeType` payload (e.g. `offering.request`)
  on an `InboundMessage` instead of a plain string `content`.

## Provider topology

- [`agent-multi-provider`](./agent-multi-provider/README.md) —
  combine per-task model selection, failover, and cost optimisation
  using the agent's `providers` array and `setProvider()`. Routing
  policy lives in user-land; the agent ships the primitives.

## Full integration

- [`coding-agent`](./coding-agent/README.md) — `@intx/agent`
  wired against `@intx/tools-posix` and
  `@intx/tools-lsp` to read, write, and reason about a real
  codebase. Demonstrates the public surface end-to-end.
