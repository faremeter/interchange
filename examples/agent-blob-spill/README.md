# agent-blob-spill

Demonstrate that oversized tool output never has to sit inline in the
conversation. The agent's default size-cap transform writes the
payload to a blob in the context store and rewrites the in-history
`tool_result` block to a `tool-output:///<callId>` URI; the agent's
`blobReader` resolves the URI back to the original bytes when needed.

This is what lets agents touch large artifacts (a 200 KB log dump, a
100 KB SQL EXPLAIN, a screenshot) without blowing up token budgets or
forcing the model to chew through a wall of text on every subsequent
turn.

## What it shows

- The size-cap transform is on by default. There is no opt-in: a
  tool returning a string larger than `sizeCapMaxChars` (10 000
  chars) is spilled automatically.
- The spill produces two artifacts:
  1. A blob under `<contextDir>/tool-output/<callId>.txt` containing
     the full original payload.
  2. An in-history `tool_result` block whose content is rewritten to
     the first `maxChars` characters of the original payload followed
     by a `[Tool output truncated: omitted N chars. Full output
available at tool-output:///<callId> -- use read_file with that
URI to see the rest.]` notice.
- `agent.blobReader.read(uri)` returns the original bytes for any
  `tool-output:///<callId>` URI that points at an existing blob. The
  `BlobReader` is the read-side of this contract; the size-cap
  transform is the write-side.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-blob-spill
bun run start
```

The default prompt asks the model to call `fetch_full_logs`, a tool
the example registers whose result is a 25 000-character synthetic log
dump. The cycle is:

1. Model emits a `tool_use` block for `fetch_full_logs`.
2. The example's tool handler returns the full 25 000 chars.
3. The size-cap transform writes the bytes to
   `<contextDir>/tool-output/<callId>.txt` and replaces the
   `tool_result` block's text content with a marker referencing
   `tool-output:///<callId>`.
4. The agent re-invokes the model with the truncated `tool_result`;
   the model produces its final text reply.
5. The example then walks `history()`, finds the spill URI, calls
   `agent.blobReader.read(uri)`, and prints the resolved length plus
   the first few lines of the blob.

Output looks like:

```
assistant: I summarised the log; everything looks routine.

spill URI:               tool-output:///fetch_full_logs_1
in-history truncation:   [Tool output truncated: omitted 15020 chars. Full output available at tool-output:///fetch_full_logs_1 -- use read_file with that URI to see the rest.]
in-history block chars:  10128
resolved blob bytes:     25020
first lines of blob:
  ----- noisy tool emission -----
  log entry 0: nothing of consequence happened
  log entry 1: nothing of consequence happened
```

To start fresh:

```bash
rm -rf ../../tmp/agent-blob-spill
```

## Walkthrough

The interesting code lives in two places:

1. **`src/noisy-tool.ts`** registers a `stringTool` whose handler
   returns a 25 000-character string. The handler does not concern
   itself with spilling — that is the size-cap transform's job. From
   the tool author's perspective, you return strings as you would
   for any other tool.

2. **`src/cli.ts`** constructs the agent with `tools: [noisy]`, sends
   the prompt, and then traverses `history()` looking for a
   `tool_result` block whose text matches `tool-output:///<id>`. When
   found, the CLI calls `agent.blobReader.read(uri)` to materialise
   the original bytes.

The default size cap is **10 000 characters**. Override it by passing a
`sizeCapMaxChars` field on the env handed to `createAgent(def, env)` —
use a smaller cap to force spilling for every tool result, or a larger
one to allow more inline content per turn.

## Why spill at all?

A `tool_result` lives in the conversation history forever. Every
subsequent turn re-sends the full history to the model — so a single
50 KB SQL EXPLAIN result becomes 50 KB on the wire for every turn
after the one that produced it. Spilling caps the in-history block
at `maxChars` (10 000 by default) and appends a short truncation
notice that names the spill URI, so the model knows the full payload
exists and can `read_file` it back if it needs to.

The blob also lives in git, so the audit and rewind stories
([`agent-audit-log`](../agent-audit-log/README.md),
[`agent-rewind`](../agent-rewind/README.md)) cover spilled content
without any extra work — the blob file is committed alongside
`turns.jsonl` for the cycle that produced it.

## Reading a spill back from a tool

The example reads the blob from outside the agent for simplicity, but
in practice the model will typically call a "read this URI" tool when
it wants the contents. The posix tool package's `read_file` already
understands `tool-output:///` URIs (see
[`examples/coding-agent`](../coding-agent/README.md)); plug
`createPosixTools({ ..., blobReader: agent.blobReader })` into your
own assembly to get the same behaviour for arbitrary tools.
