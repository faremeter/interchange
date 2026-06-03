# @intx/inference-discovery-anthropic

Anthropic provider plug-in for the discovery rig. Captures Claude
wire responses across streaming and non-streaming variants of each
capability and writes them into the shared fixture corpus.

See [`@intx/inference-discovery`](../inference-discovery/README.md)
for the runtime, the plug-in contract, and the `discover` CLI.

## Models

- `claude-sonnet-4-5-20250929`
- `claude-opus-4-1-20250805`
- `claude-haiku-4-5-20251001`

The full per-capability list is in `SUPPORT_MATRIX` in
`@intx/inference-discovery/catalog`.

## Usage

```ts
import { createAnthropicPlugin } from "@intx/inference-discovery-anthropic";

const plugin = createAnthropicPlugin({ apiKey: process.env.ANTHROPIC_API_KEY });
// Hand off to runCapture from @intx/inference-discovery.
```

In practice the `bin/discover.ts` CLI does this wiring for you;
construct the plug-in directly only when writing tests or one-off
scripts.

## Environment

| Variable            | Purpose                         |
| ------------------- | ------------------------------- |
| `ANTHROPIC_API_KEY` | Sent as the `x-api-key` header. |

The key is redacted in captured fixtures.

## Multi-step capabilities

Three capability families drive multi-step exchanges before the
runner writes the bundle:

- **Files API** (`files-api-reference`,
  `files-api-reference-streaming`) — the plug-in first uploads the
  intent's media asset to the Anthropic files endpoint, then uses
  the returned file id to construct the messages body for the
  second step. Each step writes its own `upload/` and `generate/`
  subdirectory under the run root.
- **Multi-turn function calling**
  (`function-calling-multi-turn`,
  `function-calling-multi-turn-streaming`,
  `function-calling-with-thinking`,
  `function-calling-with-thinking-streaming`) — turn 1 is sent as
  usual; the plug-in extracts the model's content blocks from the
  parsed response and sends a turn-2 body that echoes the
  assistant turn verbatim and appends the tool result. Each turn
  writes its own `turn-1/` and `turn-2/` subdirectory.
- **Redacted thinking**
  (`redacted-thinking`, `redacted-thinking-streaming`) — turn 1
  produces a redacted thinking block that turn 2 must echo back
  verbatim; the plug-in extracts the redacted block from the
  response and assembles the turn-2 body around it.

The Files API and code-execution capabilities additionally set
per-step beta-flag headers (`files-api-2025-04-14`,
`code-execution-2025-05-22`). All other capabilities are
single-step.
