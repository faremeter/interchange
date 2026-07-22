# @intx/inference-discovery-google-genai

Google GenAI provider plug-in for the discovery rig. Captures
Gemini wire responses across the capability matrix and writes them
into the shared fixture corpus.

See [`@intx/inference-discovery`](../inference-discovery/README.md)
for the runtime, the plug-in contract, and the `discover` CLI.

## Models

- `gemini-2.5-flash` — text, vision, audio, video, document,
  function calling (multi-turn and with-thinking), code execution,
  grounding, and the files API. Streaming and non-streaming
  variants of each.
- `gemini-2.5-flash-image` — image output, streaming and
  non-streaming.
- `gemini-2.5-pro` — the same text, vision, audio, video, document,
  function-calling, code-execution, grounding, and files-API surface
  as `gemini-2.5-flash`. It only runs in thinking mode (a zero
  thinking budget is rejected), so the request builder uses the
  dynamic thinking budget where flash disables thinking.

The full per-capability list is in `SUPPORT_MATRIX` in
`@intx/inference-discovery/catalog`.

## Usage

```ts
import { createGoogleGenaiPlugin } from "@intx/inference-discovery-google-genai";

const plugin = createGoogleGenaiPlugin({ apiKey: process.env.GOOGLE_API_KEY });
// Hand off to runCapture from @intx/inference-discovery.
```

In practice the `bin/discover.ts` CLI does this wiring for you;
construct the plug-in directly only when writing tests or one-off
scripts.

## Environment

| Variable         | Purpose                              |
| ---------------- | ------------------------------------ |
| `GOOGLE_API_KEY` | Sent as the `x-goog-api-key` header. |

The key is redacted in captured fixtures.

## Multi-step capabilities

Two capability families drive multi-step exchanges before the
runner writes the bundle:

- **Files API** (`files-api-reference`,
  `files-api-reference-streaming`) — the plug-in first uploads the
  intent's media asset to the `generativelanguage` upload endpoint,
  then uses the returned file URI and MIME type to construct the
  generate-content body for the second step. Each step writes its
  own `upload/` and `generate/` subdirectory under the run root.
- **Multi-turn function calling**
  (`function-calling-multi-turn`,
  `function-calling-multi-turn-streaming`,
  `function-calling-with-thinking`,
  `function-calling-with-thinking-streaming`) — turn 1 is sent as
  usual; the plug-in extracts the model's assistant content from
  the parsed response (reconstructing it from the SSE stream when
  turn-1 is streamed), derives a tool follow-up from the intent's
  `followUp` (or synthesises one from the intent's tools), and
  sends a turn-2 body that echoes the assistant turn verbatim and
  appends the tool response. Each turn writes its own `turn-1/` and
  `turn-2/` subdirectory.

All other capabilities are single-step.
