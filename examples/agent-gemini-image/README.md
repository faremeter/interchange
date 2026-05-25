# agent-gemini-image

End-to-end Gemini image-generation example. Reads a prompt, runs it
against `gemini-2.5-flash-image` with `responseModalities: ["text",
"image"]`, and writes the returned image bytes to disk while
streaming the model's accompanying text to stdout.

This example uses `runInference` directly rather than the higher-
level `@intx/agent` surface because `inference.image_output` is a
streaming event whose payload (typically a base64 image megabyte or
so) belongs in a streaming consumer, not behind a single
`agent.send()` await.

## What it shows

- Wiring a `gemini-2.5-flash-image` `InferenceSource` against the
  Gemini adapter.
- Asking for both modalities via
  `inferenceOptions: { responseModalities: ["text", "image"] }`.
- Consuming the event stream: `inference.text.delta` for the
  running narrative, `inference.image_output` for the atomic image
  part, `inference.usage` for the token tally, `inference.error`
  for any protocol mismatch the parser surfaces.
- Decoding the base64 payload on the `image_output` event into raw
  bytes and writing them to disk under a per-event filename
  (call timestamp + block index, distinct per image-output event).

## Running

```bash
export GEMINI_API_KEY=...
cd examples/agent-gemini-image
bun run start "a small illustration of a red apple on a white background"
```

The image lands in the current working directory under a filename
like `gemini-image-<timestamp>-<block-index>.png`. The MIME type
the model emits determines the file extension; today's
`gemini-2.5-flash-image` returns `image/png` exclusively.

## Notes

- The example does not retry, throttle, or back off. Real
  production use would compose against the harness's existing
  timeout/retry plumbing.
- An empty or refused response surfaces a non-zero exit code with
  an explanatory stderr message. A successful response writes the
  image(s) and prints the count.
- The `outputDir` and `fetch` overrides are exposed on the
  `MainOptions` type (also exported alongside `main`) so an
  integration test can redirect the file writes and replay a
  captured fixture instead of hitting the live endpoint.
