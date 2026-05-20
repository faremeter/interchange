# Gemini Discovery: Observed vs Documented

This note records the live wire behaviour of the Gemini Developer API
as observed during the INTR-78 capture campaign. For each capability
slice we contrast the public documentation against the bytes the API
actually emitted, and call out every place the two diverged. The
captures were taken on 2026-05-20 against the v1beta REST surface
(`https://generativelanguage.googleapis.com/v1beta`) using the
Developer API key flow (`x-goog-api-key`); the Vertex AI surface is
not covered.

The capture corpus lives at
`packages/inference-testing/wire/gemini/`. Layout conventions are
documented in
`packages/inference-testing/wire/gemini/README.md`. The fixtures are
the ground truth: where the docs and the wire disagree, the wire
wins. This document is the narrative companion to those bytes.

Twelve capabilities were captured, spanning text, function calling,
thinking, multimodal input, image output, code execution, search
grounding, and the Files API. Eleven captures used `gemini-2.5-flash`;
the image-output capture used `gemini-2.5-flash-image` (the only
2.5-family Developer-API model that returns inline image bytes).

A companion taxonomy document — to be added in INTR-78's next task
(6a) under `docs/INFERENCE.md` — will generalize the observations
below into the abstractions that INTR-79 and INTR-80 need to consume.
This note records facts; design decisions belong in the taxonomy
document, not here.

## text-non-streaming

### Documented

The Gemini API generateContent reference
(https://ai.google.dev/api/generate-content) describes a JSON POST to
`models/{model}:generateContent` with a `contents` array of
`{role, parts}` entries. The response is a `GenerateContentResponse`
with a `candidates` array, each containing `content.parts`, a
`finishReason` enum, and an `index`. A `usageMetadata` block reports
token counts. The reference makes no commitment to fields like
`modelVersion`, `responseId`, or `serviceTier`.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/text-non-streaming/request.json`)
is a minimal single-turn user prompt with no `generationConfig`. The
response
(`packages/inference-testing/wire/gemini/text-non-streaming/response.json`)
returns one candidate with a single text part, `finishReason: "STOP"`,
and `index: 0`. The top-level object carries `usageMetadata`,
`modelVersion: "gemini-2.5-flash"`, and `responseId`.

Two undocumented usageMetadata details appear: `promptTokensDetails`
breaks the prompt count down by modality (here, `TEXT: 9`), and
`thoughtsTokenCount: 19` is present despite no explicit thinking
config — the model implicitly spent thinking tokens even when the
caller did not request them. `serviceTier: "standard"` appears
both inside `usageMetadata` and as the `x-gemini-service-tier`
response header
(`packages/inference-testing/wire/gemini/text-non-streaming/response-headers.json`).

### Discrepancies

- `modelVersion` and `responseId` are present on every captured
  response but absent from the response schema in the public docs.
- `usageMetadata.thoughtsTokenCount` and
  `usageMetadata.promptTokensDetails` are emitted even when the
  caller does not enable thinking or multimodal input.
- `usageMetadata.serviceTier` is undocumented at the field level but
  consistently present and mirrored by `x-gemini-service-tier`.

## text-streaming

### Documented

The streaming reference
(https://ai.google.dev/api/generate-content#method:-models.streamgeneratecontent)
describes `streamGenerateContent` as returning the same response
shape as the non-streaming endpoint, chunked. The `?alt=sse` query
parameter switches the response from a JSON-array stream to
Server-Sent Events with `data: <json>` events separated by blank
lines.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/text-streaming/request.json`)
sets `generationConfig.maxOutputTokens: 400` and disables thinking
with `thinkingConfig.thinkingBudget: 0`. The raw byte stream
(`packages/inference-testing/wire/gemini/text-streaming/response.sse`)
contains eight `data:` events, each followed by a blank line. Each
event payload is a complete `GenerateContentResponse` with one
candidate whose `content.parts[0].text` is the delta for that chunk
(not a cumulative concatenation). Every chunk carries its own
`usageMetadata`, `modelVersion`, and `responseId`; `responseId` is
constant across all eight chunks of a single stream
(`JtwNauyyHb_g_uMP0rqx0Ak`). The terminal chunk is identified by
`candidates[0].finishReason: "STOP"` — earlier chunks omit
`finishReason` entirely. The response headers
(`packages/inference-testing/wire/gemini/text-streaming/response-headers.json`)
include `content-type: text/event-stream` and
`content-disposition: attachment`.

### Discrepancies

- The SSE stream has no `[DONE]` sentinel; termination is signalled
  by the presence of `finishReason` on the candidate of the final
  chunk. Consumers that pattern-match on `data: [DONE]` (the OpenAI
  convention) will hang.
- Every chunk repeats `usageMetadata`, `modelVersion`, and
  `responseId`. `candidatesTokenCount` grows monotonically across
  chunks; clients that sum per-chunk usage will double-count.
- Each `data:` payload is the full response envelope, not a delta
  patch — only `parts[0].text` is the new content. The streaming
  reference does not spell this out.
- `content-disposition: attachment` is set on a streaming JSON-like
  response, which can confuse browsers that interpret the header
  literally.

## function-calling-multi-turn

### Documented

The function calling guide
(https://ai.google.dev/gemini-api/docs/function-calling) describes
the round-trip pattern: the caller declares
`tools[].functionDeclarations`, the model responds with one or more
`functionCall` parts inside `candidates[0].content.parts`, the
caller executes the function and sends back a `functionResponse`
part in a `user`-role turn, and the model produces a final text
answer. The guide documents `toolConfig.functionCallingConfig.mode`
(`AUTO`, `ANY`, `NONE`) and `allowedFunctionNames`.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/function-calling-multi-turn/turn-1/request.json`)
declares one function (`getCurrentWeather`), sets
`toolConfig.functionCallingConfig.mode: "ANY"` with
`allowedFunctionNames: ["getCurrentWeather"]`, and disables thinking
with `thinkingConfig.thinkingBudget: 0`. Turn 1's response
(`packages/inference-testing/wire/gemini/function-calling-multi-turn/turn-1/response.json`)
contains exactly one `functionCall` part with `name` and `args`. The
candidate carries `finishReason: "STOP"` plus an undocumented
`finishMessage: "Model generated function call(s)."` — the
finishReason does not distinguish tool-call from final-text turns;
only the part shape does.

Turn 2's request
(`packages/inference-testing/wire/gemini/function-calling-multi-turn/turn-2/request.json`)
echoes the model's turn-1 reply verbatim as a `model`-role entry and
appends a `user`-role entry containing one `functionResponse` part.
The response
(`packages/inference-testing/wire/gemini/function-calling-multi-turn/turn-2/response.json`)
is a plain text answer.

### Discrepancies

- `candidates[0].finishMessage` is emitted on tool-call turns but is
  not documented in the response reference. The string is
  human-readable (`"Model generated function call(s)."`) and is not
  suitable for programmatic dispatch — callers must still inspect
  `parts` to detect a tool call.
- `finishReason: "STOP"` is used for both tool-call and final-text
  turns. The enum value carries no information about turn intent.

## function-calling-thinking

### Documented

The thinking guide
(https://ai.google.dev/gemini-api/docs/thinking) introduces
`generationConfig.thinkingConfig.thinkingBudget` and
`includeThoughts`. When `includeThoughts: true`, the model may emit
parts with `thought: true` containing reasoning text. The guide also
introduces `thoughtSignature` as an opaque token that callers must
echo back verbatim in subsequent turns to preserve the model's
internal state. The signatures are described as opaque and
unspecified in size or encoding.

### Observed

The turn-1 request
(`packages/inference-testing/wire/gemini/function-calling-thinking/turn-1/request.json`)
sets `thinkingConfig.thinkingBudget: 1024` and
`includeThoughts: true`. The turn-1 response
(`packages/inference-testing/wire/gemini/function-calling-thinking/turn-1/response.json`)
returns two parts in `candidates[0].content.parts`: a `text` part
with `thought: true` (the visible reasoning), followed by a
`functionCall` part carrying a `thoughtSignature` field. The
signature is a 408-character base64-looking blob
(`Cq4CAQw51sfdlVRevc++U+r6uQYT2YPGkyMiQ8NVA8OHqgTQIykg7eHC...`).
`usageMetadata.thoughtsTokenCount: 63` is reported.

The turn-2 request
(`packages/inference-testing/wire/gemini/function-calling-thinking/turn-2/request.json`)
echoes the entire turn-1 model turn — the thought part and the
functionCall part with its `thoughtSignature` — byte-for-byte. The
signature string in the turn-2 request matches the turn-1 response
character-for-character. The response
(`packages/inference-testing/wire/gemini/function-calling-thinking/turn-2/response.json`)
is the final text answer.

### Discrepancies

- `thoughtSignature` was attached to the `functionCall` part, not to
  the thought (`text` with `thought: true`) part. The signature
  travels with the action, not with the reasoning.
- The signature must round-trip verbatim. A caller that reconstructs
  the model turn from a structured representation (e.g.,
  `{name, args}` only) will silently strip the signature and lose
  the model's internal state. The captured fixtures pin the exact
  bytes so simulator-backed tests can assert the round-trip.
- The signature is 408 characters of opaque base64. The docs make
  no commitment to size, but downstream consumers should not assume
  it stays short — future model versions may emit larger blobs.
- `usageMetadata.thoughtsTokenCount` is reported even when the
  caller is not relaying thoughts onward, so it represents work the
  model did, not bytes the caller will see.

## image-input

### Documented

The vision guide (https://ai.google.dev/gemini-api/docs/vision)
describes inline image input via a part of shape
`{inlineData: {mimeType, data}}` where `data` is a base64-encoded
payload and `mimeType` is `image/jpeg`, `image/png`, or similar.
The guide states images count against the prompt token budget.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/image-input/request.json`)
contains two parts: a text instruction
(`"Describe the picture in one short sentence."`) and an
`inlineData` part with `mimeType: "image/jpeg"` and a base64 payload
(~23 KB of base64, corresponding to a small JPEG). The response
(`packages/inference-testing/wire/gemini/image-input/response.json`)
returns a single text part. The `usageMetadata.promptTokensDetails`
array splits the prompt by modality:
`{TEXT: 9, IMAGE: 258}`. A `thoughtsTokenCount: 1202` is reported
despite no thinking config being set.

### Discrepancies

- The model implicitly spent 1202 thinking tokens on a request that
  did not enable thinking. `thoughtsTokenCount` appearing without an
  opt-in is a billing surprise worth documenting.
- The model produced an inaccurate caption for the captured asset
  (a NASA lunar polar water-signature visualisation, described as
  generic albedo/water comparison). This is a content accuracy
  observation, not a wire-shape discrepancy, but it informs the
  baseline a simulator should expect.

## image-output

### Documented

The image-generation guide
(https://ai.google.dev/gemini-api/docs/image-generation) describes
the `gemini-2.5-flash-image` (and preview variants) workflow for
producing image output. The caller sets
`generationConfig.responseModalities: ["TEXT", "IMAGE"]` and the
model returns candidates whose parts include `inlineData` entries
with `mimeType: "image/png"` and a base64 payload.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/image-output/request.json`)
asks the model to generate a small illustration and sets
`responseModalities: ["TEXT", "IMAGE"]`. The response
(`packages/inference-testing/wire/gemini/image-output/response.json`)
contains one candidate whose `content.parts` array has two entries
in order: first a `text` part (a one-sentence introductory
caption), then an `inlineData` part with `mimeType: "image/png"`
and a base64 payload approximately 1.17 MB on disk. The
`usageMetadata` carries a new structure: `candidatesTokensDetails`,
mirroring `promptTokensDetails`, reports `{IMAGE: 1290}`. The
`modelVersion` in the response is `gemini-2.5-flash-image`, matching
the requested model.

### Discrepancies

- The model returned a text caption alongside the image. The docs
  imply image output is the deliverable; in practice both modalities
  are emitted together in the captured run, and the text part
  precedes the image part in the array.
- `usageMetadata.candidatesTokensDetails` is undocumented in the
  generateContent reference. It is the candidate-side analogue of
  `promptTokensDetails`.
- The inline image payload is the unaltered base64 from the model
  (no chunking, no link indirection); response bodies for this model
  are accordingly large. Simulators replaying this fixture must
  expect multi-megabyte payloads.

## audio-input

### Documented

The audio guide (https://ai.google.dev/gemini-api/docs/audio)
describes inline audio input via the same
`inlineData: {mimeType, data}` shape used for images, with audio
MIME types such as `audio/wav`, `audio/mp3`, and `audio/ogg`. The
guide notes audio is counted in tokens but does not specify the
modality label.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/audio-input/request.json`)
sends a `audio/wav` payload alongside a text instruction
(`"Transcribe the spoken words in this audio clip."`). The response
(`packages/inference-testing/wire/gemini/audio-input/response.json`)
is a single text part. `usageMetadata.promptTokensDetails` splits
the prompt as `{TEXT: 11, AUDIO: 128}`. `thoughtsTokenCount: 44` is
reported.

### Discrepancies

- The `AUDIO` modality label appears in `promptTokensDetails`
  but is not enumerated in the response schema.
- The transcription returned (`"I got that. Yeah."`) is a free-form
  text part with no timestamps, no confidence score, and no
  diarization. The docs do not promise any structured-transcript
  output, but consumers expecting structure (offsets, speaker
  labels) will not find it on this endpoint.

## video-input

### Documented

The video guide (https://ai.google.dev/gemini-api/docs/video)
describes inline video input via `inlineData` with video MIME types
(`video/mp4`, `video/webm`, etc.). The guide explains that video is
counted in tokens and that audio tracks within the video are also
processed.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/video-input/request.json`)
sends a small `video/mp4` payload with a text instruction. The
response
(`packages/inference-testing/wire/gemini/video-input/response.json`)
is a single text part. `usageMetadata.promptTokensDetails` splits
the prompt across three modalities:
`{TEXT: 12, VIDEO: 1315, AUDIO: 131}`. The audio track inside the
mp4 was tokenised separately even though the caller submitted a
single video file.

### Discrepancies

- Submitting a single `inlineData` video part is reported in
  `promptTokensDetails` as two modalities (`VIDEO` and `AUDIO`).
  Callers tallying modalities to predict cost must account for the
  audio track inside the video being charged separately.
- The `VIDEO` modality label is observed but not enumerated in the
  response schema.

## pdf-input

### Documented

The document-processing guide
(https://ai.google.dev/gemini-api/docs/document-processing)
describes PDF input via `inlineData` with
`mimeType: "application/pdf"`. The guide says the model uses both
extracted text and visual layout when reasoning about the document.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/pdf-input/request.json`)
sends a `application/pdf` payload with the prompt
`"Summarize this PDF in one short sentence."`. The response
(`packages/inference-testing/wire/gemini/pdf-input/response.json`)
is a one-sentence summary. `usageMetadata.promptTokensDetails`
labels the document modality as `DOCUMENT`:
`{TEXT: 10, DOCUMENT: 258}`. `thoughtsTokenCount: 1068` is
reported.

### Discrepancies

- The modality label for a PDF is `DOCUMENT`, not `PDF`. The label
  is not enumerated in any public document we found.
- The summary returned referenced characters and plot details that
  are not on the first page of the sample asset (Tom Sawyer; the
  summary mentioned Injun Joe). The model appears to mix the
  inline PDF bytes with pretraining knowledge. The wire does not
  distinguish "from the document" from "from training".

## code-execution

### Documented

The code-execution guide
(https://ai.google.dev/gemini-api/docs/code-execution) describes
enabling the tool by adding `{codeExecution: {}}` to the `tools`
array. The model returns parts of three kinds during a code-using
turn: an optional `text` part with reasoning, an `executableCode`
part containing `{language, code}`, and a `codeExecutionResult` part
containing `{outcome, output}`. The documented `outcome` enum
includes `OUTCOME_OK`, `OUTCOME_FAILED`, `OUTCOME_DEADLINE_EXCEEDED`.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/code-execution/request.json`)
asks the model to compute the 20th Fibonacci number using Python
and declares `tools: [{ codeExecution: {} }]`. The response
(`packages/inference-testing/wire/gemini/code-execution/response.json`)
returns three parts in `candidates[0].content.parts`:

1. `executableCode` with `language: "PYTHON"` and a multi-line code
   string.
2. `codeExecutionResult` with `outcome: "OUTCOME_OK"` and an
   `output` string ending in a trailing newline.
3. `text` with the natural-language summary of the result.

A new top-level usage field appears: `toolUsePromptTokenCount: 220`
and a `toolUsePromptTokensDetails` array with `{TEXT: 220}`. This is
the prompt cost of the tool itself (the model re-prompting itself
with the executed-code output), distinct from the original prompt
tokens.

### Discrepancies

- The order of parts is `executableCode`, `codeExecutionResult`,
  `text`. The guide does not specify ordering; consumers that pair
  by `parts[i]` index rather than by part kind are safe on this
  capture but may need to tolerate other orderings on other runs.
- `usageMetadata.toolUsePromptTokenCount` and
  `toolUsePromptTokensDetails` are emitted but absent from the
  documented usageMetadata schema. They represent additional
  billable work that callers must surface separately if they want
  the user to see all tokens charged.
- The `language` field is `"PYTHON"` (uppercase). No other language
  was attempted on this capture.

## google-search-grounding

### Documented

The grounding-with-Google-Search guide
(https://ai.google.dev/gemini-api/docs/google-search) describes
enabling search grounding by adding `{googleSearch: {}}` to `tools`.
The model returns the answer as text parts and attaches
`groundingMetadata` to the candidate. The documented
`groundingMetadata` fields are `webSearchQueries: string[]`,
`groundingChunks: [{web: {uri, title}}]`, and `groundingSupports:
[{segment: {startIndex, endIndex, text}, groundingChunkIndices:
number[], confidenceScores?: number[]}]`.

### Observed

The captured request
(`packages/inference-testing/wire/gemini/google-search-grounding/request.json`)
asks about the 2025 Nobel Prize in Physics and declares
`tools: [{ googleSearch: {} }]`. The response
(`packages/inference-testing/wire/gemini/google-search-grounding/response.json`)
returns two text parts (a long answer, then a shorter recap) inside
`candidates[0].content.parts`, plus `groundingMetadata` containing
all three documented keys:

- `webSearchQueries` is two strings.
- `groundingChunks` is eight entries, each
  `{web: {uri, title}}` where `uri` is a
  `vertexaisearch.cloud.google.com/grounding-api-redirect/...`
  redirect.
- `groundingSupports` is eight entries pairing 1:1 with chunks
  (though each support may reference multiple chunks via
  `groundingChunkIndices`). Each support has a `segment` with
  `startIndex`, `endIndex`, `text`, and a `groundingChunkIndices`
  array.

The `groundingMetadata` block additionally contains a fourth
field — `searchEntryPoint` — whose `renderedContent` is a
self-contained HTML/CSS snippet that renders the Google "Search
Suggestions" chip carousel. The snippet includes inline `<style>`
rules, light/dark theme media queries, the Google logo as SVG
paths, and `<a class="chip">` elements linking to the same
redirect URIs as the chunks. The snippet is multiple kilobytes.
Also new in `usageMetadata`: `toolUsePromptTokenCount: 161` and
`toolUsePromptTokensDetails`, as with code execution.

### Discrepancies

- `groundingMetadata.searchEntryPoint` is not documented in the
  grounding guide but is consistently emitted alongside the
  documented keys. Consumers must decide whether to forward or
  strip it; under Google's grounding terms-of-service this snippet
  is the chip carousel that callers are expected to render to the
  end user, so stripping it is a policy decision, not a free choice.
- The `groundingChunks[].web.uri` values are vertexaisearch
  redirect URLs, not the source URLs themselves. Consumers wanting
  raw URLs must follow the redirect; the title is the only
  in-payload hint of the underlying domain (`lbl.gov`,
  `nobelprize.org`, etc.).
- The supports in this capture do not include `confidenceScores`,
  contrary to what the guide implies as a typical shape. The field
  is optional in practice.
- The text answer is repeated as two near-identical parts (one long,
  one short). The guide does not document why two text parts are
  emitted; the second appears to be a redundant recap.

## files-api

### Documented

The Files API reference
(https://ai.google.dev/gemini-api/docs/files) describes a two-step
flow: upload bytes to `POST /upload/v1beta/files` (resumable or
simple), receive a file resource with `name`, `uri`,
`expirationTime`, and `state`, then reference the file in a
subsequent `generateContent` call via a `fileData: {mimeType,
fileUri}` part. The documented TTL is 48 hours.

### Observed

The capture is a true two-step fixture. The top-level metadata
(`packages/inference-testing/wire/gemini/files-api/metadata.json`)
records `sequence: ["upload", "generate"]`,
`uri-contract: "documentary"`, `uri-ttl-hours: 48`, plus the
uploaded URI and expiration time.

Step 1 (upload) request
(`packages/inference-testing/wire/gemini/files-api/upload/request.json`)
is a synthetic descriptor — not a JSON request body — recording
`method: "POST"`, `url: "https://generativelanguage.googleapis.com/
upload/v1beta/files"`, the source asset path, MIME type, and
content-length. The captured request headers
(`packages/inference-testing/wire/gemini/files-api/upload/request-headers.json`)
include `x-goog-upload-protocol: raw`, `x-goog-upload-file-name:
sample.pdf`, `content-type: application/pdf`, and
`content-length: 4193`. The response
(`packages/inference-testing/wire/gemini/files-api/upload/response.json`)
wraps the resource in a top-level `file` object:

```
{ "file": { "name": "files/bazi800pl384", "mimeType":
"application/pdf", "sizeBytes": "4193", "createTime": ...,
"updateTime": ..., "expirationTime": ..., "sha256Hash": ...,
"uri": "https://generativelanguage.googleapis.com/v1beta/files/
bazi800pl384", "state": "ACTIVE", "source": "UPLOADED" } }
```

`sizeBytes` is a string, not a number. The response headers
(`packages/inference-testing/wire/gemini/files-api/upload/response-headers.json`)
come from `server: UploadServer` (not the standard
`scaffolding on HTTPServer2`), include an
`x-guploader-uploadid` correlation token, and omit
`transfer-encoding: chunked` (the upload host responds with
`content-length` instead).

Step 2 (generate) request
(`packages/inference-testing/wire/gemini/files-api/generate/request.json`)
sends a normal `generateContent` body with a `fileData` part
referencing the uploaded URI verbatim. The response
(`packages/inference-testing/wire/gemini/files-api/generate/response.json`)
is a plain text summary of the PDF.

### Discrepancies

- The upload response wraps the resource in a top-level `file` key.
  Naive code that expects the resource at the root will need to
  dereference one extra level.
- The `name` field is `"files/{id}"` (the relative resource name)
  while `uri` is `"https://...v1beta/files/{id}"` (no `files/`
  prefix inside the URI path beyond `/files/{id}`). The two are not
  interchangeable; consumers must use `uri` for `fileData.fileUri`.
- `sizeBytes` is serialised as a JSON string. The reference does
  not flag this; clients deserialising to a numeric type will
  need to coerce.
- `sha256Hash` is itself base64-encoded (a 64-character base64
  blob whose decoded form is the hex SHA-256), not the more common
  hex string.
- The simple-upload path accepted the `x-goog-upload-protocol: raw`
  header for a 4 KB PDF and did not require the resumable session
  URL handshake the docs describe as the default. Larger payloads
  may still require resumable; this fixture only proves the raw
  path works for small assets.
- The captured `fileUri` is documentary. The resource expires 48
  hours after upload; downstream simulator consumers read bytes
  from the fixture, not from the live URI. Tests that hit the URI
  will receive 404 once the TTL elapses.

## Cross-cutting observations

These items are not specific to any one capability.

**Response envelope fields are stable across capabilities.** Every
captured non-streaming response (and every streaming chunk) carries
`modelVersion` and `responseId` at the top level. `modelVersion`
echoes the model used (including the suffix for
`gemini-2.5-flash-image`). `responseId` is a short opaque token; in
streaming responses it is constant across all chunks of a single
turn, making it usable as a stream identifier.

**`usageMetadata` is consistently extended beyond the public
schema.** Across capabilities we observed `promptTokensDetails`,
`candidatesTokensDetails`, `thoughtsTokenCount`,
`toolUsePromptTokenCount`, `toolUsePromptTokensDetails`, and
`serviceTier`. None are documented in the generateContent reference.
Modality labels seen in details arrays: `TEXT`, `IMAGE`, `AUDIO`,
`VIDEO`, `DOCUMENT`. Consumers building cost-attribution UI must
account for all of these.

**`thoughtsTokenCount` can appear without an opt-in.** Seven
captures with no `thinkingConfig` (`text-non-streaming`,
`image-input`, `audio-input`, `video-input`, `pdf-input`,
`code-execution`, and `google-search-grounding`) still emitted
non-zero `thoughtsTokenCount`. The model decides to spend thinking
tokens regardless of the caller's opt-in. The opt-in
(`includeThoughts`) appears to control whether the thoughts are
_returned_, not whether they are spent.

**`finishReason: "STOP"` is overloaded.** It marks normal
completion for plain text, function-call turns, code-execution
turns, and grounded turns. Callers cannot use `finishReason` alone
to distinguish between a tool-call turn and a final-text turn; they
must inspect the `parts` array.

**`finishMessage` is undocumented but emitted on tool-call turns.**
The string is human-readable ("Model generated function call(s).")
and should not be treated as machine-parseable.

**Response headers consistently include
`x-gemini-service-tier`** on the generateContent surface; the Files
API upload host does not emit it (it uses a different server,
`UploadServer`, and omits the Gemini-specific headers).

**Request authentication uses `x-goog-api-key`.** Every captured
request has this header; in the fixtures the value is replaced with
the literal string `<redacted>`. The simulator must assert the
header is present, not its value.

**SSE has no `[DONE]` sentinel.** Termination is signalled by the
presence of `candidates[0].finishReason` on the last chunk. Each
chunk is a full response envelope, not a delta patch.

**Fixture regeneration gotcha (capture vs commit).** The capture
script emits files via `JSON.stringify(..., 2)`, but Prettier
reformats short string arrays onto a single line (e.g.,
`["TEXT","IMAGE"]`, `["turn-1","turn-2"]`). After a fresh capture
run the developer must run `make format` before committing, or
`make lint` will reject the unformatted files. The shape assertions
in the capture script are not affected; the issue is purely
whitespace in committed JSON. This applies to any capability whose
request or response contains short string arrays.

**The image-output model is structurally identical to text models
on the wire.** Apart from the larger response payload and the
addition of `inlineData` parts and `candidatesTokensDetails`, the
envelope is the same `GenerateContentResponse`. There is no
separate image-generation endpoint or response shape.

**Tool tokens (`toolUsePromptTokenCount`) are charged separately.**
Both `code-execution` and `google-search-grounding` report a
non-zero `toolUsePromptTokenCount` distinct from the original
`promptTokenCount`. Callers summing only `promptTokenCount` and
`candidatesTokenCount` will under-count actual usage.

**Headers redaction is on the request side only.** Response headers
are committed verbatim, including correlation tokens
(`x-guploader-uploadid` on uploads, `server-timing` durations on
generateContent). These are not secrets but are not stable across
runs, so equality-on-headers assertions in simulator tests should
match on key presence, not full value.
