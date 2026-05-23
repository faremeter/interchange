# Anthropic Discovery: Observed vs Documented

This note records the live wire behaviour of Anthropic's `/v1/messages`
and `/v1/files` surfaces as observed by the `@intx/inference-discovery-anthropic`
plug-in. For each captured capability we contrast Anthropic's published
shape against the bytes the API actually emitted and call out every
place the two diverged.

The capture corpus lives at `packages/inference-testing/wire/anthropic/`.
The model-to-capability matrix is authoritatively defined in the
`SUPPORT_MATRIX` export of `@intx/inference-discovery/catalog`
(`packages/inference-discovery/src/catalog/support-matrix.ts`). The
fixtures are the ground truth: where the docs and the wire disagree,
the wire wins. This document is the narrative companion to those bytes.

## Source of truth

The per-(model, capability) behaviors documented below are the
_narrative_ layer. The _canonical_ representation is the typed
`SUPPORT_MATRIX`. Tooling — the discovery rig, INTR-79's compat-replay
layer, and any future readers — must consume the matrix
programmatically, not parse this prose. When this document and the
matrix disagree, the matrix wins.

## Models in scope

Three first-party Anthropic models are exercised. The plug-in pins the
`anthropic-version: 2023-06-01` header on every request and authenticates
with `x-api-key`. Per-capability beta flags
(`anthropic-beta: files-api-2025-04-14`,
`anthropic-beta: code-execution-2025-05-22`) live on the per-step
headers map for the steps that need them, not in `buildAuthHeaders`.

| Model                        | Tier      | Extended thinking | Vision | Document input | Code execution | Web search |
| ---------------------------- | --------- | ----------------- | ------ | -------------- | -------------- | ---------- |
| `claude-sonnet-4-5-20250929` | Workhorse | yes               | yes    | yes            | yes            | yes        |
| `claude-opus-4-1-20250805`   | Flagship  | yes               | yes    | yes            | yes            | yes        |
| `claude-haiku-4-5-20251022`  | Cheap     | yes               | yes    | yes            | yes            | yes        |

Anthropic does not expose audio input, video input, or image output on
any first-party model, so those entries land as `outcome: "unsupported"`
in `SUPPORT_MATRIX` with a notes line explaining why.

## Capability dimensions captured

Per model, the following capabilities are exercised end to end:

- `plain-text(-streaming)` — single user message, single text reply.
- `function-calling` — single-turn tool declaration, no follow-up.
- `function-calling-multi-turn(-streaming)` — turn-1 issues the tool
  call, turn-2 echoes the assistant content blocks verbatim and supplies
  a `tool_result` user message.
- `function-calling-with-thinking(-streaming)` — same as multi-turn but
  with `thinking: {type: "enabled", budget_tokens: 1024}` enabled on
  both turns.
- `vision-input(-streaming)` — inline base64 image in the user content
  array.
- `document-input(-streaming)` — inline base64 PDF in the user content
  array.
- `code-execution(-streaming)` — server-side `code_execution_20250522`
  tool with the corresponding beta header.
- `reasoning-content(-streaming)` — `thinking` enabled, no tool calls,
  exposes assistant thinking blocks.
- `grounding(-streaming)` — server-side `web_search_20250305` tool. The
  observed wire shape determines whether these rows land as `captured`
  (with notes describing the divergence from Gemini's
  `groundingMetadata` blob) or `unsupported` (with notes describing
  what was observed and what a future `web-search(-streaming)`
  capability would look like).
- `files-api-reference(-streaming)` — turn-1 uploads the PDF via a real
  multipart POST to `/v1/files` with `anthropic-beta: files-api-2025-04-14`;
  turn-2 generates with a `{type: "document", source: {type: "file", file_id}}`
  reference.
- `redacted-thinking(-streaming)` — turn-1 issues the documented canary
  prompt that triggers `redacted_thinking` content blocks; turn-2
  echoes the assistant blocks verbatim and prompts a brief follow-up
  so the round-trip is exercised on the wire.

## Per-capability observed vs documented

The Observed subsections below are populated alongside the fixture
corpus. Each section pins the exact wire shape that landed on disk and
cites the fixture path so a reader can verify directly.

### plain-text and plain-text-streaming

**Documented:** `POST /v1/messages` with `{model, max_tokens, messages: [{role: "user", content: <string>}]}`.
Streaming sets `stream: true` and emits the named SSE event stream
described under "Anthropic SSE protocol" below.

**Observed:** _(to be filled in alongside the captured fixture)._

### function-calling

**Documented:** Same as plain-text plus `tools: [{name, description, input_schema}]`.
Assistant response includes a `tool_use` content block.

**Observed:** _(to be filled in alongside the captured fixture)._

### function-calling-multi-turn and -streaming

**Documented:** Turn-2 echoes the assistant content blocks verbatim
and appends a user message whose content array contains a `tool_result`
block carrying `{tool_use_id, content}`.

**Observed:** _(to be filled in alongside the captured fixture)._

### function-calling-with-thinking and -streaming

**Documented:** Adds `thinking: {type: "enabled", budget_tokens: 1024}`
to the request. Assistant response includes `thinking` content blocks
with a `signature` field whose presence the client must round-trip in
turn-2.

**Observed:** _(to be filled in alongside the captured fixture)._

### vision-input and -streaming

**Documented:** User message content is an array containing
`{type: "image", source: {type: "base64", media_type, data}}` and a
text block.

**Observed:** _(to be filled in alongside the captured fixture)._

### document-input and -streaming

**Documented:** User message content array contains
`{type: "document", source: {type: "base64", media_type: "application/pdf", data}}`.

**Observed:** _(to be filled in alongside the captured fixture)._

### code-execution and -streaming

**Documented:** `tools: [{type: "code_execution_20250522", name: "code_execution"}]`
plus the `anthropic-beta: code-execution-2025-05-22` request header.
Assistant response includes server-side tool blocks describing the
executed code and its output.

**Observed:** _(to be filled in alongside the captured fixture)._

### reasoning-content and -streaming

**Documented:** Same enablement as function-calling-with-thinking but
without tools. Assistant response surfaces thinking blocks containing
the model's reasoning before the final answer.

**Observed:** _(to be filled in alongside the captured fixture)._

### grounding and -streaming

**Documented (Anthropic):** `tools: [{type: "web_search_20250305", name: "web_search"}]`.
Assistant response includes server-side tool blocks describing the
search and citations attached to the assistant text.

**Documented (catalog vocabulary):** `grounding` was introduced for
Gemini's Google Search grounding, which surfaces a top-level
`groundingMetadata` blob on each candidate. Anthropic's `web_search` is
structurally a tool invocation pattern.

**Observed:** _(decision lands here: whether the captured wire shape
fits the `grounding` semantics with a divergence note, or whether a
dedicated `web-search(-streaming)` capability is the honest framing.)_

### files-api-reference and -streaming

**Documented:** Two-step upload. The upload is
`POST /v1/files` with `multipart/form-data` (`file` field) and the
`anthropic-beta: files-api-2025-04-14` request header. The response
carries `{id, ...}`. The generate request references the uploaded file
via `{type: "document", source: {type: "file", file_id}}`.

**Observed:** _(to be filled in alongside the captured fixture; pins
the precise multipart boundary handling and the upload response
envelope shape.)_

### redacted-thinking and -streaming

**Documented:** Anthropic publishes a magic canary string that
deterministically triggers a `redacted_thinking` content block. The
block carries an opaque encrypted `data` field; clients must echo it
back verbatim on subsequent turns or the conversation breaks.

**Observed:** _(to be filled in alongside the captured fixture; pins
the exact `content_block_start` shape for `redacted_thinking` — one-shot
or delta-streamed — and confirms the round-trip acceptance on turn-2.)_

## Cross-cutting observations

The list below names the recurring patterns worth documenting once the
fixtures land. Each item is populated from the actual captures.

- `redacted_thinking` wire shape — whether `content_block_start` is
  one-shot (carries `data` directly) or delta-streamed.
- Signature carriage — whether `signature_delta` events appear before
  `content_block_stop`, and the interleave ordering across multiple
  thinking blocks at distinct content indices when text or `tool_use`
  blocks are interleaved.
- Citation wire shape — Anthropic emits citations inline with assistant
  content when documents are attached; pin the precise shape.
- Server-side tool wire shapes — `code_execution_tool_use`,
  `code_execution_tool_result`, `server_tool_use`, and
  `web_search_tool_result` blocks observed in responses.
- SSE event terminator and trailer behavior — Anthropic's protocol uses
  named `event:` lines (`message_start`, `content_block_start`,
  `content_block_delta`, `content_block_stop`, `message_delta`,
  `message_stop`, `ping`). Confirm whether any post-`message_stop`
  chunks appear in practice and document the tolerance expected of a
  replay simulator.

## Regeneration

```bash
bun bin/discover.ts --provider anthropic --all
```

Requires `ANTHROPIC_API_KEY` in `.env`. The plug-in hard-fails when `CI`
is set (mirrors the other rigs). Estimated cost is on the order of
$5–$20 across all captures, dominated by the extended-thinking and
multimodal requests; the `--only` and `--model` flags allow targeted
re-captures during iteration.
