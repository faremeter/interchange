# Gemini Wire Fixtures

Raw HTTP captures of the Gemini Developer API, one directory per
capability slice. These files are consumed by `@intx/inference-testing`
to drive simulator-backed tests against real-world wire shapes; they
are deliberately stored as sibling-of-`src/` files so the package
TypeScript build (`tsc -b`) ignores them.

## Layouts

Three structural shapes appear in this tree. The per-fixture files
are the same in every shape — the difference is whether they are
grouped under per-step subdirectories.

### Single-step

The basic shape: one HTTP request, one HTTP response, captured side
by side at the top of the capability directory.

```
<capability>/
  request.json           # request body sent
  request-headers.json   # request headers (x-goog-api-key value redacted)
  response.json          # for non-streaming endpoints
  response.sse           # raw bytes for streaming endpoints (mutually exclusive with response.json)
  response-headers.json  # response headers as received
  metadata.json          # { capability, model, endpoint, capturedAt, scriptVersion }
```

`response.json` and `response.sse` are mutually exclusive: a capability
that uses `generateContent` writes `response.json`; one that uses
`streamGenerateContent?alt=sse` writes `response.sse` as the literal
bytes off the socket (no UTF-8 decode), so the simulator can replay
byte-for-byte.

### Multi-step with numbered turns

Used by function-calling captures where the same conversation
crosses multiple `generateContent` calls. The top-level
`metadata.json` carries a `sequence: ["turn-1", "turn-2"]` array;
each turn is a single-step shape one level deeper.

```
<capability>/
  metadata.json          # { capability, model, endpoint, capturedAt, scriptVersion, sequence: [...] }
  turn-1/
    request.json
    request-headers.json
    response.json        # or response.sse for the streaming variants
    response-headers.json
  turn-2/
    request.json
    request-headers.json
    response.json        # or response.sse for the streaming variants
    response-headers.json
```

### Multi-step with named steps

Used by `files-api` and its streaming variant. The step names are
semantic (`upload`, `generate`) rather than numbered because the two
steps are not symmetric. `upload` is always a binary POST to the
Files API endpoint and writes `response.json`; `generate` is an
inference call against `generateContent` (or `streamGenerateContent`
in the streaming variant) and writes `response.json` or
`response.sse` accordingly. The top-level `metadata.json` carries a
`sequence: ["upload", "generate"]` plus capability-specific fields
documenting the uploaded file's URI and TTL.

```
files-api/
  metadata.json          # base fields + sequence + uriContract, uriTtlHours,
                          # uploadedFileUri, uploadedFileExpiresAt, assetPath
  upload/
    request.json         # synthetic descriptor: target endpoint, MIME, asset path, content length
    request-headers.json
    response.json
    response-headers.json
  generate/
    request.json
    request-headers.json
    response.json        # response.sse in files-api-streaming
    response-headers.json
```

The `assetPath` in the top-level `metadata.json` points at the
sample asset committed under `bin/gemini-discover/assets/`; the
upload step does not inline the asset bytes into its `request.json`.

## Conventions

`request-headers.json` always contains `"x-goog-api-key": "<redacted>"`.
The header name is preserved so the simulator can assert it was
present; the value is never committed.

`metadata.json` fields use camelCase throughout, including the
capability-specific extras on `files-api/metadata.json` and
`files-api-streaming/metadata.json` (`uriContract`, `uriTtlHours`,
`uploadedFileUri`, `uploadedFileExpiresAt`, `assetPath`).

The captured `fileUri` in the Files API captures is documentary. The
resource expires 48 hours after upload; downstream simulator
consumers read fixture bytes, not live URIs. Tests that resolve the
URI against the live Gemini Files API will receive 404 once the TTL
elapses.

## Regeneration

```
bun bin/gemini-discover.ts --only <capability> [--only <capability>]...
bun bin/gemini-discover.ts --all
```

Requires `GOOGLE_API_KEY` in the environment (bun auto-loads `.env`).
The script refuses to run with `CI` set.

The `JSON.stringify(..., 2)` writer disagrees with Prettier on short
string arrays — Prettier collapses them to a single line while
`JSON.stringify` always multi-lines. Run `make format` after any
fresh capture run to normalise the fixtures before committing.

The script version is a module constant in `bin/gemini-discover.ts`
(`SCRIPT_VERSION`). Bump it when changing the fixture shape so old
fixtures can be detected as stale.

## Capability Catalogue

### Non-streaming (`generateContent`)

- `text-non-streaming` — `gemini-2.5-flash`
- `function-calling-multi-turn` — `gemini-2.5-flash`, multi-step (`turn-1`, `turn-2`)
- `function-calling-thinking` — `gemini-2.5-flash`, multi-step (`turn-1`, `turn-2`)
- `image-input` — `gemini-2.5-flash`, base64 image/jpeg via `inlineData`
- `image-output` — `gemini-2.5-flash-image`, `responseModalities: ["TEXT","IMAGE"]`
- `audio-input` — `gemini-2.5-flash`, base64 audio/wav via `inlineData`
- `video-input` — `gemini-2.5-flash`, base64 video/mp4 via `inlineData`
- `pdf-input` — `gemini-2.5-flash`, base64 application/pdf via `inlineData`
- `code-execution` — `gemini-2.5-flash`, `tools: [{ codeExecution: {} }]`
- `google-search-grounding` — `gemini-2.5-flash`, `tools: [{ googleSearch: {} }]`
- `files-api` — `gemini-2.5-flash`, multi-step (`upload`, `generate`)

### Streaming (`streamGenerateContent?alt=sse`)

- `text-streaming` — `gemini-2.5-flash`
- `function-calling-multi-turn-streaming` — `gemini-2.5-flash`, multi-step
- `function-calling-thinking-streaming` — `gemini-2.5-flash`, multi-step
- `image-input-streaming` — `gemini-2.5-flash`
- `image-output-streaming` — `gemini-2.5-flash-image`
- `audio-input-streaming` — `gemini-2.5-flash`
- `video-input-streaming` — `gemini-2.5-flash`
- `pdf-input-streaming` — `gemini-2.5-flash`
- `code-execution-streaming` — `gemini-2.5-flash`
- `google-search-grounding-streaming` — `gemini-2.5-flash`
- `files-api-streaming` — `gemini-2.5-flash`, multi-step (the `upload`
  step stays non-streaming because it is a binary POST, not an
  inference call)

Each streaming capability mirrors its non-streaming sibling's request
body exactly; only the endpoint and capture helper differ. See
`docs/GEMINI_DISCOVERY.md` § Streaming variants for the per-shape
observations about how the streaming wire compares to the non-streaming
counterpart.
