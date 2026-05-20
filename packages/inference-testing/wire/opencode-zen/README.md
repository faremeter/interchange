# OpenCode Zen Wire Fixtures

Raw HTTP captures of the OpenCode Zen relay (OpenAI-compatible Chat
Completions). Mirrors the layout of `wire/gemini/` but is grouped
per-model because the same wire shape covers many models behind the
relay. These files are consumed by `@intx/inference-testing` to drive
simulator-backed tests against real-world wire shapes; they are
deliberately stored as sibling-of-`src/` files so the package
TypeScript build (`tsc -b`) ignores them.

## Layout

```
<model>/
  <capability>/
    request.json           # request body sent
    request-headers.json   # request headers (authorization value redacted)
    response.json          # for non-streaming endpoints
    response.sse           # raw bytes for streaming endpoints (mutually exclusive with response.json)
    response-headers.json  # response headers as received
    metadata.json          # { capability, model, endpoint, capturedAt, scriptVersion }
```

Model directory names preserve the dot in the model id (e.g.,
`kimi-k2.6/`); they are not sanitised.

`response.json` and `response.sse` are mutually exclusive: a
non-streaming `POST /chat/completions` writes `response.json`; the
same endpoint with `stream: true` writes `response.sse` as the
literal bytes off the socket (no UTF-8 decode), so the simulator can
replay byte-for-byte. The OpenAI Chat Completions SSE shape ends
with `data: [DONE]\n\n`.

## Conventions

`request-headers.json` always contains `"Authorization": "<redacted>"`
(case insensitive on the original header name). The header name is
preserved verbatim so the simulator can assert it was present; the
value is never committed.

`metadata.json` fields use camelCase throughout. The `endpoint`
field is the relay-side endpoint name (`chat/completions`), not a
fully qualified URL.

## Regeneration

```
bun bin/opencode-discover.ts --all
bun bin/opencode-discover.ts --only text-non-streaming --only text-streaming
bun bin/opencode-discover.ts --probe                    # writes probe-<model>.json under dispatch/
bun bin/opencode-discover.ts --only text-streaming --model kimi-k2.6
```

Requires `OPENAI_API_KEY` and `OPENAI_BASE_URL` (the relay base
URL, `https://opencode.ai/zen/go/v1`) in the environment; bun
auto-loads `.env` from cwd. The script refuses to run with `CI`
set.

Run `make format` after any fresh capture run to normalise the
fixtures before committing; `JSON.stringify(..., 2)` and Prettier
disagree on the layout of short string arrays.

The script version is a module constant in `bin/opencode-discover.ts`
(`SCRIPT_VERSION`). Bump it when changing the fixture shape so old
fixtures can be detected as stale.

## Model registry

The authoritative list of in-scope models with their probed
capability flags lives at `bin/opencode-discover/models.ts`. The
flags there are populated by running `bun bin/opencode-discover.ts
--probe`; the probe responses themselves are written to
`dispatch/intr-78-phase-2/1a-opencode_rig/probe-<model>.json` for
audit and are not part of the committed fixture corpus.

## `_evidence/` directory

Subdirectories under `_evidence/` (underscore-prefixed to keep them
out of the capability-name namespace) hold findings that are not
themselves capability captures but reference the corpus. The
inaugural entry is `_evidence/kimi-k2.6-routing/`, documenting the
finding that the OpenCode Zen relay routes `kimi-k2.6` requests to
more than one upstream platform (Moonshot AI and Fireworks AI), with
materially different response envelopes per upstream. Its `README.md`
contains a self-contained report plus the raw side-by-side request
and response bodies for each backend; third parties can read it
without needing the rest of the corpus.
