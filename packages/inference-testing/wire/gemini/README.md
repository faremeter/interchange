# Gemini Wire Fixtures

Raw HTTP captures of the Gemini Developer API, one directory per
capability slice. These files are consumed by `@intx/inference-testing`
to drive simulator-backed tests against real-world wire shapes; they
are deliberately stored as sibling-of-`src/` files so the package
TypeScript build (`tsc -b`) ignores them.

## Layout

```
packages/inference-testing/wire/gemini/<capability>/
  request.json           # request body sent
  request-headers.json   # request headers (x-goog-api-key value redacted)
  response-headers.json  # response headers as received
  response.json          # for non-streaming endpoints
  response.sse           # raw bytes for streaming endpoints (mutually exclusive with response.json)
  metadata.json          # { capability, model, endpoint, capturedAt, scriptVersion }
```

Each capability directory contains either `response.json` or
`response.sse`, never both. The streaming variant is captured as the
literal bytes off the socket (no UTF-8 decode), so the simulator can
replay byte-for-byte.

`request-headers.json` always contains `"x-goog-api-key": "<redacted>"`.
The header name is preserved so the simulator can assert it was
present; the value is never committed.

## Regeneration

```
bun bin/gemini-discover.ts --only <capability> [--only <capability>]...
bun bin/gemini-discover.ts --all
```

Requires `GOOGLE_API_KEY` in the environment (bun auto-loads `.env`).
The script refuses to run with `CI=1` set.

The script version is a module constant in `bin/gemini-discover.ts`
(`SCRIPT_VERSION`). Bump it when changing the fixture shape so old
fixtures can be detected as stale.

## Capability Catalogue

- `text-non-streaming` — `gemini-2.5-flash`, `generateContent`
- `text-streaming` — `gemini-2.5-flash`, `streamGenerateContent?alt=sse`
