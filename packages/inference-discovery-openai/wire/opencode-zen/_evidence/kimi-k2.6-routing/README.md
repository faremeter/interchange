# OpenCode Zen routes `kimi-k2.6` to two different upstream backends

This directory documents a finding from the INTR-78 wire-discovery campaign in
the `faremeter/interchange` repository: the OpenCode Zen relay
(`https://opencode.ai/zen/go/v1/chat/completions`) routes requests for
`model: "kimi-k2.6"` to **more than one upstream platform**, and the upstream
platforms emit materially different response envelopes for the same conceptual
data. Adapters that consume openai-compatible vendors through a relay must
detect the response shape per response, not cache it per model id.

## Finding

Two captures were taken against the relay on 2026-05-20, both with
`model: "kimi-k2.6"` in the request body and both with the standard
`Authorization: Bearer …` header. The relay routed one to Moonshot AI's
direct serving infrastructure and the other to Fireworks AI's hosted serving
infrastructure. The body itself names the upstream — the relay strips
upstream-vendor response headers, so the `model` field is the only signal a
caller has to know which backend handled the request.

| Aspect                     | Moonshot path                                                      | Fireworks path                                    |
| -------------------------- | ------------------------------------------------------------------ | ------------------------------------------------- |
| Request `model`            | `kimi-k2.6`                                                        | `kimi-k2.6`                                       |
| Response `model`           | `moonshotai/kimi-k2.6-20260420`                                    | `accounts/fireworks/models/kimi-k2p6`             |
| Response `id` format       | `gen-…`                                                            | `chatcmpl-…`                                      |
| Reasoning field            | `choices[0].message.reasoning` (string)                            | `choices[0].message.reasoning_content` (string)   |
| Additional reasoning shape | `choices[0].message.reasoning_details[]` (array of `{type, text}`) | absent                                            |
| Refusal field              | `choices[0].message.refusal` (null)                                | absent                                            |
| Response `server` header   | (stripped by relay; not captured in probe)                         | `cloudflare` only (relay's CDN; no upstream hint) |
| Response message keys      | `content, reasoning, reasoning_details, refusal, role`             | `content, reasoning_content, role`                |

The "reasoning field" row is the consequential one for downstream consumers.
A naive adapter that looks for `message.reasoning_content` (the OpenAI-style
field name used by Kimi-via-Fireworks, DeepSeek, GLM, Qwen, and MiMo through
the same relay) will silently drop the reasoning content whenever the relay
routes the same `kimi-k2.6` request to Moonshot's direct path. The adapter
needs both code paths.

## Files

```
moonshot-path/
  request.json          The exact request body sent.
  response.json         The exact response body returned. The `model` field
                        carries Moonshot's native model id.
  probe-context.json    HTTP status info from the probe context.

fireworks-path/
  request.json          The exact request body sent.
  request-headers.json  Request headers, Authorization redacted to <redacted>.
  response.json         The exact response body returned. The `model` field
                        carries Fireworks' native model id.
  response-headers.json Response headers as captured. Only `server: cloudflare`
                        — the relay strips upstream-vendor headers.
  metadata.json         Capture metadata (timestamp, script version, endpoint).
```

The `moonshot-path/` files come from a baseline probe that the INTR-78 Phase 2
discovery script ran during its setup pass; that probe is local-only orchestration
scratch in the originating repository. The `fireworks-path/` files are the
committed wire fixture at
`packages/inference-discovery-openai/wire/opencode-zen/kimi-k2.6/reasoning-non-streaming/`
on the `intr-78-empirical-discovery-and-multimodal-design-gemini-and` branch.

## Both requests sent `model: "kimi-k2.6"`

Verbatim from `moonshot-path/request.json`:

```json
{
  "model": "kimi-k2.6",
  "messages": [
    {
      "role": "user",
      "content": "A bat and a ball cost $1.10 together. The bat costs $1.00 more than the ball. How much does the ball cost? Explain your reasoning step by step before stating the final answer."
    }
  ]
}
```

Verbatim from `fireworks-path/request.json`:

```json
{
  "model": "kimi-k2.6",
  "messages": [
    {
      "role": "user",
      "content": "A farmer has 17 sheep. All but 9 die. How many are left? Reason carefully before answering."
    }
  ]
}
```

The user messages differ — the probe used a classic reasoning prompt, the
production capture used the sheep-riddle prompt — but **the request-level model
identifier is identical**. The relay's backend selection cannot be a function
of the prompt content because the model id is what's routed; the prompt is
relayed as opaque payload to whichever upstream the relay chose.

The response payloads confirm both upstreams understood and answered the prompt
correctly (Moonshot solved bat-and-ball at $0.05; Fireworks solved the sheep
riddle at 9). Both return HTTP 200. The semantics are the same. The envelope
shape is not.

## The smoking gun: response `model` field

The structural envelope difference (`message.reasoning` vs
`message.reasoning_content`) could in principle be explained as "two prompt
shapes triggering two encoder paths" — except that the relay (or the upstream)
also tags each response with the upstream's own native model identifier:

- `moonshot-path/response.json` reports `"model": "moonshotai/kimi-k2.6-20260420"`
  — Moonshot's own dated build identifier. The `id` is `gen-…`, Moonshot's
  convention.
- `fireworks-path/response.json` reports `"model": "accounts/fireworks/models/kimi-k2p6"`
  — Fireworks' account-scoped model path. The version even differs textually
  (`k2p6` vs `k2.6`). The `id` is `chatcmpl-…`, Fireworks' convention.

Two different upstream platforms physically served two requests for the
same relay-level `kimi-k2.6` identifier.

## Implications for downstream adapter design

Any adapter that consumes openai-compatible models through a relay endpoint
must not assume:

1. **A given model id is bound to one upstream.** The relay may route
   different requests for the same id to different platforms, and the platforms
   may emit different envelopes.
2. **The response envelope shape is stable per model id.** A response that
   carries `message.reasoning` today may carry `message.reasoning_content`
   tomorrow, depending on which upstream the relay selected.
3. **Upstream-vendor identification can be inferred from response headers.**
   The relay strips them. The only reliable signal is the `model` field in the
   response body (and possibly the `id` format).

The safe pattern is shape-detection per response: inspect the message object's
keys and accommodate the union of fields.

## Caveats

- **The two captures used different prompts.** The Moonshot-path probe used
  the bat-and-ball riddle; the Fireworks-path fixture used the sheep riddle.
  This is a confound only insofar as one might wonder whether the prompt
  content drove the field naming. The presence of the upstream's native model
  identifier in the response body rules that hypothesis out: the prompt content
  cannot determine which upstream is the registered serving infrastructure.
- **A single observation per backend.** The Moonshot route was seen once
  (during the probe); the Fireworks route was seen once (during the production
  capture). The relay may route a given request to either upstream on any
  individual call. Repeated captures would establish whether the routing is
  random, session-sticky, or load-balanced. The qualitative finding —
  "the relay routes to multiple backends for one model id" — holds on either
  account.
- **Session affinity may exist within a single OpenCode Zen TCP session or API
  key.** The two captures were taken minutes apart with the same API key but
  through different processes. Whether the relay has any kind of stable
  routing for a single client is not characterized here.
- **The fixture's recorded request was redacted before commit.** The
  `Authorization` header value in `fireworks-path/request-headers.json` is
  `<redacted>`. The probe-side request was not captured at the header level
  during the lightweight probe.

## How to reproduce

The capture rig lives at `bin/opencode-discover/` on the
`intr-78-empirical-discovery-and-multimodal-design-gemini-and` branch of
`faremeter/interchange` (see commit `029899f` and successors). Given an
OpenCode Zen API key:

```bash
export OPENAI_API_KEY=...           # an OpenCode Zen token
export OPENAI_BASE_URL=https://opencode.ai/zen/go/v1
bun bin/opencode-discover.ts --model kimi-k2.6 --probe
bun bin/opencode-discover.ts --only reasoning-non-streaming --model kimi-k2.6
```

The two invocations should produce a probe-record JSON under
`dispatch/intr-78-phase-2/1a-opencode_rig/probe-kimi-k2.6.json` and an updated
production fixture under
`packages/inference-discovery-openai/wire/opencode-zen/kimi-k2.6/reasoning-non-streaming/`.
The `model` field in each `response.json` will identify which upstream the
relay routed to that time. Re-running may produce different routing.

## Source of the captures in this directory

- `moonshot-path/`: extracted from the probe artifact
  `dispatch/intr-78-phase-2/1a-opencode_rig/probe-kimi-k2.6.json` in the
  interchange repository (gitignored — local-only orchestration scratch).
  The probe record is preserved into this evidence directory because the
  `dispatch/` tree itself is not committed.
- `fireworks-path/`: copied verbatim from the production capture at
  `packages/inference-discovery-openai/wire/opencode-zen/kimi-k2.6/reasoning-non-streaming/`
  in this same repository (this evidence directory is itself part of the
  same fixture tree). The production capture is the authoritative copy
  for tooling that consumes the corpus; the copy here exists so a third
  party reading just this evidence directory has the full record without
  cross-referencing.
