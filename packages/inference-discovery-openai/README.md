# @intx/inference-discovery-openai

OpenAI-protocol provider plug-in for the discovery rig. The package
is organised in two layers:

- `protocol/` — the OpenAI Chat Completions wire format. Auth
  header construction, endpoint URL assembly, request-body builder,
  and the multi-step iterator. Reusable across any relay that
  speaks the OpenAI surface.
- `deployments/` — concrete deployments built on the protocol
  layer. Each deployment names a provider, lists its models,
  declares its auth and redaction policy, and (where needed)
  overrides reasoning extraction. Two deployments ship today: the
  OpenCode Zen relay and first-party OpenAI.

See [`@intx/inference-discovery`](../inference-discovery/README.md)
for the runtime, the plug-in contract, and the `discover` CLI.

## OpenCode Zen

OpenCode Zen is an OpenAI-compatible Chat Completions relay that
fronts upstream model providers behind a single endpoint. The
`/zen/v1` tier exposes hosted GPT, Claude, and Gemini alongside the
open-weights catalog (Moonshot, Z.AI, DeepSeek, Alibaba, Xiaomi
MiMo). Earlier captures targeted the narrower `/zen/go/v1` open-
weights tier; the v1 endpoint is a superset and existing fixtures
re-run unchanged against it.

```ts
import { createOpencodeZenPlugin } from "@intx/inference-discovery-openai";

const plugin = createOpencodeZenPlugin({
  apiKey: process.env.OPENCODE_API_KEY,
  baseUrl: process.env.OPENCODE_BASE_URL,
});
// Hand off to runCapture from @intx/inference-discovery.
```

Models: `kimi-k2.6`, `kimi-k2.7-code`, `kimi-k3`, `glm-5.1`,
`deepseek-v4-pro`, `qwen3.6-plus`, `mimo-v2-omni`.

For the per-model, per-capability behaviour observed at capture
time — including the discrepancies between vendor documentation
and the actual wire bytes — see
[`docs/discovery.md`](./docs/discovery.md).
The matrix entries for this deployment live in `SUPPORT_MATRIX`;
two vision entries are marked `refused` and `http-error` and so
produce no fixtures.

### Reasoning trace extraction

OpenCode Zen routes `kimi-k2.6` between two upstream backends that
emit reasoning content under different field paths. The deployment
ships a reasoning extractor that probes the known paths and records
which one held the non-empty value. For non-streaming reasoning
captures the runner writes the result to `reasoning-trace.json`
next to the response so a later routing change is detectable from
the fixtures alone; streaming reasoning captures do not get the
sidecar (the runner does not parse SSE bodies), and the routing
signal lives in the captured event stream itself.

### Environment

| Variable            | Purpose                                                      |
| ------------------- | ------------------------------------------------------------ |
| `OPENCODE_API_KEY`  | Sent as `Authorization: Bearer <key>`. Redacted in fixtures. |
| `OPENCODE_BASE_URL` | Relay base URL (e.g. `https://opencode.ai/zen/v1`).          |

## OpenAI

The `openai` deployment probes first-party `api.openai.com` directly,
under provider name `openai` (distinct from `opencode-zen`, though both
write fixtures into this package's `wire/` tree). The base URL is fixed
to `https://api.openai.com/v1`; the deployment reads only `OPENAI_API_KEY`.

```ts
import { createOpenAIPlugin } from "@intx/inference-discovery-openai";

const plugin = createOpenAIPlugin({ apiKey: process.env.OPENAI_API_KEY });
// Hand off to runCapture from @intx/inference-discovery.
```

Models: `gpt-5.5`.

`gpt-5.5` captures `plain-text`, `function-calling`,
`function-calling-multi-turn`, `vision-input`, and `structured-output`
(plus the streaming variants the OpenAI-protocol body builder emits).
`reasoning-content` is marked `unsupported`: first-party
`api.openai.com` Chat Completions responses carry no reasoning field for
the gpt-5 series (OpenAI surfaces reasoning only via the Responses API,
which this plug-in does not probe). The `function-calling-multi-turn`
and `vision-input` streaming variants carry no rows because the body
builder does not build them — a rig gap, not a provider limitation.

### Environment

| Variable         | Purpose                                                      |
| ---------------- | ------------------------------------------------------------ |
| `OPENAI_API_KEY` | Sent as `Authorization: Bearer <key>`. Redacted in fixtures. |

## Adding a new deployment

A new OpenAI-compatible relay is a new file under `deployments/`
that imports the protocol-layer helpers, declares the provider
name + model list + redaction policy, and exports a factory that
returns a plug-in for the runtime.

Then register the new plug-in in `bin/discover.ts` and add its
(model, capability) entries to `SUPPORT_MATRIX` in
`@intx/inference-discovery/catalog`. Run `bin/discover --provider
<new-name> --all` against a funded account to produce
the fixture corpus.
