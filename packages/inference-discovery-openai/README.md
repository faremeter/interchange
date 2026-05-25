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
  overrides reasoning extraction. Today the only deployment is
  OpenCode Zen.

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
  apiKey: process.env.OPENAI_API_KEY,
  baseUrl: process.env.OPENAI_BASE_URL,
});
// Hand off to runCapture from @intx/inference-discovery.
```

Models: `kimi-k2.6`, `glm-5.1`, `deepseek-v4-pro`, `qwen3.6-plus`,
`mimo-v2-omni`.

For the per-model, per-capability behaviour observed at capture
time — including the discrepancies between vendor documentation
and the actual wire bytes — see
[`docs/OPENCODE_DISCOVERY.md`](../../docs/OPENCODE_DISCOVERY.md).
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

| Variable          | Purpose                                                      |
| ----------------- | ------------------------------------------------------------ |
| `OPENAI_API_KEY`  | Sent as `Authorization: Bearer <key>`. Redacted in fixtures. |
| `OPENAI_BASE_URL` | Relay base URL (e.g. `https://opencode.ai/zen/v1`).          |

## Adding a new deployment

A new OpenAI-compatible relay is a new file under `deployments/`
that imports the protocol-layer helpers, declares the provider
name + model list + redaction policy, and exports a factory that
returns a plug-in for the runtime.

Then register the new plug-in in `bin/discover.ts` and add its
(model, capability) entries to `SUPPORT_MATRIX` in
`@intx/inference-discovery/catalog`. Run `bun bin/discover.ts
--provider <new-name> --all` against a funded account to produce
the fixture corpus.
