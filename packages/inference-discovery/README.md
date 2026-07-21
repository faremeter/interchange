# @intx/inference-discovery

The shared runtime for the inference discovery rig. Defines the
provider plug-in contract, drives capture runs, and owns the
capability catalog and support matrix that say which
(provider, model, capability) tuples the rig knows how to record.

The output of a discovery run is a fixture bundle on disk under
`packages/inference-testing/wire/<provider>/<model>/<capability>/`,
which `@intx/inference-testing` then replays in tests. This package
does not perform the replay; it produces the bytes that the replay
layer consumes.

Discovery makes real, paid network calls to upstream model providers.
It must never run in CI. The `assertNotCI` guard exported here aborts
the process before any plug-in is constructed if the `CI` environment
variable is set.

## Surface

The package exports two entry points:

- `@intx/inference-discovery` — the runtime: plug-in contract,
  capture runner, CLI parser, manifest builder, content-type
  detection, CI guard, env validation, write-capture.
- `@intx/inference-discovery/catalog` — the data: the `Capability`
  enum (the shared `@intx/types` capability vocabulary extended with
  the probe-only capabilities this rig records), the `INTENTS` table,
  the `SUPPORT_MATRIX` listing every (provider, model, capability)
  tuple the rig knows about, `catalogCapabilitiesFor` (which seeds the
  tenant catalog from the matrix), and the `FixtureManifest` schema.

## Driving one capture

```ts
import { runCapture } from "@intx/inference-discovery";
import { INTENTS, getFixtureDir } from "@intx/inference-discovery/catalog";
import { createSomeProviderPlugin } from "@intx/inference-discovery-some-provider";

const plugin = createSomeProviderPlugin({ apiKey });

await runCapture({
  plugin,
  model: "some-model",
  capability: "plain-text",
  intent: INTENTS["plain-text"],
  outDir: getFixtureDir({
    provider: plugin.name,
    model: "some-model",
    capability: "plain-text",
    outcome: "captured",
  }),
});
```

`runCapture` walks the plug-in's capture-step generator, POSTs
each step, detects SSE vs JSON by content-type, and writes four
files into the step's subdirectory: `request.json` and
`response.{json,sse}` carry the bodies verbatim, while
`request-headers.json` and `response-headers.json` carry the
headers with the plug-in's redaction lists applied. After the
generator exhausts it writes `manifest.json` at the run root.

Plug-ins that capture reasoning capabilities can opt into a
`reasoning-trace.json` sidecar; the runner calls the plug-in's
extractor and writes the result alongside the response.

## Catalog

`SUPPORT_MATRIX` is the canonical list of what the rig captures.
Each entry carries an outcome of `captured`, `misled`, `refused`,
`http-error`, or `unsupported`. `captured` and `misled` entries
produce fixtures; the others are negative documentation recording
why no fixture exists — a deliberate refusal, an observed upstream
error, or a capability the provider does not implement (see the
`glm-5.1` refusal and `deepseek-v4-pro` HTTP-error vision entries
for examples). A `misled` entry is an HTTP 200 whose body did not
carry the documented shape the capability implies — the model
responded but the contract did not fire — so its fixture records
what the wire actually returned (see the `gemini-2.5-flash`
safety-classification entry).

`INTENTS` maps each capability to the prompt, tools, follow-up
turns, and media references the plug-in uses to assemble the
request body. Intents are deliberately single-sentence and
low-token so the captured responses stay focused on shape rather
than substance.

## Seeding the catalog

The tenant catalog's per-offering capability set is seeded one way
from this matrix. `catalogCapabilitiesFor(provider, model)` reads the
fixture-bearing rows for a `(provider, model)` tuple and returns the
capabilities that tuple has proven. A `-streaming` capture also lights
up its buffered base — a streaming flow can be collected into a
buffered one — while a buffered capture does not imply the streaming
variant. The result is projected onto the shared `@intx/types`
capability vocabulary, so the probe-only capabilities this rig records
never reach a catalog row.

The direction is one way: the matrix is authoritative and the catalog
mirrors it. A catalog-side CI guard rejects any seeded offering that
claims a wire capability without a fixture-bearing row, so the catalog
cannot drift from what discovery observed.

## The `discover` CLI

`bin/discover.ts` at the repo root wires this package together with
the provider plug-in packages and exposes them as a single command:

```
bin/discover --provider <name> (--all | --model <name> | --only <capability>)
```

`--all` is mutually exclusive with `--model` and `--only`; at least
one of the three must be present (an invocation with none errors
out). Available providers and their required environment variables
are listed by `bin/discover --help`. The CLI calls
`assertNotCI` first, validates the requested provider's environment
variables via `requireEnvSet`, filters `SUPPORT_MATRIX` to the
matching captured entries, and runs each through `runCapture`.

Each invocation incurs per-request usage charges against the
upstream provider; an `--all` run touches every (model, capability)
pair in the matrix for the selected provider.

## See also

- [`docs/OPENCODE_DISCOVERY.md`](../../docs/OPENCODE_DISCOVERY.md) —
  observed-vs-documented narrative for the OpenCode Zen relay, with
  pointers into the captured corpus.
- [`@intx/inference-testing`](../inference-testing/README.md) —
  consumes the fixtures this rig produces.
