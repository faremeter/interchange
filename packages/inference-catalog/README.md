# @intx/inference-catalog

A standalone, declarative catalog of the inference models and providers the
system knows about. It bakes resolved facts — canonical and display model
names, per-deployment providers (plugin, base URL), and each offering's
capabilities and quirks — into typed constants with **no runtime
dependencies**. Importing the package pulls no discovery rig, no database, no
hub, no network, and runs no side effects.

This is the declarative counterpart to `@intx/inference-discovery`, which stays
the exploratory probing rig. The catalog is the typed authoring source for the
dev seed; the hub remains the runtime source of truth for what agents actually
use, so model and provider configuration stays tunable and policy-controlled at
runtime.

## Exports

- `@intx/inference-catalog` — models, providers and their offerings, the
  `OPENAI_FIRSTPARTY_QUIRKS` constant, and the `Capability` type.
- `@intx/inference-catalog/models` — the flat `{ canonicalName, displayName }[]`
  list on its own, self-contained and dependency-free.

The `Capability` type is owned by this package rather than re-exported from
`@intx/types`, so consumers get the capability vocabulary without taking a
dependency on `@intx/types`.

Pricing, credential material, and deployment topology are **not** in this
package — they are dev-only fixtures that live with the seed.

## Capabilities are baked, with provenance

Each offering carries three related fields:

- `discoverySource` — the `(provider, model)` tuple in the discovery support
  matrix the wire capabilities are drawn from, or `null` when the tuple has not
  been probed.
- `curatedCapabilities` — model capabilities the matrix cannot prove
  (`long-context`, `prompt-caching`), curated by hand.
- `capabilities` — the resolved list the offering advertises: the matrix wire
  set for `discoverySource` followed by `curatedCapabilities`, baked to a
  literal.

The provenance (`discoverySource`, `curatedCapabilities`) is kept alongside the
baked `capabilities` on purpose. It is what lets the guard recompute the baked
value and what lets a future maintainer understand why a baked array holds what
it holds.

## The re-bake contract

The catalog's content — which models, providers, and offerings exist — is
hand-authored. Nothing syncs it from a live source; adding or changing an entry
is a manual edit.

Consistency, however, is machine-enforced. The guard test (`catalog.test.ts`)
recomputes each offering's `capabilities` from its `discoverySource` and fails
if the baked literal has drifted. When discovery captures a new session that
proves a new capability, the support matrix changes and this guard fails — the
signal that the catalog is stale and must be **re-baked**. The failure message
prints the value to bake in, so re-baking is a copy of the recomputed array
into the offering's `capabilities`. The guard also pins the local capability
vocabulary against `@intx/types`, and — for the brands the catalog and the
discovery matrix share — the provider base URLs against the discovery brand
map, catching those drifting too.
