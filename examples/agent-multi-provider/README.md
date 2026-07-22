# agent-multi-provider

Combine three flavours of routing policy on top of
`@intx/agent`'s `sources` array and `setSource()` method:

- **model-per-task** — choose a "cheap" or "smart" model per send
  based on a per-prompt heuristic (here: prompt length).
- **failover** — if the primary source's call rejects, swap to a
  fallback source and retry once.
- **cost optimisation** — the model-per-task heuristic doubles as a
  cost knob; cheap models get the short, easy prompts and the smart
  model only sees the long, expensive ones.

None of this logic lives inside `@intx/agent`. The package
exposes the registry and a hot-swap primitive (`setSource`) and
that's it — by design. Routing strategies vary too widely between
deployments to ship inside the agent surface, so the example shows
the pattern in user-land.

## What it shows

- The example helper passes both `InferenceSource` entries via
  `spec.sources` and names the active one via `spec.defaultSource`.
  At the env level only one source is active at a time;
  `setSource(source)` rotates the active source (id, credentials,
  model, defaults, capabilities) in place.
- A small policy module (`./src/policy.ts`) decides which source
  applies per send. `routeSource` picks the model tier from the
  prompt; `withFailover` retries against a fallback on rejection.
- The reactor reads the active source lazily at the start of each
  inference call, so a `setSource` call between sends takes effect
  on the very next inference request.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-multi-provider
bun run start "what is the boiling point of water" "explain quantum tunneling in two paragraphs, with a worked example involving a hydrogen atom and an electrostatic barrier"
```

Output looks like:

```
> what is the boiling point of water
  routed to: tier=cheap model=claude-haiku-4-5-20251001
  attempts:  primary
  served by: primary
  reply:     assistant
  text:      100C.

> explain quantum tunneling in two paragraphs, ...
  routed to: tier=smart model=claude-sonnet-5
  attempts:  primary
  served by: primary
  reply:     assistant
  text:      ...
```

If the primary call fails (network outage, 5xx, expired credentials)
the line for that prompt shows `attempts: primary -> fallback` and
`served by: fallback`. The agent keeps running on whichever source
served the most recent send until the next `routeSource` call swaps
the model again.

To start fresh:

```bash
rm -rf ../../tmp/agent-multi-provider
```

## Walkthrough

The interesting code lives in two places:

1. **`src/policy.ts`** has three small functions:
   - `pickModelTier(prompt)` is the routing heuristic. Replace this
     with whatever maps your prompts to a tier — classify intent,
     consult past token usage, check whether the request needs tool
     access, etc.
   - `routeSource({ prompt, primary, models })` overlays the
     chosen model on the primary source and returns the
     `InferenceSource` (with a freshly synthesized id) that should be
     applied for this send.
   - `withFailover({ primary, fallback, applySource, invoke })`
     wraps a send: applies the primary, runs `invoke()`, and on
     rejection swaps to the fallback and retries once. The function
     reports which source ultimately served the request so the
     caller can log or meter accordingly.

2. **`src/cli.ts`** wires the policy into a per-prompt loop. Before
   each `agent.send(prompt)` it calls `routeSource` to pick the
   model, then `withFailover` to apply the source and run the
   send. The loop reports the attempt chain and the served source
   so the routing decisions are visible to the reader.

## Why no failover inside `@intx/agent`?

Failover is policy. The questions a real failover policy has to
answer (which errors retry, how many retries, exponential backoff,
budget caps, half-open circuit breakers, per-tenant overrides) vary
too much between deployments to bake into the agent surface.

`setSource()` is the primitive the agent provides; everything else
is user code that layers the policy you want on top. The example
implements the simplest useful policy (one retry against a fallback)
so the shape is visible.

## Extending the model-per-task heuristic

A real heuristic might consider:

- whether the prompt is likely to need tool use (smart model required
  for reliable function calling),
- recent token usage on this `contextDir`,
- a tenant- or user-specific budget,
- the size of the existing conversation history (larger context →
  smarter model),
- whether the user explicitly requested a model via a structured
  payload (see
  [`agent-structured-payload`](../agent-structured-payload/README.md)).

The agent surface doesn't care which one you pick. The pattern is
always the same: derive an `InferenceSource`, call `setSource`, then
`send`.
