# agent-multi-provider

Combine three flavours of routing policy on top of
`@interchange/agent`'s `providers` array and `setProvider()` method:

- **model-per-task** — choose a "cheap" or "smart" model per send
  based on a per-prompt heuristic (here: prompt length).
- **failover** — if the primary provider's call rejects, swap to a
  fallback provider and retry once.
- **cost optimisation** — the model-per-task heuristic doubles as a
  cost knob; cheap models get the short, easy prompts and the smart
  model only sees the long, expensive ones.

None of this logic lives inside `@interchange/agent`. The package
exposes the registry and a hot-swap primitive (`setProvider`) and
that's it — by design. Routing strategies vary too widely between
deployments to ship inside the agent surface, so the example shows
the pattern in user-land.

## What it shows

- `createAgent({ providers, defaultModel })` accepts a list of
  configs. The active one is `providers[0]`; `setProvider(config)`
  rotates the credentials and model in place.
- A small policy module (`./src/policy.ts`) decides which config
  applies per send. `routeProvider` picks the model tier from the
  prompt; `withFailover` retries against a fallback on rejection.
- The reactor reads `providerConfig` lazily at the start of each
  inference call, so a `setProvider` call between sends takes effect
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
  routed to: tier=cheap model=claude-3-5-haiku-20241022
  attempts:  primary
  served by: primary
  reply:     assistant
  text:      100C.

> explain quantum tunneling in two paragraphs, ...
  routed to: tier=smart model=claude-3-5-sonnet-20241022
  attempts:  primary
  served by: primary
  reply:     assistant
  text:      ...
```

If the primary call fails (network outage, 5xx, expired credentials)
the line for that prompt shows `attempts: primary -> fallback` and
`served by: fallback`. The agent keeps running on whichever provider
served the most recent send until the next `routeProvider` call swaps
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
   - `routeProvider({ prompt, primary, models })` overlays the
     chosen model on the primary config and returns the
     `ProviderConfig` that should be applied for this send.
   - `withFailover({ primary, fallback, applyProvider, invoke })`
     wraps a send: applies the primary, runs `invoke()`, and on
     rejection swaps to the fallback and retries once. The function
     reports which provider ultimately served the request so the
     caller can log or meter accordingly.

2. **`src/cli.ts`** wires the policy into a per-prompt loop. Before
   each `agent.send(prompt)` it calls `routeProvider` to pick the
   model, then `withFailover` to apply the provider and run the
   send. The loop reports the attempt chain and the served provider
   so the routing decisions are visible to the reader.

## Why no failover inside `@interchange/agent`?

Failover is policy. The questions a real failover policy has to
answer (which errors retry, how many retries, exponential backoff,
budget caps, half-open circuit breakers, per-tenant overrides) vary
too much between deployments to bake into the agent surface.

`setProvider()` is the primitive the agent provides; everything else
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
always the same: derive a `ProviderConfig`, call `setProvider`, then
`send`.
