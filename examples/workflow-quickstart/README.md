# workflow-quickstart

The smallest complete `@intx/workflow` package: a workflow that revises
a marketing tagline in a bounded loop until it is short enough, then
publishes it.

Every other example in this directory consumes `@intx/agent` — one
agent, one conversation. A workflow is the layer above: a DAG of steps
the runtime schedules, checkpoints into a durable event log, and can
resume after a crash. This example is the reference for what an author
has to write, and for what a package has to declare so a host can run
it.

It is deliberately built from `loop` and `action`, the two primitives
with the least surface area elsewhere in the repository, and the loop's
body is an agent step **with a tool** — the composition an author is
most likely to reach for and the one with the most moving parts.

## What it shows

- `defineWorkflow` with the plural `steps` shape, and the
  `interchange.*` manifest fields that make the result shippable.
- `loop` — a bounded rework loop whose body is a whole workflow, run
  once per iteration as a child run, with `while` / `carry` resolved
  from `interchange.loops` by export name.
- A `step` inside that loop body whose agent carries a tool, and the
  `invokeStep` seam that turns its `AgentDefinition` into a running
  agent.
- `action` — deterministic host TypeScript with no agent in front of
  it, resolved from `interchange.actions` by export name, performing
  its one external effect through the capability- and ledger-checked
  `EffectContext`.
- `escalation` — the loop's `onExhausted` arm, and the runtime pruning
  that guarantees exactly one of the two arms runs.
- `runLocal` — running the whole thing in-process against an in-memory
  substrate, which is how you test a workflow without deploying it.

Not shown: `map`, `gate`, `awaitSignal`, `sleep`, `childWorkflow`,
`onTrigger`, approval gates, and crash-resume. Those are covered by the
package's own test suite.

## The package layout

A workflow is shipped as a package. The host reads four optional fields
under `interchange` in its `package.json`. This example declares three of
them; it has no custom directors, so it omits `directors`.

```json
"interchange": {
  "workflow": "./src/workflow.ts",
  "loops": "./src/loops.ts",
  "actions": "./src/actions.ts"
}
```

| Field       | What the host does with it                                                                    |
| ----------- | --------------------------------------------------------------------------------------------- |
| `workflow`  | Imports the module and takes the one exported value that validates as a `WorkflowDefinition`. |
| `directors` | Imports the module and registers every exported `defineDirector` factory.                     |
| `loops`     | Imports the module; a `loop`'s `while` / `carry` strings resolve to exports **by name**.      |
| `actions`   | Imports the module; an `action`'s `handler` string resolves to an export **by name**.         |

Four things follow from that, none of which the type system tells you:

1. **The `workflow` module must export exactly one `WorkflowDefinition`.**
   Zero is an error and so is two. A loop body is itself a full
   `defineWorkflow` result, so it has to stay a module-private const —
   exporting it makes the entry ambiguous and the deployment is
   refused.
2. **Refs bind to export names, not to anything the compiler checks.**
   `while: "stillTooLong"` is a string. Rename the export without
   renaming the ref and nothing fails until the host resolves it.
3. **An absent field fails closed, not open.** There is no built-in
   default registry for loops or actions: a package that declares a
   `loop` but ships no `loops` module resolves to a registry that
   throws. (`directors` is the exception — it falls back to the
   built-in registry.)
4. **All three may name the same module.** Nothing requires the split
   used here; it exists so each file can be read on its own.

This example points those fields at TypeScript source, the same way its
`exports` block does, because it has no build step. A published package
points them at its built output.

## Running

Against the real Anthropic API:

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/workflow-quickstart
bun run start "The only project management tool your growing startup will ever truly need"
```

It prints the run's terminal status and each step's output, and writes
the accepted tagline to `<repo-root>/tmp/workflow-quickstart/context/tagline.txt`.
The per-step agent workdirs land beside it; that whole tree is covered
by the repo-wide `tmp/` gitignore.

Without `ANTHROPIC_API_KEY` the example prints what to set and exits
non-zero, via the same `resolveAgentSource` helper every `agent-*`
example uses.

## Walkthrough

**`src/workflow.ts`** is the definition, and it is _data_: every step is
a plain record, and `while`, `carry` and `handler` are strings rather
than functions. That is what lets the deploy substrate hash a workflow,
show an operator the grants it implies, and freeze the approved shape.
It also means none of the code the workflow eventually runs is reachable
from the definition — which is what the other three modules are for.

The DAG is three steps:

```
revise (loop) ──converged──> publish (action)
              └─exhausted──> giveUp (escalation)
```

`onExhausted` names `giveUp`; the _converged_ arm is every other step
that depends on the loop. The runtime routes to one arm and prunes the
other's whole downstream closure, so exactly one runs.

**`src/loops.ts`** holds `while` and `carry`. They must be pure: the
runtime re-runs them on every forward pass and again on every resume,
replaying them over the recorded inputs and outputs to re-derive where
the loop got to. Their type (`LoopFn`) receives only data — no effect
context, no authorize, no abort signal — so an effectful implementation
is not expressible.

The non-obvious part is **which argument `while` reads.** It is called
as `while(iterationOutput, iterationInput)`, and judging the output is
the natural reading. But the loop's own step output is
`{ outcome, iterations, carry }`, where `carry` is the _converging
iteration's input_, and the converging iteration's output is not exposed
at all. A `while` that judged the output would converge on a value no
downstream step can read. Judging the carry state is what makes
`steps.revise.output.carry` the accepted tagline — which is exactly what
the `publish` action consumes.

The price is one extra pass: a `loop` is a do-while, so the pass that
produces the accepted tagline is followed by one more that confirms it.
The body agent's system prompt tells it to return an already-short
tagline unchanged, so that pass is cheap. Loops whose result is a side
effect rather than a value do not have this problem; they can judge the
output and ignore `carry` entirely.

**`src/workflow.ts`'s loop body** is a whole `defineWorkflow`. Each
iteration is a separate child run of it — its own run id, its own event
log, its own step outputs — so the body's output arrives at `while` and
`carry` as a record keyed by step id (`{ shorten: { reply } }`), not as
the last step's value. The iteration's input arrives as the body's
`trigger.payload`.

**`src/step-invoker.ts`** is the seam between the definition and a live
agent. A `step` carries an `AgentDefinition` — a description, not an
instance — and the runtime never instantiates it; it hands the
definition to `env.invokeStep`. `runLocal` supplies a stub invoker by
default that authorizes the step and returns `{ output: null }` without
running any inference, which is enough to exercise a DAG's routing and
not enough for a step whose agent has a tool. So this example wires its
own: build a `BaseEnv`, adapt the workflow-level authorize onto the
agent's `AuthorizeFn` slot with the step's `AuthorizeContext` captured,
`createAgent`, one `agent.send`, close.

Two details there are worth copying. The step's input is arbitrary JSON
and `agent.send` takes a string, so a non-string input is
`JSON.stringify`'d — the deployed host does the same, which is why the
body agent's system prompt is written against the JSON it sees. And the
workdir is derived from `{ runId, stepId }`, so each loop iteration —
a distinct child run — gets a fresh agent with a fresh conversation.
That independence is what makes the passes comparable.

**`src/actions.ts`** is the action handler. A deployed handler is a bare
module export: it receives exactly `(input, ctx, signal)` — no closure
over host configuration, no injected services. Everything it needs
arrives through `input`, which is why this workflow threads `outputPath`
from the trigger payload all the way through the loop's carry state
rather than reading it from the process.

Three rules the runtime cannot enforce, and that the handler author
owns:

1. Every external effect goes through `ctx.perform`. Nothing stops a
   handler calling `writeFile` directly — and one that does is invisible
   to the capability check and replays on every resume.
2. Each effect is idempotent under its `effectId`, or atomic with its
   ledger record. On a crash resume the handler _body_ is replayed; only
   the effects are deduplicated, by `effectId`, against the ledger.
3. The returned output is deterministic given its effects' results,
   because that replay reconstructs it.

`ctx.perform` refuses any `capability` not listed in the action's
`effect.requires`. The capability string is the author's own vocabulary;
the deploy-time walk turns it into an `effect:fs:write` grant for an
operator to approve.

**`src/cli.ts`** runs it. `runLocal` supplies the whole
`WorkflowRuntimeEnv` — in-memory event log, blob substrate, scheduler,
signal channel — and drives the same runtime body the deployed sidecar
drives. The caller supplies exactly what the definition left as a string
or a declaration: `authorize`, `loopFns`, `actionResolver`,
`invokeStep`.

`authorize` is **required, with no default.** The deployed authorize
refuses to answer when it cannot resolve a decision, and the local
surface refuses to invent one — a permissive default here made an
authorization failure invisible until the workflow was deployed. There
is deliberately no permit-everything helper in the package's public API;
each call site declares its own, as `allowAll` does here.

## Testing a workflow

The test lives in [`tests/workflow-quickstart/`](../../tests/workflow-quickstart/),
not here — every example in this repository pairs with a directory under
`tests/` that the `Makefile`'s `test-unit` target enumerates by name.

It drives `main()` with a scripted inference source, so the loop, the
tool call and the action all execute with no network. One assertion in
it is worth understanding before you write your own:

> A tool call the authorize seam refuses still comes back to the model
> as a well-formed `tool_result` carrying the refusal text. The model
> answers it, the step completes, the loop converges, and the run
> reaches a clean terminal status. Run status, step outputs, iteration
> count, and even the _presence_ of a `tool_result` all look healthy on
> a run where the tool never executed.

So a test that asserts a tool ran must assert on the `tool_result`'s
**content**, never on its shape.

## Next

- [`agent-quickstart`](../agent-quickstart/README.md) — the layer below:
  one agent, one prompt, one reply.
- [`agent-rich-tool`](../agent-rich-tool/README.md) — what a step's
  agent parking on an approval gate looks like from inside the agent.
