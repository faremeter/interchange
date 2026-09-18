# Workflow Authoring

## What this document is, and what it is not

This is a **skeleton, not a guide**. It is an enumeration of the facts an
author must know to write a workflow against `@intx/workflow` and deploy it
through `@intx/workflow-host`. Each entry states the fact and names the
symbol or module that is its home in the source tree today. None of the
entries is written up properly yet. That work is still to do.

The list came from one method, and the method is the reason the list is
worth a file of its own. Someone authored a complete workflow against this
API cold, with no guide, and recorded every fact they had to read source or
test files to learn. The result is `examples/workflow-quickstart`. At the
time that author wrote them down, none of the twenty-four entries below was
stated in `README.md`, `CONVENTIONS.md`, `DEV.md`, `docs/`, or any package
README. That is a fact about the day the list was made, not a standing claim
about the tree.

Two properties make the form below correct, and a rewrite must keep both:

- **Each entry names where the fact lives in the source tree.** An author who
  needs the full truth can go read that symbol. A guide that only paraphrased
  the behaviour would send nobody anywhere. Where a fact has both a prose home
  and an implementation, the entry names the prose home first, because that is
  what a reader should read.
- **The pointer doubles as a staleness check.** When a named symbol moves or
  disappears, the entry is wrong, and it is visibly wrong. A paraphrase rots
  silently.

**Entries describe current behaviour, not intended behaviour.** Some of the
behaviour below is surprising, and some of it may be changed rather than
documented. An entry that a later change invalidates must be corrected here
in the same change.

**Entries graduate, and the list must show it.** A fact whose home becomes a
doc comment on the symbol that owns the behaviour has found its proper home,
and the entry's job narrows to a pointer. That is the intended direction of
travel for every entry here, so more of them will move this way. Repoint such
an entry at its new home rather than leave it claiming the fact is written
down nowhere.

`examples/workflow-quickstart` is a working consumer that exercises a number
of these facts, and its README explains the ones that shaped its design. It
is the best companion to this list. The pointers below still name the source
symbol that defines each behaviour, because that symbol is the authority.

---

## Packaging

**1.** The `interchange.workflow` module must export EXACTLY ONE value that
validates as a `WorkflowDefinition`. Zero is an error and so is two. A loop
body, a `childWorkflow` body and an `onTrigger` body are each a full
`defineWorkflow` result, so any of them defined in the same module must stay
a module-private `const`. An export makes the entry ambiguous and the
deployment is refused.

Where it lives today: `selectWorkflowDefinition`,
`packages/workflow-host/src/workflow-definition-loader.ts`

**2.** `interchange.loops` and `interchange.actions` resolve a loop's
`while`/`carry` and an action's `handler` BY EXPORT NAME against the named
module's namespace object. The ref is a plain string with no compile-time
link to the export. A rename of the export without a rename of the ref fails
at establish, not at authoring time.

Where it lives today: `loadWorkflowLoopFnsFromClosure` and
`loadWorkflowActionHandlersFromClosure`,
`packages/workflow-host/src/workflow-definition-loader.ts`

**3.** An absent `interchange.loops` or `interchange.actions` field fails
CLOSED. It composes to a registry that throws on any lookup. This is the
opposite of `interchange.directors`, which falls back to the built-in
registry. A workflow that declares a loop but ships no loops module therefore
deploys, and then fails when its refs are resolved.

Where it lives today: the same two loaders,
`packages/workflow-host/src/workflow-definition-loader.ts`

**4.** Five of the `interchange.*` fields name a module -- `tools`,
`workflow`, `directors`, `loops` and `actions` -- and all five may name the
SAME module. The sixth field, `credentials`, is a declaration array, not a
module ref. Every deployed loop fixture points `workflow`, `loops` and
`actions` at one bundled entry. Every field is optional, and an omission is
not caught at deployment: per entry 3, an absent `loops` or `actions` fails
closed only when a ref is resolved, and an absent `directors` falls back to
the built-in registry.

Where it lives today: `PackageJSON`, `packages/types/src/package-json.ts`
(the fixtures are `tests/workflow-deploy/fixtures/loop-*-workflow.ts`)

**5.** A deployed action handler is a BARE MODULE EXPORT. It receives exactly
`(input, ctx, signal)` and nothing else: no closure over host configuration,
no injected services. Any per-deployment configuration it needs must arrive
through its `input` selector, which in practice means the author threads it
from the trigger payload.

Where it lives today: the `ActionHandler` doc comment,
`packages/workflow/src/definition/primitives.ts`. The mechanism that makes
the handler a bare export is `loadWorkflowActionHandlersFromClosure`
(`packages/workflow-host/src/workflow-definition-loader.ts`), which returns a
bare `(ref) => ActionHandler`.

---

## Loop semantics

**6.** THE MOST IMPORTANT ONE. `while` and `carry` are called as
`(iterationOutput, iterationInput)`. On convergence, `runLoop` breaks BEFORE
it calls `carryFn`. The two fields the loop publishes are therefore NOT two
views of the same thing: `carry` is the settling iteration's INPUT, the state
that iteration worked from, and `final` is that same iteration's OUTPUT. A
`while` that judges the iteration output is the natural form and it works:
the value it settled on is `final`. A `while` that judges the carry state
also works, and leaves its answer in `carry`. An author must pick the field
that matches the argument their `while` reads. The scoped per-iteration step
ids are not selector paths, so `final` is the only way a downstream step
reaches inside the last iteration.

Where it lives today: the `LoopPrimitive` doc comment,
`packages/workflow/src/definition/primitives.ts` (implemented by `runLoop`,
`packages/workflow/src/runtime/run.ts`)

**7.** A loop iteration's output, as `while`/`carry` see it, is the body run's
per-step output RECORD keyed by step id, not the last step's value. A
one-step body named `shorten` yields `{ shorten: <that step's output> }`.
`final` publishes that record unchanged, so a downstream selector must go
through the body step id.

Where it lives today: the `final` bullet of the `LoopPrimitive` doc comment,
`packages/workflow/src/definition/primitives.ts` (built by
`hydrateChildOutputs`, `packages/workflow/src/runtime/run.ts`)

**8.** The loop's step output shape is
`{ outcome: "converged" | "exhausted", iterations: number, carry: unknown, final: unknown }`.
Keys may be ADDED to it but never renamed or removed: the record is persisted
inline on the loop's `StepCompleted`, and a resumed run whose log predates a
rename fails on the missing key.

Where it lives today: the `LoopPrimitive` doc comment,
`packages/workflow/src/definition/primitives.ts` (implemented by `runLoop`,
`packages/workflow/src/runtime/run.ts`)

**9.** `onExhausted` must name a step that lists the loop in its own `after`.
The "converged" arm is every OTHER after-dependent of the loop. The runtime
prunes the not-taken arm's whole downstream closure, so exactly one arm runs.

Where it lives today: `routeLoopOutcome`,
`packages/workflow/src/runtime/run.ts`

**10.** A loop iteration's `AuthorizeContext.runId` is
`<parentRunId>__<loopId>__<index>`, distinct per iteration. That is what lets
a host give each iteration its own workdir, and therefore its own fresh agent
conversation.

Where it lives today: `loopBodyRunId`,
`packages/workflow/src/runtime/step-scope.ts` (`runLoop` in
`packages/workflow/src/runtime/run.ts` is the caller)

**11.** `escalation` needs no handler ref and no resolver. It is a pure
runtime node that emits `{ escalatedTo, payload }`. That makes it the cheapest
possible `onExhausted` arm.

Where it lives today: `runEscalation`,
`packages/workflow/src/runtime/run.ts`

---

## Step invocation

**12.** `runLocal`'s DEFAULT `invokeStep` runs no inference at all. It calls
the workflow `authorize` and returns `{ output: null }`. A step whose agent
declares tools therefore never calls a tool under the default invoker. A
local run of a tool-bearing workflow needs a caller-supplied `invokeStep`
that constructs a real agent.

Where it lives today: `createDefaultStepInvoker`,
`packages/workflow/src/runlocal/run-local.ts`

**13.** The step invoker owns the step's OUTPUT SHAPE. Whatever it returns
becomes the step's `output`, which is what downstream selectors read and what
a loop's `while`/`carry` receive. Nothing in the definition or the runtime
declares it. The deployed host returns `{ reply, turn }`.

Where it lives today: `stepResultFromSend`,
`packages/workflow-host/src/adapters/step-invoker.ts`

**14.** A step input that is not a string is `JSON.stringify`'d before
`agent.send`. This determines how a step agent's system prompt must be
written: it sees JSON text, not a structured payload.

Where it lives today: `synthesizeInputContent`,
`packages/workflow-host/src/adapters/step-invoker.ts`

**15.** The agent layer is workflow-unaware: its `AuthorizeFn` has no slot for
workflow vocabulary. The step's `AuthorizeContext`
(`{ stepId, attempt, runId }`) reaches the workflow-typed `authorize` only
because the invoker captures it in a closure. A host that skips that
adaptation silently drops the context from every tool and capability check
the step makes.

Where it lives today: `wrapAuthorize`,
`packages/workflow-host/src/adapters/step-invoker.ts`

**16.** An invoker may return
`{ suspend: { correlationId, kind: "approval", approvalSnapshot } }` instead
of an output. Only an `"approval"` suspend is representable: the `"input"`
park is minted exclusively by the runtime's trigger-budget re-arm.

Where it lives today: `StepInvokeResult`,
`packages/workflow/src/runtime/env.ts`

---

## Actions and effects

**17.** Actions get NO default-input convention. `applyDefaultInput` covers
only `step` and a `map`'s inner step, so an action with no `input` selector
receives nothing. Every action's input selector must be written out.

Where it lives today: `applyDefaultInput`,
`packages/workflow/src/definition/workflow.ts`

**18.** `ctx.perform` refuses any `capability` not listed in the action's
`effect.requires`, calls `env.authorize` before the effect, and on a ledger
hit returns the recorded result WITHOUT a run of the effect. The capability
string is the author's own vocabulary. The deploy-time walk turns each one
into an `effect:<cap>` grant.

Where it lives today: `EffectContext`,
`packages/workflow/src/runtime/env.ts`

**19.** Three handler-author obligations the runtime cannot enforce: every
external effect goes through `ctx.perform`; each effect is idempotent under
its `effectId`, or atomic with its ledger record; and a returned output is
deterministic given its effects' results, because a crash resume replays the
handler BODY against ledger hits.

Where it lives today: the `ActionPrimitive` doc comment,
`packages/workflow/src/definition/primitives.ts`

---

## Testing

**20.** A tool call the authorize seam REFUSES still comes back to the model
as a well-formed `tool_result` that carries the refusal text. The model
answers it, the step completes, the loop converges, and the run reaches a
clean terminal status. Run status, step outputs, iteration count and even the
PRESENCE of a `tool_result` all look healthy on a run where the tool never
executed. A test that asserts a tool ran must assert on the `tool_result`'s
CONTENT.

Where it lives today: `toolResultTexts`,
`tests/workflow-deploy/nested-tool-invoke-helpers.ts`, whose docstring states
the rule and whose callers are the deployed tool-invoke round-trips. The
`examples/workflow-quickstart` README records it too, and the assertion in
`tests/workflow-quickstart/cli.test.ts` depends on it.

**21.** bun 1.4.2's console reporter never prints passing test NAMES. It
prints the file header (and only when the file writes output) plus aggregate
counts. Neither a pty nor the dots reporter changes that. The junit reporter
(`--reporter=junit --reporter-outfile`) is the only way to get a per-test name
list out of a green run.

Where it lives today: nowhere in the repository.

---

## Definition-time rules

**22.** `defineWorkflow` REJECTS a `schedule` trigger outright (it is
reserved, not implemented), and rejects an `inboundMailPolicy` on a workflow
with no mail trigger.

Where it lives today: `normalize`,
`packages/workflow/src/definition/workflow.ts`

**23.** A loop body may contain `awaitSignal`, `childWorkflow` and a nested
loop, but may NOT contain `sleep` or `onTrigger`. The `awaitSignal` permission
is a definition-time one only: an untimed gate inside a `childWorkflow` the
body spawns passes this check and is refused at runtime instead (entry 26).

Where it lives today: the `LoopPrimitive` doc comment,
`packages/workflow/src/definition/primitives.ts` (enforced by
`validateLoopBody`, `packages/workflow/src/definition/workflow.ts`, which
also imposes the nesting-depth bound the doc comment does not state)

**24.** `defineTool` requires a package-namespaced bundle id
(`"@vendor/pkg/name"` or `"pkg/name"`). The model-facing tool NAME is a
separate, unconstrained string.

Where it lives today: `validateNamespacedId`,
`packages/agent/src/namespace.ts`

**25.** KNOWN LIMITATION. An `onTrigger` section body can silently NOT RUN
when a second top-level run of the same deployment reaches it. The body's run
id is derived as `<sectionStepId>__<eventIndex>`, with no parent-run prefix, so
it is the same string for every run of that deployment. A run whose body log is
already terminal short-circuits and returns the earlier result: the body never
executes, and the run still reports a clean terminal status.

Contrast the loop path, which derives `<runId>__<loopId>__<index>` through
`loopBodyRunId` and therefore re-roots per run. The asymmetry is the defect.

This predates tool-bearing section bodies, but it costs more now than it did:
what a skipped body skips is real tool work rather than only non-inference
primitives. Tracked as INTR-552, which must resolve how already-running bodies
keyed under the current derivation are handled before the derivation changes.

Where it lives today: the `onTrigger` section path in `runOnTrigger`,
`packages/workflow/src/runtime/run.ts`, against `loopBodyRunId`,
`packages/workflow/src/runtime/step-scope.ts`

**26.** An untimed park is REFUSED wherever nothing upstream could answer it:
beneath a `childWorkflow` child, at any depth. A park is not only an
`awaitSignal` an author writes: an agent step that suspends on a tool declared
`approval: "ask"` is refused the same way, as is the input park of a step with
a trigger budget. The child carries no address of its own and its spawner
awaits its terminal rather than driving it across parks, so no signal can
reach the gate. The step that asked fails, naming the gate, before any
suspension is written. A gate carrying a `timeout` is exempt, because its own
timer resolves it in process. The seam that spawns a run states the fact as
`hasUpstreamSignalResolver`, which is why the deployment's own run and a
container-driven suspendable body both keep their gates.

Where it lives today: the "Crash and suspension behavior of a `loop` body"
section of `packages/workflow/README.md`, the `hasUpstreamSignalResolver`
doc comment, `packages/workflow/src/runtime/env.ts`, and the
`AwaitSignalPrimitive` and `ChildWorkflowPrimitive` doc comments,
`packages/workflow/src/definition/primitives.ts` (enforced by
`parkOnSignalResult`, `packages/workflow/src/runtime/run.ts`)
