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
CLOSED. It composes to a registry that throws on any lookup. A workflow that
declares a loop but ships no loops module therefore deploys, and then fails
when its refs are resolved. This is the opposite of `interchange.directors`:
a step with no `director` uses the built-in registry, and a custom director
id that does not resolve fails the deploy probe, not a later establish.

A director id is `<package-name>/<local-name>`. A prefix matching the
workflow package's own name loads the workflow package's own
`interchange.directors` module; any other legal prefix loads the module of
the package laid out under `node_modules/<package-name>`. A direct dep
(workspace member or pinned registry dep -- both lay out under
`node_modules/` identically) lands there, and a published tarball that
bundles its own `node_modules` also places packages there. A director id
whose slot cannot be resolved, or that carries no package prefix, is
unresolvable. The id prefix is an npm package name, not a path: `.` / `..`
segments and extra slashes beyond a scope cannot look up a package outside
the workflow's own `node_modules`. A package's exported director ids must
sit under its own name, so the approved `director:<id>` grant names the
package whose code runs.

Where it lives today: the same two loaders, plus
`loadWorkflowDirectorRegistryFromClosure`,
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
`(input, ctx, signal)` and nothing else: no closure over host configuration.
The one injected service is the handler's consumer-scoped credentials
capability, which `ctx.perform` gives each `run` as `{ credentials }`. Its
consumer is `action:<handler>`. The action declares each handle it resolves
in `credentials: [...]`, and a root `credentialBindings` entry with
`action: "<handler>"` binds each declared handle. Deploy refuses a declared
handle with no binding, except the reserved hub handle `interchange.hub`,
which the hub binds itself (see 32). Any other per-deployment configuration
it needs must arrive through its `input` selector, which in practice means
the author threads it from the trigger payload.

Where it lives today: the `ActionHandler` doc comment,
`packages/workflow/src/definition/primitives.ts`. The mechanism that makes
the handler a bare export is `loadWorkflowActionHandlersFromClosure`
(`packages/workflow-host/src/workflow-definition-loader.ts`), which returns a
bare `(ref) => ActionHandler`.

---

## Loop semantics

**6.** THE MOST IMPORTANT ONE. `while` and `carry` are called as
`(iterationOutput, iterationInput)`, and only for an iteration that
completed. On convergence, `runLoop` breaks BEFORE it calls `carryFn`.
`carry` is the last attempt's INPUT. On convergence that is the state the
converging iteration worked from, and `final` is that same iteration's
OUTPUT. A `while` that judges the iteration output is the natural form and
it works: the value it settled on is `final`. A `while` that judges the
carry state also works, and leaves its answer in `carry`. An author must
pick the field that matches the argument their `while` reads. The scoped
per-iteration step ids are not selector paths, so `final` is the only way a
downstream step reaches inside a completed iteration.

`onIterationFailure` is absent or `"end"` unless the author sets
`"tolerate"`. A tolerated failure does not call `while` or `carry`, so the
next attempt receives the same input, and the attempt still counts toward
`maxIterations`. `final` stays the last completed attempt's output, or
`null` when every attempt was tolerated, so `final` and `carry` can describe
different attempts. A cancelled attempt, or a failure that is an abort
teardown, fails the loop. The field stays off the definition when unset, so
an existing loop's hash does not change.

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
`final` is `null` when every attempt was a tolerated failure. Keys may be
ADDED to it but never renamed or removed: the record is persisted
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

**12.** `runLocal`'s defaults supply no external capability. Its DEFAULT
`invokeStep` runs no inference at all. It calls the workflow `authorize` and
returns `{ output: null }`. A step whose agent declares tools therefore never
calls a tool under the default invoker. A local run of a tool-bearing
workflow needs a caller-supplied `invokeStep` that constructs a real agent.

To run a definition outside a sidecar the way a deployed source-ref run
does, use the real step invoker from `@intx/workflow-host/local`:

```ts
const snapshotRef = {
  current: createLocalCredentialsSnapshot({ definition, directors, delivery }),
};
const authorize = createCredentialsBackedAuthorize(
  snapshotRef,
  createGrantEvaluator({ floorFor: () => [] }),
);
const invokeStep = createWorkflowStepInvoker({
  workflowAuthorize: authorize,
  agentFactory: createToolBearingAgentFactory(),
  buildEnv: async (req) => {
    const stepId = req.authzContext.stepId;
    if (stepId === undefined) throw new Error("step has no id");
    const env = { ...stepEnvBase(stepId), capabilities };
    attachStepTools(env, {
      factories: req.agent.toolFactories.map((factory) => ({
        packageName: factory.id,
        declaredCredentials: [],
        factory,
      })),
      pluginFactories: [],
    });
    attachStepCredentialWiring(env, {
      materialCell,
      resolveGrants: () => snapshotStepGrants(snapshotRef, stepId),
      providers,
    });
    return env;
  },
});
await runLocal(definition, {
  authorize,
  invokeStep,
  hasUpstreamSignalResolver: false,
}).complete;
```

- `createLocalCredentialsSnapshot` gives every step id the grants a
  deployed run holds: the capability walk's `tool:` and `effect:` grants, a
  `credential:` use grant for each binding in `delivery`, and any `grants`
  the caller passes (a hub's requirement grants need hub state).
- The step env base holds the inference sources, context storage
  (`@intx/storage-isogit/browser` in a browser), an absolute `workdir`
  (required when the host has no `process.cwd`), the directors, and `deps`
  with the inference adapter's `fetch`. `capabilities` is the step's
  `RuntimeCapabilities` (`createRuntimeCapabilities({})` from
  `@intx/types/runtime-capabilities` is enough); the step tool builders layer
  each tool's `credentials` capability onto it. `materialCell` holds the
  `CredentialDelivery`, and `providers` is a credential provider registry,
  such as `createHttpCredentialProvider` with an injected `fetch`.
- Limits of a local run: a `childWorkflow` child authorizes against the
  parent's full grant set, where a deployed child gets a narrower capped set.
  An `ask` tool parks its step on an approval, but `runLocal` gives its
  caller no way to learn the park's correlation id, so a local run cannot
  answer the approval. With `hasUpstreamSignalResolver: false`, as above,
  such a step fails at once. Pass `true` only when the caller answers an
  `awaitSignal` gate by name through the run's `signal`; an `ask` tool in
  that run waits for an answer that never comes.
- Pass `pluginDefinitions` to `createLocalCredentialsSnapshot` when an agent
  declares plugins; the walk refuses a plugin it has no declarations for.

`runLocal` gives action handlers no credentials by default: an action that
declares `credentials` fails before its handler runs. Pass
`actionCredentials` to supply them. `createActionCredentialsFactory` from
`@intx/harness` builds a factory over a material cell, a grants resolver and
a provider registry, the same builder the deployed host uses. A
`childWorkflow` child inherits the option. `runLocal` refuses the option
together with a custom `invokeAction`, because a custom action-runner does
not use it.

Where it lives today: `createDefaultStepInvoker` and `RunLocalOptions`,
`packages/workflow/src/runlocal/run-local.ts`; the local entry,
`packages/workflow-host/src/local.ts` (exercised by
`tests/workflow/run-local-real-step-invoker.test.ts`)

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

**17.** A step's `inference` selector resolves beside `input` into per-call
options for that step's model call: `maxTokens`, `temperature`,
`thinking: { enabled, budgetTokens }`, and `effort`
(`low | medium | high | max`). Each provider adapter maps `effort` onto what
its model accepts (see `docs/INFERENCE.md`, Thinking and Reasoning). Where a
model has no place for it -- thinking set off on classic Anthropic or Gemini,
the Gemini image models, gpt-5.6 Chat Completions with tools -- the adapter
drops it with a warning and the step does not fail. When the thinking budget
an effort maps to does not fit under the output cap, the adapter refuses the
request before it reaches the provider, the same as a provider rejecting it:
the agent fails over to no other source, answers with an error reply, and the
step completes with that reply as its output. On classic Anthropic, where the
cap is 4096 unless set, an effort above `low` needs an explicit `maxTokens`.
`null` or `undefined` means the agent's defaults. A non-object, or any other
key (`systemPrompt`, `tools`, `providerOptions`, ...), fails the step as a
selector error: a run may not displace the definition approved at deploy
time. The options ride that send alone, are not recorded on `StepStarted`,
and sit beneath any option the director names outright.

Where it lives today: `resolveStepInference`,
`packages/workflow/src/runtime/selectors.ts`; `mergeInferenceOptions`,
`packages/inference/src/reactor.ts`

---

## Actions and effects

**18.** Actions get NO default-input convention. `applyDefaultInput` covers
only `step` and a `map`'s inner step, so an action with no `input` selector
receives nothing. Every action's input selector must be written out.

Where it lives today: `applyDefaultInput`,
`packages/workflow/src/definition/workflow.ts`

**19.** `ctx.perform` refuses any `capability` not listed in the action's
`effect.requires`, calls `env.authorize` before the effect, and on a ledger
hit returns the recorded result WITHOUT a run of the effect. The capability
string is the author's own vocabulary. The deploy-time walk turns each one
into an `effect:<cap>` grant. A handler gets its credentials only inside
`run`, as `run: async ({ credentials }) => ...`, so every authenticated call
that `run` awaits goes through the effect grant and the ledger. So `action()`
refuses `credentials` without an `effect.requires` capability. The scope
closes when `run` settles: after that, `credentials.resolve` rejects and every
handle from the scope refuses use. A ledger hit does not call `run`, so it
resolves no credential. This guards against an author mistake; it is
not an isolation boundary, because a handler can still call `fetch` itself.

Where it lives today: `EffectContext`,
`packages/workflow/src/runtime/env.ts`

**20.** A crash resume does not call an action's handler again. It skips a
completed action, and it fails an action that was in flight with
`crash-mid-invocation`, in a top-level run and in a body run. The effects of
that handler possibly did not finish. The rule rests on a premise about the
store and has known exceptions, and the effect ledger is not what stops a
repeat. Two handler-author obligations follow:
every external effect goes through `ctx.perform`, and an effect that a
workflow can perform again is safe to repeat. The `ActionPrimitive` doc
comment states the rule, the premise, the exceptions, the effect on `after`
dependents and the obligations in full.

Where it lives today: the `ActionPrimitive` doc comment,
`packages/workflow/src/definition/primitives.ts` (enforced by `runAction` and
`executeRunBody`, `packages/workflow/src/runtime/run.ts`; exercised by
`packages/workflow/src/runtime/resume-mid-action-step.test.ts` and
`packages/workflow/src/runtime/loop-action-crash-resume.test.ts`, and on a
deployed sidecar by
`tests/workflow-deploy/body-action-restart-effect-once.test.ts` and
`tests/workflow-deploy/body-action-crash-mid-effect.test.ts`)

---

## Testing

**21.** A tool call the authorize seam REFUSES still comes back to the model
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

**22.** bun 1.4.2's console reporter never prints passing test NAMES. It
prints the file header (and only when the file writes output) plus aggregate
counts. Neither a pty nor the dots reporter changes that. The junit reporter
(`--reporter=junit --reporter-outfile`) is the only way to get a per-test name
list out of a green run.

Where it lives today: nowhere in the repository.

---

## Definition-time rules

**23.** `defineWorkflow` REJECTS a `schedule` trigger outright (it is
reserved, not implemented), and rejects an `inboundMailPolicy` on a workflow
with no mail trigger. A `mailAccept` needs no mail trigger: every run has an
address, so a workflow of any trigger may declare who it accepts mail from.
`defineWorkflow` also rejects a grant requirement in the `mail.accept`
namespace, so `mailAccept` is the only way to author an accept-rule. Every
key of `inboundMailPolicy` defaults to REJECT, so a workflow declaring none
receives only mail the signature gate found nothing to complain about.
Before that gate, the hub refuses hub-relayed mail from a sender that no
`mailAccept` rule and no operator grant admits. A workflow declaring no
`mailAccept` receives hub-relayed mail only when an operator grant on the run
principal admits the sender. That needs a run principal, so the first mail
that fires such a workflow is always refused. Every other message is dropped
before the workflow sees it. The HTTP trigger route is governed by its own
authorization, not by this gate. Mail cannot start a deployment that has no
mail trigger: the hub refuses the first mail to it. The `mailAccept` rules of
such a workflow take effect after its own trigger starts the run.

A workflow that sends mail and waits for the reply declares `correspondent`,
whatever its trigger:

```ts
defineWorkflow({
  id: "vendor-quote",
  trigger: { type: "manual" },
  mailAccept: { correspondent: true },
  steps: { ask: step({ agent: quoteAgent }) },
});
```

Where it lives today: [`INBOUND_MAIL_POLICY.md`](./INBOUND_MAIL_POLICY.md)
for the policy itself; `normalize`,
`packages/workflow/src/definition/workflow.ts` for the rejections

**24.** A loop body may contain `awaitSignal`, `childWorkflow` and a nested
loop, but may NOT contain `sleep` or `onTrigger`. The `awaitSignal` permission
is a definition-time one only: an untimed gate inside a `childWorkflow` the
body spawns passes this check and is relayed at runtime onto the parent
(entry 27). A same-name awaiter in that child and the parent is rejected
here, because the relay puts both names on the parent's channel.

Where it lives today: the `LoopPrimitive` doc comment,
`packages/workflow/src/definition/primitives.ts` (enforced by
`validateLoopBody`, `packages/workflow/src/definition/workflow.ts`, which
also imposes the nesting-depth bound the doc comment does not state)

**25.** `defineTool` requires a package-namespaced bundle id
(`"@vendor/pkg/name"` or `"pkg/name"`). The model-facing tool NAME is a
separate, unconstrained string.

Where it lives today: `validateNamespacedId`,
`packages/agent/src/namespace.ts`

**26.** An `onTrigger` section body run id is
`<parentRunId>__<sectionId>__<eventIndex>`, from `sectionBodyRunId`. A body
already recorded as `<sectionId>__<eventIndex>` stays on that recorded id.
The next event of that parent, which has no recorded child yet, gets the
prefixed id. A new run uses only prefixed ids.

A `workflow_run` row already written under `section__<index>` keeps that
primary key. Completion does not delete the row, and this change does not
rewrite it. Two deployments that already share the key keep colliding until
the row is gone: the second terminal takes the foreign return and is
dropped. A section body's approval park is registered on the parent run,
not on the body id.

Where it lives today: `sectionBodyRunId`,
`packages/workflow/src/runtime/step-scope.ts`; the recorded-id lookup in
`runOnTrigger` and `planOnTriggerResume`,
`packages/workflow/src/runtime/run.ts`; `workflow_run.id`,
`packages/db/src/schema/workflow-run.ts`

**27.** A `childWorkflow` child carries no address of its own. The spawning
step drives the child across parks and relays each one onto the parent run:
an approval, with the body's snapshot, an author `awaitSignal`, and the input
park of a step with a trigger budget. A gate inside a loop in the child
climbs the same way until it reaches the deployment's addressable run. The
hub approval is registered on that run. A signal posted at the deployment
run id is what reaches the child. The child run id is not an address. The
spawn step awaits one signal at a time. A second park that is already live
in the child stays queued until the decision for the first is relayed down.
A queued park whose gate has already moved on is not given a signal that
arrived while it waited.

A parked child survives a process restart. Resume reattaches the same
child run from its log and reestablishes the relay on the spawn step. The
child stays unaddressable; the parent is the resume root. On drain the
relay sits on the spawn step, which defaults to `"wait"`, so the drain
signal does not abort it. That covers the whole time the step is in
flight, including a parked relay and a child that is still mid-step. An
explicit `"cancel"` still aborts the child through `localAbort`. The
child's own `drainBehavior` is not what drain consults.

An untimed park is still REFUSED in a run whose env says nothing upstream
can answer it. The step that asked fails, naming the gate, before any
suspension is written. A gate carrying a `timeout` is exempt, because its
own timer resolves it in process. The seam that spawns a run states the fact
as `hasUpstreamSignalResolver`. The deployment's own run sets it true. A
`childWorkflow` spawner forces it true as well, because the parent relays,
and inheriting a parent false would refuse a gate the relay can answer. A
loop or `onTrigger` body inherits its container's flag. An input park inside
a loop or `onTrigger` body is still rejected: those handles do not re-arm an
input park.

Where it lives today: the "Crash and suspension behavior of a `loop` body"
section of `packages/workflow/README.md`, the `hasUpstreamSignalResolver`
doc comment, `packages/workflow/src/runtime/env.ts`, and the
`AwaitSignalPrimitive` and `ChildWorkflowPrimitive` doc comments,
`packages/workflow/src/definition/primitives.ts` (enforced by
`parkOnSignalResult`, `packages/workflow/src/runtime/run.ts`)

**28.** An action step inside an `onTrigger` body or a `childWorkflow` body
runs. `buildChildRunEnv` installs `invokeAction` for that body only when the
pre-rewrite body reaches an action: `createDefaultActionInvoker` closed over
the body's capped authorize and a new in-memory effect ledger. Handlers for
the actions this env runs are resolved at spawn. A loop nested in the body
inherits that env through `createLoopIterationHandle`, so its action steps
use the same invoker and ledger. A grandchild is a different env. An action
there, including one the deployment establish pass does not check, is
resolved when that grandchild's own `buildChildRunEnv` runs. A body action
gets the same credentials in `perform`'s `run` as a top-level action, built
from the run's live material. Bind the body's handler in the root
`credentialBindings`: deploy resolves only root bindings and refuses bindings
on a body. The child run's grants keep a credential use grant only for a
consumer the body runs: a tool factory, or an action that declares
`credentials`. A body action follows the crash rule in entry 20, with the
same premise and exceptions, because its body run resumes from its own log.
So the body's in-memory ledger does not have to survive a restart.

Where it lives today: `buildChildRunEnv`,
`packages/workflow-host/src/child/substrate-factory.ts`;
`createDefaultActionInvoker`, `packages/workflow/src/runlocal/run-local.ts`;
`createLoopIterationHandle`,
`packages/workflow/src/runtime/loop-iteration-handle.ts` (a restart around an
action in a top-level `childWorkflow` or `onTrigger` body is exercised by
`tests/workflow-deploy/body-action-restart-effect-once.test.ts` and
`tests/workflow-deploy/body-action-crash-mid-effect.test.ts`)

**29.** A caller addresses an `onTrigger` body's `awaitSignal` by the
`correlationId` on the container's relay `SignalAwaited`, together with the
author `signalName`. The relay mints that id when the park is committed.
Resume of a park that is still open reuses the id on that event. A delivery
with no `correlationId` does not complete that park. A loop that is the
deployment container, a top-level `awaitSignal`, and a relay park logged
with no `correlationId` stay name-FIFO, including a signal queued before
the await.

Where it lives today: `driveContainerSignalRelay`,
`packages/workflow/src/runtime/run.ts`; `handleSignalReceived`,
`packages/workflow/src/state-machine/transition.ts`; `DeliverSignal`,
`packages/hub-api/src/routes/workflows.ts`

**30.** Two `onTrigger` sections may await the same author signal name.
A section may also share that name with a top-level `awaitSignal` or
with a `loop` that relays the name. The section park is correlated, so
a name-only delivery still binds the gate or the loop and does not
complete the section. Two same-name gates inside one section body, two
top-level gates, and a gate beside a loop stay rejected.

Where it lives today: `validateConcurrentAwaitSignalNames` and
`collectRelayedAwaitSignalNames`,
`packages/workflow/src/definition/workflow.ts`; the name-mode guard in
`driveContainerSignalRelay`, `packages/workflow/src/runtime/run.ts`

**31.** A tool declares the credential handles it resolves in
`defineTool({ ..., credentials: [{ handle }] })`. The declaration is part of
the approved projection and its hash. Deploy refuses a declared handle that
no root `credentialBindings` entry names for the tool, and a binding that
names a handle the tool does not declare. The reserved hub handle
`interchange.hub` needs no binding; the hub binds it itself. The check works
per tool package: the factories that share a package id share one set of
declarations, and every tool of the package can resolve every handle bound
to it. A package whose factories declare nothing is not held to its
bindings. A credential failure reaches the author in one of three ways:

- At install and approve, the probe gate refuses the definition with
  `credential_bindings_invalid` and names each problem.
- When the step's agent is built with delivered credential material, a
  declared handle with no delivered binding throws, and the step fails with
  `StepFailed`. A child run that received no credential material skips this
  check, so its tool fails at `resolve` instead, as in the next case.
- When a tool resolves a handle at run time and the consumer holds no
  `credential:{id}` / `use` grant, the resolve throws inside the tool call.
  The model receives an error `tool_result`, and the step and the run can
  still complete (see 21). Assert on the tool result's content.

Where it lives today: `defineTool`, `packages/agent/src/tool.ts`;
`checkCredentialBindings`,
`packages/workflow-deploy/src/credential-binding-check.ts`;
`reconcileDeclaredCredentials`, `packages/harness/src/credential-capability.ts`

---

**32.** A run calls the hub REST API as its own run principal. A tool or an
action declares the reserved handle `interchange.hub` (in
`defineTool({ credentials })` or `action({ credentials })`) and adds no
`credentialBindings` entry for it. An action resolves the handle inside
`perform`'s `run`, as any other handle. The handle resolves to an `http`
mediated credential pinned to the hub's origin, so the consumer fetches
relative paths such as `/api/tenants/<tenantId>/workflows/runs/<runId>`.

- The run holds only the grants its definition's grant requirements
  delegate to it. Name the run itself as `workflow-run:self`; see "Grant
  Requirements on Definitions" in AUTH.md.
- A run token reaches only the tenant routes on the run-token allow-list.
  Every other tenant route refuses it with 403, whatever the run's grants. A
  route outside `/api/tenants/:tenantId` does not authenticate a run token,
  so such a route that requires authentication answers 401.
- A `childWorkflow` body and a loop body share the top-level run's
  principal, so their calls act as the top-level run.
- The token stops working when the run principal is deactivated, for
  example when the run ends. A consumer can read the header it sends, so do
  not log or return the request headers.

Where it lives today: "Workflow run tokens" in `docs/AUTH.md`;
`RUN_TOKEN_ROUTES`, `packages/hub-api/src/middleware/run-token-auth.ts`.
