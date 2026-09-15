# @intx/workflow

Workflow definition surface, state machine, and abstract runtime
for multi-step agent workflows.

This package is host-agnostic. It exposes the data types that
describe a workflow, the state machine that interprets a run's
event log, and the abstract `WorkflowRuntimeEnv` the runtime body
takes its dependencies from. It does not know how a run is
persisted, scheduled, or spawned — those are the host's job.

Multi-entry exports:

- `@intx/workflow/definition` — `WorkflowDefinition`, `defineWorkflow`,
  `hashDefinition`, the `stepId` shape rule. The on-disk form a
  workflow lives in. It also carries the canonical step walk
  (`walkStepTree`, `walkWorkflowSteps`, `executableStepIds`); see
  "Walking a definition's steps" below.
- `@intx/workflow/state-machine` — the event union, the transition
  function, the `RunState` projection. Pure functions over the
  workflow-run log.
- `@intx/workflow/runtime` — `runtimeRun` (the body that drives a run
  forward) plus the `WorkflowRuntimeEnv` interface every concrete
  host implements. The body switches on env keys; it never branches
  on the host process it runs in.
- `@intx/workflow/runlocal` — an in-memory adapter for tests. The
  scheduler, RepoStore, blob substrate, and spawn-child callback all
  exist purely in process memory so tests can drive the runtime
  without a substrate.

For a production host (workflow-run repo backing, scheduler that
honors wall-clock fire times, signal channel that observes commits,
DI seams for mail bus / signing key / subprocess spawner), see
`@intx/workflow-host`. For the deploy-time capability walk, the
operator-approval gate that consumes it, and the address derivation
and per-step inference-source pinning a deploy needs, see
`@intx/workflow-deploy`.

## Tools are available wherever inference runs

Every agent step can call tools, no matter which primitive encloses
it. A step at the top level, a step inside a `loop` body, a step
inside an inline `onTrigger` section body, and a step inside a
`childWorkflow` child all get the same tool-bearing execution
environment. There is no nesting depth and no primitive at which an
agent silently runs at reduced capability.

This is an invariant of the system, not a property of the primitives
that happen to exist today. A new body-bearing primitive inherits it:
if the primitive runs inference, its steps get tools.

The `childWorkflow` boundary is where that word carries weight. A
step in a child gets the same tools, and a tool declared
`approval: "ask"` reaches the provider exactly as it does at the top
level. What the child cannot do is hold the resulting approval:
nothing upstream can answer a park inside it, so the runtime refuses
the park and fails the step, naming the gate. The capability is not
quietly withdrawn -- the step that depends on it stops, and the
operator's approval decision is the reason a run failed rather than
a decision that had no effect. Keep such a step in the deployment's
own run, in a `loop` body, or in an `onTrigger` section body, where
a decision can be relayed down.

A toolless execution path is not an acceptable shortcut, and the
reason is that it is silent. The deploy-time capability walk descends
into every inline body, so an agent's `tool:<name>` reaches the
frozen surface and the operator is required to approve it before the
deploy is accepted. A runtime that then builds that agent without its
tools does not fail: the provider receives an empty tool list, the
step completes, and nothing is logged at any level. The operator made
a security decision that had no effect, and the only evidence is the
work the agent did not do. Two layers answering the same question
differently is the defect, whichever layer is more permissive.

The corollary for the authorization layer: a tool reaching the
provider is not authority to invoke it. The run's grants still gate
every call, and a spawned body's grants are capped to what the body's
own definition declares. Tools being present is what makes that gate
meaningful — a gate over an empty toolset decides nothing.

## Walking a definition's steps

`stepOrder` lists the steps of ONE definition record. A workflow's
executable surface is larger: a `loop` carries an inline body, and an
inline `onTrigger` section or `childWorkflow` carries a full nested
definition. A consumer that answers "which steps run here" off
`stepOrder` alone under-counts every nested body.

`walkStepTree` is the one traversal every such consumer goes through. It
visits steps in pre-order (a step before the bodies it carries, and a
body's steps before the next sibling), throws on a `stepOrder` entry with
no matching step, and takes the descent as an explicit argument rather
than an implicit house rule. Two named descents cover the cases in use:

- `EXECUTABLE_STEP_DESCENT` — loop bodies, inline onTrigger bodies, and
  inline childWorkflow bodies. This is the setting that yields every step
  id the deployment can execute. `executableStepIds(definition)` is the
  deduplicated id list under it, and the deploy-time capability walk
  descends with it so an operator approves everything a step can run.
- `LOOP_BODY_DESCENT` — loop bodies only. This is the setting that stays
  inside one flat step-id namespace: a loop body resolves against the
  enclosing definition's map, while an inline section or child body is
  lifted to its own definition keyed under its own ref. The deploy's
  per-step inference-source pin descends with it.

`walkWorkflowSteps` and `walkNestedWorkflowSteps` are the live
`WorkflowDefinition` entry points; they read each primitive's nested
bodies through `nestedWorkflowBodies`, whose switch is exhaustive so a
newly-added primitive kind fails at compile time rather than silently
reading as a leaf. `walkStepTree` itself is generic over the step and
tree types, so the inert wire projection — whose step values are
`unknown` and are validated as the caller descends — rides the same
traversal.

## Consuming a real agent step's structured output

Structural selectors (`map.over`, `input.from`, `project`, `merge`) do
pure path navigation. They cannot destructure or parse a value; they
only walk keys and indices that are already present as JS structure.

The production step-invoker (`createWorkflowStepInvoker` in
`@intx/workflow-host`) surfaces every real agent step's output as a
`{ reply, turn }` envelope: `reply` is the agent's final text and `turn`
is the final assistant `ConversationTurn`. An agent's structured output
therefore lands as the reply _string_ (a real agent that "returns
`{ tasks }`" surfaces `{ reply: "{\"tasks\":[…]}", turn }`), and the
terminal-tool call arguments do **not** survive on `turn` — the final
turn is the follow-up text turn, whose content is a single text block,
not the earlier `tool_use` block. So the only structured surface the
envelope exposes is the reply text.

Consequently a bare `map.over` / `input.from` selector cannot fan out
over — or read a field from — a real agent step's output. To feed a real
agent's structured output into a downstream `map.over` or `input.from`,
bridge it through a parse `action`: a host handler (wired via
`env.invokeAction`) reads `steps.<agent>.output.reply`, parses it, and
returns a plain object the downstream selectors can navigate (e.g.
`steps.parsePlan.output.tasks`). A loop's pure `while`/`carry` LoopFns
are the other host-JS seam that can read the envelope directly, since
they receive the resolved child output as data. `tests/workflow-deploy/
per-level-pipeline-real-agents.test.ts` is a worked example of the
parse-`action` bridge. This is documented guidance, not a defect: the
selector DSL is intentionally a pure, statically-inspectable path
vocabulary (so the deploy-time capability walk can compute grants
without executing user code), and parsing an opaque agent reply is host
work that belongs at an `action`/LoopFn seam.

## Crash and suspension behavior of a `loop` body

Two facts govern what a `loop` body can and cannot survive.

First, crashes. An `action` runs at most once. A mid-invocation crash fails the
run and the effect is never re-run -- and this is true everywhere, not just in a
loop: a top-level action or agent step that crashes mid-invocation also settles
`RunFailed` (it is not re-invoked on resume). So a crash inside a loop iteration
fails the run exactly as a crash in any other step does; loops are not special
here.

Second, suspension. The body-ban forbids a loop body from containing a `sleep`
or an `onTrigger`. It does NOT forbid an `awaitSignal`, a `childWorkflow`, or a
nested `loop`. A loop iteration runs through the suspendable-child seam, so its
body can park on an `awaitSignal` and resume: the park relays up through the
container's signal path and delivery resumes the iteration, the same way an
onTrigger section body parks. A parked iteration also survives a crash -- a
restart re-drives the loop, re-establishes the container's signal relay, and
resumes the iteration on the next delivery -- so an `awaitSignal` loop body is a
durable human-in-the-loop pause, not just an in-process one.

A loop body may also spawn a `childWorkflow` grandchild: the child is lifted to
a ref and runs as its own child run, depth-counted against the tree-wide spawn
ceiling exactly like any other child.

A `childWorkflow` child cannot hold a human gate, and neither can anything
inside it. The relay that makes a loop iteration's pause durable is severed at
the `childWorkflow` boundary: a child run carries no address of its own, and
the spawning step waits for its terminal rather than driving it across parks,
so no decision can be routed back down. A body nested inside the child
inherits that answer, so this holds at any depth rather than only for a gate
written directly in the child.

An untimed wait beneath that boundary fails the step that asked for it, naming
the gate, rather than waiting on a signal nobody can send. A gate carrying a
`timeout` is unaffected, because its own timer resolves it without anything
upstream.

Where the spawn step sits at a workflow root, an `onFailure` handler on it
absorbs this failure like any other child failure, so a workflow that routes
around child failures swallows it; leave that step unrouted if you want to see
it. A spawn step in a `loop` body cannot carry `onFailure` at all -- routing is
honored only at a workflow root, and a loop body is not one -- so the failure
surfaces there regardless.

To hold a human gate, keep the `awaitSignal` in a run the control plane can
address: the deployment's own run, whether directly or in a `loop` body or
`onTrigger` section body of it, whose container relays the decision down. What
no container can do is reach across the `childWorkflow` boundary.

Of those homes the `onTrigger` section body carries a caveat the relay does not
cause. A section body's run id omits the enclosing run, so a second top-level
run of the same deployment finds the first run's body log already terminal and
short-circuits it: the body never executes, nobody is asked, and the run still
reports a clean terminal status (tracked as INTR-552). A gate a repeating
deployment depends on belongs in the deployment's own run or in a `loop` body,
whose iteration run ids re-root per run.

A loop body may contain a nested `loop`. An inner loop resolves its body ref
from the same top-level bodies map (a loop iteration inherits its parent's env),
its body-child run ids carry the container run id so iterations stay unique
across nesting, and its own signal park relays up through the outer container
exactly as a leaf gate relays up through its container -- one layer at a time
until it reaches the run whose channel has a real upstream. On crash the resume
composes per level: whichever levels durably parked re-establish their relay,
and a level whose container relay had not yet flushed re-drives it fresh from the
body's own parked gate when its `runLoop` re-runs during re-adoption. Nesting
depth is bounded at definition time (a small static limit), since deep nesting
is authored, not dynamic. One topology is unsupported and fails loud on resume:
two sibling loops in the same body both parked on author signals at once (the
container relays one name at a time).

`sleep` stays banned: a parked sleep leaves the step `awaiting-timer`, and every
container park -- including a nested loop's -- relays a signal park, not a timer
park, so a loop body still has no timer-park resume path (separate work,
INTR-485). `onTrigger` stays banned too -- a run carries a single subscription
layer.

Practical guidance: use a loop to repeat a self-contained unit -- which may park
on an `awaitSignal`, spawn a `childWorkflow`, or run a nested `loop` -- until a
pure `while`/`carry` says stop. Keep the gate in the body itself: an untimed
`awaitSignal` inside a `childWorkflow` the body spawns, at any depth, fails the
step rather than waiting. Model a `sleep` delay at a top-level step or an
onTrigger section, not in a loop body. Keep a loop body's action idempotent
where practical, since a mid-invocation crash fails the run and the effect is
never re-run.

These primitives are composed end to end by the interchange-demo dispatch
orchestrator -- an outer per-level `loop` wrapping a Phase-5 verification `loop`
whose per-task fix `loop` nests a retry `loop` and parks on an `awaitSignal`
operator escalation, with crash-resume exactly-once effects and the demo's
resume cases handled through engine resume. It is authored on the engine, and
its behavior asserted, in `tests/workflow/dispatch-orchestrator.test.ts` (a
stubbed-invoker routing/resume test) and
`tests/workflow-deploy/dispatch-orchestrator-real-agents.test.ts` (the same
composition driven by real agents through the production step-invoker).
