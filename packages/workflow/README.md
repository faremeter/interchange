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
  workflow lives in.
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
`@intx/workflow-host`. For deploy-time validation, capability walk,
and the agent-deploy-trivial-workflow dichotomy, see
`@intx/workflow-deploy`.

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
pure `while`/`carry` says stop. Model a `sleep` delay at a top-level step or an
onTrigger section, not in a loop body. Keep a loop body's action idempotent
where practical, since a mid-invocation crash fails the run and the effect is
never re-run.
