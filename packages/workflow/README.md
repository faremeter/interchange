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
