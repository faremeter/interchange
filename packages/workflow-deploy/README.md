# @intx/workflow-deploy

Deploy-time validation, capability walk, operator-approval gating,
address derivation, and per-step source pinning for the code-sourced
deploy.

This package is the deploy-side counterpart to `@intx/workflow`. It
takes a `WorkflowDefinition`, computes the per-step grant declarations
the workflow will require, gates them against an operator-supplied
`ApprovalSet`, and derives the deployment addresses the run occupies.

Address derivation is a pure function of `(runId, stepId, domain)`:

- **Single-step workflow**: the lone step has no distinct address --
  it IS the deployment head (`deriveRunAddress`, `<runId>@<domain>`).
- **Multi-step workflow**: each step derives a per-step run address of
  the form `<runId>-<stepId>@<domain>` (`deriveStepAddress`).

`resolveStepAddress` owns the head/step collapse decision. Because the
derivation carries no per-deploy state, the supervisor reconstructs the
same addresses at spawn time from the host-sourced step count alone.

Public surface:

- `walkCapabilities(workflow, registry, pluginDefs)` — the pure
  capability walk; reused to populate per-step capability declarations
  and as the input to the approval gate.
- `createApprovalSetGate(approvals)` / `createApprovalSourceGate(source)`
  — operator-approval gating against a flat `ApprovalSet` or an async
  source. An `ApprovalSet` holds typed approval items: the grant-shape
  strings the walk surfaces, and the `GrantRequirement` records a
  definition declares.
- `isApprovedGrantRequirement(approvals, requirement)` — whether the
  operator approved a declared requirement, compared as a whole record
  (`source`, `resource`, `action`, `effect`, `conditions`). A requirement
  is multi-axis and has no grant-string form, so the comparison is
  structural.
- `pickStepInferenceSource(...)` / `pinInertStepSources(...)` /
  `buildInertProjectionStepSources(...)` / `buildInertBodyStepSources(...)`
  — resolve each step's inference source against the operator-approved
  grant set, so an unapproved source fails the deploy closed.
  `pinInertStepSources` owns the flat-map collision rule and is
  parameterized by a per-step leaf resolver; it traverses through
  `walkStepTree` from `@intx/workflow` under `LOOP_BODY_DESCENT`, the
  descent that stays inside one flat step-id namespace.
  `buildInertBodyStepSources` pins a lifted body: an
  agent-bearing step resolves through the gate, while a step that cannot
  invoke inference takes the deploy's default source as an inert
  placeholder.
- `collectAgentBearingStepIds(...)` — the ids of the steps that can actually
  invoke inference (`agent`, or `map` over one). Every other primitive is
  pinned a source to satisfy the wire requirement that each step carry one but
  never issues a request through it, so a consumer deciding what a step is
  entitled to — credential delivery, notably — asks this rather than reading
  the pinned map.
- `enumerateInertBodies(...)` — lift each inline trigger body (onTrigger
  section or childWorkflow child), transitively, out of a frozen inert
  projection so the hub can stage it and pin its per-step sources. The
  enumeration is purely structural; each body step's `(provider, model)`
  preference is read at pin time by `buildInertBodyStepSources`.
- `inertNestedBodies(step, descent)` — the nested body projections one step of
  a frozen inert projection carries, filtered by a `StepWalkDescent`. This is
  the inert counterpart of `@intx/workflow`'s live `nestedWorkflowBodies`, so
  the inert representation states its descent rule in one place too. Pair it
  with `walkStepTree` to walk an inert projection under any descent;
  `inertFlatNamespaceStepIds` is that pairing at `LOOP_BODY_DESCENT`.
- `deriveRunAddress` / `deriveStepAddress` / `resolveStepAddress` /
  `deriveRunAgentId` / `deriveStepAgentId` / `deriveWorkflowRunRepoId`
  — the pure address and id derivation helpers.
- `extractFoldedBody(definition)` — read the launch-relevant fields back
  out of a folded single-step definition.

The capability walk emits the v1 grant-shape vocabulary: `tool:`,
`director:`, `capability:`, `inference.source:`, `mail.address:`,
`mail.send:`. The shapes are deliberately uniform with what the
existing agent-deploy already enforces implicitly — the parity test
in this package's test suite is the structural-identity check that
backs the capability-surface uniformity claim.
