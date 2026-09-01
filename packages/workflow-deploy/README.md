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
  source.
- `pickStepInferenceSource(...)` / `pinInertStepSources(...)` /
  `buildInertProjectionStepSources(...)` — resolve each step's inference
  source against the operator-approved grant set, so an unapproved source
  fails the deploy closed. `pinInertStepSources` is the shared walk (step
  order, loop-body recursion, flat-map collision rule) parameterized by a
  per-step leaf resolver.
- `enumerateInertBodies(...)` — lift each inline trigger body (onTrigger section or childWorkflow child), transitively,
  out of a frozen inert projection and surface its declared
  `(provider, model)` preference for per-body source pinning.
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
