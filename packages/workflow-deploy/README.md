# @intx/workflow-deploy

Deploy-time validation, capability walk, operator-approval gating,
and the workflow deploy orchestrator.

This package is the deploy-side counterpart to `@intx/workflow`. It
takes a `WorkflowDefinition`, computes the per-step grant
declarations the workflow will require, gates them against an
operator-supplied `ApprovalSet`, and routes the deployment by step
count:

- **Single-step workflow**: the lone step has no distinct address --
  it IS the deployment head. Deploy once at the head
  (`ins_<deploymentId>@<deploymentDomain>`) through the single-step
  hand-off, staging the head's deploy tree and firing the
  `agent.deploy` frame in one call.
- **Multi-step workflow**: derive per-step agent addresses as
  `ins_<deploymentId>-<stepId>@<deploymentDomain>`, instantiate one
  `agent-state` repo per step, and write per-step deploy trees.

Public surface:

- `createWorkflowDeployOrchestrator(opts)` — the orchestrator.
- `walkCapabilities(workflow)` — the pure capability walk; reused
  to populate per-step `capability-declarations.json` and as the
  input to the approval gate.
- `createApprovalSetGate(approvals)` / `createApprovalSourceGate(source)`
  — operator-approval gating against a flat `ApprovalSet` or an
  async source.

The capability walk emits the v1 grant-shape vocabulary: `tool:`,
`director:`, `capability:`, `inference.source:`, `mail.address:`,
`mail.send:`. The shapes are deliberately uniform with what the
existing agent-deploy already enforces implicitly — the parity test
in this package's test suite is the structural-identity check that
backs the capability-surface uniformity claim.
