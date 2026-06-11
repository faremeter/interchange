# @intx/workflow-deploy

Deploy-time validation, capability walk, operator-approval gating,
and the workflow deploy orchestrator.

This package is the deploy-side counterpart to `@intx/workflow`. It
takes a `WorkflowDefinition`, computes the per-step grant
declarations the workflow will require, gates them against an
operator-supplied `ApprovalSet`, and routes the deployment along
one of two paths.

The orchestrator branches on the trivial-vs-multi-step dichotomy:

- **Trivial workflow** (single step, caller supplies
  `trivialBindings`): preserve the caller's existing agent address
  and write the underlying agent's deploy tree onto the existing
  `agent-state` repo via the legacy path. The on-disk and on-wire
  surfaces stay bit-identical to what the pre-collapse
  `SessionService.launchSession` produced.
- **Multi-step workflow**: derive per-step agent addresses as
  `ins_<deploymentId>-<stepId>@<deploymentDomain>`, instantiate one
  `agent-state` repo per step, and write per-step deploy trees.

The asymmetry is intentional: it is the agent-deploy uniformity
claim's escape hatch. Without it, the existing
`tests/hub-agent/deploy-flow.test.ts` could not pass with zero
source changes after the collapse.

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
