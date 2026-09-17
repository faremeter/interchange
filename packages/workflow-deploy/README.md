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
- `createApprovalSet(grants, requirements)` — the edge of the approval
  vocabulary. An `ApprovalSet` is one approved surface behind one operator
  decision, carrying the two kinds of approved item in two fields:
  `grants`, a `Set` of the grant-shape strings the walk surfaces, and
  `requirements`, the `GrantRequirement` records a definition declares.
  The fields are separate because the two membership tests are different
  operations — a string is found by `Set.has`, a record only by structural
  comparison — so each kind sits beside the test that can answer for it.
  Every requirement is validated here, which is what lets the comparison
  below use `node:util`'s `isDeepStrictEqual` safely: that predicate treats
  a present-but-undefined optional key as different from an absent one, and
  `GrantRequirement` admits an object or `null` for `conditions`, never
  `undefined`, so such a record is rejected loudly at construction instead
  of silently failing to match at the gate.
- `approvalSetFromItems(items)` / `approvalItemsFromSet(approvals)` — the
  boundary between the in-memory `ApprovalSet` and the flat `ApprovalItem`
  list a freeze persists (`FrozenApprovalBundle.approvedGrants`). The
  persisted form stays flat and the in-memory form does not, because the two
  owe different things: a stored row owes compatibility to rows already
  written, and a flat array is append-friendly and order-independent, while
  the code reading it owes itself a shape where each kind sits beside its own
  membership test. `approvalSetFromItems` partitions by kind and routes the
  requirement half through `createApprovalSet`, so a rehydrated record gets
  the same parse a freshly-built one does.
- `createApprovalSetGate(approvals)` / `createApprovalSourceGate(source)`
  — operator-approval gating against an `ApprovalSet` or an async
  source. Both gates check only `grants`, because the walk never surfaces
  a requirement. The requirement half is gated by
  `gateAndFreezeProbeResult` in `@intx/hub-sessions` — the code-sourced
  path is where a declared requirement arrives, outside the wire-hash
  preimage the walk covers.
- `isApprovedGrantRequirement(approvals, requirement)` — whether the
  operator approved a declared requirement, compared as a whole record
  (`source`, `resource`, `action`, `effect`, `conditions`) against
  `approvals.requirements`. A requirement is multi-axis and has no
  grant-string form, so the comparison is structural.
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
  It dispatches through a table keyed by `Primitive["kind"]` — the same union
  the live reader's `never` assignment guards — so a body-bearing primitive
  added to one reader and forgotten in the other is a compile error rather
  than a container that silently reads as a leaf. A step whose kind is outside
  that closed set throws, as does a container kind carrying neither an inline
  body nor a `{ ref }`; a `{ ref }` body yields nothing, because the
  referenced asset is deployed and walked on its own.
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
