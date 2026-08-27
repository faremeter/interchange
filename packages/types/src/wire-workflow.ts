// Wire contracts for a workflow definition projected onto an `agent.deploy`
// frame: the per-step schema and the projection shapes the sidecar deploy
// router and the workflow-process child validate. Extracted from `sidecar.ts`
// so both files stay focused; `sidecar.ts` re-exports the public names, so
// `@intx/types/sidecar` consumers are unaffected.

import { type } from "arktype";

import { CredentialBinding } from "./credentials";
import { InferenceSource } from "./runtime";

/**
 * Fields every wire step carries regardless of `kind`. All other keys pass
 * through unmodified (arktype's default), including the nested `agent`, inner
 * `step`, `body`, `on`, and selector fields -- typed nowhere here on purpose,
 * because two producers feed this schema: the live-deploy passthrough ships a
 * step whose `agent.toolFactories` are functions that JSON-encode to `null`,
 * while the live->inert projector in `@intx/workflow-deploy` ships a reified
 * plain-data agent. Both must validate; reifying the grant surface into plain
 * data is the projector's job, not this envelope's.
 */
const commonStepFields = {
  "id?": "string",
  "after?": "string[]",
} as const;

/**
 * A wire step: its `kind` must be one of the ten known primitives, plus the
 * common `id`/`after` fields; all other keys pass through unmodified. Exported
 * so the live->inert projector's producer and its mutation-test suite validate
 * a single step against the same schema the deploy frame applies to every step.
 *
 * The load-bearing check here is the KIND discriminant -- a step with no `kind`
 * or a `kind` outside the set is rejected at this boundary rather than carried
 * through opaque, and the membership is what makes a step's canonical JSON
 * deterministic across the child->hub boundary. Per-variant field validation is
 * deliberately NOT done here (deeper authoring-time validation -- required
 * fields, selector resolvability, DAG shape -- lives on `@intx/workflow`), so
 * the ten variants collapse to one schema over the kind enum rather than ten
 * near-identical arms whose fields were all optional passthrough anyway.
 */
export const WorkflowStep = type({
  kind: "'step' | 'map' | 'gate' | 'awaitSignal' | 'sleep' | 'childWorkflow' | 'escalation' | 'action' | 'loop' | 'onTrigger'",
  ...commonStepFields,
});
export type WorkflowStep = typeof WorkflowStep.infer;

/**
 * The `steps` record on a wire projection: every value must validate
 * against the closed `WorkflowStep` union. The runtime constraint runs
 * through a `.narrow` over a `Record<string, unknown>` rather than a typed
 * `{ "[string]": WorkflowStep }` on purpose: the inferred type stays
 * `Record<string, unknown>` so the existing live-deploy producer
 * (`toWireWorkflowDefinition`, which hands a `Record<string, unknown>`
 * steps map to `sendAgentDeploy`) still typechecks, while the runtime
 * validation is fully closed over the primitive-kind set.
 */
const WorkflowSteps = type({ "[string]": "unknown" }).narrow((steps, ctx) => {
  for (const [stepId, step] of Object.entries(steps)) {
    const parsed = WorkflowStep(step);
    if (parsed instanceof type.errors) {
      return ctx.mustBe(
        `a record whose every step matches a known workflow primitive ` +
          `variant; step ${JSON.stringify(stepId)} did not (${parsed.summary})`,
      );
    }
  }
  return true;
});

/**
 * Workflow projection carried on an `agent.deploy` frame. Its presence
 * at the deploy router routes the frame to the workflow deploy path --
 * single- or multi-step, both of which spawn the workflow-process child
 * -- as opposed to a per-step provision frame.
 *
 * `definition` is the wire projection of `WorkflowDefinition` from
 * `@intx/workflow`. The arktype validator enforces the structural
 * envelope the workflow-process child re-parses on the sidecar after
 * materialization (`packages/hub-sessions/src/workflow-kind.ts`'s
 * `workflowDefinitionEnvelopeSchema`): `id`, `triggers`, `steps`,
 * `stepOrder`, optional `state`. The wire validator MUST require every
 * field the envelope requires — this projection is the approved surface
 * the source-ref child re-verifies its closure-evaluated definition
 * against, and the child rejects a tree missing any envelope-required
 * field. Deeper validation of authoring-time primitive shape lives on the
 * workflow definition surface in `@intx/workflow`, not on the wire.
 *
 * `sources` pins an ordered, non-empty inference-source list per step in
 * `definition.stepOrder` so the workflow-process child can resolve inference
 * at step invocation without a round trip to the hub. The list is the step's
 * failover chain: element 0 is the active source (its id is the step's
 * `defaultSource`), and the reactor fails over forward through the tail on a
 * transient inference error. A workflow step pins a single-element list (no
 * per-step failover); a single-agent instance pins the instance's full
 * ordered source chain. Every `stepOrder` entry must have a matching
 * `sources` entry; the validator rejects frames that violate this at the
 * boundary.
 */
export const WorkflowProjectionDefinition = type({
  id: "string > 0",
  triggers: "unknown[]",
  stepOrder: "string[]",
  steps: WorkflowSteps,
  "state?": "Record<string, unknown>",
  // The definition's credential bindings, projected verbatim by the
  // live->inert projector (`projectDefinition`). This MUST stay in sync with
  // that projector: because of the `"+": "delete"` below, a binding the
  // projector emits but this schema omits would be silently stripped at the
  // wire boundary, desyncing the hub-resolved bindings from the projection
  // the sidecar validates and re-verifies. Bindings are the operator-approved
  // credential request surface (no secret material), so they belong in the
  // hashed projection.
  "credentialBindings?": CredentialBinding.array(),
  "+": "delete",
}).narrow((value, ctx) => {
  // Every `stepOrder` entry must name a defined step. A legitimately projected
  // definition always satisfies this (the authoring validator enforces it), so
  // this rejects only a projector-bypassing or tampered wire frame -- closing a
  // phantom-stepOrder entry at the trust boundary for every consumer, rather
  // than letting a downstream reader index `steps[missing]` as `undefined` and
  // silently take a default path.
  for (const stepId of value.stepOrder) {
    if (!Object.prototype.hasOwnProperty.call(value.steps, stepId)) {
      return ctx.mustBe(
        `a workflow projection whose stepOrder names only defined steps; ${JSON.stringify(stepId)} has no matching entry in steps`,
      );
    }
  }
  return true;
});
export type WorkflowProjectionDefinition =
  typeof WorkflowProjectionDefinition.infer;

/**
 * A workflow projection paired with its per-step inference-source pins and the
 * hub-approved wire hash, with the invariant that every `stepOrder` entry has a
 * `sources` failover chain. This is the shared base for BOTH the top-level
 * deploy frame (`AgentDeployWorkflow`, which intersects its extras onto this)
 * AND each extracted trigger body (onTrigger section or childWorkflow child)
 * under `referencedDefinitions` -- so the field set and the coverage narrow are
 * defined once and a body's sources cover the body's stepOrder just as the
 * top-level's cover the top-level's.
 */
export const WorkflowProjectionWithSources = type({
  definition: WorkflowProjectionDefinition,
  sources: { "[string]": InferenceSource.array().atLeastLength(1) },
  // The hub-approved wire hash of `definition`'s projection -- the freeze anchor
  // the hub gate wrote (`computeWireDefinitionHash`). The sidecar feeds it to
  // the child as the `DEFINITION_HASH` it re-verifies its own recompute
  // against, rather than trusting a sidecar-computed hash. At the top level it
  // pins the deployment's content handle; per body it pins the body's
  // projection, which is re-verified in-memory as part of the parent's
  // already-re-verified closure. Optional on the wire because the frame schema
  // does not force it; the production hub builder always stamps it.
  "approvedWireHash?": "string > 0",
}).narrow((value, ctx) => {
  for (const stepId of value.definition.stepOrder) {
    if (!Object.prototype.hasOwnProperty.call(value.sources, stepId)) {
      return ctx.mustBe(
        `a workflow projection whose sources cover every step in stepOrder; ${JSON.stringify(stepId)} is missing`,
      );
    }
  }
  return true;
});
export type WorkflowProjectionWithSources =
  typeof WorkflowProjectionWithSources.infer;
