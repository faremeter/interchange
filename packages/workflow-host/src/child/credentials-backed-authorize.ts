// The workflow-level authorize closure a run child gives its steps, and the
// grant evaluator it delegates to.
//
// The closure resolves a step's grants from the run's credentials snapshot by
// base step id, and the evaluator ranks those grants (plus any pinned-tool
// floor) with `evaluateGrants`. Nothing here imports a Node API, so a host
// that cannot load the run child can still build the same pair.

import { evaluateGrants, type GrantRule } from "@intx/authz";
import type { AuthzCallResult } from "@intx/inference";
import type { WorkflowAuthorizeFn } from "@intx/workflow";
import { baseStepId } from "@intx/workflow";

import type { CredentialsSnapshot } from "../supervisor/credentials";

/**
 * The active credentials snapshot, held through a mutable reference: the
 * authorize closure reads it on every call, so a `grants-updated` control
 * frame swaps the snapshot in place without the caller having to reconstruct
 * the closure.
 */
export type CredentialsSnapshotRef = {
  current: CredentialsSnapshot | null;
};

/**
 * Evaluates a `(resource, action)` request against one step's grants. The
 * slot exists so the host wires its own grant-rule semantics; tests inject a
 * spy that records inputs.
 */
export type GrantEvaluator = (input: {
  resource: string;
  action: string;
  stepId: string;
  attempt: number | undefined;
  runId: string | undefined;
  grants: readonly unknown[];
}) => Promise<AuthzCallResult>;

/**
 * The grants of `stepId` in `snapshot`. The snapshot is keyed per base step,
 * so a map iteration's scoped id `<base>[<index>]` resolves to its base entry
 * and every iteration shares the base step's grants; `baseStepId` is the
 * identity on an unscoped id. Throws the caller's `noEntry` message when the
 * snapshot has no entry for the step.
 */
export function resolveSnapshotStepGrants(
  snapshot: CredentialsSnapshot,
  stepId: string,
  noEntry: (lookupStepId: string) => string,
): readonly unknown[] {
  const lookupStepId = baseStepId(stepId);
  const entry = snapshot.steps.find((s) => s.stepId === lookupStepId);
  if (entry === undefined) {
    throw new Error(noEntry(lookupStepId));
  }
  return entry.grants;
}

/**
 * Construct the workflow-level authorize closure backed by a mutable
 * credentials snapshot reference. The closure evaluates a
 * `(resource, action)` request against the snapshot entry for the
 * originating step, looked up by base step id, through the grant evaluator.
 *
 * The workflow runtime supplies `stepId` from the `AuthorizeContext` it
 * threads through every step's invoker, so a call without a step id, or
 * before any snapshot is active, is a programming error. The closure throws
 * loudly rather than deciding.
 */
export function createCredentialsBackedAuthorize(
  ref: CredentialsSnapshotRef,
  evaluate: GrantEvaluator,
): WorkflowAuthorizeFn {
  return async (resource, action, ctx) => {
    const stepId = ctx?.stepId;
    if (stepId === undefined) {
      throw new Error(
        "credentials-backed authorize: missing stepId in AuthorizeContext; the runtime body must thread it through every step invocation",
      );
    }
    const snapshot = ref.current;
    if (snapshot === null) {
      throw new Error(
        "credentials-backed authorize: no credentials snapshot is active; the host must set one before any step runs",
      );
    }
    const grants = resolveSnapshotStepGrants(
      snapshot,
      stepId,
      (lookupStepId) =>
        `credentials-backed authorize: the credentials snapshot has no entry for step ${lookupStepId}${
          lookupStepId === stepId
            ? ""
            : ` (normalized from scoped invocation id ${stepId})`
        }`,
    );
    return evaluate({
      resource,
      action,
      stepId,
      attempt: ctx?.attempt,
      runId: ctx?.runId,
      grants,
    });
  };
}

/**
 * The grant evaluator a run child's authorize closure delegates to. It ranks
 * the step's snapshot grants together with the step's pinned-tool floor
 * (`floorFor`, keyed by base step id) through `evaluateGrants`.
 *
 * The floor supplies the `tool:<name>` authority a pinned tool never got from
 * the hub's capability walk. It is ADDITIVE: `evaluateGrants` ranks by
 * specificity then effect, so a declared `deny` (priority 2) still beats the
 * derived `ask`/`allow` and an explicit denial is honored -- the floor only
 * raises the minimum authority to the tool's static mark. A step with no floor
 * contributes no rows, which can only fail more closed. A source-ref step has
 * no floor: its `tool:<name>` grant is already in the snapshot.
 */
export function createGrantEvaluator(opts: {
  readonly floorFor: (baseStepId: string) => readonly GrantRule[];
}): GrantEvaluator {
  return async ({ resource, action, stepId, grants }) => {
    const floor = opts.floorFor(baseStepId(stepId));
    const result = await evaluateGrants(
      // The credentials snapshot types `grants` as `readonly unknown[]`.
      // This module casts at the boundary where `evaluateGrants` reads the
      // rows.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- credentialsSnapshot.steps[*].grants is typed unknown[]; evaluateGrants reads GrantRule
      [...(grants as readonly GrantRule[]), ...floor],
      resource,
      action,
    );
    return {
      effect: result.effect,
      matchingGrants: [],
      resolvedBy: null,
    };
  };
}
