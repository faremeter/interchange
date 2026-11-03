// The workflow-level authorize closure a run child gives its steps, and the
// grant evaluator it delegates to.
//
// The closure resolves a step's grants through a grants resolver, and the
// evaluator ranks those grants with `evaluateGrants`. Nothing here imports a
// Node API, so a host that cannot load the run child can still build the same
// pair.

import {
  evaluateGrants,
  RUN_CHILD_CONDITION_REGISTRY,
  type GrantRule,
} from "@intx/authz";
import type { AuthzCallResult } from "@intx/inference";
import { WireGrantRule } from "@intx/types/grant-wire";
import type { WorkflowAuthorizeFn } from "@intx/workflow";
import { baseStepId } from "@intx/workflow";
import { type } from "arktype";

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
  grants: readonly GrantRule[];
}) => Promise<AuthzCallResult>;

/**
 * The grants of `stepId` in `snapshot`. The snapshot is keyed per base step,
 * so a map iteration's scoped id `<base>[<index>]` resolves to its base entry
 * and every iteration shares the base step's grants; `baseStepId` is the
 * identity on an unscoped id. Throws the caller's `noEntry` message when the
 * snapshot has no entry for the step.
 */
function resolveSnapshotStepGrants(
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
 * Resolves the grants a step's authority is decided against. One resolver
 * serves both a run's authorize closure and its step credential context, so
 * the two read the same grants.
 */
export type StepGrantsResolver = (stepId: string) => readonly GrantRule[];

/**
 * A grants resolver over a mutable credentials snapshot reference. It reads
 * the reference on every call, so a swapped snapshot takes effect without a
 * rebuild. A call before any snapshot is active, or for a step the snapshot
 * has no entry for, is a programming error: it throws, with `label` naming
 * the caller.
 *
 * The snapshot types its grants as `unknown[]`, and a snapshot that crossed
 * JSON carries each `expiresAt` as a string, which `evaluateGrants` cannot
 * compare with its clock. The resolver parses the grants as wire grant rules,
 * which turns the string back into a `Date`, so `evaluateGrants` can skip an
 * expired row for every caller of this resolver. An entry that is not a
 * grant rule throws. The parse runs again only when the step's grants array
 * changes.
 */
export function createSnapshotGrantsResolver(
  ref: CredentialsSnapshotRef,
  label: string,
): StepGrantsResolver {
  const parsed = new WeakMap<readonly unknown[], readonly GrantRule[]>();
  return (stepId) => {
    const snapshot = ref.current;
    if (snapshot === null) {
      throw new Error(
        `${label}: no credentials snapshot is active for step ${stepId}; the host must set one before any step runs`,
      );
    }
    const raw = resolveSnapshotStepGrants(
      snapshot,
      stepId,
      (lookupStepId) =>
        `${label}: the credentials snapshot has no entry for step ${lookupStepId}${
          lookupStepId === stepId
            ? ""
            : ` (normalized from scoped invocation id ${stepId})`
        }`,
    );
    const cached = parsed.get(raw);
    if (cached !== undefined) return cached;
    const grants = WireGrantRule.array()(raw);
    if (grants instanceof type.errors) {
      throw new Error(
        `${label}: the grants of step ${stepId} are not grant rules: ${grants.summary}`,
      );
    }
    parsed.set(raw, grants);
    return grants;
  };
}

/**
 * Reads the current grants of a run as one array. The reader returns the same
 * array object until the run's grants change, so a caller can cache work
 * derived from it on the array's identity.
 */
export type RunGrantsReader = () => readonly unknown[];

/**
 * A grants reader over a run's mutable credentials snapshot reference: the
 * union of the grants of every step in the current snapshot, with one entry
 * per grant id. A row without a string id is kept once per object. The union
 * is computed again only when the snapshot object changes, which a
 * `grants-updated` or `authority.set` frame does by replacing it. A read
 * while no snapshot is active throws, with `label` naming the caller: the
 * run must not keep running on frozen grants after it loses its authority.
 */
export function createSnapshotGrantsReader(
  ref: CredentialsSnapshotRef,
  label: string,
): RunGrantsReader {
  let cached: { snapshot: CredentialsSnapshot; grants: unknown[] } | undefined;
  return () => {
    const snapshot = ref.current;
    if (snapshot === null) {
      throw new Error(
        `${label}: no credentials snapshot is active; the host must set one before any step runs`,
      );
    }
    if (cached?.snapshot !== snapshot) {
      cached = { snapshot, grants: uniqueGrants(snapshot) };
    }
    return cached.grants;
  };
}

function uniqueGrants(snapshot: CredentialsSnapshot): unknown[] {
  const seenIds = new Set<string>();
  const seenRows = new Set<unknown>();
  const grants: unknown[] = [];
  for (const step of snapshot.steps) {
    for (const grant of step.grants) {
      const id = grantId(grant);
      if (id === undefined) {
        if (seenRows.has(grant)) continue;
        seenRows.add(grant);
      } else {
        if (seenIds.has(id)) continue;
        seenIds.add(id);
      }
      grants.push(grant);
    }
  }
  return grants;
}

function grantId(grant: unknown): string | undefined {
  if (typeof grant !== "object" || grant === null || !("id" in grant)) {
    return undefined;
  }
  return typeof grant.id === "string" ? grant.id : undefined;
}

/**
 * Construct the workflow-level authorize closure. The closure evaluates a
 * `(resource, action)` request against the originating step's grants from
 * `resolveGrants`, through the grant evaluator.
 *
 * The workflow runtime supplies `stepId` from the `AuthorizeContext` it
 * threads through every step's invoker, so a call without a step id is a
 * programming error. The closure throws loudly rather than deciding.
 */
export function createCredentialsBackedAuthorize(
  resolveGrants: StepGrantsResolver,
  evaluate: GrantEvaluator,
): WorkflowAuthorizeFn {
  return async (resource, action, ctx) => {
    const stepId = ctx?.stepId;
    if (stepId === undefined) {
      throw new Error(
        "credentials-backed authorize: missing stepId in AuthorizeContext; the runtime body must thread it through every step invocation",
      );
    }
    const grants = resolveGrants(stepId);
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
 * the step's snapshot grants through `evaluateGrants`.
 *
 * A snapshot grant's conditions come from the run's declared grant
 * requirements, or are the `{ tool }` condition of a credential use row. Only
 * a `credential:` resource matches such a row, and the harness checks those
 * with its own registry. The child evaluates a time window against its own
 * clock, so
 * a time-window grant applies inside its window, as on the hub, and an
 * approval that turns a conditioned `ask` into `allow` keeps working. Any
 * other condition key needs request context the child does not have, so it
 * throws and the authorization fails closed.
 */
export function createGrantEvaluator(): GrantEvaluator {
  return async ({ resource, action, grants }) => {
    const result = await evaluateGrants([...grants], resource, action, {
      conditions: RUN_CHILD_CONDITION_REGISTRY,
    });
    return {
      effect: result.effect,
      matchingGrants: [],
      resolvedBy: null,
    };
  };
}
