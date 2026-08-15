// Fold a workflow's direct childWorkflow references into a single approved
// grant surface. A `childWorkflow` step names a separately-deployed workflow
// asset by `definitionRef`; the referenced child's needs-surface is invisible
// to the parent's approval and its runtime ceiling unless it is folded in here.
//
// The fold reads each direct child's already-stored `approved_grant_surface`
// and unions it. Because a deployed child's stored surface is itself the
// transitive fold of its own children, a parent unions only its DIRECT
// children -- grandchildren are already inside the child's surface, so there is
// no recursion and no cross-definition re-walk. This is the recurrence
//
//   pinned(W) = ownWalk(W) ∪ ⋃ pinned(directChild(W))
//
// computed at W's deploy; the caller supplies `ownWalk(W)` and unions it with
// the child contribution this module returns.
//
// A cross-workflow reference cycle (a → b → a) is structurally un-deployable:
// to deploy `a` its child `b` must already be approved with a stored surface,
// and vice versa, so neither can be first. That deploy-ordering requirement is
// the cycle guard -- an unresolved or unapproved child is rejected by
// `ChildWorkflowNotApprovedError`. A self-reference is rejected directly,
// before any blob read, so a stale prior-version self-fold can never happen.

import { and, eq } from "drizzle-orm";

import type { WorkflowDefinition } from "@intx/workflow/definition";
import type { ApprovedGrantSurface, GrantEffect } from "@intx/types";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import {
  readApprovedGrantSurface,
  resolveDefinitionForAsset,
  type DBExecutor,
} from "@intx/db";
import { asset } from "@intx/db/schema";

import type { AssetService } from "./asset-service";
import { hydrateDefinition } from "./workflow-kind";

/** A childWorkflow reference the fold discovered, labelled by its step path. */
export interface ChildWorkflowRef {
  /** The step id, or `parentStepId/bodyStepId` for a childWorkflow nested in an
   * onTrigger body -- carried only to make errors point at the right step. */
  readonly stepId: string;
  readonly definitionRef: string;
}

/** A childWorkflow whose `definitionRef` is the very asset being deployed. */
export class ChildWorkflowSelfReferenceError extends Error {
  readonly definitionRef: string;

  constructor(definitionRef: string) {
    super(
      `child workflow ${JSON.stringify(definitionRef)} references the ` +
        `workflow being deployed; a workflow cannot fold its own surface`,
    );
    this.name = "ChildWorkflowSelfReferenceError";
    this.definitionRef = definitionRef;
  }
}

/** A childWorkflow `definitionRef` names no workflow asset in the deploy
 * tenant. Absent and cross-tenant collapse into one error on purpose: a
 * distinct cross-tenant error would confirm the asset's existence to a tenant
 * that cannot see it. */
export class ChildWorkflowNotFoundError extends Error {
  readonly definitionRef: string;

  constructor(definitionRef: string) {
    super(
      `child workflow ${JSON.stringify(definitionRef)} is not a workflow ` +
        `asset in this tenant`,
    );
    this.name = "ChildWorkflowNotFoundError";
    this.definitionRef = definitionRef;
  }
}

/** A childWorkflow `definitionRef` names an asset that is not workflow-kind. */
export class ChildWorkflowKindError extends Error {
  readonly definitionRef: string;
  readonly kind: string;

  constructor(definitionRef: string, kind: string) {
    super(
      `child workflow ${JSON.stringify(definitionRef)} is a ` +
        `${JSON.stringify(kind)} asset, not a workflow`,
    );
    this.name = "ChildWorkflowKindError";
    this.definitionRef = definitionRef;
    this.kind = kind;
  }
}

/** A childWorkflow's referenced content resolves to no approved definition
 * carrying a grant surface. Covers a child that is not deployed, not yet
 * approved, or a participant in a reference cycle -- all of which mean the
 * parent has no surface to fold and must not deploy first. */
export class ChildWorkflowNotApprovedError extends Error {
  readonly definitionRef: string;

  constructor(definitionRef: string) {
    super(
      `child workflow ${JSON.stringify(definitionRef)} is not deployed and ` +
        `approved with a grant surface; deploy the child before the parent`,
    );
    this.name = "ChildWorkflowNotApprovedError";
    this.definitionRef = definitionRef;
  }
}

/**
 * Enumerate the childWorkflow references reachable from a definition's authored
 * form. A `childWorkflow` is legal at top level and one subscription layer
 * deep, inside an onTrigger section's authored `{ inline }` body (loops ban
 * childWorkflow; nested onTrigger is banned, so there is no deeper nesting).
 * The fold runs before the deploy step rewrites an inline body to `{ ref }`, so
 * a `{ ref }` body -- already its own asset with its own folded surface -- is
 * skipped here, mirroring `collectOnTriggerBodyGrants`.
 */
export function enumerateChildWorkflowRefs(
  definition: WorkflowDefinition,
): ChildWorkflowRef[] {
  const refs: ChildWorkflowRef[] = [];
  for (const stepId of definition.stepOrder) {
    const primitive = definition.steps[stepId];
    if (primitive === undefined) {
      throw new Error(
        `child-workflow fold: step ${stepId} listed in stepOrder is missing ` +
          `from steps`,
      );
    }
    if (primitive.kind === "childWorkflow") {
      refs.push({ stepId, definitionRef: primitive.definitionRef });
      continue;
    }
    if (primitive.kind === "onTrigger" && "inline" in primitive.body) {
      const body = primitive.body.inline;
      for (const bodyStepId of body.stepOrder) {
        const bodyPrimitive = body.steps[bodyStepId];
        if (bodyPrimitive === undefined) {
          throw new Error(
            `child-workflow fold: onTrigger body step ${bodyStepId} listed ` +
              `in stepOrder is missing from steps`,
          );
        }
        if (bodyPrimitive.kind === "childWorkflow") {
          refs.push({
            stepId: `${stepId}/${bodyStepId}`,
            definitionRef: bodyPrimitive.definitionRef,
          });
        }
      }
    }
  }
  return refs;
}

// Effect strength for the fold: the strongest effect wins, matching the
// documented grant precedence (`deny` over `ask` over `allow`). The capability
// walk mints only `ask`/`allow` today, so for currently-reachable inputs this
// reduces to the walk's ask-wins merge; carrying `deny` through correctly keeps
// the fold from ever weakening a stronger effect a future producer might stamp.
const EFFECT_STRENGTH: Record<GrantEffect, number> = {
  allow: 0,
  ask: 1,
  deny: 2,
};

function strongestEffect(a: GrantEffect, b: GrantEffect): GrantEffect {
  return EFFECT_STRENGTH[a] >= EFFECT_STRENGTH[b] ? a : b;
}

/**
 * Union two approved grant surfaces into a ceiling. Grants are set-unioned and
 * sorted for a deterministic stored value. When the same grant carries an
 * effect on both sides, the stronger effect wins (`deny` > `ask` > `allow`), so
 * folding can never silently downgrade an approval gate.
 */
export function mergeGrantSurfaces(
  base: ApprovedGrantSurface,
  incoming: ApprovedGrantSurface,
): ApprovedGrantSurface {
  const grants = new Set<string>(base.grants);
  for (const grant of incoming.grants) {
    grants.add(grant);
  }
  const grantEffects: Record<string, GrantEffect> = { ...base.grantEffects };
  for (const [grant, effect] of Object.entries(incoming.grantEffects)) {
    const existing = grantEffects[grant];
    grantEffects[grant] =
      existing === undefined ? effect : strongestEffect(existing, effect);
  }
  return { grants: [...grants].sort(), grantEffects };
}

/** The read-only capabilities the fold needs: an executor for the definition
 * store and the asset service for the child's `workflow.json`. */
export interface ChildWorkflowFoldDeps {
  readonly db: DBExecutor;
  readonly assetService: AssetService;
}

export interface ChildWorkflowFoldParams {
  /** The parent definition, in its authored form (inline onTrigger bodies not
   * yet extracted). */
  readonly definition: WorkflowDefinition;
  /** The asset id of the workflow being deployed, for the self-reference
   * check. */
  readonly deployingAssetId: string;
  /** The deploy tenant; every referenced child must belong to it. */
  readonly tenantId: string;
}

/**
 * Resolve and union the approved grant surfaces of a parent's direct
 * childWorkflow references. Returns the child contribution
 * (`⋃ pinned(directChild)`); the caller unions it with the parent's own walk
 * to form `pinned(parent)`. A parent with no childWorkflow steps yields an
 * empty surface.
 *
 * Each reference is validated fail-closed and in this order: a self-reference
 * is rejected first; then the asset is confirmed to exist in the deploy tenant
 * and be workflow-kind BEFORE its blob is read (so a cross-tenant asset is
 * never touched); then its current content resolves to an approved definition
 * whose stored surface is folded in. A child referenced by more than one step
 * is folded once.
 */
export async function resolveChildWorkflowSurface(
  params: ChildWorkflowFoldParams,
  deps: ChildWorkflowFoldDeps,
): Promise<ApprovedGrantSurface> {
  let folded: ApprovedGrantSurface = { grants: [], grantEffects: {} };
  const seen = new Set<string>();
  for (const { definitionRef } of enumerateChildWorkflowRefs(
    params.definition,
  )) {
    if (seen.has(definitionRef)) {
      continue;
    }
    seen.add(definitionRef);
    const surface = await resolveOneChildSurface(definitionRef, params, deps);
    folded = mergeGrantSurfaces(folded, surface);
  }
  return folded;
}

async function resolveOneChildSurface(
  definitionRef: string,
  params: ChildWorkflowFoldParams,
  deps: ChildWorkflowFoldDeps,
): Promise<ApprovedGrantSurface> {
  if (definitionRef === params.deployingAssetId) {
    throw new ChildWorkflowSelfReferenceError(definitionRef);
  }

  // Same-tenant + kind validation before any blob read: a cross-tenant or
  // absent asset is never read. The query is scoped to the deploy tenant, so a
  // cross-tenant asset is indistinguishable from a missing one here.
  const assetRow = await deps.db.query.asset.findFirst({
    where: and(
      eq(asset.id, definitionRef),
      eq(asset.tenantId, params.tenantId),
    ),
  });
  if (assetRow === undefined) {
    throw new ChildWorkflowNotFoundError(definitionRef);
  }
  if (assetRow.kind !== "workflow") {
    throw new ChildWorkflowKindError(definitionRef, assetRow.kind);
  }

  // Bind to the child's current content: hydrate its workflow.json, hash it,
  // and resolve the definition row that content keys. This pins the child's
  // surface at parent-deploy time; the runtime binds the child's content
  // afresh at spawn time, and on drift the child fails closed against this
  // pinned ceiling.
  //
  // workflow.json holds the inert wire projection, so its hash is the wire
  // hash directly: the stored `wire_hash` is `computeLiveDefinitionHash(live)`
  // = `computeWireDefinitionHash(projectLiveToInert(live))`, and the hydrated
  // definition IS that inert projection. Re-projecting it (as
  // `computeLiveDefinitionHash` would) throws, because an already-inert agent
  // step is no longer a live factory to project.
  const childDefinition = await hydrateDefinition(
    deps.assetService,
    definitionRef,
  );
  const wireHash = await computeWireDefinitionHash(childDefinition);
  const resolved = await resolveDefinitionForAsset(deps.db, {
    assetId: definitionRef,
    wireHash,
  });
  if (resolved === null) {
    throw new ChildWorkflowNotApprovedError(definitionRef);
  }
  const surface = await readApprovedGrantSurface(
    deps.db,
    resolved.definitionId,
    resolved.currentVersion,
  );
  if (surface === null) {
    throw new ChildWorkflowNotApprovedError(definitionRef);
  }
  return surface;
}
