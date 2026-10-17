// Serializable projection of the deploy-time capability walk.
//
// The capability walk produces per-step grant declarations keyed by two
// `Map`s (grant strings plus a tool-grant-to-effect map) alongside the
// definition's grant requirements. Persisting that walk so a run can
// materialize grants without re-reading and re-walking a `workflow.json`
// blob needs a plain-data shape: the `Map`s flatten to arrays and records
// so the whole thing survives a JSON round-trip.
//
// `perStep[i].grantEffects` covers TOOL grants only, mirroring the walk's
// `GrantDeclarations.grantEffects`; director/capability/inference.source/
// mail.* grants live in `grants` and carry no effect entry.
//
// `grantRequirements` is the full, unfiltered requirement list (both
// creator- and invoker-sourced). Consumers filter it by source themselves;
// the snapshot does not filter here.

import { type } from "arktype";

import { grantEffects, GrantRequirement, type GrantEffect } from "./grants";

const Effect = type.enumerated(...grantEffects);

const GrantWalkStepSnapshot = type({
  stepId: "string",
  grants: "string[]",
  grantEffects: {
    "[string]": Effect,
  },
});

export const GrantWalkSnapshot = type({
  perStep: GrantWalkStepSnapshot.array(),
  grantRequirements: GrantRequirement.array(),
});

export type GrantWalkSnapshot = typeof GrantWalkSnapshot.infer;

/**
 * Serialize the un-flattened capability walk into a plain-data
 * `GrantWalkSnapshot`: the per-step grant declarations (each step's grant
 * strings plus its tool-grant `grantEffects` map, converted from the
 * walk's `Map` to a plain object) and the definition's full, unfiltered
 * `grantRequirements`. This preserves the per-step grouping and the effect
 * data a flattened grant set discards. A definition that declares no
 * requirements snapshots an empty list.
 */
export function serializeGrantWalk(
  walk: {
    readonly perStep: ReadonlyMap<
      string,
      {
        readonly grants: readonly string[];
        readonly grantEffects: ReadonlyMap<string, GrantEffect>;
      }
    >;
  },
  grantRequirements: readonly GrantRequirement[] | undefined,
): GrantWalkSnapshot {
  const perStep = [...walk.perStep].map(([stepId, declarations]) => ({
    stepId,
    grants: [...declarations.grants],
    grantEffects: Object.fromEntries(declarations.grantEffects),
  }));
  return {
    perStep,
    grantRequirements: [...(grantRequirements ?? [])],
  };
}
