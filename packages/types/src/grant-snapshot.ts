// Serializable projection of the deploy-time capability walk, so a run can
// materialize grants without re-reading and re-walking a `workflow.json`
// blob: the walk's `Map`s flatten to arrays and records, surviving a JSON
// round-trip. `perStep[i].grantEffects` covers TOOL grants only; other
// grants live in `grants` and carry no effect entry. `grantRequirements`
// is the full, unfiltered requirement list; consumers filter it by source.

import { type } from "arktype";

import { grantEffects, GrantRequirement } from "./grants";

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
