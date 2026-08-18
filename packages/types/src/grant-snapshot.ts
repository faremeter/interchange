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
