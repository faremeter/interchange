// Arktype validators for GrantRule wire serialization.
//
// GrantRule.expiresAt is a Date | null at runtime, but JSON round-trips
// turn it into a string | null. This validator accepts either form and
// coerces strings back to Date instances, making it safe to use when
// deserializing persisted configs (e.g. agent.json on sidecar restart).

import { type } from "arktype";

import { grantEffects, grantSources } from "./grants";

const Effect = type.enumerated(...grantEffects);
const Source = type.enumerated(...grantSources);

const DateOrNull = type("Date | null").or(type("string.date.parse"));

export const WireGrantRule = type({
  id: "string",
  resource: "string",
  action: "string",
  effect: Effect,
  source: Source,
  conditions: "Record<string, unknown> | null",
  expiresAt: DateOrNull,
  roleId: "string | null",
  principalId: "string | null",
});

export type WireGrantRule = typeof WireGrantRule.infer;
