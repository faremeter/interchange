import { type } from "arktype";

export const grantEffects = ["allow", "deny", "ask"] as const;
export type GrantEffect = (typeof grantEffects)[number];

export const grantOrigins = ["system", "role", "creator", "invoker"] as const;
export type GrantOrigin = (typeof grantOrigins)[number];

const Effect = type.enumerated(...grantEffects);
const Origin = type.enumerated(...grantOrigins);

const effectDescription =
  "Outcome when this grant is the one resolved for a request: `allow` permits the action, `deny` blocks it, `ask` requires interactive approval before proceeding. When several grants match, the most specific wins, and at equal specificity the strongest effect wins (`deny` over `ask` over `allow`).";

const originDescription =
  "Records where the grant came from: `system` (built-in), `role` (granted via a role), `creator` (from the agent definition author), or `invoker` (delegated by whoever launched the agent). Origin is provenance only; it does not affect evaluation precedence.";

const conditionsDescription =
  "Optional map of named conditions that must all pass for the grant to apply, evaluated against a condition registry at authorization time. A grant with conditions is skipped (fails closed) when no registry is available to evaluate them.";

const specificityDescription =
  "Computed match-strength score used to rank grants: the count of non-wildcard characters in the resource and action patterns, with exact (wildcard-free) patterns scored far above prefix globs. Higher wins; ties are broken by effect priority.";

export const CreateGrant = type({
  "roleId?": "string | null",
  "principalId?": "string | null",
  resource: "string",
  action: "string",
  effect: Effect.describe(effectDescription),
  "conditions?": type("Record<string, unknown> | null").describe(
    conditionsDescription,
  ),
  origin: Origin.describe(originDescription),
  "expiresAt?": "string | null",
});

export const UpdateGrant = type({
  "effect?": Effect.describe(effectDescription),
  "conditions?": type("Record<string, unknown> | null").describe(
    conditionsDescription,
  ),
  "expiresAt?": "string | null",
});

export const GrantResponse = type({
  id: "string",
  tenantId: "string",
  "roleId?": "string | null",
  "roleName?": "string | null",
  "principalId?": "string | null",
  "principalName?": "string | null",
  resource: "string",
  action: "string",
  effect: Effect.describe(effectDescription),
  "conditions?": type("Record<string, unknown> | null").describe(
    conditionsDescription,
  ),
  origin: Origin.describe(originDescription),
  "expiresAt?": "string | null",
  createdAt: "string",
  updatedAt: "string",
});

export const EvaluateRequest = type({
  resource: "string",
  action: "string",
});

export const MatchedGrant = type({
  id: "string",
  resource: "string",
  action: "string",
  effect: Effect.describe(effectDescription),
  origin: Origin.describe(originDescription),
  "specificity?": type("number").describe(specificityDescription),
});
export type MatchedGrant = typeof MatchedGrant.infer;

export const EvaluateResult = type({
  effect: Effect.describe(
    "The resolved outcome for the query: the effect of the winning grant, or `deny` when no grant matched (authorization fails closed).",
  ),
  matchingGrants: MatchedGrant.array().describe(
    "Every grant that matched the requested resource and action, including the one that won. Useful for debugging why a request was allowed, denied, or required approval.",
  ),
});
