import { type } from "arktype";

export const grantEffects = ["allow", "deny", "ask"] as const;
export type GrantEffect = (typeof grantEffects)[number];

export const grantOrigins = ["system", "role", "creator", "invoker"] as const;
export type GrantOrigin = (typeof grantOrigins)[number];

export const grantRequirementSources = ["creator", "invoker"] as const;
export type GrantRequirementSource = (typeof grantRequirementSources)[number];

const Effect = type.enumerated(...grantEffects);
const Origin = type.enumerated(...grantOrigins);
const GrantSourceType = type.enumerated(...grantRequirementSources);

const effectDescription =
  "Outcome when this grant is the one resolved for a request: `allow` permits the action, `deny` blocks it, `ask` requires interactive approval before proceeding. When several grants match, the most specific wins, and at equal specificity the strongest effect wins (`deny` over `ask` over `allow`).";

const originDescription =
  "Records where the grant came from: `system` (built-in), `role` (granted via a role), `creator` (from the workflow definition author), or `invoker` (delegated by whoever launched the workflow run). Origin is provenance only; it does not affect evaluation precedence.";

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
}).narrow((g, ctx) => {
  // A grant targets exactly one of a role or a principal -- the same invariant
  // the `grant_target_exactly_one` DB CHECK enforces. Rejecting both/neither
  // here surfaces a malformed request as a 400 rather than a database 500.
  const targets = (g.roleId != null ? 1 : 0) + (g.principalId != null ? 1 : 0);
  if (targets !== 1) {
    return ctx.mustBe(
      "a grant with exactly one target: set roleId or principalId, not both and not neither",
    );
  }
  return true;
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

export const GrantRequirement = type({
  resource: "string",
  action: "string",
  "effect?": Effect.describe(
    "Effect to assign the materialized grant: `allow`, `deny`, or `ask`. Defaults to `allow` when omitted.",
  ),
  source: GrantSourceType.describe(
    "Whose authority the grant is resolved against at launch: `creator` (the definition author) or `invoker` (whoever launched the workflow run) -- satisfied only if that party actually holds the requested capability. Tenant-owned credential use is not a grant requirement: it is authorized by ownership at resolution and its consumer-scoping grant is stamped directly (see CREDENTIALS.md).",
  ),
  "conditions?": "Record<string, unknown> | null",
});
export type GrantRequirement = typeof GrantRequirement.infer;
