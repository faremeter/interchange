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
  "Records where the grant came from: `system` (built-in), `role` (granted via a role), `creator` (from the workflow definition author), or `invoker` (delegated by the workflow deployment's deployer, its invoker of record). Origin is provenance only; it does not affect evaluation precedence.";

const conditionsDescription =
  "Optional map of named conditions that must all pass for the grant to apply, evaluated against a condition registry at authorization time. A check with no request to evaluate conditions against, such as delegation at launch, treats a conditioned `deny` or `ask` as applying and a conditioned `allow` as not applying.";

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
  "consumer?": type("string > 0").describe(
    "The workflow consumer to evaluate as, such as `action:<handler>` or `tool:<package>`. A grant scoped to a consumer by a `{ tool }` condition matches only that consumer; without it, such a grant matches nothing.",
  ),
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

/**
 * The resource a grant requirement uses to name the run it is materialized
 * for. Launch replaces it with that run's own `workflow-run:<runId>`, the
 * top-level run, which child bodies share. It lets a run hold a grant on
 * itself without a `workflow-run:*` grant on every run in the tenant. The
 * delegator still needs authority over the concrete run id.
 */
export const SELF_RUN_RESOURCE = "workflow-run:self";

export const GrantRequirement = type({
  resource: "string",
  action: "string",
  "effect?": Effect.describe(
    "Effect to assign the materialized grant: `allow`, `deny`, or `ask`. Defaults to `allow` when omitted.",
  ),
  source: GrantSourceType.describe(
    "Whose authority the grant is resolved against at launch: `creator` (the definition author) or `invoker` (the workflow deployment's deployer, its invoker of record) -- satisfied only if that party actually holds the requested capability. Tenant-owned credential use is not a grant requirement: it is authorized by ownership at resolution and its consumer-scoping grant is stamped directly (see CREDENTIALS.md).",
  ),
  "conditions?": "Record<string, unknown> | null",
});
export type GrantRequirement = typeof GrantRequirement.infer;

/**
 * Whether a requirement's resource uses `self` anywhere but the whole
 * `SELF_RUN_RESOURCE`. Launch binds only that exact resource, so `self`
 * elsewhere would be stored literally and match nothing. The comparison
 * ignores case and surrounding whitespace, so a near miss is caught too.
 */
export function misplacesSelfRunResource(resource: string): boolean {
  if (resource === SELF_RUN_RESOURCE) return false;
  return resource
    .split(":")
    .some((part) => part.trim().toLowerCase() === "self");
}

export const ApprovalItem = type("string").or(GrantRequirement);

/**
 * One thing an operator approved for a workflow deployment. The approved
 * surface has two kinds of entry and they live on different axes, so the
 * vocabulary carries both rather than flattening one into the other:
 *
 *   - A grant-shape string (`tool:<name>`, `effect:<cap>`,
 *     `inference.source:<provider>:<model>`, ...) -- what the deploy-time
 *     capability walk surfaces per step.
 *   - A `GrantRequirement` -- what the definition declares, and what the run
 *     path materializes into real grant rows on the run principal. It is not
 *     expressible as a grant string: `source` names WHOSE authority is
 *     delegated, which is not a resource/action fact, and `conditions` is an
 *     open record.
 *
 * Keeping both in one vocabulary keeps "what the operator approved" a single
 * record with a single gate, and lets the gate compare a declared requirement
 * to an approved one structurally rather than through a lossy string form.
 */
export type ApprovalItem = typeof ApprovalItem.infer;
