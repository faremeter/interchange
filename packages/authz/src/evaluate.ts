import type {
  AuthzResult,
  ConditionRegistry,
  Effect,
  GrantRule,
  GrantStore,
  MatchedGrant,
} from "./types";
import { matchPattern } from "./patterns";
import { grantSpecificity } from "./specificity";
import { evaluateConditions } from "./conditions";

export const EFFECT_PRIORITY: Record<Effect, number> = {
  allow: 0,
  ask: 1,
  deny: 2,
};

/**
 * How an evaluation treats a grant that carries conditions. A registry
 * evaluates each condition against the request. `"pessimistic"` is for a
 * check that has no request to evaluate a condition against, such as
 * delegation: a conditioned `deny` or `ask` applies as if its condition held,
 * and a conditioned `allow` does not. The result is then never more permissive
 * than any request could see, and a run that receives delegated authority
 * never holds more than its delegator holds without conditions.
 */
export type ConditionMode = ConditionRegistry | "pessimistic";

export type EvalOptions = {
  conditions: ConditionMode;
  principalId?: string;
  tenantId?: string;
  consumer?: string;
};

/**
 * What `authorize` takes beyond the query: the condition mode, and the
 * consumer a conditioned grant may be scoped to. The principal and tenant come
 * from the positional arguments that also select the grants.
 */
export type AuthorizeOptions = Pick<EvalOptions, "conditions" | "consumer">;

/**
 * Evaluate grants against a resource/action query.
 *
 * This is the core evaluation logic, separated from grant collection
 * so it can be tested with synthetic grant lists.
 *
 * `opts.conditions` decides how a grant with conditions takes part; see
 * `ConditionMode`. With a registry, an unknown condition key causes an error.
 */
export async function evaluateGrants(
  grants: GrantRule[],
  resource: string,
  action: string,
  opts: EvalOptions,
): Promise<AuthzResult> {
  const now = new Date();
  const mode = opts.conditions;
  const ctx = {
    now,
    resource,
    action,
    principalId: opts.principalId ?? "",
    tenantId: opts.tenantId ?? "",
    consumer: opts.consumer ?? "",
  };

  const matching: MatchedGrant[] = [];

  for (const g of grants) {
    if (g.expiresAt !== null && g.expiresAt < now) continue;
    if (!matchPattern(g.resource, resource)) continue;
    if (!matchPattern(g.action, action)) continue;

    if (g.conditions && Object.keys(g.conditions).length > 0) {
      if (mode === "pessimistic") {
        if (g.effect === "allow") continue;
      } else if (!(await evaluateConditions(g.conditions, ctx, mode))) {
        continue;
      }
    }

    matching.push({
      id: g.id,
      resource: g.resource,
      action: g.action,
      effect: g.effect,
      origin: g.origin,
      specificity: grantSpecificity(g.resource, g.action),
    });
  }

  if (matching.length === 0) {
    return { effect: null, matchingGrants: [], resolvedBy: null };
  }

  // Sort ascending by specificity, then by effect priority.
  // Last element wins -- which is the most specific, and at equal
  // specificity the strongest effect (deny > ask > allow).
  //
  // The `ask > allow` half of that ordering is a load-bearing security
  // invariant, not just a convention: a tool's static approval mark is
  // materialized as an `ask` floor grant on the run principal, and a
  // workflow that declares a competing `allow` grant for the same
  // `tool:<name>/invoke` resource lands at equal specificity. `ask`
  // outranking `allow` here is what stops a workflow from declaring its
  // way below a tool's approval gate. Do not reorder EFFECT_PRIORITY
  // without accounting for that floor.
  matching.sort((a, b) => {
    const specDiff = a.specificity - b.specificity;
    if (specDiff !== 0) return specDiff;
    return (EFFECT_PRIORITY[a.effect] ?? 0) - (EFFECT_PRIORITY[b.effect] ?? 0);
  });

  const resolvedBy = matching[matching.length - 1];
  if (!resolvedBy) {
    return { effect: null, matchingGrants: [], resolvedBy: null };
  }

  return {
    effect: resolvedBy.effect,
    matchingGrants: matching,
    resolvedBy,
  };
}

/**
 * Authorize a principal for a resource/action within a tenant.
 *
 * Collects all relevant grants via the provided store, matches them
 * against the requested resource and action, and returns the resolved
 * effect.
 *
 * Returns null when no grants match (fail-closed). The caller
 * interprets the result in context: HTTP routes return 403, the
 * agent runtime blocks the tool call.
 */
export async function authorize(
  store: GrantStore,
  principalId: string,
  tenantId: string,
  resource: string,
  action: string,
  opts: AuthorizeOptions,
): Promise<AuthzResult> {
  const grants = await store.collectGrants(principalId, tenantId);
  return evaluateGrants(grants, resource, action, {
    ...opts,
    principalId,
    tenantId,
  });
}
