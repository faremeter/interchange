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

const EFFECT_PRIORITY: Record<Effect, number> = {
  allow: 0,
  ask: 1,
  deny: 2,
};

export type EvalOptions = {
  registry?: ConditionRegistry;
  principalId?: string;
  tenantId?: string;
};

/**
 * Evaluate grants against a resource/action query.
 *
 * This is the core evaluation logic, separated from grant collection
 * so it can be tested with synthetic grant lists.
 *
 * When a condition registry is provided, grants with non-null
 * conditions are evaluated against it. Unknown condition keys
 * cause an error. When no registry is provided, grants with
 * non-null conditions are skipped (fail-closed).
 */
export async function evaluateGrants(
  grants: GrantRule[],
  resource: string,
  action: string,
  opts?: EvalOptions,
): Promise<AuthzResult> {
  const now = new Date();
  const registry = opts?.registry;
  const ctx = {
    now,
    resource,
    action,
    principalId: opts?.principalId ?? "",
    tenantId: opts?.tenantId ?? "",
  };

  const matching: MatchedGrant[] = [];

  for (const g of grants) {
    if (g.expiresAt !== null && g.expiresAt < now) continue;
    if (!matchPattern(g.resource, resource)) continue;
    if (!matchPattern(g.action, action)) continue;

    if (g.conditions && Object.keys(g.conditions).length > 0) {
      if (!registry) continue;
      if (!(await evaluateConditions(g.conditions, ctx, registry))) continue;
    }

    matching.push({
      id: g.id,
      resource: g.resource,
      action: g.action,
      effect: g.effect,
      source: g.source,
      specificity: grantSpecificity(g.resource, g.action),
    });
  }

  if (matching.length === 0) {
    return { effect: null, matchingGrants: [], resolvedBy: null };
  }

  // Sort ascending by specificity, then by effect priority.
  // Last element wins -- which is the most specific, and at equal
  // specificity the strongest effect (deny > ask > allow).
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
  registry?: ConditionRegistry,
): Promise<AuthzResult> {
  const grants = await store.collectGrants(principalId, tenantId);
  const opts: EvalOptions = { principalId, tenantId };
  if (registry) opts.registry = registry;
  return evaluateGrants(grants, resource, action, opts);
}
