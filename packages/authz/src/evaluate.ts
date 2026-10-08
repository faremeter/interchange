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
  consumer?: string;
};

/**
 * Evaluate grants against a resource/action query.
 *
 * Conditioned grants need a registry: unknown condition keys error,
 * and without a registry the grants are skipped (fail-closed).
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
    consumer: opts?.consumer ?? "",
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
      origin: g.origin,
      specificity: grantSpecificity(g.resource, g.action),
    });
  }

  if (matching.length === 0) {
    return { effect: null, matchingGrants: [], resolvedBy: null };
  }

  // Sort ascending by specificity, then effect priority; last wins
  // (deny > ask > allow at equal specificity). The `ask > allow` half is
  // a load-bearing security invariant: a tool's static approval mark is
  // an `ask` floor grant on the run principal, and a competing `allow`
  // at equal specificity must not slide a workflow below that gate.
  // Do not reorder EFFECT_PRIORITY without accounting for the floor.
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
 * Collect grants for the principal/tenant from the store and evaluate
 * them against the resource/action. Null when nothing matches
 * (fail-closed); callers interpret the result (HTTP 403, blocked call).
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
