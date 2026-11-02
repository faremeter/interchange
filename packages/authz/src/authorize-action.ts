import { evaluateGrants } from "./evaluate";
import type { EvalOptions } from "./evaluate";
import type { GrantRule } from "./types";

/**
 * A fail-closed authorization decision. `ok: true` authorizes the action;
 * `ok: false` carries the structured reason it was withheld.
 */
export type AuthorizeDecision =
  | { ok: true }
  | { ok: false; reason: "deny" | "ask" | "no_matching_grant" };

/**
 * Decide whether an action on a resource is authorized against a set of
 * already-collected grants, failing closed.
 *
 * This owns the single fail-closed rule: any effect other than `allow` -- an
 * explicit `deny`, an `ask`, or no matching grant at all -- withholds
 * authorization. Callers branch on `ok` and never re-derive the rule from a
 * raw effect.
 *
 * Distinct from `authorize`: that one collects grants from a store and returns
 * the raw `AuthzResult` for an HTTP caller to interpret. This verb takes
 * pre-collected grants and returns the collapsed decision. `opts` is passed to
 * `evaluateGrants` verbatim: the caller chooses the condition mode, because
 * only the caller knows whether it has a request to evaluate conditions
 * against.
 */
export async function authorizeAction(
  grants: GrantRule[],
  resource: string,
  action: string,
  opts: EvalOptions,
): Promise<AuthorizeDecision> {
  const result = await evaluateGrants(grants, resource, action, opts);
  switch (result.effect) {
    case "allow":
      return { ok: true };
    case "deny":
      return { ok: false, reason: "deny" };
    case "ask":
      return { ok: false, reason: "ask" };
    case null:
      return { ok: false, reason: "no_matching_grant" };
    default: {
      const _exhaustive: never = result.effect;
      throw new Error(`unhandled effect: ${String(_exhaustive)}`);
    }
  }
}

/**
 * An `authorizeAction` decision that also says until when an `allow` lasts.
 * `expiresAt` is null when the grants allow the action with no end.
 */
export type AuthorizeUntilDecision =
  | { ok: true; expiresAt: Date | null }
  | Extract<AuthorizeDecision, { ok: false }>;

/**
 * Decide an action like `authorizeAction` with pessimistic conditions, and
 * when the grants allow it, find the instant at which grant expiry ends that
 * `allow`. Pessimistic mode is what makes the instant exact: it decides a
 * conditioned grant without the request, so no condition, such as a time
 * window, can end the `allow` at another time.
 *
 * Grants only drop out of the set as time passes. While the decision is
 * `allow`, the grant that resolves it is an `allow` that outranks every other
 * match, and removing a grant it outranks does not change the decision. So
 * the decision can stop being `allow` only when an `allow` grant expires. The
 * check evaluates the grants that are still live after each `allow` expiry,
 * earliest first, and returns the first one after which the action is no
 * longer allowed. That is not always the latest expiry: a specific `allow`
 * over a broader `deny` ends the decision when the specific `allow` ends.
 */
export async function authorizeActionUntil(
  grants: GrantRule[],
  resource: string,
  action: string,
  opts: EvalOptions & { conditions: "pessimistic" },
): Promise<AuthorizeUntilDecision> {
  const decision = await authorizeAction(grants, resource, action, opts);
  if (!decision.ok) return decision;
  const expiries = new Set<number>();
  for (const g of grants) {
    if (g.effect === "allow" && g.expiresAt !== null) {
      expiries.add(g.expiresAt.getTime());
    }
  }
  for (const t of [...expiries].sort((a, b) => a - b)) {
    const live = grants.filter(
      (g) => g.expiresAt === null || g.expiresAt.getTime() > t,
    );
    const later = await authorizeAction(live, resource, action, opts);
    if (!later.ok) return { ok: true, expiresAt: new Date(t) };
  }
  return { ok: true, expiresAt: null };
}
