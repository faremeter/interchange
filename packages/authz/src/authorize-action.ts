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
 * `evaluateGrants` verbatim -- this middle layer never defaults a condition
 * registry, so a caller that omits one keeps the fail-closed "skip conditioned
 * grants" behavior at the edge where that choice belongs.
 */
export async function authorizeAction(
  grants: GrantRule[],
  resource: string,
  action: string,
  opts?: EvalOptions,
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
