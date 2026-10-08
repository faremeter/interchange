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
 * Collapse already-collected grants into a fail-closed decision: anything
 * other than `allow` -- deny, ask, or no matching grant -- withholds. Callers
 * branch on `ok` and never re-derive the rule from a raw effect.
 *
 * Unlike `authorize`, this takes pre-collected grants and returns the
 * collapsed decision. `opts` passes through to `evaluateGrants` verbatim:
 * no registry is defaulted here, so omitting one keeps the fail-closed skip
 * of conditioned grants at the edge where that choice belongs.
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
