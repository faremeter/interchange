import type { ConditionRegistry, GrantRule } from "./types";
import { MAIL_ACCEPT_GRAMMAR, type MailAcceptCoordinate } from "./coord";
import { evaluateCoordinateGrants } from "./coord-evaluation";

/**
 * Outcome of an inbound-mail admission decision.
 *
 * `effect` is the effect of the winning grant across the sender's
 * coordinates (see `evaluateMailAdmission` for the ordering), or `null` when
 * no `mail.accept` grant matched any coordinate (default-deny). `ask` never
 * surfaces here -- a delivery seam cannot satisfy an interactive prompt, so
 * an `ask` that wins is treated as no admission (`admit: false`,
 * `effect: null`).
 */
export type MailAdmissionResult = {
  admit: boolean;
  effect: "allow" | "deny" | null;
};

/**
 * Decide whether an inbound mail sender is admitted.
 *
 * Both admission scopes (start-a-run and deliver-to-existing) call this one
 * helper. It picks the winning `mail.accept` grant with
 * `evaluateCoordinateGrants`, which owns the namespace filter, the tier
 * ordering, and the conditioned-grant guard.
 *
 * Behavior (fail-closed):
 * - A null `senderCoordinates` means the sender is unresolvable/unknown and
 *   is never admitted.
 * - The recipient's grants are filtered to the `mail.accept` namespace before
 *   evaluation. This is load-bearing: a broad `*` or `mail.*` grant matches a
 *   `mail.accept:...` resource under `matchPattern`, so without the filter any
 *   such grant would auto-admit all mail and defeat the accept model.
 * - The tiers are principal, then asset, then tenant, then a namespace-wide
 *   grant such as `mail.accept:*`. The coordinate type ranks first, before
 *   pattern specificity. So `allow mail.accept:principal:<id>` admits that
 *   sender past a `deny mail.accept:tenant:<id>` or a `deny mail.accept:*`,
 *   and a `deny mail.accept:tenant:<id>` refuses that tenant past an
 *   `allow mail.accept:*`. A wildcard id does not lower the tier: an
 *   `allow mail.accept:principal:*` outranks an exact
 *   `deny mail.accept:tenant:<id>`. Inside one tier the more specific
 *   coordinate pattern wins, and effect breaks a tie. The action does not
 *   count, so a `deny` with action `*` refuses past an `accept` allow of the
 *   same coordinate.
 *
 * Conditioned `mail.accept` grants require a condition `registry`. Without
 * one, any conditioned grant in the namespace makes this helper THROW rather
 * than guess; the delivery seam turns that throw into a fail-closed drop and
 * the misconfiguration surfaces loudly. Callers that legitimately use
 * conditioned `mail.accept` grants MUST pass a registry.
 */
export async function evaluateMailAdmission(args: {
  senderCoordinates: MailAcceptCoordinate[] | null;
  recipientGrants: GrantRule[];
  registry?: ConditionRegistry;
}): Promise<MailAdmissionResult> {
  const { senderCoordinates, recipientGrants, registry } = args;

  if (senderCoordinates === null) {
    return { admit: false, effect: null };
  }

  const winner = await evaluateCoordinateGrants({
    grammar: MAIL_ACCEPT_GRAMMAR,
    coordinates: senderCoordinates,
    grants: recipientGrants,
    registry,
  });

  if (winner === null) {
    return { admit: false, effect: null };
  }
  switch (winner.effect) {
    case "allow":
      return { admit: true, effect: "allow" };
    case "deny":
      return { admit: false, effect: "deny" };
    case "ask":
      return { admit: false, effect: null };
    default: {
      const _exhaustive: never = winner.effect;
      throw new Error(`unhandled effect: ${String(_exhaustive)}`);
    }
  }
}
