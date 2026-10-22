import { isMailAcceptNamespaceResource } from "@intx/types/mail-accept";

import type { ConditionRegistry, GrantRule, MatchedGrant } from "./types";
import { EFFECT_PRIORITY, evaluateGrants } from "./evaluate";
import {
  MAIL_ACCEPT_ACTION,
  isCoordType,
  mailAcceptResource,
  type CoordType,
  type MailAcceptCoordinate,
} from "./coord";

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
 * This composes the existing grant-evaluation engine; it does not
 * reimplement authorization. Both admission scopes (start-a-run and
 * deliver-to-existing) call this one helper.
 *
 * Behavior (fail-closed):
 * - A null `senderCoordinates` means the sender is unresolvable/unknown and
 *   is never admitted.
 * - The recipient's grants are filtered to the `mail.accept` namespace before
 *   evaluation. This is load-bearing: a broad `*` or `mail.*` grant matches a
 *   `mail.accept:...` resource under `matchPattern`, so without the filter any
 *   such grant would auto-admit all mail and defeat the accept model.
 * - Each sender coordinate is a distinct resource string; `matchPattern` has
 *   no alternation, so each coordinate is evaluated on its own. The grants
 *   that match any coordinate then form ONE set, the same as a principal's
 *   direct and role grants form one set in `evaluateGrants`, and one grant
 *   wins. The order is: the coordinate tier the grant names (principal, then
 *   asset, then tenant, then a namespace-wide grant such as `mail.accept:*`
 *   that names no coord type), then pattern specificity, then effect
 *   priority (deny > ask > allow).
 * - The coordinate type ranks first, before pattern specificity. So
 *   `allow mail.accept:principal:<id>` admits that sender past a
 *   `deny mail.accept:tenant:<id>` or a `deny mail.accept:*`, and a
 *   `deny mail.accept:tenant:<id>` refuses that tenant past an
 *   `allow mail.accept:*`. A wildcard id does not lower the tier: an
 *   `allow mail.accept:principal:*` outranks an exact
 *   `deny mail.accept:tenant:<id>`.
 * - The tier comes from the literal type segment of the grant. A grant whose
 *   type segment holds a wildcard (`mail.accept:principal*`, `mail.accept:*`)
 *   names no coord type, so it ranks as namespace-wide, below every grant that
 *   names a coordinate.
 *
 * Conditioned `mail.accept` grants require a condition `registry`:
 * `evaluateGrants` skips conditioned grants without one, and a skipped
 * conditioned DENY would fail OPEN. So when no registry is supplied and any
 * matched-namespace grant carries conditions, this helper THROWS rather than
 * evaluating past a deny it cannot honor; the delivery seam turns that throw
 * into a fail-closed drop and the misconfiguration surfaces loudly. Callers
 * that legitimately use conditioned `mail.accept` grants MUST pass a registry.
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

  const acceptGrants = recipientGrants.filter((g) =>
    isMailAcceptNamespaceResource(g.resource),
  );

  // Without a registry, evaluateGrants silently skips conditioned grants -- a
  // skipped conditioned DENY would fail OPEN. Refuse to evaluate rather than
  // admit past a deny we cannot honor; fail loud so the misconfiguration is
  // fixed instead of silently swallowed.
  if (registry === undefined) {
    const conditioned = acceptGrants.find(
      (g) => g.conditions !== null && Object.keys(g.conditions).length > 0,
    );
    if (conditioned !== undefined) {
      throw new Error(
        `evaluateMailAdmission: mail.accept grant ${JSON.stringify(conditioned.resource)} carries conditions but no condition registry was supplied; pass a registry or author mail.accept grants unconditioned`,
      );
    }
  }

  const opts = registry ? { registry } : undefined;

  // A namespace-wide grant matches every coordinate, so key the pooled set on
  // grant id to count it once.
  const matched = new Map<string, MatchedGrant>();
  for (const coord of senderCoordinates) {
    const result = await evaluateGrants(
      acceptGrants,
      mailAcceptResource(coord.coordType, coord.id),
      MAIL_ACCEPT_ACTION,
      opts,
    );
    for (const g of result.matchingGrants) matched.set(g.id, g);
  }

  let winner: MatchedGrant | undefined;
  for (const g of matched.values()) {
    if (winner === undefined || outranks(g, winner)) winner = g;
  }

  if (winner === undefined) {
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

const COORD_TIER = {
  tenant: 1,
  asset: 2,
  principal: 3,
} satisfies Record<CoordType, number>;

// The tier of a `mail.accept` grant is the coord type its resource names. A
// grant only matches coordinates of the type it names, so this is also the
// tier of the coordinate it matched. A grant whose type segment is not a
// literal coord type (`mail.accept`, `mail.accept:*`) ranks lowest.
function coordTier(resource: string): number {
  const coordType = resource.split(":")[1];
  return coordType !== undefined && isCoordType(coordType)
    ? COORD_TIER[coordType]
    : 0;
}

function outranks(a: MatchedGrant, b: MatchedGrant): boolean {
  const tierDiff = coordTier(a.resource) - coordTier(b.resource);
  if (tierDiff !== 0) return tierDiff > 0;
  const specDiff = a.specificity - b.specificity;
  if (specDiff !== 0) return specDiff > 0;
  return EFFECT_PRIORITY[a.effect] > EFFECT_PRIORITY[b.effect];
}
