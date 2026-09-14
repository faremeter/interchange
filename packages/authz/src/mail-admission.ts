import type { ConditionRegistry, GrantRule } from "./types";
import { evaluateGrants } from "./evaluate";
import {
  MAIL_ACCEPT_ACTION,
  MAIL_ACCEPT_NAMESPACE,
  mailAcceptResource,
  type MailAcceptCoordinate,
} from "./coord";

/**
 * Outcome of an inbound-mail admission decision.
 *
 * `effect` reports the resolved accept-policy effect across the sender's
 * coordinates: `deny` when any coordinate resolves a deny, `allow` when a
 * coordinate resolves an allow and none deny, and `null` when no
 * `mail.accept` grant matched any coordinate (default-deny). `ask` never
 * surfaces here -- a delivery seam cannot satisfy an interactive prompt, so
 * an `ask` that is the strongest matching effect is treated as no admission
 * (`admit: false`, `effect: null`).
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
 *   no alternation, so coordinates are evaluated independently and combined
 *   with deny-wins semantics.
 *
 * Conditioned `mail.accept` grants are only honored when a condition
 * `registry` is supplied; `evaluateGrants` skips conditioned grants without
 * one. A skipped conditioned deny would fail OPEN, so callers that rely on
 * conditioned denies MUST pass a registry. When no registry is passed,
 * `mail.accept` denies must be unconditioned.
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

  const acceptGrants = recipientGrants.filter(
    (g) =>
      g.resource === MAIL_ACCEPT_NAMESPACE ||
      g.resource.startsWith(`${MAIL_ACCEPT_NAMESPACE}:`),
  );

  const opts = registry ? { registry } : undefined;

  let sawAllow = false;

  for (const coord of senderCoordinates) {
    const result = await evaluateGrants(
      acceptGrants,
      mailAcceptResource(coord.coordType, coord.id),
      MAIL_ACCEPT_ACTION,
      opts,
    );

    if (result.effect === "deny") {
      return { admit: false, effect: "deny" };
    }
    if (result.effect === "allow") {
      sawAllow = true;
    }
  }

  if (sawAllow) {
    return { admit: true, effect: "allow" };
  }

  return { admit: false, effect: null };
}
