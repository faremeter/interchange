import { parseAddress } from "@intx/types/agent-address";
import {
  MAIL_SEND_NAMESPACE,
  isMailSendNamespaceResource,
} from "@intx/types/mail-send";

import type { ConditionRegistry, GrantRule, MatchedGrant } from "./types";
import {
  MAIL_SEND_GRAMMAR,
  isGrammarCoordType,
  type MailSendCoordinate,
} from "./coord";
import { evaluateCoordinateGrants } from "./coord-evaluation";
import { matchPattern } from "./patterns";

/**
 * Outcome of a send decision for one recipient.
 *
 * `effect` is the effect of the winning `mail.send` grant across the
 * recipient's coordinates, or `null` when no grant matched (default deny).
 * An `ask` that wins is treated as no allow (`allowed: false`,
 * `effect: null`): a relay cannot satisfy an interactive prompt. `grant` is
 * the winning grant, so a caller can name the row that decided.
 */
export type MailSendResult = {
  allowed: boolean;
  effect: "allow" | "deny" | null;
  grant: MatchedGrant | null;
};

/**
 * Decide whether a sender may send mail to one recipient.
 *
 * `recipientCoordinates` are the recipient's coordinates: its domain, and its
 * tenant, asset and principal when the hub resolves them. `senderGrants` are
 * the sender's direct and role grants. The winning `mail.send` grant decides,
 * by tier (principal, then asset, then tenant, then domain, then a
 * namespace-wide grant such as `mail.send:*:*`), then the specificity of the
 * coordinate pattern (the action does not count), then effect. A broad `*`
 * or `mail.*` grant is outside the namespace and never allows a send. A
 * recipient with no coordinates matches no grant, so it is refused.
 */
export async function evaluateMailSend(args: {
  recipientCoordinates: readonly MailSendCoordinate[];
  senderGrants: GrantRule[];
  registry: ConditionRegistry;
}): Promise<MailSendResult> {
  const winner = await evaluateCoordinateGrants({
    grammar: MAIL_SEND_GRAMMAR,
    coordinates: args.recipientCoordinates,
    grants: args.senderGrants,
    registry: args.registry,
  });

  if (winner === null) {
    return { allowed: false, effect: null, grant: null };
  }
  switch (winner.effect) {
    case "allow":
      return { allowed: true, effect: "allow", grant: winner };
    case "deny":
      return { allowed: false, effect: "deny", grant: winner };
    case "ask":
      return { allowed: false, effect: null, grant: winner };
    default: {
      const _exhaustive: never = winner.effect;
      throw new Error(`unhandled effect: ${String(_exhaustive)}`);
    }
  }
}

// Dot-separated labels of letters, digits and inner hyphens.
const HOST_NAME =
  /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*$/;

/**
 * The `domain` coordinate id of a mail address: the part after the `@`, as
 * `parseAddress` splits it, in lower case and without a trailing root dot.
 * Every spelling of one domain therefore gives one id, so a domain grant
 * decides for all of them. Returns `null` when the address does not parse or
 * its domain is not a host name. A recipient whose domain is `null` has no
 * domain coordinate.
 */
export function mailAddressDomain(address: string): string | null {
  const parsed = parseAddress(address);
  if (parsed === null) return null;
  let domain = parsed.domain.toLowerCase();
  if (domain.endsWith(".")) domain = domain.slice(0, -1);
  return HOST_NAME.test(domain) ? domain : null;
}

/**
 * Why a resource can never decide a send as its author means, or `null` when
 * it can, or when it is a grant for other namespaces.
 *
 * The send check reads only grants in the `mail.send` namespace. A pattern
 * outside it that the author wrote toward `mail.send`, such as
 * `mail.send*:domain:x` or `mail.*:domain:x`, matches some send resources but
 * never decides a send, so it is refused. A pattern that matches every send
 * resource, such as `*` or `mail.*`, serves other namespaces and is accepted.
 *
 * A usable resource is either `mail.send:<coord type>:<id>`, with a coord
 * type of the send grammar and a non-empty id, or a pattern whose type
 * segment holds a `*` and whose id part, when present, is one non-empty
 * segment. Any other shape matches no coordinate, so a deny row of that shape
 * would block nothing.
 *
 * Domain coordinates are lower-case host names without a trailing dot (see
 * `mailAddressDomain`), and grant matching is case sensitive. A `domain` id
 * must therefore have that form, and a resource whose type segment holds a
 * `*` must be lower case, because it can match a domain coordinate. A grant
 * id also matches literally, so `mail.send:domain:*example.com` matches
 * `evilexample.com`; `mail.send:domain:*.example.com` names the subdomains
 * of `example.com`.
 */
export function mailSendResourceProblem(resource: string): string | null {
  const quoted = JSON.stringify(resource);
  if (!isMailSendNamespaceResource(resource)) {
    return nearMailSendPattern(resource)
      ? `resource ${quoted} is outside the ${MAIL_SEND_NAMESPACE} namespace, so it never decides a send; write a ${MAIL_SEND_NAMESPACE}:<coord type>:<id> grant, and a separate grant for other namespaces`
      : null;
  }
  if (resource === MAIL_SEND_NAMESPACE) {
    return `${MAIL_SEND_NAMESPACE} resource ${quoted} names no coordinate`;
  }

  const rest = resource.slice(MAIL_SEND_NAMESPACE.length + 1);
  const [coordType = "", ...idSegments] = rest.split(":");
  const lowerCase = `${MAIL_SEND_NAMESPACE} resource ${quoted} can match a domain coordinate, so it must be lower case`;
  const shape = `${MAIL_SEND_NAMESPACE} resource ${quoted} must be ${MAIL_SEND_NAMESPACE}:<coord type>:<id>`;
  const id = idSegments.join(":");
  // A coordinate id holds no `:` and is not empty, so an id part with more
  // than one segment, or an empty one, matches no coordinate.
  if (idSegments.length > 1 || (idSegments.length === 1 && id.length === 0)) {
    return shape;
  }
  // A wildcard type segment also matches tenant, asset and principal ids, and
  // a coordinate id may hold a `.`, so its id part gets no host-name check:
  // `mail.send:*:evil.com.` can match a non-domain id.
  if (coordType.includes("*")) {
    return rest === rest.toLowerCase() ? null : lowerCase;
  }
  if (!isGrammarCoordType(MAIL_SEND_GRAMMAR, coordType)) {
    return `${MAIL_SEND_NAMESPACE} resource ${quoted} names an unknown coord type; use one of ${MAIL_SEND_GRAMMAR.coordTypes.join(", ")}`;
  }
  if (idSegments.length !== 1) return shape;
  if (coordType === "domain") {
    if (id !== id.toLowerCase()) return lowerCase;
    // A domain coordinate is a host name without a trailing dot. Test a
    // pattern with each `*` read as one letter.
    if (!HOST_NAME.test(id.replaceAll("*", "a"))) {
      return `${MAIL_SEND_NAMESPACE} resource ${quoted} names no domain coordinate; write the domain as a host name in lower case, without a trailing dot`;
    }
  }
  return null;
}

// Whether a pattern outside the namespace was written toward `mail.send` and
// matches only some send resources. Its text before the first `*` is a start
// of `mail.send:` that names at least the `mail.` family, so the author typed
// toward the namespace; a shorter text, such as the `m` of `m*:read`, or a
// pattern that starts with `*`, such as `*:prn_a`, serves other namespaces.
// A pattern that ends in `*` and matches `mail.send:` matches every send
// resource, so it is a broad grant and not a near miss, unless its text names
// the whole namespace, as `mail.send*` does: no other namespace starts with
// that text, so it is a mistyped `mail.send:*`.
function nearMailSendPattern(resource: string): boolean {
  const star = resource.indexOf("*");
  if (star < 0) return false;
  const prefix = resource.slice(0, star);
  if (!prefix.startsWith("mail.")) return false;
  if (!`${MAIL_SEND_NAMESPACE}:`.startsWith(prefix)) return false;
  if (prefix === MAIL_SEND_NAMESPACE) return true;
  return !(
    resource.endsWith("*") && matchPattern(resource, `${MAIL_SEND_NAMESPACE}:`)
  );
}

/**
 * Who sends: a deployment named by its run address (the relay path, where the
 * sidecar connection already binds the address), or a principal the caller
 * has already authenticated (the REST path).
 */
export type MailSendSender =
  | { kind: "address"; address: string }
  | { kind: "principal"; principalId: string; tenantId: string };

/**
 * The send decision for one recipient, keyed by the caller's string. A caller
 * decides on `allowed` alone. An `evaluated` decision's `effect` and `grant`
 * describe the winning grant for a log: `effect` is `null` when no grant
 * matched, and an `ask` winner is not allowed although `grant` names it. An
 * `unresolved` decision means the recipient's coordinates could not be
 * resolved, a fault and not a grant decision; it is never allowed.
 */
export type MailSendDecision =
  | ({ recipient: string; outcome: "evaluated" } & MailSendResult)
  | { recipient: string; outcome: "unresolved"; allowed: false };

export type MailSendAuthorization =
  | { ok: true; decisions: MailSendDecision[] }
  | { ok: false; reason: "sender_unresolved" };

/**
 * Decide, for each recipient of one mail, whether the sender may send it.
 * The hub implements it against its database; a fault rejects the promise,
 * and the caller then refuses every recipient.
 */
export type MailSendAuthorizer = (args: {
  sender: MailSendSender;
  recipients: readonly string[];
}) => Promise<MailSendAuthorization>;
