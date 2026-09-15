/**
 * Vocabulary for the RELATIONAL `mail.accept:<relation>` approval markers.
 *
 * A relation names one class of counterparty that a `mail.accept` policy may
 * approve at deploy time: the workflow invoker, the workflow itself, the
 * tenant, or an established correspondent. The deploy-time
 * marker string is OPAQUE: the approval gate compares it by exact set
 * membership and nothing parses it. Only the concrete coordinate grammar in
 * `./coord` (`mail.accept:<coord-type>:<id>`) is parsed and matched at run
 * time. This module therefore provides a formatter (relation to marker
 * string), the relation enum, and the shared default resolver — but no parser
 * and no predicate, because nothing consumes them.
 *
 * The relation token space is unambiguous against the concrete-coordinate
 * space: a concrete coordinate has three segments with a `CoordType` in segment
 * two, while a relation token has two segments with a relation word in segment
 * two. Segment count is the disambiguator; `tenant` is a member of both sets,
 * so the two never overlap only because the segment counts differ.
 */

import { MAIL_ACCEPT_NAMESPACE } from "./coord";

export type MailAcceptRelation =
  | "invoker"
  | "self"
  | "tenant"
  | "correspondent";

export type AuthoredMailAccept =
  | {
      invoker?: boolean;
      self?: boolean;
      tenant?: boolean;
      correspondent?: boolean;
    }
  | undefined
  | null;

export function mailAcceptRelationToken(relation: MailAcceptRelation): string {
  return `${MAIL_ACCEPT_NAMESPACE}:${relation}`;
}

/**
 * The single authoritative home for the relational enablement rule. Callers
 * must never re-derive it: a relation is enabled only when authored `true`. A
 * wholly absent authored value yields the empty set -- a definition accepts
 * nothing on the relational axis unless it opts in, matching the admission
 * gate's default-deny.
 */
export function resolveMailAcceptRelations(
  authored: AuthoredMailAccept,
): Set<MailAcceptRelation> {
  const enabled = new Set<MailAcceptRelation>();

  if (authored?.invoker === true) enabled.add("invoker");
  if (authored?.self === true) enabled.add("self");
  if (authored?.tenant === true) enabled.add("tenant");
  if (authored?.correspondent === true) enabled.add("correspondent");

  return enabled;
}
