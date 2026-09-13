/**
 * Vocabulary for the RELATIONAL `mail.accept:<relation>` approval markers.
 *
 * A relation names one class of counterparty that a `mail.accept` policy may
 * approve at deploy time: the workflow invoker, the workflow itself, the
 * tenant, an established correspondent, a parent, or a child. The deploy-time
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
  | "correspondent"
  | "parent"
  | "child";

export type AuthoredMailAccept =
  | {
      invoker?: boolean;
      self?: boolean;
      tenant?: boolean;
      correspondent?: boolean;
      parent?: boolean;
      child?: boolean;
    }
  | undefined
  | null;

export function mailAcceptRelationToken(relation: MailAcceptRelation): string {
  return `${MAIL_ACCEPT_NAMESPACE}:${relation}`;
}

/**
 * The single authoritative home for the relational default-on rule. Callers
 * must never re-derive it: `parent` and `child` are enabled unless authored
 * `false`; `invoker`, `self`, `tenant`, and `correspondent` are disabled unless
 * authored `true`. A wholly absent authored value yields `{parent, child}`.
 */
export function resolveMailAcceptRelations(
  authored: AuthoredMailAccept,
): Set<MailAcceptRelation> {
  const enabled = new Set<MailAcceptRelation>();

  if (authored?.invoker === true) enabled.add("invoker");
  if (authored?.self === true) enabled.add("self");
  if (authored?.tenant === true) enabled.add("tenant");
  if (authored?.correspondent === true) enabled.add("correspondent");
  if (authored?.parent !== false) enabled.add("parent");
  if (authored?.child !== false) enabled.add("child");

  return enabled;
}
