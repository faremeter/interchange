/**
 * Grammar for `<namespace>:<coord-type>:<id>` authorization resources.
 *
 * A coordinate names one durable target that a grant in a coordinate
 * namespace applies to. A `CoordGrammar` fixes the namespace, the action its
 * grants carry, and its coordinate types, ordered lowest tier first. The
 * namespace is one segment: it may not contain a `:`, because the coord type
 * is read as the second colon-separated segment of a resource.
 *
 * The resource string is exactly three colon-separated segments (the
 * namespace, the coord type, and the id), so the id may not contain a `:` --
 * an id with a colon would split into extra segments and make the
 * three-segment shape ambiguous. The id is a literal: it may not contain a
 * `*`, which grant matching reads as a wildcard, so a built or parsed resource
 * always names one concrete target. `isValidCoordId` (from `@intx/types`,
 * where the authored `MailAccept` shape lives) owns both id rules, so the
 * authoring boundary, the resource builder, and the parser share one
 * definition.
 *
 * In `mail.accept` an `asset` coordinate names a workflow asset, not one
 * workflow definition. One asset backs many definitions (one per wire hash),
 * so a grant on an asset trusts every definition that the asset backs. The
 * grant stays valid when a definition's content changes.
 */

import { MAIL_ACCEPT_NAMESPACE, isValidCoordId } from "@intx/types/mail-accept";
import { MAIL_SEND_NAMESPACE } from "@intx/types/mail-send";

export type CoordGrammar<T extends string> = {
  readonly namespace: string;
  readonly action: string;
  /** The coordinate types, lowest tier first. */
  readonly coordTypes: readonly T[];
};

export type Coordinate<T extends string> = { coordType: T; id: string };

export function defineCoordGrammar<const T extends string>(
  grammar: CoordGrammar<T>,
): CoordGrammar<T> {
  if (grammar.namespace.includes(":")) {
    throw new Error(
      `defineCoordGrammar: namespace must be one segment without ":" (received ${JSON.stringify(grammar.namespace)})`,
    );
  }
  if (new Set(grammar.coordTypes).size !== grammar.coordTypes.length) {
    throw new Error(
      `defineCoordGrammar: ${grammar.namespace} coord types must be unique`,
    );
  }
  return grammar;
}

export function isGrammarCoordType<T extends string>(
  grammar: CoordGrammar<T>,
  value: string,
): value is T {
  return grammar.coordTypes.some((coordType) => coordType === value);
}

/**
 * The tier of a coord type in `grammar`: its 1-based position in
 * `coordTypes`, or 0 when `value` is not one of them.
 */
export function coordTypeTier<T extends string>(
  grammar: CoordGrammar<T>,
  value: string,
): number {
  return grammar.coordTypes.findIndex((coordType) => coordType === value) + 1;
}

export function coordResource<T extends string>(
  grammar: CoordGrammar<T>,
  coordType: T,
  id: string,
): string {
  if (!isValidCoordId(id)) {
    throw new Error(
      `${grammar.namespace} resource: id must be a non-empty string without ":" or "*" (received ${JSON.stringify(id)})`,
    );
  }
  return `${grammar.namespace}:${coordType}:${id}`;
}

export function parseCoordResource<T extends string>(
  grammar: CoordGrammar<T>,
  resource: string,
): Coordinate<T> | null {
  const segments = resource.split(":");
  if (segments.length !== 3) return null;

  const [namespace, coordType, id] = segments;
  if (namespace !== grammar.namespace) return null;
  if (coordType === undefined || !isGrammarCoordType(grammar, coordType)) {
    return null;
  }
  if (id === undefined || !isValidCoordId(id)) return null;

  return { coordType, id };
}

export const MAIL_ACCEPT_ACTION = "accept";

export const MAIL_ACCEPT_GRAMMAR = defineCoordGrammar({
  namespace: MAIL_ACCEPT_NAMESPACE,
  action: MAIL_ACCEPT_ACTION,
  coordTypes: ["tenant", "asset", "principal"],
});

/** The coordinate types the hub resolves for a local principal. */
export type CoordType = (typeof MAIL_ACCEPT_GRAMMAR.coordTypes)[number];

export type MailAcceptCoordinate = Coordinate<CoordType>;

export function isCoordType(value: string): value is CoordType {
  return isGrammarCoordType(MAIL_ACCEPT_GRAMMAR, value);
}

export function mailAcceptResource(coordType: CoordType, id: string): string {
  return coordResource(MAIL_ACCEPT_GRAMMAR, coordType, id);
}

export function parseMailAcceptResource(
  resource: string,
): MailAcceptCoordinate | null {
  return parseCoordResource(MAIL_ACCEPT_GRAMMAR, resource);
}

export const MAIL_SEND_ACTION = "send";

/**
 * `mail.send` adds a `domain` coordinate below the local coordinates. Every
 * recipient address whose domain is a host name has one, so such a recipient
 * that the hub cannot resolve to a local principal is still named by its
 * domain.
 */
export const MAIL_SEND_GRAMMAR = defineCoordGrammar({
  namespace: MAIL_SEND_NAMESPACE,
  action: MAIL_SEND_ACTION,
  coordTypes: ["domain", "tenant", "asset", "principal"],
});

export type MailSendCoordType = (typeof MAIL_SEND_GRAMMAR.coordTypes)[number];

export type MailSendCoordinate = Coordinate<MailSendCoordType>;

export function mailSendResource(
  coordType: MailSendCoordType,
  id: string,
): string {
  return coordResource(MAIL_SEND_GRAMMAR, coordType, id);
}
