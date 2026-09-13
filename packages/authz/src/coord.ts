/**
 * Grammar for `mail.accept:<coord-type>:<id>` authorization resources.
 *
 * A coordinate names one durable target that a `mail.accept` grant applies to:
 * a principal, a definition, or a tenant. The resource string is exactly three
 * colon-separated segments (`mail.accept`, the coord type, and the id), so the
 * id may not contain a `:` — an id with a colon would split into extra
 * segments and make the three-segment shape ambiguous.
 */

export type CoordType = "principal" | "definition" | "tenant";

export const MAIL_ACCEPT_NAMESPACE = "mail.accept";
export const MAIL_ACCEPT_ACTION = "accept";

export type MailAcceptCoordinate = { coordType: CoordType; id: string };

const COORD_TYPES = {
  principal: true,
  definition: true,
  tenant: true,
} satisfies Record<CoordType, true>;

export function isCoordType(value: string): value is CoordType {
  return Object.hasOwn(COORD_TYPES, value);
}

export function mailAcceptResource(coordType: CoordType, id: string): string {
  if (id.length === 0) {
    throw new Error("mailAcceptResource: id must not be empty");
  }
  if (id.includes(":")) {
    throw new Error(
      `mailAcceptResource: id must not contain ":" (received ${JSON.stringify(id)})`,
    );
  }
  return `${MAIL_ACCEPT_NAMESPACE}:${coordType}:${id}`;
}

export function parseMailAcceptResource(
  resource: string,
): MailAcceptCoordinate | null {
  const segments = resource.split(":");
  if (segments.length !== 3) return null;

  const [namespace, coordType, id] = segments;
  if (namespace !== MAIL_ACCEPT_NAMESPACE) return null;
  if (coordType === undefined || !isCoordType(coordType)) return null;
  if (id === undefined || id.length === 0) return null;

  return { coordType, id };
}
