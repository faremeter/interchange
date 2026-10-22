/**
 * Grammar for `mail.accept:<coord-type>:<id>` authorization resources.
 *
 * A coordinate names one durable target that a `mail.accept` grant applies to:
 * a principal, an asset, or a tenant. An `asset` coordinate names a workflow
 * asset, not one workflow definition. One asset backs many definitions (one
 * per wire hash), so a grant on an asset trusts every definition that the
 * asset backs. The grant stays valid when a definition's content changes.
 *
 * The resource string is exactly three colon-separated segments
 * (`mail.accept`, the coord type, and the id), so the id may not contain a
 * `:` — an id with a colon would split into extra segments and make the
 * three-segment shape ambiguous. The id is a literal: it may not contain a
 * `*`, which grant matching reads as a wildcard, so a built or parsed resource
 * always names one concrete target. `isValidCoordId` (from `@intx/types`, where
 * the authored `MailAccept` shape lives) owns both id rules, so the authoring
 * boundary, this resource builder, and the parser share one definition.
 */

import { MAIL_ACCEPT_NAMESPACE, isValidCoordId } from "@intx/types/mail-accept";

export type CoordType = "principal" | "asset" | "tenant";

export const MAIL_ACCEPT_ACTION = "accept";

export type MailAcceptCoordinate = { coordType: CoordType; id: string };

const COORD_TYPES = {
  principal: true,
  asset: true,
  tenant: true,
} satisfies Record<CoordType, true>;

export function isCoordType(value: string): value is CoordType {
  return Object.hasOwn(COORD_TYPES, value);
}

export function mailAcceptResource(coordType: CoordType, id: string): string {
  if (!isValidCoordId(id)) {
    throw new Error(
      `mailAcceptResource: id must be a non-empty string without ":" or "*" (received ${JSON.stringify(id)})`,
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
  if (id === undefined || !isValidCoordId(id)) return null;

  return { coordType, id };
}
