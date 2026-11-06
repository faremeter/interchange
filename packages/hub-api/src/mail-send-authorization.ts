import { and, eq } from "drizzle-orm";

import {
  evaluateMailSend,
  MAIL_SEND_CONDITION_REGISTRY,
  mailAddressDomain,
  type MailSendAuthorizer,
  type MailSendCoordinate,
  type MailSendDecision,
  type MailSendSender,
} from "@intx/authz";
import type { DB, GrantStoreFactory } from "@intx/db";
import { resolveLocalMailParty, type LocalMailParty } from "@intx/db";
import { principal } from "@intx/db/schema";
import { getLogger } from "@intx/log";

const logger = getLogger(["hub-api", "mail-send-authorization"]);

export type MailSendAuthorizerDeps = {
  db: DB["db"];
  grantStoreFor: GrantStoreFactory;
};

/**
 * Decide, for each recipient, whether the sender may send it mail: the
 * sender's `mail.send` grants (direct and role, unexpired, in its tenant)
 * against the recipient's coordinates. A recipient whose domain is a host
 * name has a `domain` coordinate; a local run or user also has its principal
 * and tenant coordinates, and a run its definition's asset. A recipient's coordinates do
 * not depend on its signing key or run state, so a deny on a recipient always
 * reaches it.
 *
 * A sender address that names no deployment with a run principal is
 * `sender_unresolved`, and no recipient is decided. The sender's principal
 * status is not read: a run deactivates its principal when it ends, and mail
 * it sent before then, held or still in flight, is decided by its grants. To
 * stop a run's sends, deny or remove its send grant.
 *
 * A recipient whose coordinates cannot be resolved (an ambiguous address, a
 * query fault) gets an `unresolved` decision and is logged; the other
 * recipients are still decided. A recipient with no coordinate at all -- not
 * a local party, and no domain that is a host name, such as an address
 * literal -- matches no grant, so it is refused.
 *
 * The send check has no consumer, so it evaluates conditions with
 * `MAIL_SEND_CONDITION_REGISTRY` and not the hub's full registry. A stored
 * grant with a condition outside it throws, and the send fails closed.
 *
 * A fault while resolving the sender, reading its grants or evaluating a
 * grant rejects the returned promise. The caller refuses every recipient
 * then.
 */
export function createMailSendAuthorizer(
  deps: MailSendAuthorizerDeps,
): MailSendAuthorizer {
  return async ({ sender, recipients }) => {
    const resolved = await resolveSender(deps.db, sender);
    if (!resolved.ok) return resolved;

    const senderGrants = await deps
      .grantStoreFor(deps.db)
      .collectGrants(resolved.principalId, resolved.tenantId);

    const decisions: MailSendDecision[] = [];
    for (const recipient of recipients) {
      let coordinates: MailSendCoordinate[];
      try {
        coordinates = await recipientCoordinates(deps.db, recipient);
      } catch (cause) {
        logger.error`Refused mail to ${recipient}: resolving its coordinates failed: ${cause instanceof Error ? cause.message : String(cause)}`;
        decisions.push({ recipient, outcome: "unresolved", allowed: false });
        continue;
      }
      const result = await evaluateMailSend({
        recipientCoordinates: coordinates,
        senderGrants,
        registry: MAIL_SEND_CONDITION_REGISTRY,
      });
      decisions.push({ recipient, outcome: "evaluated", ...result });
    }
    return { ok: true, decisions };
  };
}

async function resolveSender(
  db: DB["db"],
  sender: MailSendSender,
): Promise<
  | { ok: true; principalId: string; tenantId: string }
  | { ok: false; reason: "sender_unresolved" }
> {
  if (sender.kind === "principal") {
    return {
      ok: true,
      principalId: sender.principalId,
      tenantId: sender.tenantId,
    };
  }

  const party = await resolveLocalMailParty(db, sender.address);
  if (party === null || party.kind !== "run") {
    return { ok: false, reason: "sender_unresolved" };
  }
  // Deploy creates the run principal, so it exists before the first trigger.
  const [row] = await db
    .select({ id: principal.id })
    .from(principal)
    .where(
      and(
        eq(principal.id, party.principalId),
        eq(principal.tenantId, party.tenantId),
      ),
    )
    .limit(1);
  if (row === undefined) return { ok: false, reason: "sender_unresolved" };
  return { ok: true, principalId: party.principalId, tenantId: party.tenantId };
}

async function recipientCoordinates(
  db: DB["db"],
  recipient: string,
): Promise<MailSendCoordinate[]> {
  const coordinates: MailSendCoordinate[] = [];
  const domain = mailAddressDomain(recipient);
  if (domain !== null) coordinates.push({ coordType: "domain", id: domain });
  const party = await resolveLocalMailParty(db, recipient);
  if (party !== null) coordinates.push(...partyCoordinates(party));
  return coordinates;
}

function partyCoordinates(party: LocalMailParty): MailSendCoordinate[] {
  const coordinates: MailSendCoordinate[] = [
    { coordType: "principal", id: party.principalId },
    { coordType: "tenant", id: party.tenantId },
  ];
  if (party.kind === "run" && party.assetId !== null) {
    coordinates.push({ coordType: "asset", id: party.assetId });
  }
  return coordinates;
}
