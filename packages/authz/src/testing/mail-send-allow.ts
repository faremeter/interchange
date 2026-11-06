// A permissive MailSendAuthorizer for tests.
//
// It allows every recipient of every sender. Useful when a test exercises
// mail delivery and not the send check. Production callers must use the
// hub's database-backed authorizer.

import type { MailSendAuthorizer } from "../mail-send";
import type { MatchedGrant } from "../types";

// The grant each decision names, so a decision has the shape the real check
// gives an allowed send: an `allow` effect and the grant that decided it.
const PERMIT_ALL_GRANT: MatchedGrant = {
  id: "grt_permit_all_mail_send",
  resource: "mail.send:*:*",
  action: "send",
  effect: "allow",
  origin: "system",
  specificity: 0,
};

export const permitAllMailSend: MailSendAuthorizer = async ({
  recipients,
}) => ({
  ok: true,
  decisions: recipients.map((recipient) => ({
    recipient,
    outcome: "evaluated",
    allowed: true,
    effect: "allow",
    grant: PERMIT_ALL_GRANT,
  })),
});
