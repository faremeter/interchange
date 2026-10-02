// Admission gate for mail that arrives over IMAP rather than over the hub
// control socket.
//
// The socket path (`mail.inbound` in `@intx/hub-agent`'s hub link) gates every
// message on a sender the HUB verified and stamped on the frame, beside the
// message. IMAP carries no such field. What a mailbox hands us is the message
// and nothing else, so this gate takes the sender of record from the message's
// own visible `From`.
//
// That is strictly weaker, and in one specific way:
//
//   The socket path can catch a message whose visible `From` disagrees with the
//   sender its transport vouched for -- the `mismatchedFrom` outcome. Here the
//   two values are THE SAME FIELD by construction, so that outcome can never be
//   raised and a workflow author's setting for it has no effect. Everything else
//   the policy controls still applies: an unsigned message is `missing`, a
//   message from a sender we hold no key for is `unknown`, and a bad signature
//   is `invalid`, each rejected unless the author relaxed it.
//
// Recovering the socket path's strength needs an authenticated envelope sender
// -- SPF, DKIM, or DMARC evaluated at an ingress that then stamps the result --
// which is a separate piece of work and deliberately not faked here.

import { getLogger } from "@intx/log";
import {
  decideInboundAdmission,
  outcomeForVerdict,
  verifyInboundSignature,
  type ResolvedInboundMailPolicy,
} from "@intx/hub-agent";
import {
  buildMessageHeaders,
  extractAddrSpec,
  parseHeaderSection,
} from "@intx/mime";
import { deriveWorkflowRunId } from "@intx/types";
import type { CryptoProvider } from "@intx/types/runtime";

import type { IngestOutcome } from "@intx/mail-imap";

import type { MultistepMailRouter } from "./workflow-run-pack-client";
import type { RunGrantsBarrier } from "./run-grants-barrier";

const logger = getLogger(["sidecar", "imap-ingress"]);

export type ImapInboundSinkDeps = {
  resolveSenderCrypto: (address: string) => CryptoProvider | undefined;
  lookupInboundMailPolicy: (address: string) => ResolvedInboundMailPolicy;
  mailRouter: MultistepMailRouter;
  /**
   * The join between this message and the run's authorization.
   *
   * Waited on BEFORE the admission gate, not after: the gate needs the sender's
   * key, and the key arrives on the same `run.grants` frame as the grants. A
   * gate run ahead of that frame resolves a known sender as `unknown` and the
   * default policy rejects the message -- so waiting first is what makes the
   * verdict reflect the sender rather than the arrival order.
   */
  runGrantsBarrier: RunGrantsBarrier;
};

/**
 * Build the sink `createImapHubTransport` hands every arriving message to.
 *
 * Returns the verdict the transport acts on: `accepted` once the deployment
 * durably stored the message, `refused` when the admission policy rejected it
 * (final -- retrying would re-reject forever), and `retry` when nothing durable
 * happened and the message should be re-offered.
 */
export function createImapInboundSink(
  deps: ImapInboundSinkDeps,
): (address: string, raw: Uint8Array) => Promise<IngestOutcome> {
  return async (address, raw) => {
    // The sender of record. `extractAddrSpec` reduces a display-name form to its
    // addr-spec; an absent or unparseable `From` yields a value the gate reports
    // as `absentFrom` or `untrustedFrom` rather than one this code invents.
    const { headers } = parseHeaderSection(raw);
    const parsed = buildMessageHeaders(headers);
    const sender =
      parsed.from === undefined ? "" : extractAddrSpec(parsed.from);

    // Wait for the run's authorization to be in place. The deployment address
    // IS the run id's address, so the run this message fires is derivable from
    // the recipient rather than from anything the message claims.
    //
    // A failure here means "not yet": the hub has not answered, or declined to.
    // `retry` leaves the message unconsumed, so the next ingest attempt asks
    // again. That is the only safe reading -- proceeding would hand the
    // supervisor a message whose grants file may be absent, and its barrier
    // answers that with a terminal `RunFailed`.
    try {
      await deps.runGrantsBarrier.ensure({
        agentAddress: address,
        runId: deriveWorkflowRunId(address),
        senderAddress: sender,
      });
    } catch (cause) {
      logger.warn`deferring IMAP mail for ${address}: ${cause instanceof Error ? cause.message : String(cause)}`;
      return "retry";
    }

    const verdict = await verifyInboundSignature(
      {
        raw,
        authenticatedSender: sender,
        messageId: parsed.messageId,
        agentAddress: address,
      },
      deps.resolveSenderCrypto,
    );
    const policy = deps.lookupInboundMailPolicy(address);
    const admission = decideInboundAdmission(verdict, policy);

    if (admission.rejectedBy !== null) {
      logger.warn`refusing IMAP mail for ${address} from ${sender === "" ? "(no From)" : sender}: ${outcomeForVerdict(verdict)} rejected by policy (${admission.rejectedBy})`;
      return "refused";
    }

    const settled = deps.mailRouter.tryRoute(address, raw);
    if (settled === null) {
      // No deployment is listening on this address. `retry` keeps the message,
      // so a deployment that hydrates later still receives it.
      logger.warn`no deployment is registered for ${address}; leaving the message for a later sweep`;
      return "retry";
    }
    try {
      await settled;
    } catch (cause) {
      // The deployment did not durably accept it -- a transient write fault, or
      // a tearing-down phase. Re-offer rather than discard.
      logger.warn`deployment ${address} did not accept the message; leaving it for a later sweep: ${cause instanceof Error ? cause.message : String(cause)}`;
      return "retry";
    }
    return "accepted";
  };
}
