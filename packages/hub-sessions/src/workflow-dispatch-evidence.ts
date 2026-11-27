import { type } from "arktype";

import { correlationIdFromSignalName } from "@intx/types/signals";

/**
 * A consumed claim-check entry. `deliveryKey` is absent on a legacy entry,
 * which a sidecar filed under the bare Message-ID.
 */
export type ConsumedWorkflowDispatch = {
  readonly messageId: string;
  readonly deliveryKey?: string;
  readonly address: string;
  readonly rejection?: {
    readonly code: string;
    readonly message: string;
  };
};

export type AcceptedWorkflowDispatch = {
  readonly runId: string;
  /**
   * The dispatch identity the event records: a delivery key or a signal id.
   * `legacy` marks an id that can be a bare Message-ID from a sidecar that
   * predates the delivery key: a `RunStarted.consumedMessageId`, or the id of
   * a `SignalReceived` on a control-plane signal name.
   */
  readonly id: string;
  readonly legacy: boolean;
  /** Event source: SignalReceived also proves mail used to resume an input gate. */
  readonly kind: "mail" | "signal";
};

/** What accepted history says about a deployment's dispatches. */
export type WorkflowDispatchEvidence = {
  readonly consumed: readonly ConsumedWorkflowDispatch[];
  readonly accepted: readonly AcceptedWorkflowDispatch[];
};

/**
 * A consumed entry a commit prunes, by the address and identity it was
 * filed under.
 */
export type PrunedWorkflowDispatch = {
  readonly address: string;
  readonly identity: string;
};

/**
 * What one commit records about dispatches: the evidence it adds, and the
 * consumed entries it prunes.
 */
export type CommitDispatchEvidence = WorkflowDispatchEvidence & {
  readonly pruned: readonly PrunedWorkflowDispatch[];
};

export const NO_DISPATCH_EVIDENCE: CommitDispatchEvidence = {
  consumed: [],
  accepted: [],
  pruned: [],
};

/**
 * The evidence of a pack's commits, in order. A consumed entry that a later
 * commit prunes is dropped, as it is from the pack's tip: its identity, a
 * sender and Message-ID, is free for new mail once it is pruned, so the
 * entry cannot tell which delivery it settles.
 */
export function dispatchEvidenceOfPack(
  commits: readonly CommitDispatchEvidence[],
): WorkflowDispatchEvidence {
  const filed = (address: string, identity: string) =>
    JSON.stringify([address, identity]);
  const consumed = new Map<string, ConsumedWorkflowDispatch>();
  const accepted: AcceptedWorkflowDispatch[] = [];
  for (const commit of commits) {
    for (const entry of commit.pruned) {
      consumed.delete(filed(entry.address, entry.identity));
    }
    for (const entry of commit.consumed) {
      consumed.set(
        filed(entry.address, entry.deliveryKey ?? entry.messageId),
        entry,
      );
    }
    for (const entry of commit.accepted) accepted.push(entry);
  }
  return { consumed: [...consumed.values()], accepted };
}

const SignalReceived = type({
  type: "'SignalReceived'",
  signalName: "string",
  signalId: "string",
  "+": "ignore",
});

const RunStarted = type({
  type: "'RunStarted'",
  "consumedDeliveryKey?": "string",
  "consumedMessageId?": "string",
  "+": "ignore",
});

/** The dispatch a decoded run event accepts, if any. */
export function acceptedDispatchOf(
  decoded: unknown,
  path: string,
): Omit<AcceptedWorkflowDispatch, "runId"> | null {
  if (typeof decoded !== "object" || decoded === null || !("type" in decoded)) {
    return null;
  }
  if (decoded.type === "SignalReceived") {
    const signal = SignalReceived(decoded);
    if (signal instanceof type.errors) {
      throw new Error(
        `workflow_dispatch_signal_invalid: ${path}: ${signal.summary}`,
      );
    }
    // Mail that resumes an input gate arrives on a control-plane signal name,
    // and a sidecar that predates the delivery key recorded its bare
    // Message-ID as the signal id. A caller cannot deliver on a control-plane
    // name, and the hub mints the ids of the approvals that do, so no caller
    // chooses such an id to match another sender's Message-ID.
    return {
      kind: "signal",
      id: signal.signalId,
      legacy: correlationIdFromSignalName(signal.signalName) !== undefined,
    };
  }
  if (decoded.type === "RunStarted") {
    const started = RunStarted(decoded);
    if (started instanceof type.errors) {
      throw new Error(
        `workflow_dispatch_run_started_invalid: ${path}: ${started.summary}`,
      );
    }
    if (started.consumedDeliveryKey !== undefined) {
      return { kind: "mail", id: started.consumedDeliveryKey, legacy: false };
    }
    return started.consumedMessageId === undefined
      ? null
      : { kind: "mail", id: started.consumedMessageId, legacy: true };
  }
  return null;
}
