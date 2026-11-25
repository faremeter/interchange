import { type } from "arktype";

import { DELIVERY_KEY_PREFIX } from "@intx/types/message-id";
import { SignalDeliverFrame } from "@intx/types/sidecar";

import type { CommittedReads } from "./repo-store";
import {
  acceptedDispatchOf,
  type AcceptedWorkflowDispatch,
  type ConsumedWorkflowDispatch,
  type WorkflowDispatchEvidence,
} from "./workflow-dispatch-evidence";
import {
  splitCombinedEventLog,
  WORKFLOW_RUN_EVENTS_FILE,
} from "./workflow-run-event-log";
import {
  addressSegmentFor,
  WORKFLOW_RUN_ADDRESSES_PREFIX,
  WORKFLOW_RUN_CONSUMED_DIR,
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "./workflow-run-kind";

const ConsumedEnvelope = type({
  messageId: "string",
  "deliveryKey?": "string",
  address: "string",
  "rejection?": {
    code: "string > 0",
    message: "string > 0",
  },
  "+": "ignore",
});

export type ReceivedWorkflowSignal = {
  readonly runId: string;
  readonly signalId: string;
};

function acceptedDispatchOfLine(
  raw: string,
  path: string,
): Omit<AcceptedWorkflowDispatch, "runId"> | null {
  let decoded: unknown;
  try {
    decoded = JSON.parse(raw);
  } catch (cause) {
    throw new Error(`workflow_dispatch_event_invalid_json: ${path}`, {
      cause,
    });
  }
  return acceptedDispatchOf(decoded, path);
}

function consumedDispatchOf(
  raw: Uint8Array,
  path: string,
): ConsumedWorkflowDispatch {
  let decoded: unknown;
  try {
    decoded = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    throw new Error(`workflow_dispatch_consumed_invalid_json: ${path}`, {
      cause,
    });
  }
  const envelope = ConsumedEnvelope(decoded);
  if (envelope instanceof type.errors) {
    throw new Error(
      `workflow_dispatch_consumed_invalid: ${path}: ${envelope.summary}`,
    );
  }
  return {
    messageId: envelope.messageId,
    ...(envelope.deliveryKey !== undefined
      ? { deliveryKey: envelope.deliveryKey }
      : {}),
    address: envelope.address,
    ...(envelope.rejection !== undefined
      ? { rejection: envelope.rejection }
      : {}),
  };
}

/** Enumerate dispatches durably accepted by one live or sealed run log. */
export async function listAcceptedWorkflowDispatches(
  reads: CommittedReads,
  runId: string,
  targetIds?: ReadonlySet<string>,
): Promise<AcceptedWorkflowDispatch[]> {
  if (targetIds?.size === 0) return [];
  const accepted: AcceptedWorkflowDispatch[] = [];
  const remaining = targetIds === undefined ? undefined : new Set(targetIds);
  const accept = (dispatch: Omit<AcceptedWorkflowDispatch, "runId"> | null) => {
    if (dispatch === null) return;
    if (remaining !== undefined && !remaining.delete(dispatch.id)) return;
    accepted.push({ runId, ...dispatch });
  };
  const runPath = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}`;
  const children = await reads.listDir(runPath);
  const combined = children.find(
    (entry) => entry.type === "blob" && entry.name === WORKFLOW_RUN_EVENTS_FILE,
  );
  if (combined !== undefined) {
    const raw = new TextDecoder().decode(
      await reads.readBlobByOid(combined.oid),
    );
    const lines = splitCombinedEventLog(raw);
    for (let index = lines.length - 1; index >= 0; index -= 1) {
      const line = lines[index];
      if (line === undefined) continue;
      accept(
        acceptedDispatchOfLine(
          line,
          `${runPath}/${WORKFLOW_RUN_EVENTS_FILE}:${String(index + 1)}`,
        ),
      );
      if (remaining?.size === 0) break;
    }
    return accepted.reverse();
  }

  const eventsPath = `${runPath}/${WORKFLOW_RUN_EVENTS_DIR}`;
  const eventEntries = (await reads.listDir(eventsPath))
    .filter((entry) => entry.type === "blob" && entry.name.endsWith(".json"))
    .sort((left, right) =>
      right.name.localeCompare(left.name, undefined, {
        numeric: true,
      }),
    );
  for (const eventEntry of eventEntries) {
    const raw = new TextDecoder().decode(
      await reads.readBlobByOid(eventEntry.oid),
    );
    accept(acceptedDispatchOfLine(raw, `${eventsPath}/${eventEntry.name}`));
    if (remaining?.size === 0) break;
  }
  return accepted.reverse();
}

/** Enumerate durable SignalReceived ids from one live or sealed run log. */
export async function listReceivedWorkflowSignals(
  reads: CommittedReads,
  runId: string,
): Promise<ReceivedWorkflowSignal[]> {
  return (await listAcceptedWorkflowDispatches(reads, runId))
    .filter((dispatch) => dispatch.kind === "signal")
    .map((dispatch) => ({ runId, signalId: dispatch.id }));
}

function consumedPathFor(address: string): string {
  return `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressSegmentFor(address)}/${WORKFLOW_RUN_CONSUMED_DIR}`;
}

/**
 * The consumed claim-check entries that `address` filed under one of
 * `identities`: delivery keys or, for legacy entries, bare Message-IDs.
 * One listing finds them, and only their blobs are read. The kind
 * validator guarantees their shape before the ref advances; parsing them
 * again keeps the projection fail-closed if that invariant is ever
 * violated.
 */
export async function readConsumedWorkflowDispatches(
  reads: CommittedReads,
  address: string,
  identities: ReadonlySet<string>,
): Promise<ConsumedWorkflowDispatch[]> {
  if (identities.size === 0) return [];
  const consumedPath = consumedPathFor(address);
  const found: ConsumedWorkflowDispatch[] = [];
  for (const entry of await reads.listDir(consumedPath)) {
    if (entry.type !== "blob" || !entry.name.endsWith(".json")) continue;
    if (!identities.has(entry.name.slice(0, -".json".length))) continue;
    found.push(
      consumedDispatchOf(
        await reads.readBlobByOid(entry.oid),
        `${consumedPath}/${entry.name}`,
      ),
    );
  }
  return found;
}

/**
 * What evidence matches an unsettled dispatch by. A signal's frame names the
 * run it is for; a mail is for its deployment, so its bytes are not needed.
 */
export type UnsettledDispatch = {
  readonly deliveryKey: string;
  readonly messageId: string;
  readonly kind: "mail" | "signal";
  readonly rawMessage: Uint8Array | null;
};

export type DispatchOutcome =
  | { readonly status: "settled" }
  | {
      readonly status: "failed";
      readonly code: string;
      readonly message: string;
    };

/** The run a dispatch is for: a mail's deployment, or a signal's target. */
export function targetRunOf(
  dispatch: UnsettledDispatch,
  anchorRunId: string,
): string {
  if (dispatch.kind === "mail") return anchorRunId;
  if (dispatch.rawMessage === null) {
    throw new Error(`signal dispatch ${dispatch.deliveryKey} has no frame`);
  }
  return SignalDeliverFrame.assert(
    JSON.parse(new TextDecoder().decode(dispatch.rawMessage)),
  ).runId;
}

// The id a consumed entry names, and whether it can be a bare Message-ID:
// a legacy entry has no delivery key.
function consumedId(consumed: ConsumedWorkflowDispatch): {
  id: string;
  legacy: boolean;
} {
  return consumed.deliveryKey === undefined
    ? { id: consumed.messageId, legacy: true }
    : { id: consumed.deliveryKey, legacy: false };
}

// An id with the delivery-key prefix is a key, even from legacy evidence:
// the hub refuses a relayed Message-ID with the prefix, so no legacy record
// holds one.
function namesByMessageId(id: string, legacy: boolean): boolean {
  return legacy && !id.startsWith(DELIVERY_KEY_PREFIX);
}

/**
 * The rows `settleFromEvidence` can settle from `evidence`: those keyed by
 * an id it names, and the mail rows whose Message-ID a legacy id names.
 */
export function rowsNamedByEvidence(
  anchor: { readonly address: string },
  evidence: WorkflowDispatchEvidence,
): { keys: string[]; messageIds: string[] } {
  const keys = new Set<string>();
  const messageIds = new Set<string>();
  const name = ({ id, legacy }: { id: string; legacy: boolean }) => {
    keys.add(id);
    if (namesByMessageId(id, legacy)) messageIds.add(id);
  };
  for (const consumed of evidence.consumed) {
    if (consumed.address === anchor.address) name(consumedId(consumed));
  }
  for (const accepted of evidence.accepted) name(accepted);
  return { keys: [...keys], messageIds: [...messageIds] };
}

/**
 * Decide what `evidence` settles among a deployment's `unsettled` rows, and
 * return the rows it leaves. Evidence a run log holds names only the rows
 * for that run.
 */
export function settleFromEvidence<D extends UnsettledDispatch>(
  anchor: { readonly id: string; readonly address: string },
  unsettled: readonly D[],
  evidence: WorkflowDispatchEvidence,
): {
  outcomes: Map<string, DispatchOutcome>;
  remaining: Map<string, D>;
} {
  const remaining = new Map(
    unsettled.map((dispatch) => [dispatch.deliveryKey, dispatch]),
  );
  const mailKeysByMessageId = new Map<string, string[]>();
  for (const dispatch of unsettled) {
    if (dispatch.kind !== "mail") continue;
    const keys = mailKeysByMessageId.get(dispatch.messageId) ?? [];
    keys.push(dispatch.deliveryKey);
    mailKeysByMessageId.set(dispatch.messageId, keys);
  }
  // The unsettled rows a piece of evidence names: the row keyed by its id,
  // and for a legacy id that is not a key, every unsettled mail row of
  // this anchor with that Message-ID. A sidecar that predates the delivery
  // key recorded the bare Message-ID and de-duplicated by it alone, so its
  // one record stands for every sender's mail with that Message-ID. The
  // legacy branch can go once no run log or claim-check that such a
  // sidecar wrote remains.
  const rowsNamedBy = (id: string, legacy: boolean): string[] => {
    const keys = remaining.has(id) ? [id] : [];
    if (!namesByMessageId(id, legacy)) return keys;
    for (const key of mailKeysByMessageId.get(id) ?? []) {
      if (key !== id && remaining.has(key)) keys.push(key);
    }
    return keys;
  };
  const outcomes = new Map<string, DispatchOutcome>();
  for (const consumed of evidence.consumed) {
    if (consumed.address !== anchor.address) continue;
    const { id, legacy } = consumedId(consumed);
    const keys = rowsNamedBy(id, legacy);
    for (const key of keys) {
      if (remaining.get(key)?.kind !== "mail") continue;
      outcomes.set(
        key,
        consumed.rejection === undefined
          ? { status: "settled" }
          : {
              status: "failed",
              code: consumed.rejection.code,
              message: consumed.rejection.message,
            },
      );
      remaining.delete(key);
    }
  }
  for (const accepted of evidence.accepted) {
    for (const key of rowsNamedBy(accepted.id, accepted.legacy)) {
      const dispatch = remaining.get(key);
      if (dispatch === undefined) continue;
      if (targetRunOf(dispatch, anchor.id) !== accepted.runId) continue;
      // Mail resuming an input gate is also recorded as SignalReceived.
      // The Hub's key uniqueness spans both dispatch kinds within this
      // anchor; RunStarted alone still cannot prove a signal delivery.
      if (accepted.kind === "mail" && dispatch.kind !== "mail") continue;
      outcomes.set(key, { status: "settled" });
      remaining.delete(key);
    }
  }
  return { outcomes, remaining };
}
