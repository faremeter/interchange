import { type } from "arktype";

import { correlationIdFromSignalName } from "@intx/types";

import type { CommittedReads } from "./repo-store";
import {
  splitCombinedEventLog,
  WORKFLOW_RUN_EVENTS_FILE,
} from "./workflow-run-event-log";
import {
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

export type ReceivedWorkflowSignal = {
  readonly runId: string;
  readonly signalId: string;
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

function projectAcceptedDispatch(
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
        projectAcceptedDispatch(
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
    accept(projectAcceptedDispatch(raw, `${eventsPath}/${eventEntry.name}`));
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

/**
 * Enumerate the workflow-run claim-check's retained consumed index. The kind
 * validator guarantees this tree shape before the ref advances; the runtime
 * validator here keeps the Git-to-database projection fail-closed if that
 * invariant is ever violated.
 */
export async function listConsumedWorkflowDispatches(
  reads: CommittedReads,
): Promise<ConsumedWorkflowDispatch[]> {
  const consumed: ConsumedWorkflowDispatch[] = [];
  for (const addressEntry of await reads.listDir(
    WORKFLOW_RUN_ADDRESSES_PREFIX,
  )) {
    if (addressEntry.type !== "tree") continue;
    const consumedPath = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${addressEntry.name}/${WORKFLOW_RUN_CONSUMED_DIR}`;
    for (const entry of await reads.listDir(consumedPath)) {
      if (entry.type !== "blob" || !entry.name.endsWith(".json")) continue;
      const raw = await reads.readBlobByOid(entry.oid);
      let decoded: unknown;
      try {
        decoded = JSON.parse(new TextDecoder().decode(raw));
      } catch (cause) {
        throw new Error(
          `workflow_dispatch_consumed_invalid_json: ${consumedPath}/${entry.name}`,
          { cause },
        );
      }
      const envelope = ConsumedEnvelope(decoded);
      if (envelope instanceof type.errors) {
        throw new Error(
          `workflow_dispatch_consumed_invalid: ${consumedPath}/${entry.name}: ${envelope.summary}`,
        );
      }
      consumed.push({
        messageId: envelope.messageId,
        ...(envelope.deliveryKey !== undefined
          ? { deliveryKey: envelope.deliveryKey }
          : {}),
        address: envelope.address,
        ...(envelope.rejection !== undefined
          ? { rejection: envelope.rejection }
          : {}),
      });
    }
  }
  return consumed;
}
