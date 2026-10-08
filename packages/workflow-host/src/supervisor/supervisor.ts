// Per-deployment supervisor.
//
// The supervisor owns one workflow-process child for the lifetime of an
// active deployment. Spawn lifecycle: mint a fresh channelId / HMAC key /
// IPC keypair, build the spawn-time env (`IPC_CHANNEL_ID`, `IPC_HMAC_KEY`,
// `HOST_PUBKEY` -- never the private or principal-signing key), spawn via the
// injected `bindings.subprocessSpawner` (so tests stub it), wire the
// control/event channels, wait for the child's `ready` frame, register the
// mail address, and forward inbound mail via `trigger.fire`.
//
// Every CancelRequested origin (`self`, `supervisor-drain`,
// `supervisor-operator`, `hub-admin`) flows through the same
// supervisor-signed path via `commitCancelRequested`; `self` is the child
// forwarding its stated reason over the control IPC.

import { type } from "arktype";

import { getLogger } from "@intx/log";

import {
  sampleStructuralCounters,
  forceRepack,
  type RepackToggle,
} from "./dispatch-attribution";

import { generateKeyPair } from "@intx/crypto";
import {
  enqueueInbox as defaultEnqueueInbox,
  dequeueToProcessing as defaultDequeueToProcessing,
  markConsumed as defaultMarkConsumed,
  scanRunsForBoot,
  readWorkflowRunLifecycle,
  replayProcessingToInbox as defaultReplayProcessingToInbox,
  StaleInboxEnqueueError,
  DEFAULT_CONSUMED_RETENTION_MS,
  type Principal,
  type WorkflowRunSupervisorPrincipal,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions/substrate";
import {
  base64Decode,
  base64Encode,
  deriveMessageId,
  deriveWorkflowRunId,
  signalName,
} from "@intx/types";
import { RepoId, type CredentialDelivery } from "@intx/types/sidecar";
import { inboundMessageToRaw } from "@intx/mail-memory";
import {
  executeSearch,
  executeThread,
  fetchFull,
  fetchHeaders,
  fetchPart,
  fetchStructure,
  type StoredEnvelope,
} from "@intx/mailbox";
import {
  isMessageTransportError,
  type ApprovalSnapshot,
  type InferenceSource,
  type InboundMessage,
  type Mail,
  type MessageHeaders,
  type MessagePart,
  type MessageTransportCondition,
  type OutboundMessage,
  type SearchQuery,
} from "@intx/types/runtime";
import type { CancelOrigin } from "@intx/workflow";

import {
  createControlChannelSender,
  generateChannelId,
  generateHmacKey,
  receiveControlChannel,
  receiveEventChannel,
  type ControlChannelSender,
  type ControlPayload,
  type EventPayload,
  type OutboundMessagePayload,
} from "../ipc/index";

import {
  assembleCredentialsSnapshot,
  type CredentialsSnapshot,
} from "./credentials";
import { commitCancelRequested } from "./cancel-signing";
import { commitRunFailed } from "./terminal-commit";
import { buildChildSpawnEnv } from "./spawn-env";
import { compactRunEvents } from "./run-event-compaction";
import { recoverInterruptedCompactions } from "./run-event-recovery";
import { decodeMail } from "@intx/mime";
import {
  commitMail,
  createMailPartReader,
  InvalidMailError,
} from "../adapters/mail-part-store";
import { mergeCredentialDelivery } from "../child/credential-cell";
import {
  createSubstrateMailboxStore,
  MAILBOX_INBOX_DIR,
  type MailboxSyncResult,
  type SubstrateMailboxStore,
} from "../adapters/substrate-mailbox-store";
import {
  createDrainTimeoutAccumulator,
  DEFAULT_DRAIN_TIMEOUT_MS,
  type DrainTimeoutAccumulator,
} from "./drain-timeout";
import {
  createRecyclePolicy,
  triggerRecycle,
  type ChildWiring,
  type RecycleAttempt,
  type RecycleOrigin,
  type RecyclePolicy,
} from "./recycle";
import type {
  DeriveMailAuditRef,
  DispatchStructuralCounters,
  DispatchSubstrateLeg,
  InboxPrimitives,
  MailAuditRef,
  SubprocessHandle,
  TerminalEventSource,
  TerminalRunEvent,
  WorkflowSupervisorBindings,
} from "./types";
import {
  createTerminalBroadcaster,
  type TerminalBroadcaster,
} from "./terminal-broadcaster";
import {
  DEFAULT_KILL_TIMEOUT_MS,
  DEFAULT_READY_TIMEOUT_MS,
  defaultClearTimer,
  defaultSetTimer,
  killChildHandle,
  waitDeadline,
} from "./child-termination";

const logger = getLogger(["workflow-host", "supervisor"]);

/** IMAP system flag marking a dispatched mailbox entry as read. */
const MAILBOX_FLAG_SEEN = "\\Seen";
/** Interchange keyword flag marking a mailbox entry dispatched as a workflow turn. */
const MAILBOX_FLAG_PROCESSED = "$Processed";
/** IMAP system flag marking a mailbox entry for expunge; a later `expunge` sweeps those entries out. */
const MAILBOX_FLAG_DELETED = "\\Deleted";

/** Unexpected-exit count within the window that latches the deployment. Overridable via bindings.crashLoopMaxCount. */
export const DEFAULT_CRASH_LOOP_MAX_COUNT = 3;

/** Sliding window (ms) over which the crash-loop count latches the deployment. Overridable via bindings.crashLoopWindowMs. */
export const DEFAULT_CRASH_LOOP_WINDOW_MS = 60_000;

/** Respawned child uptime (ms) that resets the crash counter. Overridable via bindings.crashLoopStableResetMs. */
export const DEFAULT_CRASH_LOOP_STABLE_RESET_MS = 60_000;

/** Initial respawn backoff (ms) after an unexpected exit. Overridable via bindings.respawnBackoffInitialMs. */
export const DEFAULT_RESPAWN_BACKOFF_INITIAL_MS = 1_000;

/** Cap (ms) on exponential respawn backoff, kept below the crash-loop window so slow flapping still latches. Overridable via bindings.respawnBackoffMaxMs. */
export const DEFAULT_RESPAWN_BACKOFF_MAX_MS = 30_000;

/** Watchdog for reEmitParkedCorrelations' wait on the child's parked-correlations.response: generous for a healthy child, tight enough to free a wedged one. */
export const DEFAULT_PARKED_QUERY_WATCHDOG_MS = 30_000;

/** Backstop for waitForRunTerminalOrPark: a lost park wake or wedged child must not hang the dispatch loop forever. Fails the dispatch so the mail stays reclaimable. */
export const TERMINAL_OR_PARK_BACKSTOP_MS = 300_000;

/** Public surface returned by createWorkflowSupervisor; each method advances one lifecycle transition. */
export interface WorkflowSupervisor {
  /** Spawn the child, complete the IPC handshake, push credentials, register the mail address, and start forwarding mail. Resolves on the child's ready frame. */
  spawn(opts: SpawnOpts): Promise<SpawnResult>;
  /** Sign and commit a CancelRequested under the named origin (host: supervisor-operator / hub-admin; child: self via control IPC). The caller owns the deadline for an unresponsive child. */
  requestCancel(opts: CancelRequestOpts): Promise<CancelCommitInfo>;
  /** Tear the deployment down (unregister address, kill child, dispose subscriptions, await exit). Idempotent. */
  shutdown(): Promise<void>;
  /** Send the drain control mail and arm a drainTimeout accumulator per in-flight run; expiry commits a signed CancelRequested{origin:"supervisor-drain"}. The recycle path reuses this verbatim. */
  drain(opts: DrainOpts): Promise<void>;
  /** Recycle the child (drain -> kill -> respawn with a fresh channelId). All origins funnel through triggerRecycle. */
  recycle(opts: RecycleOpts): Promise<RecycleAttempt>;
  /** Deliver a run signal via a signal.deliver control frame; the child commits SignalReceived so it stays the sole writer of runs/<runId>/events/. Throws when the child is not addressable. */
  deliverSignal(opts: DeliverSignalOpts): Promise<void>;
  /** Push a rotated inference-source list to the child. Phase-guarded to starting/running so a frame never lands in a recycling child's closing pipe. */
  deliverSources(opts: DeliverSourcesOpts): Promise<void>;
  /** Push refreshed credential material to the child's cell. A revoked credential is delivered by omitting its material so the child evicts it. */
  deliverCredentials(opts: DeliverCredentialsOpts): Promise<void>;
  /** Refresh a live run's grant floor by re-reading its durable grants.json and pushing a grants-updated frame. NO-OPs (skipped) when the child is not live; send failures are non-fatal. Never injects caller-supplied grants. */
  deliverGrants(runId: string): Promise<"pushed" | "skipped">;
  /** Re-register the child's parked correlations, recovering registers the hub missed while it was down. Best-effort: NO-OPs when the child is not addressable; failures are logged and re-driven on the next re-establishment. */
  reEmitParkedCorrelations(): Promise<void>;
  /** Credentials snapshot pushed to the child, for host audit of per-step contentHash. null before spawn. */
  getCredentialsSnapshot(): CredentialsSnapshot | null;
}

export type SpawnOpts = {
  /** Every step id in the deployment's flat step-id namespace (definition order plus loop-body steps). */
  stepOrder: readonly string[];
  /** Content hash of the deployment's workflow definition. */
  definitionHash: string;
  /** Whether the child warm-keeps its agent across messages (threaded as WARM_KEEP so the decision survives recycle). */
  warmKeep: boolean;
  /** Callback for each verified InferenceEvent the child publishes. */
  onInferenceEvent: (event: EventPayload) => void;
};

export type SpawnResult = {
  /** Child process pid. */
  pid: number;
  /** IPC channelId minted for this spawn. */
  channelId: string;
  /** Initial credentials snapshot pushed to the child. */
  credentialsSnapshot: CredentialsSnapshot;
};

export type CancelRequestOpts = {
  runId: string;
  origin: CancelOrigin;
  reason: string;
  /** ISO-8601 commit timestamp. */
  at: string;
};

export type CancelCommitInfo = {
  commitSha: string;
  seq: number;
};

export type DrainOpts = {
  /** Deadline carried on the drain frame for the child's logs; the supervisor-side timeout comes from bindings.drainTimeoutMs, not this value. */
  deadlineMs: number;
};

export type DeliverSignalOpts = {
  /** Run the signal targets. The child rejects a delivery whose runId is unknown. */
  runId: string;
  /** Signal name the run's `awaitSignal` step matches against. */
  signalName: string;
  /** Producer-supplied dedup id; the state machine rejects duplicates via observedSignalIds. */
  signalId: string;
  /** Opaque signal payload the awaiter resolves with. */
  payload: unknown;
};

export type DeliverSourcesOpts = {
  /** Ordered inference-source failover chain; element 0 is the active source. */
  sources: InferenceSource[];
  /** The default source id; the wire boundary requires it to equal `sources[0].id`. */
  defaultSource: string;
};

export type DeliverCredentialsOpts = {
  /** Refreshed credential material; the child merges it into its cell (no eviction by omission). */
  delivery: CredentialDelivery;
  /** CredentialIds to drop from the child's cell (a pure revocation pairs an empty delivery with these). */
  revoke?: string[];
};

export type RecycleOpts = {
  reason: string;
  /** Origin of the recycle request: operator (direct API call), policy (policy timer), or self (child's recycle.request). */
  origin?: RecycleOrigin;
};

type MailboxCallRequest = Extract<
  ControlPayload,
  { type: "mailbox.call.request" }
>["data"];

type MailboxCallResponse = Extract<
  ControlPayload,
  { type: "mailbox.call.response" }
>["data"];

type MailboxCallSuccess = Extract<MailboxCallResponse, { ok: true }>;

type MailboxCallSearchQuery = Extract<
  MailboxCallRequest,
  { op: "search" }
>["query"];

/** Search dates arrive as strings; a bad one is a failed call reported by the handler, not a frame rejection. */
function reviveSearchDates(query: MailboxCallSearchQuery): SearchQuery {
  const revived: SearchQuery = {};
  if (query.from !== undefined) revived.from = query.from;
  if (query.to !== undefined) revived.to = query.to;
  if (query.cc !== undefined) revived.cc = query.cc;
  if (query.bcc !== undefined) revived.bcc = query.bcc;
  if (query.header !== undefined) {
    revived.header = {
      field: query.header.field,
      contains: query.header.contains,
    };
  }
  if (query.before !== undefined)
    revived.before = reviveSearchDate(query.before);
  if (query.after !== undefined) revived.after = reviveSearchDate(query.after);
  if (query.on !== undefined) revived.on = reviveSearchDate(query.on);
  if (query.sentBefore !== undefined) {
    revived.sentBefore = reviveSearchDate(query.sentBefore);
  }
  if (query.sentAfter !== undefined) {
    revived.sentAfter = reviveSearchDate(query.sentAfter);
  }
  if (query.sentOn !== undefined)
    revived.sentOn = reviveSearchDate(query.sentOn);
  if (query.hasFlags !== undefined) revived.hasFlags = query.hasFlags;
  if (query.missingFlags !== undefined)
    revived.missingFlags = query.missingFlags;
  if (query.body !== undefined) revived.body = query.body;
  if (query.text !== undefined) revived.text = query.text;
  if (query.largerThan !== undefined) revived.largerThan = query.largerThan;
  if (query.smallerThan !== undefined) revived.smallerThan = query.smallerThan;
  if (query.and !== undefined) revived.and = query.and.map(reviveSearchDates);
  if (query.or !== undefined) revived.or = query.or.map(reviveSearchDates);
  if (query.not !== undefined) revived.not = reviveSearchDates(query.not);
  return revived;
}

function reviveSearchDate(value: string): Date {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid search date ${JSON.stringify(value)}`);
  }
  return date;
}

function missingMailbox(name: string): string {
  return `Mailbox "${name}" does not exist`;
}

/** Supervisor mailbox policy: refusals are decided here and never open the store. Create is CANNOT for every name; delete INBOX is CANNOT, other names NONEXISTENT. */
function refuseMailboxCall(
  data: MailboxCallRequest,
): { condition: MessageTransportCondition; reason: string } | undefined {
  switch (data.op) {
    case "createMailbox":
      return {
        condition: "CANNOT",
        reason: `Cannot create mailbox "${data.name}"`,
      };
    case "deleteMailbox":
      if (data.name !== MAILBOX_INBOX_DIR) {
        return { condition: "NONEXISTENT", reason: missingMailbox(data.name) };
      }
      return {
        condition: "CANNOT",
        reason: `Cannot delete mailbox "${data.name}"`,
      };
    case "move":
    case "copy":
      return refuseMoveOrCopy(data.ref.mailbox, data.toMailbox);
    case "createList":
    case "listMembers":
    case "subscribe":
    case "unsubscribe":
      return {
        condition: "CANNOT",
        reason: "Distribution list management is not implemented",
      };
    case "search":
    case "thread":
    case "sync":
    case "getMailboxStatus":
    case "append":
    case "watch":
      if (data.mailbox !== MAILBOX_INBOX_DIR) {
        return {
          condition: "NONEXISTENT",
          reason: missingMailbox(data.mailbox),
        };
      }
      return undefined;
    case "fetchHeaders":
    case "fetchStructure":
    case "fetchPart":
    case "fetchFull":
      if (data.ref.mailbox !== MAILBOX_INBOX_DIR) {
        return {
          condition: "NONEXISTENT",
          reason: missingMailbox(data.ref.mailbox),
        };
      }
      return undefined;
    case "listMailboxes":
    case "readMailPart":
      return undefined;
  }
}

function refuseMoveOrCopy(
  fromMailbox: string,
  toMailbox: string,
): { condition: MessageTransportCondition; reason: string } {
  if (fromMailbox !== MAILBOX_INBOX_DIR) {
    return { condition: "NONEXISTENT", reason: missingMailbox(fromMailbox) };
  }
  if (toMailbox !== MAILBOX_INBOX_DIR) {
    return { condition: "NONEXISTENT", reason: missingMailbox(toMailbox) };
  }
  return {
    condition: "CANNOT",
    reason: `Cannot move/copy a message within "${MAILBOX_INBOX_DIR}"`,
  };
}

function mailboxCallFailure(
  data: MailboxCallRequest,
  reason: string,
  condition?: MessageTransportCondition,
): Extract<MailboxCallResponse, { ok: false }> {
  if (condition === undefined) {
    return { requestId: data.requestId, ok: false, op: data.op, reason };
  }
  return {
    requestId: data.requestId,
    ok: false,
    op: data.op,
    reason,
    condition,
  };
}

function projectFetchedMessage(
  message: InboundMessage,
): Extract<MailboxCallSuccess, { op: "fetchFull" }>["value"] {
  const value: Extract<MailboxCallSuccess, { op: "fetchFull" }>["value"] = {
    ref: message.ref,
    headers: message.headers,
    flags: message.flags,
    signatureStatus: message.signatureStatus,
  };
  if (message.content !== undefined) value.content = message.content;
  if (message.payload !== undefined) value.payload = message.payload;
  if (message.attachments !== undefined) {
    value.attachments = message.attachments.map((attachment) => {
      const projected: {
        name: string;
        contentType: string;
        dataBase64: string;
        part?: string;
      } = {
        name: attachment.name,
        contentType: attachment.contentType,
        dataBase64: base64Encode(attachment.data),
      };
      if (attachment.part !== undefined) projected.part = attachment.part;
      return projected;
    });
  }
  return value;
}

/** Split one store delta on the caller's uidNext (new arrivals vs flag changes) and copy flags to arrays so JSON stays valid. */
function syncResultForCaller(
  mailbox: string,
  uidNext: number,
  result: MailboxSyncResult,
): Extract<MailboxCallSuccess, { op: "sync" }>["value"] {
  if (result.resync) {
    return {
      vanished: [],
      changed: [],
      newMessages: result.messages.map((message) => ({
        uid: message.uid,
        mailbox,
      })),
      fullResyncRequired: true,
    };
  }
  const newMessages: { uid: number; mailbox: string }[] = [];
  const changed: { uid: number; flags: string[] }[] = [];
  for (const message of result.changed) {
    if (message.uid >= uidNext) {
      newMessages.push({ uid: message.uid, mailbox });
    } else {
      changed.push({ uid: message.uid, flags: Array.from(message.flags) });
    }
  }
  return {
    vanished: [...result.vanished],
    changed,
    newMessages,
    fullResyncRequired: false,
  };
}

function projectPart(
  part: MessagePart,
): Extract<MailboxCallSuccess, { op: "fetchPart" }>["value"] {
  const value: Extract<MailboxCallSuccess, { op: "fetchPart" }>["value"] = {
    contentType: part.contentType,
    contentBase64: base64Encode(part.content),
  };
  if (part.encoding !== undefined) value.encoding = part.encoding;
  if (part.filename !== undefined) value.filename = part.filename;
  if (part.disposition !== undefined) value.disposition = part.disposition;
  return value;
}

/** Construct a per-deployment supervisor; all host dependencies come via bindings, never process.env or a singleton. */
export function createWorkflowSupervisor(
  bindings: WorkflowSupervisorBindings,
): WorkflowSupervisor {
  let state: SupervisorState = { phase: "idle" };
  let shutdownPromise: Promise<void> | null = null;
  // Replacement processes belong to shutdown until their ready handshake transfers ownership to the active state.
  const uninstalledChildren = new Set<SubprocessHandle>();
  // Live credential mirror seeded on every spawn and pre-trigger barrier; deliverCredentials mutates it so revocations stay durable. Kept outside the phase-union state so it survives transitions.
  let currentCredentialDelivery: CredentialDelivery | null =
    bindings.credentialDelivery !== undefined
      ? mergeCredentialDelivery(null, bindings.credentialDelivery, undefined)
      : null;
  /** RunIds the current cohort drives (supervisor-dispatched + self-discovered); drain() arms one accumulator per run from this. */
  const cohortRunIds = new Set<string>();
  /** Runs observed terminal in this process, closing the window before the working tree reflects the commit. Permanent for the deployment. */
  const terminalRunIds = new Set<string>();
  /** Per-run input correlation cache (from park.notify) so mail deliveries fire signal.deliver without a substrate read. Cleared on terminal event or cohort abort. */
  const runInputChannels = new Map<
    string,
    { correlationId: string; parkKind: "input" }
  >();
  /** Dispatch loops blocked on a park.notify for a runId; the handler resolves them so routing re-evaluates. */
  const parkNotifyWaiters = new Map<string, () => void>();
  /** Monotonic per-run input-park generation. waitForRunTerminalOrPark keys on the park edge (> sinceGen), so parks during pre-wait awaits are observed and stale entries cannot false-positive. Cleared on recycle/teardown. */
  const parkGenerations = new Map<string, number>();
  // D2 attribution (measurement-only): messageId the serial dispatch loop is servicing, keying proxied writes whose prefix carries no message identity.
  let currentDispatchMessageId: string | null = null;
  /** Per-run drainTimeout accumulators armed by drain(); shutdown stops them before teardown so no timer fires after disposal. */
  const drainAccumulators = new Map<string, DrainTimeoutAccumulator>();
  const accumulatorFactory =
    bindings.drainTimeoutAccumulatorFactory ?? createDrainTimeoutAccumulator;
  const drainNow = bindings.now ?? Date.now;
  const drainSetTimer =
    bindings.setTimer ?? ((cb: () => void, ms: number) => setTimeout(cb, ms));
  const drainClearTimer =
    bindings.clearTimer ??
    ((h: unknown) => {
      // drainSetTimer returns a setTimeout handle, so only Timeout objects flow here; the undefined branch is a no-op.
      if (h !== null && typeof h === "object") {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- handle round-trip: the matching `drainSetTimer` returns `ReturnType<typeof setTimeout>`; the accumulator preserves opaqueness, which forces a re-assertion here
        clearTimeout(h as ReturnType<typeof setTimeout>);
      }
    });
  const drainTimeoutMs = bindings.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
  const parkedQueryWatchdogMs =
    bindings.parkedQueryWatchdogMs ?? DEFAULT_PARKED_QUERY_WATCHDOG_MS;

  // Pure observability: call the dispatch-timing hook at the two per-message boundaries; a throwing observer is swallowed so it cannot wedge dispatch.
  function emitDispatchTiming(
    messageId: string,
    marker: "dispatch-start" | "reply-produced",
    atMs: number,
  ): void {
    const observer = bindings.onDispatchTiming;
    if (observer === undefined) return;
    try {
      observer({ kind: "roundtrip", messageId, marker, atMs });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`onDispatchTiming observer threw for ${messageId} (${marker}): ${message}`;
    }
  }

  // D2 per-leg attribution (measurement-only): paired start/end marks around each substrate leg; the end mark carries structural counters. Throwing observers are swallowed.
  function legMarkStart(messageId: string, leg: DispatchSubstrateLeg): number {
    if (bindings.onDispatchTiming === undefined) return 0;
    const atMs = performance.now();
    try {
      bindings.onDispatchTiming({
        kind: "leg",
        messageId,
        leg,
        phase: "start",
        atMs,
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`onDispatchTiming leg observer threw for ${messageId} (${leg} start): ${message}`;
    }
    return atMs;
  }

  function legMarkEnd(messageId: string, leg: DispatchSubstrateLeg): void {
    const observer = bindings.onDispatchTiming;
    if (observer === undefined) return;
    const atMs = performance.now();
    let counters: DispatchStructuralCounters | undefined;
    try {
      counters = sampleStructuralCounters(
        bindings.repoStore.getRepoDir(bindings.workflowRunRepoId),
      );
    } catch (cause) {
      // A throwing counter read must not perturb the measured leg; log it and emit the mark without counters.
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`structural-counter sample failed for ${messageId} (${leg}): ${message}`;
    }
    try {
      observer({
        kind: "leg",
        messageId,
        leg,
        phase: "end",
        atMs,
        ...(counters !== undefined ? { counters } : {}),
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`onDispatchTiming leg observer threw for ${messageId} (${leg} end): ${message}`;
    }
  }

  // §10c forced-repack A/B (measurement-only): every everyMessages-th dispatched message forces a repack; the serial dispatch loop is the sole writer so no commit interleaves.
  const repackToggle: RepackToggle | undefined = bindings.repackEveryMessages;
  let dispatchedSinceRepack = 0;
  function maybeRepack(runId: string): void {
    if (repackToggle === undefined) return;
    dispatchedSinceRepack += 1;
    if (dispatchedSinceRepack < repackToggle.everyMessages) return;
    dispatchedSinceRepack = 0;
    const repoDir = bindings.repoStore.getRepoDir(bindings.workflowRunRepoId);
    const result = forceRepack(repoDir);
    if (!result.ok) {
      logger.warn`forced repack failed after ${runId}: ${result.detail}`;
      return;
    }
    // The repack is not a per-message leg; log it so cadence and duration are visible without contaminating the leg series.
    const after = sampleStructuralCounters(repoDir);
    logger.info`forced repack after ${runId}: ${result.durationMs.toFixed(1)}ms; looseObjects now ${String(after.looseObjects)}, gitBytes now ${String(after.gitBytes)}`;
  }

  /** Map a proxied write's preservePrefix to a D2 leg and the dispatch message it belongs to. Returns null when no observer is wired, the prefix is unattributed, or no message is in flight. */
  function classifyProxiedWriteLeg(
    preservePrefix: string,
  ): { leg: DispatchSubstrateLeg; messageId: string } | null {
    if (bindings.onDispatchTiming === undefined) return null;
    if (currentDispatchMessageId === null) return null;
    if (/^runs\/[^/]+\/events\/$/.test(preservePrefix)) {
      return { leg: "runevent", messageId: currentDispatchMessageId };
    }
    if (preservePrefix.startsWith("agent-state/")) {
      return { leg: "wal", messageId: currentDispatchMessageId };
    }
    return null;
  }

  // Avoid clock sampling when no observer is wired (production hot path); the pre-dequeue sample keeps the claim-check read inside the measured interval.
  function dispatchTimingEnabled(): boolean {
    return bindings.onDispatchTiming !== undefined;
  }
  const inboxPrimitives: InboxPrimitives = bindings.inboxPrimitives ?? {
    enqueueInbox: defaultEnqueueInbox,
    dequeueToProcessing: defaultDequeueToProcessing,
    markConsumed: defaultMarkConsumed,
    replayProcessingToInbox: defaultReplayProcessingToInbox,
  };
  const deriveMailAuditRef: DeriveMailAuditRef =
    bindings.deriveMailAuditRef ?? defaultInProcessMailAuditRef;
  const defaultInboxWritePrincipal: WorkflowRunSupervisorPrincipal = {
    kind: "supervisor",
    anchorRunId: bindings.anchorRunId,
  };
  const inboxWritePrincipal: Principal =
    bindings.inboxWritePrincipal ?? defaultInboxWritePrincipal;
  // Resolve the consumed-dedup retention horizon once at the bindings edge; every markConsumed is threaded the value.
  const consumedRetentionMs =
    bindings.consumedRetentionMs ?? DEFAULT_CONSUMED_RETENTION_MS;
  // Resolve the ready-handshake timeout and its timers once; tests drive the race deterministically through the same injectable pair.
  const readyTimeoutMs = bindings.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS;
  const readySetTimer = bindings.setTimer ?? defaultSetTimer;
  const readyClearTimer = bindings.clearTimer ?? defaultClearTimer;
  // Resolve the crash-loop guard bounds once at the bindings edge; tests drive the guard through the existing timer/clock seams.
  const crashLoopMaxCount =
    bindings.crashLoopMaxCount ?? DEFAULT_CRASH_LOOP_MAX_COUNT;
  const crashLoopWindowMs =
    bindings.crashLoopWindowMs ?? DEFAULT_CRASH_LOOP_WINDOW_MS;
  const crashLoopStableResetMs =
    bindings.crashLoopStableResetMs ?? DEFAULT_CRASH_LOOP_STABLE_RESET_MS;
  const respawnBackoffInitialMs =
    bindings.respawnBackoffInitialMs ?? DEFAULT_RESPAWN_BACKOFF_INITIAL_MS;
  const respawnBackoffMaxMs =
    bindings.respawnBackoffMaxMs ?? DEFAULT_RESPAWN_BACKOFF_MAX_MS;
  const crashNow = bindings.recyclePolicyNow ?? defaultNow;
  /** Resolved on every enqueue so the idle dispatch loop returns to dequeueing immediately; swapped for a fresh promise on each wake. */
  let dispatchWake: { promise: Promise<void>; resolve: () => void } =
    makeDispatchWake();
  function makeDispatchWake(): {
    promise: Promise<void>;
    resolve: () => void;
  } {
    let resolver: () => void = () => undefined;
    const promise = new Promise<void>((resolve) => {
      resolver = resolve;
    });
    return { promise, resolve: resolver };
  }
  function wakeDispatch(): void {
    const prev = dispatchWake;
    dispatchWake = makeDispatchWake();
    prev.resolve();
  }
  /** Per-spawn context the recycle path respawns against; populated on spawn, cleared on shutdown. Never mutated by recycle. */
  let spawnContext: SpawnContext | null = null;
  let recyclePolicy: RecyclePolicy | null = null;
  // Mutual-exclusion latch shared by every respawn path; runRespawn sets/clears it, callers read it (recycle throws on contention, crash-respawn declines).
  let respawnInProgress = false;
  // Monotonic child-cohort generation, bumped on each transition to running; the exit-watcher ignores a stale generation.
  let childGeneration = 0;
  // Unexpected exit deferred while a respawn was in flight; drained by maybeHandleChildExit once the latch clears, stale generations dropped.
  let pendingChildExit: { generation: number; reason: string } | null = null;
  // Timestamps of recent unexpected exits (pruned to the window); the guard latches when the count reaches the max.
  const crashTimestamps: number[] = [];
  // Armed stable-run reset timer handle; re-armed on every transition to running, cleared on teardown.
  let stableRunResetTimer: unknown = null;
  // Current exponential respawn backoff (ms); doubles per respawn, resets when a respawned child runs stably.
  let respawnBackoffMs = respawnBackoffInitialMs;
  // Armed backoff waits (timer + resolver) so shutdown can cancel them all; a Set because a recycle can park a second coroutine while one is waiting.
  const respawnBackoffWaits = new Set<{
    timer: unknown;
    resolve: () => void;
  }>();

  // =====================================================================
  // CRASH-RESPAWN -- auto-recovery from an unexpected workflow-process exit
  // =====================================================================
  //
  // An unexpected exit is detected by watching `handle.exited` (the only
  // universal death signal: a clean death ends the channel readers without
  // a protocol crash callback). An exit is unexpected iff its generation is
  // still current and the phase is still `running`.
  //
  // On one, `handleUnexpectedChildExit` records the crash and either
  // RESPAWNS after an exponential backoff (reusing `runRespawn` with a
  // no-op drain) or LATCHES once the guard trips, tearing down to
  // `crash-looping` and committing a `RunFailed` so the loop is durably
  // observable. A respawned child that survives `crashLoopStableResetMs`
  // resets the counter and backoff. The backoff wait sits OUTSIDE the
  // respawn latch; the post-wait generation re-check stops a recycle that
  // installed a fresh cohort mid-wait from respawning it twice. Full
  // policy: `packages/workflow-host/README.md` "Respawn policy".

  // Frame-level violation on a live cohort's channel (clean deaths have no crash callback). On the running cohort, kill the child so the crash flows through the same respawn/crash-loop path; other phases own their teardown.
  function onChildCrash(reason: string): void {
    if (state.phase === "running") {
      logger.error`workflow-process channel crash on live cohort; forcing child down to respawn: ${reason}`;
      state.handle.kill();
      return;
    }
    logger.error`workflow-process channel crash: ${reason}`;
    // Only `recycling` is a self-termination the host must reclaim; running/starting/stopping own their teardown. Allowlist, so a future phase defaults to no self-terminate.
    const selfTerminated = state.phase === "recycling";
    void shutdownInternal({ reason, selfTerminated });
  }

  // Prune crash timestamps older than the sliding window relative to `nowMs`.
  function pruneCrashTimestamps(nowMs: number): void {
    const cutoff = nowMs - crashLoopWindowMs;
    while (true) {
      const oldest = crashTimestamps[0];
      if (oldest === undefined || oldest > cutoff) break;
      crashTimestamps.shift();
    }
  }

  function clearStableRunResetTimer(): void {
    if (stableRunResetTimer !== null) {
      readyClearTimer(stableRunResetTimer);
      stableRunResetTimer = null;
    }
  }

  // Arm the stable-run reset for the cohort that just reached running: if still live when the timer fires, reset the crash counter and backoff.
  function armStableRunResetTimer(generation: number): void {
    clearStableRunResetTimer();
    stableRunResetTimer = readySetTimer(() => {
      stableRunResetTimer = null;
      if (generation === childGeneration && state.phase === "running") {
        // Stable cohort: reset the crash counter and backoff so a flap-then-stable sequence starts over.
        crashTimestamps.length = 0;
        respawnBackoffMs = respawnBackoffInitialMs;
      }
    }, crashLoopStableResetMs);
  }

  // Wait the current backoff before respawning; cancellable so shutdown unblocks the parked coroutine, which then bails at its re-check.
  function waitRespawnBackoff(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const entry: { timer: unknown; resolve: () => void } = {
        timer: null,
        resolve,
      };
      entry.timer = readySetTimer(() => {
        respawnBackoffWaits.delete(entry);
        resolve();
      }, ms);
      respawnBackoffWaits.add(entry);
    });
  }

  // Cancel every armed backoff wait. Idempotent.
  function cancelRespawnBackoffWaits(): void {
    for (const entry of respawnBackoffWaits) {
      readyClearTimer(entry.timer);
      entry.resolve();
    }
    respawnBackoffWaits.clear();
  }

  // Bump the generation and arm the handle.exited watcher atomically with the running swap; exited resolving or rejecting both mean the process is gone.
  function armChildForRunning(handle: SubprocessHandle): void {
    childGeneration += 1;
    const generation = childGeneration;
    void handle.exited
      .then(() => {
        onChildExited(generation, "workflow-process child exited");
      })
      .catch(() => {
        onChildExited(
          generation,
          "workflow-process child exited (exit promise rejected)",
        );
      });
  }

  // Record a child exit and act on it; stale exits (a newer cohort already installed) are dropped.
  function onChildExited(generation: number, reason: string): void {
    if (generation !== childGeneration) return;
    pendingChildExit = { generation, reason };
    maybeHandleChildExit();
  }

  // Drain a recorded exit when actionable; declines while a respawn is in flight (runRespawn's finally re-invokes this) and drops stale or post-running exits.
  function maybeHandleChildExit(): void {
    if (respawnInProgress) return;
    const pending = pendingChildExit;
    if (pending === null) return;
    if (pending.generation !== childGeneration) {
      pendingChildExit = null;
      return;
    }
    if (state.phase !== "running") return;
    pendingChildExit = null;
    void handleUnexpectedChildExit(pending.reason).catch((cause) => {
      // Fire-and-forget: runRespawn already tore down to a terminal state, so surface the failure and stop.
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`crash-respawn handling failed; deployment torn down: ${message}`;
    });
  }

  // Record one unexpected exit against the crash-loop guard and either latch the deployment or respawn.
  async function handleUnexpectedChildExit(reason: string): Promise<void> {
    if (state.phase !== "running" || spawnContext === null) {
      // Raced a shutdown/recycle; the owning lifecycle path handles teardown.
      return;
    }
    // Disarm the dead cohort's stable-run timer up front: during the backoff the phase and generation still read as that cohort, so the timer would wrongly clear the counter.
    clearStableRunResetTimer();
    // Capture the crashing cohort's generation; the post-wait guard bails if a recycle installed a fresher cohort meanwhile.
    const armedGeneration = childGeneration;
    const nowMs = crashNow();
    crashTimestamps.push(nowMs);
    pruneCrashTimestamps(nowMs);
    if (crashTimestamps.length >= crashLoopMaxCount) {
      // Crash-loop latch: stop respawning and tear down to `crash-looping` so a flapping child cannot saturate the host.
      const crashCount = crashTimestamps.length;
      logger.error`workflow-process crash-looped: ${String(crashCount)} unexpected exits within ${String(crashLoopWindowMs)}ms; stopping the deployment (${reason})`;
      // The RunFailed tombstone is the only durable, queryable crash-loop signal (the phase is in-memory only). Best-effort: a missed write costs observability, not correctness.
      await shutdownInternal({
        reason: `crash-loop: ${reason}`,
        terminalPhase: "crash-looping",
        selfTerminated: true,
        terminalCommit: async () => {
          // The tombstone lands on the deployment's top-level run (deriveWorkflowRunId of the mail address), NOT the repo slug `anchorRunId`: the ids differ when the domain carries a suffix, and the repo slug would strand the write where no reader consults it.
          await commitRunFailed({
            substrate: bindings.repoStore,
            repoId: bindings.workflowRunRepoId,
            ref: bindings.workflowRunRef,
            anchorRunId: bindings.anchorRunId,
            runId: deriveWorkflowRunId(bindings.deploymentMailAddress),
            at: new Date(nowMs).toISOString(),
            message: `workflow-process crash-looped: ${String(crashCount)} unexpected exits within ${String(crashLoopWindowMs)}ms`,
          });
        },
      });
      return;
    }
    const thisBackoffMs = respawnBackoffMs;
    logger.warn`workflow-process exited unexpectedly; respawning after ${String(thisBackoffMs)}ms backoff (${reason})`;
    await waitRespawnBackoff(thisBackoffMs);
    // A recycle or shutdown may have run during the wait; bail unless this dead cohort is still the running one. No await separates this re-check from runRespawn's latch set.
    if (
      childGeneration !== armedGeneration ||
      state.phase !== "running" ||
      spawnContext === null
    ) {
      // Report the bail too, so a promised respawn that never runs is not silently dropped.
      logger.info`respawn backoff elapsed but the crashed cohort is no longer the running one; skipping respawn (phase=${state.phase}, armed generation ${String(armedGeneration)}, current generation ${String(childGeneration)}): ${reason}`;
      return;
    }
    const priorRunning = state;
    const priorContext = spawnContext;
    // Advance the backoff only now that a respawn is actually happening.
    respawnBackoffMs = Math.min(respawnBackoffMs * 2, respawnBackoffMaxMs);
    await runRespawn({
      origin: "crash",
      reason,
      prior: priorRunning,
      priorContext,
      // The child is already dead; runRespawn's kill is a no-op, but the replay step still moves stranded mail back to the inbox tail.
      drain: async () => undefined,
    });
    // Arm the stable-run reset for the respawned cohort so stability resets the counter and backoff.
    armStableRunResetTimer(childGeneration);
  }

  // Eager per-run mailbox (§3b inbound): commit each arrival into the substrate INBOX and notify the child so the warm agent's watch/mail_wait sees it mid-turn. The supervisor is the sole mailbox writer; the store is a lazy in-memory mirror of the committed subtree.
  const mailboxWritePrincipal: WorkflowRunSupervisorPrincipal = {
    kind: "supervisor",
    anchorRunId: bindings.anchorRunId,
  };
  let mailboxStore: SubstrateMailboxStore | null = null;
  // messageId -> mailbox uid for flagging dispatched turns \Seen/$Processed. In-memory only: a missing entry skips a cosmetic mark, never a delivery guarantee. Pruned once the mark completes.
  const mailboxUidByMessageId = new Map<string, number>();
  // Serializes every mailbox mutation so concurrent arrivals and flag marks never interleave against the shared mirror.
  let mailboxTail: Promise<void> = Promise.resolve();
  function runMailboxExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = mailboxTail.then(fn, fn);
    mailboxTail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }
  async function getMailboxStore(): Promise<SubstrateMailboxStore> {
    if (mailboxStore === null) {
      mailboxStore = await createSubstrateMailboxStore({
        substrate: bindings.repoStore,
        repoId: bindings.workflowRunRepoId,
        principal: mailboxWritePrincipal,
        ref: bindings.workflowRunRef,
      });
    }
    return mailboxStore;
  }

  // The long-lived mirror drops raw bytes on flush and cannot read them back; opens a fresh committed snapshot (after flushing pending writes) for reads.
  async function openCommittedMailbox(): Promise<SubstrateMailboxStore> {
    const writer = await getMailboxStore();
    if (writer.pendingWrites) await writer.flush();
    return createSubstrateMailboxStore({
      substrate: bindings.repoStore,
      repoId: bindings.workflowRunRepoId,
      principal: mailboxWritePrincipal,
      ref: bindings.workflowRunRef,
    });
  }

  function storedEnvelopeFromHeaders(
    headers: MessageHeaders,
    messageId: string,
  ): StoredEnvelope {
    // No envelope date when the message named none: arrival time is this process's observation, not the sender's date.
    const parsed =
      headers.date === undefined ? undefined : new Date(headers.date);
    const date =
      parsed === undefined || Number.isNaN(parsed.getTime())
        ? undefined
        : parsed;
    return {
      // Use the header's own id so the envelope id and inReplyTo/references come from one parser.
      messageId: headers.messageId ?? messageId,
      from: headers.from,
      to: headers.to,
      subject: headers.subject ?? "",
      date,
      inReplyTo: headers.inReplyTo,
      references: headers.references ?? [],
      interchangeType: headers.interchangeType,
      interchangeCorrelationId: headers.interchangeCorrelationId,
    };
  }

  /** Eager-commit an arrival into the substrate mailbox and notify the child, before and independent of FIFO dispatch. Best-effort: a fault here never withholds the ack (the claim-check inbox is the durable contract); the notify is sent only after the append flushes. */
  async function commitInboundToMailbox(
    messageId: string,
    rawMessage: Uint8Array,
  ): Promise<void> {
    try {
      await runMailboxExclusive(async () => {
        // Belt-and-suspenders against a double append of the same messageId; the caller already gates on a fresh enqueued outcome.
        if (mailboxUidByMessageId.has(messageId)) return;
        let decoded: ReturnType<typeof decodeMail>;
        try {
          decoded = decodeMail(rawMessage);
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`eager mailbox commit: dropping undecodable inbound mail ${messageId}: ${message}`;
          return;
        }
        const store = await getMailboxStore();
        const uid = store.append(
          rawMessage,
          storedEnvelopeFromHeaders(decoded.headers, messageId),
          [],
        );
        mailboxUidByMessageId.set(messageId, uid);
        await store.flush();
        const commit = await bindings.repoStore.resolveRef(
          mailboxWritePrincipal,
          bindings.workflowRunRepoId,
          bindings.workflowRunRef,
        );
        if (commit === null) {
          logger.error`eager mailbox commit: ${bindings.workflowRunRef} did not resolve after flush; skipping mailbox.notify for ${messageId}`;
          return;
        }
        const sender = activeControlSender();
        if (sender === null) {
          logger.info`eager mailbox commit: no active control sender; committed ${messageId} as uid ${String(uid)} without mailbox.notify`;
          return;
        }
        await sender.send({
          type: "mailbox.notify",
          data: {
            runId: deriveWorkflowRunId(bindings.deploymentMailAddress),
            mailbox: MAILBOX_INBOX_DIR,
            uid,
            headers: decoded.headers,
          },
        });
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`eager mailbox commit failed for ${messageId}; mail still delivered via claim-check dispatch: ${message}`;
    }
  }

  /** Flag a dispatched message's mailbox entry \Seen/$Processed. Fire-and-forget: a missing uid or substrate fault is logged and dropped, never failing the turn. */
  function markMailboxProcessed(messageId: string): void {
    const uid = mailboxUidByMessageId.get(messageId);
    if (uid === undefined) return;
    void runMailboxExclusive(async () => {
      try {
        const store = await getMailboxStore();
        if (store.find(uid) === undefined) return;
        store.addFlags(uid, [MAILBOX_FLAG_SEEN, MAILBOX_FLAG_PROCESSED]);
        await store.flush();
      } finally {
        // Drop the entry once the mark runs to bound the map; dedup is owned by the durable inbox index, and the delete stays inside the exclusive section.
        mailboxUidByMessageId.delete(messageId);
      }
    }).catch((cause) => {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`mailbox flag mark failed for ${messageId} (uid ${String(uid)}): ${message}`;
    });
  }

  // Resolves when the mail is durably accepted (ACK to the host) and rejects otherwise (WITHHOLD: the hub redelivers).
  async function onMailMessage(rawMessage: Uint8Array): Promise<void> {
    // Every inbound mail flows through the FIFO claim-check inbox, drained in arrival order by the dispatch loop (spawn and recycle both start one). The per-repo lock serializes enqueues against drains; the receivedAt filename prefix preserves order.
    if (
      state.phase === "idle" ||
      state.phase === "stopping" ||
      state.phase === "stopped" ||
      state.phase === "crash-looping"
    ) {
      // Tearing down: reject (WITHHOLD) so the hub redelivers to a later generation or exhausts its retry budget -- never silently drops.
      throw new Error(
        `inbound mail not accepted: supervisor phase is "${state.phase}"`,
      );
    }
    try {
      await enqueueInboundMail(rawMessage);
    } catch (cause) {
      // Both branches WITHHOLD (rethrow); the split only sets log severity.
      if (cause instanceof StaleInboxEnqueueError) {
        logger.error`inbound mail refused as stale, withholding ack (hub will redeliver): ${cause.message}`;
      } else {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`enqueueInbox failed, withholding ack (hub will redeliver): ${message}`;
      }
      throw cause;
    }
  }

  async function enqueueInboundMail(rawMessage: Uint8Array): Promise<void> {
    const messageId = await deriveMessageId(rawMessage);
    const mailAuditRef: MailAuditRef = deriveMailAuditRef(
      messageId,
      rawMessage,
    );
    const receivedAt = Date.now();
    // Inline the raw bytes on the claim-check envelope so the child can recover its step input at trigger.fired; dropped when markConsumed writes the dedup index.
    const rawMessageBase64 = base64Encode(rawMessage);
    // D2 leg: enqueueInbox runs outside the dispatch-start..reply-produced window; mark it with the same per-message key so the fit groups it with the rest.
    legMarkStart(messageId, "enqueue");
    const outcome = await inboxPrimitives.enqueueInbox(
      bindings.repoStore,
      inboxWritePrincipal,
      bindings.workflowRunRepoId,
      {
        address: bindings.deploymentMailAddress,
        messageId,
        receivedAt,
        mailAuditRef,
        rawMessage: rawMessageBase64,
      },
    );
    legMarkEnd(messageId, "enqueue");
    // Wake the dispatch loop only for a fresh enqueue; an already-present outcome adds nothing, and both outcomes ack.
    if (outcome.outcome === "enqueued") {
      // Eager-commit and notify BEFORE waking dispatch so mail_wait sees it committed; non-fatal, never withholds the ack.
      await commitInboundToMailbox(messageId, rawMessage);
      wakeDispatch();
    } else {
      // Redelivery of a durably-present message: ack fires, no new dispatch; log so at-least-once is observably effective-once.
      logger.info`inbound mail ${messageId} already durably present (${outcome.reason}); acknowledging without re-dispatch`;
    }
  }

  /** Pump child-initiated upstream control frames after ready. Unrecognized frames are dropped after a logged warning (the receiver already validated envelope and signature). */
  async function pumpUpstreamControl(
    iter: AsyncGenerator<ControlPayload, void, void>,
    cohortBroadcaster: TerminalBroadcaster,
  ): Promise<void> {
    for await (const payload of iter) {
      if (payload.type === "cancel.prepared") {
        const pending = pendingCancellations.get(payload.data.requestId);
        if (pending?.broadcaster === cohortBroadcaster)
          pending.resolve(payload.data.error);
        continue;
      }
      if (payload.type === "recycle.request") {
        logger.info`workflow-process self-initiated recycle.request: ${payload.data.reason}`;
        // Run the recycle off the iterator so it keeps draining; the recycle tears the iterator down via the child kill.
        void recycle({
          reason: `self-initiated: ${payload.data.reason}`,
          origin: "self",
        }).catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`self-initiated recycle failed: ${message}`;
        });
        return;
      }
      if (payload.type === "substrate.write.request") {
        // Run the write off the iterator so the merge.response that resolves it can still be consumed (blocking here would deadlock the round-trip).
        ownDetachedWrite(
          handleSubstrateWriteRequest(payload.data).catch((cause) => {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.error`substrate.write.request handler crashed: ${message}`;
          }),
        );
        continue;
      }
      if (payload.type === "substrate.merge.response") {
        // Resume the pending merge round-trip; the handler resolves the per-write awaiter in the merge callback.
        resolveMergeResponse(payload.data);
        continue;
      }
      if (payload.type === "outbound.message") {
        // OUTBOUND half of mailbox ownership (§3a): the supervisor performs the signed send through the host transport. Off the iterator so it keeps draining; the handler owns the outbound.result reply.
        void handleOutboundMessage(payload.data).catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`outbound.message handler crashed: ${message}`;
        });
        continue;
      }
      if (payload.type === "mailbox.mutate.request") {
        // INBOUND half of mailbox ownership (§3b): the child asks the sole mailbox writer to apply a flag write or expunge. Off the iterator; the handler owns the response.
        ownDetachedWrite(
          handleMailboxMutation(payload.data).catch((cause) => {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.error`mailbox.mutate.request handler crashed: ${message}`;
          }),
        );
        continue;
      }
      if (payload.type === "mailbox.call.request") {
        // A mailbox read, append, or refusal. Off the iterator; the handler owns the response.
        void handleMailboxCall(payload.data).catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.error`mailbox.call.request handler crashed: ${message}`;
        });
        continue;
      }
      if (payload.type === "terminal.event") {
        // Mirror every terminal-run commit to the COHORT's broadcaster (captured at pump start). A stale frame from the old cohort must never settle the new cohort's wait -- runs share the stable runId, so a wrong-cohort notify would falsely mark a live run consumed. Cohort dispose turns post-dispose notifies into no-ops.
        const event = terminalEventFromPayload(payload.data);
        cohortBroadcaster.notify(payload.data.runId, event);
        terminalRunIds.add(payload.data.runId);
        // Clean up cohort tracking for the terminated run (self-discovered runs have no dispatch-loop entry, so their cleanup happens here).
        cohortRunIds.delete(payload.data.runId);
        runInputChannels.delete(payload.data.runId);
        continue;
      }
      if (payload.type === "park.notify") {
        // The child reported a control-plane suspension: an agent step parked on a reserved signal channel.
        if (payload.data.parkKind === "input") {
          // Input parks are supervisor-owned. Register cohort membership BEFORE the correlationId (routing-hygiene invariant), then cache it so dispatch fires signal.deliver without a substrate round-trip.
          cohortRunIds.add(payload.data.runId);
          runInputChannels.set(payload.data.runId, {
            correlationId: payload.data.correlationId,
            parkKind: "input",
          });
          // Stop any drain accumulator for a parked run; a drain arms none for a run that already holds an input channel.
          const accumulator = drainAccumulators.get(payload.data.runId);
          if (accumulator !== undefined) {
            accumulator.stop();
            drainAccumulators.delete(payload.data.runId);
          }
        } else if (payload.data.parkKind === "approval") {
          // Approval parks are hub-registered through registerSuspension.
          registerSuspension({
            runId: payload.data.runId,
            correlationId: payload.data.correlationId,
            parkKind: "approval",
            ...(payload.data.snapshot !== undefined
              ? { snapshot: payload.data.snapshot }
              : {}),
          });
        } else {
          // signal-relay parks never ride park.notify (the section runtime relays them); one arriving here is a protocol violation -- log and drop.
          logger.error`park.notify for run ${payload.data.runId} carried parkKind=${payload.data.parkKind}, which is not a hub-registered kind; dropping`;
        }
        // A park of any kind suspends the run, so release a waiting dispatch loop here. Bump the generation BEFORE resolving the waiter so a loop that captured sinceGen sees the newer value.
        parkGenerations.set(
          payload.data.runId,
          (parkGenerations.get(payload.data.runId) ?? 0) + 1,
        );
        // Wake any dispatch loop waiting for this run to park.
        resolveParkNotifyWaiter(payload.data.runId);
        continue;
      }
      if (payload.type === "parked-correlations.response") {
        // The child answered a parked-correlations query; a response with no pending entry is logged and dropped, never thrown.
        resolveParkedResponse(payload.data);
        continue;
      }
      if (payload.type === "resumed.runs") {
        // Seed cohort tracking with runs the child self-discovered after reconnect or recycle.
        for (const runId of payload.data.runIds) {
          cohortRunIds.add(runId);
        }
        continue;
      }
      logger.warn`workflow-process upstream control payload ignored: type=${payload.type}`;
    }
  }

  // Pending merge round-trips keyed by the child's requestId; each entry lives across one round-trip, and the substrate may invoke the merge callback multiple times per write (lock-retry semantics).
  type PendingMerge = {
    resolve: (
      result:
        | { ok: true; files: Record<string, string | Uint8Array> }
        | { ok: false; reason: string },
    ) => void;
  };
  const pendingMerges = new Map<string, PendingMerge>();
  const pendingCancellations = new Map<
    string,
    {
      broadcaster: TerminalBroadcaster;
      resolve: (error: string | undefined) => void;
    }
  >();
  let cancellationSeq = 0;
  const cancellationCommits = new Set<Promise<CancelCommitInfo>>();
  // Repository writes started off the control pump; each settles without rejecting (handlers log failures).
  const detachedWrites = new Set<Promise<unknown>>();

  function ownDetachedWrite(write: Promise<unknown>): void {
    detachedWrites.add(write);
    void write.then(() => {
      detachedWrites.delete(write);
    });
  }

  // Every dispatch loop still running; a recycle retires the old loop while it finishes its last message.
  const dispatchLoops = new Set<Promise<void>>();

  function ownDispatchLoop(loop: Promise<void>): void {
    dispatchLoops.add(loop);
    const release = () => {
      dispatchLoops.delete(loop);
    };
    void loop.then(release, release);
  }

  /** Reject every pending merge round-trip and park-notify waiter on cohort transitions, so no closure sits on a resolver the dying control channel will never invoke. */
  function rejectCohortAwaiters(reason: string): void {
    for (const pending of pendingCancellations.values())
      pending.resolve(`cohort aborted: ${reason}`);
    pendingCancellations.clear();
    for (const [requestId, entry] of pendingMerges) {
      pendingMerges.delete(requestId);
      entry.resolve({ ok: false, reason: `cohort aborted: ${reason}` });
    }
    for (const [requestId, entry] of pendingParkedQueries) {
      pendingParkedQueries.delete(requestId);
      entry.settle(null);
    }
    for (const [runId, resolve] of parkNotifyWaiters.entries()) {
      parkNotifyWaiters.delete(runId);
      resolve();
    }
  }

  function resolveMergeResponse(
    data: Extract<ControlPayload, { type: "substrate.merge.response" }>["data"],
  ): void {
    const entry = pendingMerges.get(data.requestId);
    if (entry === undefined) {
      logger.warn`substrate.merge.response landed with no pending entry; requestId=${data.requestId} dropped`;
      return;
    }
    pendingMerges.delete(data.requestId);
    if (data.result.ok) {
      const files: Record<string, string | Uint8Array> = {};
      try {
        for (const file of data.result.files) {
          files[file.path] = base64ToBytes(file.contentBase64);
        }
      } catch (cause) {
        // base64ToBytes throws on malformed child content; a throw here would tear the pump down, so resolve the merge as a failure instead (mirrors the child-side hardening).
        const reason = cause instanceof Error ? cause.message : String(cause);
        entry.resolve({
          ok: false,
          reason: `supervisor substrate.merge.response: decode failed: ${reason}`,
        });
        return;
      }
      entry.resolve({ ok: true, files });
      return;
    }
    entry.resolve({ ok: false, reason: data.result.reason });
  }

  // Stamp the deployment identity onto a child-supplied park and hand it to the host's register sink (park.notify and re-emit both route here). Best-effort: a throwing sink is logged, never rethrown.
  function registerSuspension(park: {
    runId: string;
    correlationId: string;
    parkKind: "approval";
    snapshot?: ApprovalSnapshot;
  }): void {
    if (bindings.onSuspensionRegister === undefined) {
      logger.warn`suspension register for runId=${park.runId} but no onSuspensionRegister sink is wired; correlation ${park.correlationId} not registered`;
      return;
    }
    try {
      bindings.onSuspensionRegister({
        runId: park.runId,
        correlationId: park.correlationId,
        kind: park.parkKind,
        anchorRunId: bindings.anchorRunId,
        agentAddress: bindings.deploymentMailAddress,
        ...(park.snapshot !== undefined
          ? { approvalSnapshot: park.snapshot }
          : {}),
      });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`onSuspensionRegister sink threw for runId=${park.runId} correlationId=${park.correlationId}: ${message}`;
    }
  }

  // Pending reEmitParkedCorrelations queries keyed by the supervisor-minted requestId; the counter lives in the factory closure so a recycle cannot let a late old-child response resolve a new query.
  type ParkedCorrelations = Extract<
    ControlPayload,
    { type: "parked-correlations.response" }
  >["data"]["parked"];
  // Settle with the child's parked list, or null on cohort teardown -- a settle-with-sentinel, never a reject, so a teardown abort cannot surface as an unhandled rejection.
  type PendingParkedQuery = {
    settle: (parked: ParkedCorrelations | null) => void;
  };
  const pendingParkedQueries = new Map<string, PendingParkedQuery>();
  let parkedQuerySeq = 0;

  function resolveParkedResponse(
    data: Extract<
      ControlPayload,
      { type: "parked-correlations.response" }
    >["data"],
  ): void {
    const entry = pendingParkedQueries.get(data.requestId);
    if (entry === undefined) {
      logger.warn`parked-correlations.response landed with no pending entry; requestId=${data.requestId} dropped`;
      return;
    }
    pendingParkedQueries.delete(data.requestId);
    entry.settle(data.parked);
  }

  async function reEmitParkedCorrelations(): Promise<void> {
    // No-op (not throw) when the child is not addressable: unlike deliverSignal, the next re-establishment re-drives this, so skipping is correct, not a missed guard.
    if (state.phase !== "running" && state.phase !== "starting") {
      logger.info`reEmitParkedCorrelations: child not addressable (phase=${state.phase}); skipping`;
      return;
    }
    const controlSender = state.controlSender;
    const requestId = `pc-${String((parkedQuerySeq += 1))}`;
    const responded = new Promise<ParkedCorrelations | null>((resolve) => {
      pendingParkedQueries.set(requestId, { settle: resolve });
    });
    // Watchdog: a wedged-but-alive child never aborts the cohort, so bound the await; on expiry drop the entry and let the next re-establishment re-drive.
    let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
    const watchdog = new Promise<"timeout">((resolve) => {
      timeoutHandle = setTimeout(() => {
        timeoutHandle = null;
        if (pendingParkedQueries.delete(requestId)) {
          logger.warn`reEmitParkedCorrelations: requestId=${requestId} did not respond within ${String(parkedQueryWatchdogMs)}ms; re-registration retries on the next re-establishment`;
        }
        resolve("timeout");
      }, parkedQueryWatchdogMs);
    });
    let outcome: ParkedCorrelations | null | "timeout";
    try {
      await controlSender.send({
        type: "parked-correlations.request",
        data: { requestId },
      });
      outcome = await Promise.race([responded, watchdog]);
    } catch (cause) {
      // The downstream send failed (a closing pipe); drop the entry and let the next re-establishment re-drive.
      pendingParkedQueries.delete(requestId);
      if (timeoutHandle !== null) clearTimeout(timeoutHandle);
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.warn`reEmitParkedCorrelations query failed: ${message}; re-registration retries on the next re-establishment`;
      return;
    }
    if (timeoutHandle !== null) clearTimeout(timeoutHandle);
    // timeout or null: nothing to re-emit; the next re-establishment re-drives.
    if (outcome === "timeout" || outcome === null) return;
    for (const parked of outcome) {
      if (parked.parkKind === "input") {
        // Register cohort membership BEFORE the input channel so a live resumed run is never dropped as a dead one.
        cohortRunIds.add(parked.runId);
        runInputChannels.set(parked.runId, {
          correlationId: parked.correlationId,
          parkKind: "input",
        });
      } else {
        registerSuspension({ ...parked, parkKind: "approval" });
      }
    }
  }

  function resolveParkNotifyWaiter(runId: string): void {
    const resolve = parkNotifyWaiters.get(runId);
    if (resolve === undefined) return;
    parkNotifyWaiters.delete(runId);
    resolve();
  }

  /** OUTBOUND half of mailbox ownership (§3a): the child forwards the outbound message and sender address; the supervisor signs and sends via the host transport so the mail carries the agent's signature. Failures surface to the child as { ok: false, reason }. */
  async function handleOutboundMessage(
    data: Extract<ControlPayload, { type: "outbound.message" }>["data"],
  ): Promise<void> {
    const controlSender = activeControlSender();
    if (controlSender === null) {
      // Mid-recycle/teardown: no sender to write outbound.result on; the child's awaiter fails on pipe close. Drop and log.
      logger.warn`outbound.message received outside running phase; requestId=${data.requestId} dropped (child awaiter will fail on pipe close)`;
      return;
    }
    try {
      const message = outboundMessageFromPayload(data.message);
      // completeReferences marks a connector reply (inReplyTo with no References): grow its chain from the in-mailbox parent only then. An already-present references is the caller's chain; a missing parent leaves it unset and the transport derives [inReplyTo].
      if (
        data.completeReferences === true &&
        message.inReplyTo !== undefined &&
        message.references === undefined
      ) {
        const inReplyTo = message.inReplyTo;
        const references = await runMailboxExclusive(async () => {
          const store = await openCommittedMailbox();
          const parent = store.messages.find(
            (m) => m.envelope.messageId === inReplyTo,
          );
          if (parent === undefined) return undefined;
          return [...parent.envelope.references, parent.envelope.messageId];
        });
        if (references !== undefined && references.length > 0) {
          message.references = references;
        }
      }
      const receipt = await bindings.mailBus.sendOutbound(
        data.senderAddress,
        message,
      );
      await controlSender.send({
        type: "outbound.result",
        data: {
          requestId: data.requestId,
          result: {
            ok: true,
            messageId: receipt.messageId,
            status: receipt.status,
          },
        },
      });
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      await controlSender.send({
        type: "outbound.result",
        data: {
          requestId: data.requestId,
          result: { ok: false, reason },
        },
      });
    }
  }

  /** Apply a child-requested mailbox mutation (INBOUND half, §3b): flag writes target one uid, expunge sweeps \Deleted. Applied under runMailboxExclusive and flushed before the reply so the child's next committed read sees it. Failures return { ok: false, reason }. */
  async function handleMailboxMutation(
    data: Extract<ControlPayload, { type: "mailbox.mutate.request" }>["data"],
  ): Promise<void> {
    // Capture the sender once: re-fetching after the flush could misroute the reply to a successor cohort. Null means mid-recycle/teardown -- drop and warn.
    const controlSender = activeControlSender();
    if (controlSender === null) {
      logger.warn`mailbox.mutate.request received outside running phase; requestId=${data.requestId} dropped (child awaiter will fail on pipe close)`;
      return;
    }
    // The supervisor owns exactly one mailbox (the substrate INBOX); reject any other name rather than silently mutate the wrong target.
    if (data.mailbox !== MAILBOX_INBOX_DIR) {
      await controlSender.send({
        type: "mailbox.mutate.response",
        data: {
          requestId: data.requestId,
          result: {
            ok: false,
            reason: `unknown mailbox "${data.mailbox}"; only ${MAILBOX_INBOX_DIR} is writable`,
            condition: "NONEXISTENT",
          },
        },
      });
      return;
    }
    try {
      const expungedUids = await runMailboxExclusive(async () => {
        const store = await getMailboxStore();
        if (data.op === "expunge") {
          // Snapshot the \Deleted uids before removing; the sweep runs synchronously under the lock, so no target can vanish first.
          const uids = store.messages
            .filter((m) => m.flags.has(MAILBOX_FLAG_DELETED))
            .map((m) => m.uid);
          for (const uid of uids) {
            store.remove(uid);
            // Drop map entries pointing at removed uids (not load-bearing; keeps the map from retaining dead uids).
            for (const [messageId, mappedUid] of mailboxUidByMessageId) {
              if (mappedUid === uid) mailboxUidByMessageId.delete(messageId);
            }
          }
          await store.flush();
          return uids;
        }
        if (data.op === "addFlags") {
          store.addFlags(data.uid, data.flags);
        } else {
          store.removeFlags(data.uid, data.flags);
        }
        await store.flush();
        return undefined;
      });
      await controlSender.send({
        type: "mailbox.mutate.response",
        data: {
          requestId: data.requestId,
          result:
            expungedUids === undefined
              ? { ok: true }
              : { ok: true, expungedUids },
        },
      });
    } catch (cause) {
      // Reply on the captured sender; a broken-pipe reply propagates to the pump's catch and the child's awaiter is rejected by cancelAll. Accepted: a mutation can flush while its reply is undeliverable.
      const reason = cause instanceof Error ? cause.message : String(cause);
      await controlSender.send({
        type: "mailbox.mutate.response",
        data: {
          requestId: data.requestId,
          result: { ok: false, reason },
        },
      });
    }
  }

  /** Answer one mailbox operation. Refusals never open the store; reads/sync/status/append run under runMailboxExclusive and reply on the sender captured before the lock. readMailPart reads committed blobs without the lock; fetchFull verifies with no key; append does not notify. */
  async function handleMailboxCall(data: MailboxCallRequest): Promise<void> {
    const controlSender = activeControlSender();
    if (controlSender === null) {
      logger.warn`mailbox.call.request received outside running phase; requestId=${data.requestId} dropped (child awaiter will fail on pipe close)`;
      return;
    }
    const refusal = refuseMailboxCall(data);
    if (refusal !== undefined) {
      await controlSender.send({
        type: "mailbox.call.response",
        data: mailboxCallFailure(data, refusal.reason, refusal.condition),
      });
      return;
    }
    try {
      const answer = await answerMailboxCall(data);
      await controlSender.send({
        type: "mailbox.call.response",
        data: answer,
      });
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      const condition = isMessageTransportError(cause)
        ? cause.condition
        : undefined;
      await controlSender.send({
        type: "mailbox.call.response",
        data: mailboxCallFailure(data, reason, condition),
      });
    }
  }

  async function answerMailboxCall(
    data: MailboxCallRequest,
  ): Promise<MailboxCallSuccess> {
    switch (data.op) {
      case "listMailboxes":
        return {
          requestId: data.requestId,
          ok: true,
          op: "listMailboxes",
          value: [{ name: MAILBOX_INBOX_DIR }],
        };
      case "watch":
        return { requestId: data.requestId, ok: true, op: "watch" };
      case "readMailPart": {
        // The blobs were committed under this principal and ref; any other identity reads a different tree.
        const reader = createMailPartReader({
          substrate: bindings.repoStore,
          repoId: bindings.workflowRunRepoId,
          principal: mailboxWritePrincipal,
          ref: bindings.workflowRunRef,
        });
        const bytes = await reader.read(data.partRef);
        return {
          requestId: data.requestId,
          ok: true,
          op: "readMailPart",
          value: { contentBase64: base64Encode(bytes) },
        };
      }
      case "search": {
        const query = reviveSearchDates(data.query);
        return runMailboxExclusive(async () => {
          const store = await openCommittedMailbox();
          const value = await executeSearch(data.mailbox, store, query);
          const success: MailboxCallSuccess = {
            requestId: data.requestId,
            ok: true,
            op: "search",
            value,
          };
          return success;
        });
      }
      case "thread": {
        const query =
          data.query === undefined ? undefined : reviveSearchDates(data.query);
        return runMailboxExclusive(async () => {
          const store = await openCommittedMailbox();
          const value =
            query === undefined
              ? await executeThread(data.mailbox, store, data.algorithm)
              : await executeThread(data.mailbox, store, data.algorithm, query);
          const success: MailboxCallSuccess = {
            requestId: data.requestId,
            ok: true,
            op: "thread",
            value,
          };
          return success;
        });
      }
      case "fetchHeaders":
      case "fetchStructure":
      case "fetchFull":
      case "fetchPart": {
        const ref = data.ref;
        return runMailboxExclusive(async () => {
          const store = await openCommittedMailbox();
          if (data.op === "fetchHeaders") {
            const value = await fetchHeaders(ref, store);
            const success: MailboxCallSuccess = {
              requestId: data.requestId,
              ok: true,
              op: "fetchHeaders",
              value,
            };
            return success;
          }
          if (data.op === "fetchStructure") {
            const value = await fetchStructure(ref, store);
            const success: MailboxCallSuccess = {
              requestId: data.requestId,
              ok: true,
              op: "fetchStructure",
              value,
            };
            return success;
          }
          if (data.op === "fetchPart") {
            const value = projectPart(
              await fetchPart(ref, data.partPath, store),
            );
            const success: MailboxCallSuccess = {
              requestId: data.requestId,
              ok: true,
              op: "fetchPart",
              value,
            };
            return success;
          }
          const value = projectFetchedMessage(
            await fetchFull(ref, store, () => undefined),
          );
          const success: MailboxCallSuccess = {
            requestId: data.requestId,
            ok: true,
            op: "fetchFull",
            value,
          };
          return success;
        });
      }
      case "sync":
        return runMailboxExclusive(async () => {
          const store = await getMailboxStore();
          const result = store.sync({
            uidValidity: data.uidValidity,
            highestModSeq: data.highestModSeq,
          });
          const success: MailboxCallSuccess = {
            requestId: data.requestId,
            ok: true,
            op: "sync",
            value: syncResultForCaller(data.mailbox, data.uidNext, result),
          };
          return success;
        });
      case "getMailboxStatus":
        return runMailboxExclusive(async () => {
          const store = await getMailboxStore();
          const unseen = store.messages.filter(
            (message) => !message.flags.has(MAILBOX_FLAG_SEEN),
          ).length;
          const success: MailboxCallSuccess = {
            requestId: data.requestId,
            ok: true,
            op: "getMailboxStatus",
            value: {
              total: store.messages.length,
              unseen,
              recent: 0,
              uidNext: store.uidNext,
              uidValidity: store.uidValidity,
              highestModSeq: store.highestModSeq,
            },
          };
          return success;
        });
      case "append": {
        const appended: {
          headers: MessageHeaders;
          content?: string;
          payload?: InboundMessage["payload"];
        } = { headers: data.headers };
        if (data.content !== undefined) appended.content = data.content;
        if (data.payload !== undefined) appended.payload = data.payload;
        const flags = data.flags === undefined ? [] : data.flags;
        // Encode before the lock: a missing Message-ID or bad Date throws before the message enters the mirror.
        const stored = inboundMessageToRaw(appended);
        return runMailboxExclusive(async () => {
          const store = await getMailboxStore();
          const uid = store.append(stored.raw, stored.envelope, flags);
          await store.flush();
          const success: MailboxCallSuccess = {
            requestId: data.requestId,
            ok: true,
            op: "append",
            value: { uid, mailbox: data.mailbox },
          };
          return success;
        });
      }
      case "createMailbox":
      case "deleteMailbox":
      case "move":
      case "copy":
      case "createList":
      case "listMembers":
      case "subscribe":
      case "unsubscribe":
        // Refusals are sent before this switch; reaching one means the tables disagree.
        throw new Error(`mailbox call ${data.op} has no answer`);
    }
  }

  async function handleSubstrateWriteRequest(
    data: Extract<ControlPayload, { type: "substrate.write.request" }>["data"],
  ): Promise<void> {
    const controlSender = activeControlSender();
    if (controlSender === null) {
      // Arrived after the sender was cleared (recycling/draining/stopping): the child's waiter fails on pipe close, and there is no sender to reply on -- drop and log.
      logger.warn`substrate.write.request received outside running phase; requestId=${data.requestId} dropped (child waiter will fail on pipe close)`;
      return;
    }
    const validatedRepoId = RepoId(data.repoId);
    if (validatedRepoId instanceof type.errors) {
      onChildCrash(
        `substrate.write.request repoId failed validation: ${validatedRepoId.summary}`,
      );
      return;
    }
    // The child proxies workflow-run writes only; any other repo kind is a protocol violation.
    if (validatedRepoId.kind !== "workflow-run") {
      await controlSender.send({
        type: "substrate.write.response",
        data: {
          requestId: data.requestId,
          result: {
            ok: false,
            reason: `supervisor substrate.write.request: repoId.kind must be "workflow-run", got ${JSON.stringify(validatedRepoId.kind)}`,
          },
        },
      });
      return;
    }
    // Author proxied writes as the workflow-process principal scoped to this deployment; the kind handler accepts it for runs/<runId>/ writes, preserving the on-disk audit semantics of direct child writes.
    const writePrincipal: WorkflowRunWorkflowProcessPrincipal = {
      kind: "workflow-process",
      anchorRunId: bindings.anchorRunId,
    };
    // Terminal detection comes from the kind handler's newlyTerminalRuns signal, not a file sniff; holding the response on it keeps the child's progress gated on the inbox transition landing.
    // D2 leg classification (measurement-only): runs/<runId>/events/ -> the run-event bracket leg; agent-state/... -> the WAL leg; any other prefix stays unmarked.
    const legClassification = classifyProxiedWriteLeg(data.preservePrefix);
    if (legClassification !== null) {
      legMarkStart(legClassification.messageId, legClassification.leg);
    }
    try {
      const { commitSha, newlyTerminalRuns } =
        await bindings.repoStore.writeTreePreservingPrefix(
          writePrincipal,
          validatedRepoId,
          data.ref,
          {
            preservePrefix: data.preservePrefix,
            message: data.message,
            merge: async (existing) => {
              const sender = activeControlSender();
              if (sender === null) {
                throw new Error(
                  "supervisor substrate.write.request: control channel unavailable for merge round-trip",
                );
              }
              const result = await new Promise<
                | { ok: true; files: Record<string, string | Uint8Array> }
                | { ok: false; reason: string }
              >((resolve) => {
                pendingMerges.set(data.requestId, { resolve });
                const wireExisting: {
                  path: string;
                  contentBase64: string;
                }[] = [];
                for (const [path, bytes] of existing) {
                  wireExisting.push({
                    path,
                    contentBase64: bytesToBase64(bytes),
                  });
                }
                void sender
                  .send({
                    type: "substrate.merge.request",
                    data: {
                      requestId: data.requestId,
                      existing: wireExisting,
                    },
                  })
                  .catch((cause) => {
                    pendingMerges.delete(data.requestId);
                    const reason =
                      cause instanceof Error ? cause.message : String(cause);
                    resolve({
                      ok: false,
                      reason: `supervisor substrate.merge.request send failed: ${reason}`,
                    });
                  });
              });
              if (!result.ok) {
                throw new Error(
                  `supervisor substrate.write.request: child merge failed: ${result.reason}`,
                );
              }
              return result.files;
            },
          },
        );
      // D2 leg end: stamped before the response so the leg measures its own commit, not the markConsumed the markconsumed leg owns.
      if (legClassification !== null) {
        legMarkEnd(legClassification.messageId, legClassification.leg);
      }
      await controlSender.send({
        type: "substrate.write.response",
        data: {
          requestId: data.requestId,
          result: { ok: true, commitSha },
        },
      });
      // Seal terminal runs off the hot path (fold per-event files into one events.jsonl); a failure leaves the run per-event, which readers handle. The fold adds no terminal event, so it cannot re-fire this coupling.
      for (const { runId } of newlyTerminalRuns) {
        ownDetachedWrite(
          compactRunEvents({
            substrate: bindings.repoStore,
            repoId: validatedRepoId,
            ref: data.ref,
            anchorRunId: bindings.anchorRunId,
            runId,
          }).catch((cause) => {
            logger.warn`compaction of run ${runId} failed: ${cause instanceof Error ? cause.message : String(cause)}`;
          }),
        );
      }
    } catch (cause) {
      // Drop any merge awaiter the substrate never reached; if the reply path already resolved it, the delete is a no-op.
      pendingMerges.delete(data.requestId);
      const reason = cause instanceof Error ? cause.message : String(cause);
      await controlSender.send({
        type: "substrate.write.response",
        data: {
          requestId: data.requestId,
          result: { ok: false, reason },
        },
      });
    }
  }

  function activeControlSender(): ControlChannelSender | null {
    if (
      state.phase === "starting" ||
      state.phase === "running" ||
      state.phase === "recycling"
    ) {
      return state.controlSender;
    }
    return null;
  }

  async function wireChild(args: {
    channelId: string;
    hmacKey: Uint8Array;
    ipcKeypair: { privateKey: Uint8Array; publicKey: Uint8Array };
    handle: SubprocessHandle;
    onInferenceEvent: (event: EventPayload) => void;
  }): Promise<{
    wiring: ChildWiring;
    readyPromise: Promise<{ childPid: number }>;
    controlIncoming: AsyncGenerator<ControlPayload, void, void>;
  }> {
    const controlSender = createControlChannelSender({
      privateKeySeed: args.ipcKeypair.privateKey,
      channelId: args.channelId,
      writer: args.handle.controlWriter,
    });

    const controlIncoming = receiveControlChannel({
      publicKey: { bootstrapFromReady: true },
      channelId: args.channelId,
      reader: args.handle.controlReader,
      onCrash: onChildCrash,
    });

    const readyPromise = waitForReady(controlIncoming);

    const eventIter = receiveEventChannel({
      hmacKey: args.hmacKey,
      channelId: args.channelId,
      reader: args.handle.eventReader,
      // Route event-channel crashes through the same funnel as control-channel crashes so both drive respawn/crash-loop uniformly.
      onCrash: onChildCrash,
    });
    const eventPump = pumpEvents(eventIter, args.onInferenceEvent);

    return {
      wiring: {
        handle: args.handle,
        controlSender,
        channelId: args.channelId,
        eventPump,
      },
      readyPromise,
      controlIncoming,
    };
  }

  async function spawn(opts: SpawnOpts): Promise<SpawnResult> {
    if (state.phase !== "idle") {
      throw new Error(
        `supervisor: spawn called in phase ${state.phase}; expected idle`,
      );
    }
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    const ipcKeypair = await (bindings.ipcKeyPairFactory ?? generateKeyPair)();
    const env = buildChildSpawnEnv({
      substrateEnv: bindings.substrateEnv,
      dynamicSpawnEnv: bindings.dynamicSpawnEnv,
      channelId,
      hmacKey,
      hostPublicKey: ipcKeypair.publicKey,
      anchorRunId: bindings.anchorRunId,
      deploymentMailAddress: bindings.deploymentMailAddress,
      stepCount: bindings.stepCount,
      definitionHash: opts.definitionHash,
      warmKeep: opts.warmKeep,
    });

    const handle = bindings.subprocessSpawner({
      binaryPath: bindings.binaryPath,
      env,
    });

    let wired: Awaited<ReturnType<typeof wireChild>>;
    try {
      wired = await wireChild({
        channelId,
        hmacKey,
        ipcKeypair,
        handle,
        onInferenceEvent: opts.onInferenceEvent,
      });
    } catch (cause) {
      // wireChild threw before any state record owns the handle; kill the fresh child directly so it is not orphaned.
      await killChildHandle(handle, DEFAULT_KILL_TIMEOUT_MS, {
        setTimer: readySetTimer,
        clearTimer: readyClearTimer,
        logger,
      });
      throw cause;
    }

    // A startup teardown before the handshake (a throw in credentials assembly) kills the child and rejects readyPromise; attach a benign handler so the rejection is never unhandled.
    void wired.readyPromise.catch(() => {
      /* handled by the ready-handshake fold when the handshake runs */
    });

    // Cohort abort controller covers terminal-event watcher and dispatch-loop lifetime; the abort fires on shutdown and on every recycle installNewChild. The cohort broadcaster matches the same lifetime.
    state = {
      phase: "starting",
      handle,
      controlSender: wired.wiring.controlSender,
      channelId,
      eventPump: wired.wiring.eventPump,
      onInferenceEvent: opts.onInferenceEvent,
      mailUnsubscribe: null,
      credentialsSnapshot: null,
      terminalCohortAbort: new AbortController(),
      terminalBroadcaster: createTerminalBroadcaster(),
      dispatchLoop: null,
      replayDone: null,
      sweepDone: null,
    };

    // From here to the successful return the state record is "starting" (then "running"); a throw anywhere routes through shutdownInternal, the single owner of starting/running teardown.
    try {
      const credentialsSnapshot = await assembleCredentialsSnapshot({
        repoStore: bindings.repoStore,
        principal: bindings.readPrincipal,
        stepOrder: opts.stepOrder,
        anchorRunId: bindings.anchorRunId,
        deriveStepAddress: bindings.deriveStepAddress,
        ...(bindings.deriveStepRepoId !== undefined
          ? { deriveStepRepoId: bindings.deriveStepRepoId }
          : {}),
      });
      state.credentialsSnapshot = credentialsSnapshot;

      // Replay orphaned processing/ entries back to inbox/ before the first dequeue: a prior crash can leave one unowned, and the FIFO contract requires it to resume in its original position. runDispatchLoop awaits this gate so fresh mail cannot ship ahead of the orphan.
      // One runs/ scan feeds both recovery consumers (replay and compaction sweep), avoiding a second O(total-runs) walk.
      const scanDone = scanRunsForBoot(
        bindings.repoStore,
        bindings.workflowRunRepoId,
      );
      const replayDone = scanDone
        .then(({ ownedMessageIds }) =>
          inboxPrimitives.replayProcessingToInbox(
            bindings.repoStore,
            inboxWritePrincipal,
            bindings.workflowRunRepoId,
            bindings.deploymentMailAddress,
            { ownedMessageIds },
          ),
        )
        .then(() => {
          wakeDispatch();
        })
        .catch((cause) => {
          // Documented best-effort: a failed replay parks orphans and the loop ships fresh mail ahead of them. Making it fatal caused spurious crashes (a first spawn legitimately has no processing/ dir); left as logged best-effort.
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`boot recovery scan or processing replay failed on spawn: ${message}`;
        });
      // Hold the replay promise on the active-state record so shutdown awaits its settlement before tearing bindings down.
      state.replayDone = replayDone;

      // Re-seal runs a crash left terminal-but-per-event when their fold never ran. Must NOT gate dispatch (housekeeping); shutdown awaits it via the active-state record.
      const sweepDone = scanDone
        .then(({ pendingSealRunIds }) =>
          recoverInterruptedCompactions({
            substrate: bindings.repoStore,
            repoId: bindings.workflowRunRepoId,
            ref: bindings.workflowRunRef,
            anchorRunId: bindings.anchorRunId,
            pendingSealRunIds,
          }),
        )
        .then(({ sealed, failed }) => {
          if (sealed > 0) {
            logger.info`recovery sweep sealed ${String(sealed)} interrupted run(s)`;
          }
          if (failed.length > 0) {
            const detail = failed
              .map((f) => `${f.runId} (${f.message})`)
              .join("; ");
            logger.warn`recovery sweep left ${String(failed.length)} run(s) unsealed: ${detail}`;
          }
        })
        .catch((cause) => {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`boot recovery scan or compaction sweep failed on spawn: ${message}`;
        });
      state.sweepDone = sweepDone;

      bindings.mailBus.registerAddress(bindings.deploymentMailAddress);
      const mailUnsubscribe = bindings.mailBus.subscribeMailForAddress(
        bindings.deploymentMailAddress,
        onMailMessage,
      );
      state.mailUnsubscribe = mailUnsubscribe;

      // Bound the ready handshake: fold ready / child-exit / timeout into values so the single deadline clear runs on every path (a rejecting race would leak an armed timer). Kill on timeout uses SIGTERM->SIGKILL because a wedged child may ignore SIGTERM.
      const readyOutcome = wired.readyPromise.then(
        (info) => ({ kind: "ready" as const, info }),
        (err: unknown) => ({ kind: "failed" as const, err }),
      );
      const readyDeadline = waitDeadline(readySetTimer, readyTimeoutMs);
      const readyRace = await Promise.race([
        readyOutcome,
        readyDeadline.promise.then(() => ({ kind: "timeout" as const })),
      ]);
      readyClearTimer(readyDeadline.handle);
      if (readyRace.kind === "timeout") {
        await killChildHandle(wired.wiring.handle, DEFAULT_KILL_TIMEOUT_MS, {
          setTimer: readySetTimer,
          clearTimer: readyClearTimer,
          logger,
        });
        // SIGTERM->SIGKILL is deliberate: a wedged child may ignore the plain kill; the outer catch's later kill is idempotent.
        throw new Error(
          `workflow-host supervisor: child did not emit ready within ${readyTimeoutMs}ms; killed`,
        );
      }
      if (readyRace.kind === "failed") {
        // The child exited during the handshake; the outer catch releases the subscription and registration.
        throw readyRace.err;
      }
      const readyInfo = readyRace.info;

      // Push the credentialsSnapshot before the mail buffer drains: without it the child's authorize closure throws on a null snapshot before the first step can commit. Same channel as trigger.fire, so grants-updated lands first. Suppressed when onRunStart is wired -- the per-run barrier is then the sole grants source.
      if (bindings.onRunStart === undefined) {
        await wired.wiring.controlSender.send({
          type: "grants-updated",
          data: {
            snapshot: {
              steps: credentialsSnapshot.steps.map((s) => ({
                stepId: s.stepId,
                address: s.address,
                grants: [...s.grants],
                contentHash: s.contentHash,
              })),
            },
          },
        });
      }

      // Deliver the credential material on EVERY spawn: a restored run resumes without a trigger, so the per-trigger barrier would never re-deliver the cell. NOT suppressed when onRunStart is wired (a resume has no trigger). Reads the live mirror so earlier revocations stay evicted.
      if (currentCredentialDelivery !== null) {
        await wired.wiring.controlSender.send({
          type: "credentials-updated",
          data: { delivery: currentCredentialDelivery },
        });
      }

      // Transition to running; the dispatch loop picks up pre-ready buffered mail through the FIFO inbox, order preserved by the receivedAt prefix.
      const startingPhaseCohortAbort = state.terminalCohortAbort;
      if (startingPhaseCohortAbort === null) {
        throw new Error(
          "supervisor: terminalCohortAbort missing after spawn handshake",
        );
      }
      const startingPhaseBroadcaster = state.terminalBroadcaster;
      const dispatchLoop = runDispatchLoop(
        wired.wiring.controlSender,
        startingPhaseCohortAbort,
        startingPhaseBroadcaster,
        replayDone,
      );
      ownDispatchLoop(dispatchLoop);
      // Log structural dispatch-loop failures (per-iteration faults are already swallowed by the loop's own catch).
      void dispatchLoop.catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`dispatch loop terminated with error: ${message}`;
      });
      state = {
        phase: "running",
        handle,
        controlSender: wired.wiring.controlSender,
        channelId,
        eventPump: wired.wiring.eventPump,
        onInferenceEvent: opts.onInferenceEvent,
        mailUnsubscribe,
        credentialsSnapshot,
        terminalCohortAbort: startingPhaseCohortAbort,
        terminalBroadcaster: startingPhaseBroadcaster,
        dispatchLoop,
        replayDone,
        sweepDone,
      };
      // Bump the generation and arm the exit-watcher atomically with the running transition.
      armChildForRunning(handle);
      // Kick the loop in case mail landed before its first wake await; the first dequeue is unconditional.
      wakeDispatch();

      // Cache the spawn context for the recycle path; recycle reuses the same anchors and never mutates them.
      const now = bindings.recyclePolicyNow ?? defaultNow;
      spawnContext = {
        stepOrder: opts.stepOrder,
        definitionHash: opts.definitionHash,
        warmKeep: opts.warmKeep,
        onInferenceEvent: opts.onInferenceEvent,
        spawnedAt: now(),
      };

      // Start the upstream control pump; it exits when the child closes its end of the channel (shutdown or recycle's kill). Closes over the cohort broadcaster captured at pump start so stale terminal frames route to the disposed cohort, not the successor.
      void pumpUpstreamControl(
        wired.controlIncoming,
        startingPhaseBroadcaster,
      ).catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`upstream control pump failed: ${message}`;
      });

      // Trigger A: re-register the freshly-ready child's parked correlations, recovering registers the hub missed while down. Fire-and-forget after the pump arms, watchdog-bounded.
      void reEmitParkedCorrelations().catch((cause) => {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`re-emit of parked correlations on re-establishment failed: ${message}`;
      });

      // Arm the recycle policy; a no-op when all bounds are undefined.
      if (bindings.recyclePolicy !== undefined) {
        const setTimer = bindings.recyclePolicySetTimer ?? defaultSetTimer;
        const clearTimer =
          bindings.recyclePolicyClearTimer ?? defaultClearTimer;
        recyclePolicy = createRecyclePolicy({
          bounds: bindings.recyclePolicy,
          now,
          spawnedAt: spawnContext.spawnedAt,
          ...(bindings.readRssBytes !== undefined
            ? { readRssBytes: bindings.readRssBytes }
            : {}),
          ...(bindings.readGrantsAgeMs !== undefined
            ? { readGrantsAgeMs: bindings.readGrantsAgeMs }
            : {}),
          setTimer,
          clearTimer,
          trigger: async (reason) => {
            await recycle({ reason, origin: "policy" });
          },
        });
      }

      return {
        pid: readyInfo.childPid,
        channelId,
        credentialsSnapshot,
      };
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      // Preserve the original spawn cause: if shutdownInternal ever throws, log the secondary error rather than masking the startup failure.
      await shutdownInternal({
        reason: `spawn failed during startup: ${message}`,
      }).catch((shutdownCause) => {
        const inner =
          shutdownCause instanceof Error
            ? shutdownCause.message
            : String(shutdownCause);
        logger.error`shutdown after spawn failure also threw: ${inner}`;
      });
      throw cause;
    }
  }

  /** Wrap the cohort broadcaster's source so iterators settle with done:true on cohort abort; otherwise an armed accumulator would block past the abort. */
  function perCohortTerminalSource(
    cohortAbort: AbortController | null,
    broadcaster: TerminalBroadcaster | null,
  ): TerminalEventSource | null {
    if (cohortAbort === null) return null;
    if (broadcaster === null) return null;
    const signal = cohortAbort.signal;
    return (runId: string) => ({
      [Symbol.asyncIterator](): AsyncIterator<TerminalRunEvent> {
        if (signal.aborted) {
          return {
            next: () => Promise.resolve({ value: undefined, done: true }),
            return: (value?: unknown) => Promise.resolve({ value, done: true }),
          };
        }
        const inner = broadcaster.source(runId)[Symbol.asyncIterator]();
        let onAbort: (() => void) | null = null;
        const abortPromise = new Promise<{
          value: TerminalRunEvent | undefined;
          done: true;
        }>((resolve) => {
          onAbort = () => resolve({ value: undefined, done: true });
          signal.addEventListener("abort", onAbort, { once: true });
        });
        function detach(): void {
          if (onAbort !== null) {
            signal.removeEventListener("abort", onAbort);
            onAbort = null;
          }
        }
        return {
          async next(): Promise<IteratorResult<TerminalRunEvent>> {
            if (signal.aborted) {
              detach();
              if (typeof inner.return === "function") {
                await inner.return(undefined).catch(() => {
                  /* swallowed: best-effort finalisation. */
                });
              }
              return { value: undefined, done: true };
            }
            const result = await Promise.race([inner.next(), abortPromise]);
            if (result.done === true) {
              detach();
              if (signal.aborted && typeof inner.return === "function") {
                await inner.return(undefined).catch(() => {
                  /* swallowed: best-effort finalisation. */
                });
              }
            }
            return result;
          },
          async return(): Promise<IteratorResult<TerminalRunEvent>> {
            detach();
            if (typeof inner.return === "function") {
              await inner.return(undefined).catch(() => {
                /* swallowed: best-effort finalisation. */
              });
            }
            return { value: undefined, done: true };
          },
        };
      },
    });
  }

  /** Forward one dequeued entry as trigger.fire and record the runId in-flight. The runId is the local part of the deployment's mail address; the resolved Mail rides as the trigger payload. */
  async function forwardDispatchedEntry(
    sender: ControlChannelSender,
    messageId: string,
    receivedAt: number,
    runId: string,
    payload: Mail,
  ): Promise<string> {
    await sender.send({
      type: "trigger.fire",
      data: {
        runId,
        messageId,
        receivedAt,
        payload,
      },
    });
    cohortRunIds.add(runId);
    return runId;
  }

  /** Resolve a dequeued mail to the run's input: decode it and commit the part bytes to the workflow-run substrate (a direct write -- the child's control loop cannot proxy one without deadlock). Deterministic rejections return { ok: false } so the caller drops the mail; transient substrate failures throw so the mail stays reclaimable. */
  async function prepareMail(
    envelope: { messageId: string; rawMessage?: string },
    runId: string,
  ): Promise<
    | { ok: true; mail: Mail }
    | { ok: false; rejection: { code: string; message: string } }
  > {
    if (envelope.rawMessage === undefined) {
      return {
        ok: false,
        rejection: {
          code: "malformed_mail",
          message: `inbound mail ${envelope.messageId} carries no rawMessage bytes`,
        },
      };
    }
    let decoded: ReturnType<typeof decodeMail>;
    try {
      decoded = decodeMail(base64Decode(envelope.rawMessage));
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      return {
        ok: false,
        rejection: {
          code: "malformed_mail",
          message: `inbound mail ${envelope.messageId} could not be decoded: ${message}`,
        },
      };
    }
    const writePrincipal: WorkflowRunSupervisorPrincipal = {
      kind: "supervisor",
      anchorRunId: bindings.anchorRunId,
    };
    try {
      const mail = await commitMail(
        {
          substrate: bindings.repoStore,
          repoId: bindings.workflowRunRepoId,
          principal: writePrincipal,
          runId,
          ref: bindings.workflowRunRef,
        },
        envelope.messageId,
        decoded,
      );
      return { ok: true, mail };
    } catch (cause) {
      if (cause instanceof InvalidMailError) {
        return {
          ok: false,
          rejection: { code: "malformed_mail", message: cause.message },
        };
      }
      throw cause;
    }
  }

  /** Push the run's grants snapshot before trigger.fire. Returns true on barrier failure (the run is already settled as RunFailed); false when passed or when onRunStart is unwired. A throw from the sink or the send becomes a synthesized RunFailed, never swallowed. */
  async function pushRunGrants(
    sender: ControlChannelSender,
    runId: string,
    broadcaster: TerminalBroadcaster,
  ): Promise<boolean> {
    if (bindings.onRunStart === undefined) return false;
    try {
      const snapshot = await bindings.onRunStart({
        runId,
        anchorRunId: bindings.anchorRunId,
      });
      await sender.send({
        type: "grants-updated",
        data: {
          snapshot: {
            steps: snapshot.steps.map((s) => ({
              stepId: s.stepId,
              address: s.address,
              grants: [...s.grants],
              contentHash: s.contentHash,
            })),
          },
        },
      });
      // Deliver the credential material on the same barrier so a first-step tool already has it. Reads the live mirror, so earlier rotations/revocations are reflected and a recycled child inherits the current set.
      if (currentCredentialDelivery !== null) {
        await sender.send({
          type: "credentials-updated",
          data: { delivery: currentCredentialDelivery },
        });
      }
      return false;
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`onRunStart grants barrier failed for run ${runId}; failing the run: ${message}`;
      broadcaster.notify(runId, {
        kind: "RunFailed",
        seq: 0,
        at: new Date().toISOString(),
        error: {
          message: `workflow-host supervisor: run ${runId} not authorized; grants barrier failed before trigger.fire: ${message}`,
        },
      });
      return true;
    }
  }

  /** One dispatch iteration: dequeue the FIFO-first entry, route it (signal.deliver / trigger.fire / wait), then markConsumed once the child durably took it up. Returns true if a dispatch landed, false if the inbox was empty. */
  async function dispatchOne(
    sender: ControlChannelSender,
    cohortAbort: AbortController,
    broadcaster: TerminalBroadcaster,
  ): Promise<boolean> {
    if (cohortAbort.signal.aborted) return false;
    const beforeDequeueMs = dispatchTimingEnabled() ? performance.now() : 0;
    const dequeued = await inboxPrimitives.dequeueToProcessing(
      bindings.repoStore,
      inboxWritePrincipal,
      bindings.workflowRunRepoId,
      bindings.deploymentMailAddress,
    );
    if (dequeued === null) return false;
    const envelope = dequeued.envelope;
    const runId = deriveWorkflowRunId(bindings.deploymentMailAddress);
    const messageId = envelope.messageId;
    let rejection:
      | {
          code: string;
          message: string;
        }
      | undefined;
    const rejectTerminalRun = () => {
      if (rejection !== undefined) return;
      rejection = {
        code: "workflow_run_terminal",
        message: `Workflow run ${runId} is terminal and cannot be fired again`,
      };
      logger.warn`rejecting inbound mail ${messageId}: workflow run ${runId} is terminal`;
    };
    currentDispatchMessageId = messageId;
    emitDispatchTiming(messageId, "dispatch-start", beforeDequeueMs);
    // D2 dequeue leg: the start mark re-stamps the pre-dequeue sample (the roundtrip bracket includes the read), and the end is now. Retroactive stamping keeps the leg keyed by the messageId, known only after the dequeue.
    if (bindings.onDispatchTiming !== undefined) {
      try {
        bindings.onDispatchTiming({
          kind: "leg",
          messageId,
          leg: "dequeue",
          phase: "start",
          atMs: beforeDequeueMs,
        });
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`onDispatchTiming leg observer threw for ${messageId} (dequeue start): ${message}`;
      }
    }
    legMarkEnd(messageId, "dequeue");
    // In-memory cohort membership is not the lifecycle authority (empty for a new run, after terminal, while rediscovering); consult the durable log before firing. A live log re-enters cohort tracking; a terminal log is rejected permanently.
    if (!cohortRunIds.has(runId)) {
      const lifecycle = terminalRunIds.has(runId)
        ? "terminal"
        : await readWorkflowRunLifecycle(
            bindings.repoStore,
            bindings.workflowRunRepoId,
            runId,
          );
      if (lifecycle === "terminal") {
        rejectTerminalRun();
      } else if (lifecycle === "live") {
        cohortRunIds.add(runId);
      }
    }

    if (rejection === undefined) {
      // Subscribe before the grants barrier so a synthetic RunFailed from a barrier failure can be captured.
      const preIter = broadcaster.source(runId)[Symbol.asyncIterator]();
      // Per-run grants barrier: push this run's snapshot before the trigger/signal on the same channel, so grants-updated lands first. A barrier failure synthesizes a RunFailed and the trigger is NOT fired.
      const barrierFailed = await pushRunGrants(sender, runId, broadcaster);
      if (barrierFailed) {
        // Wait for the synthetic RunFailed before consuming, then clean up cohort tracking (synthetic events bypass pumpUpstreamControl's cleanup).
        await waitForRunTerminal(preIter, cohortAbort.signal);
        cohortRunIds.delete(runId);
        runInputChannels.delete(runId);
      } else {
        // Dispose the pre-created iterator; the normal path mints fresh ones when waiting.
        if (typeof preIter.return === "function") {
          await preIter.return();
        }
        // Unified dispatch: park → signal.deliver; no live run → trigger.fire; in-flight but undecided → wait for terminal or park, then re-evaluate.
        while (!cohortAbort.signal.aborted) {
          if (terminalRunIds.has(runId)) {
            rejectTerminalRun();
            break;
          }
          // Capture the park generation before this iteration's pre-wait awaits so a park during them is still accepted; re-captured each iteration so "parked" is not re-counted.
          const sinceGen = parkGenerations.get(runId) ?? 0;
          // Routing hygiene: a channel entry with no live cohort run is a stale hazard from a dead incarnation; drop it before the signal branch. The resumed.runs invariant keeps live runs out of this branch.
          if (!cohortRunIds.has(runId) && runInputChannels.has(runId)) {
            runInputChannels.delete(runId);
          }
          const inputChannel = runInputChannels.get(runId);
          if (inputChannel !== undefined) {
            // Prepare the mail here, the single site that knows this payload's provenance is mail; the signal.deliver payload is the final resume decision. Done before minting the terminal watcher so a failure cannot leak an unfinalized iterator.
            const prepared = await prepareMail(envelope, runId);
            if (!prepared.ok) {
              // A deterministically malformed turn-2 mail cannot resume the parked agent: consume it (break to markConsumed) rather than replaying poison forever. The run stays parked; a transient write failure still throws and stays reclaimable.
              logger.error`signal.deliver for run ${runId}: dropping malformed inbound mail ${envelope.messageId}: ${prepared.rejection.message}`;
              break;
            }
            // Mint the terminal watcher only after the payload resolves so a terminal right after the signal is not missed.
            const iter = broadcaster.source(runId)[Symbol.asyncIterator]();
            let waitEntered = false;
            try {
              await sender.send({
                type: "signal.deliver",
                data: {
                  runId,
                  signalName: signalName(inputChannel.correlationId),
                  signalId: envelope.messageId,
                  payload: prepared.mail,
                },
              });
              // Invalidate the channel: the correlation is consumed, and the run re-parks on a fresh one. The wait keys on the park-generation edge, not this level state.
              runInputChannels.delete(runId);
              // The message was dispatched as a turn: mark its eager mailbox entry \Seen/$Processed. Fire-and-forget off the dispatch path.
              markMailboxProcessed(envelope.messageId);
              // Durable-consume contract: hold markConsumed until the run re-parks or terminates. Re-park/terminal only follow the COMMITTED SignalReceived, so the substrate subscription is the ack; a crash before it leaves the entry in processing/ for replay re-delivery.
              waitEntered = true;
              await waitForRunTerminalOrPark(
                iter,
                cohortAbort.signal,
                runId,
                sinceGen,
              );
            } finally {
              // waitForRunTerminalOrPark finalizes its iterator; only a send throw before the wait leaks one, so finalize only then.
              if (!waitEntered && typeof iter.return === "function") {
                await iter.return(undefined).catch(() => {
                  /* best-effort finalisation of the watcher iterator. */
                });
              }
            }
            break;
          }
          if (!cohortRunIds.has(runId)) {
            // A deterministically malformed first trigger cannot start the run: record the rejection on the consumed entry and drop it; a transient failure propagates as a dispatch fault and stays reclaimable.
            const prepared = await prepareMail(envelope, runId);
            if (!prepared.ok) {
              if (rejection === undefined) rejection = prepared.rejection;
              logger.error`trigger.fire for run ${runId}: rejecting malformed inbound mail ${envelope.messageId}: ${prepared.rejection.message}`;
              break;
            }
            // Subscribe the terminal watcher before the trigger fires: the broadcaster drops notifies without a listener, so a late terminal would otherwise hang the wait to the backstop.
            const iter = broadcaster.source(runId)[Symbol.asyncIterator]();
            let waitEntered = false;
            try {
              await forwardDispatchedEntry(
                sender,
                envelope.messageId,
                envelope.receivedAt,
                runId,
                prepared.mail,
              );
              // The message was dispatched as a turn: mark its eager mailbox entry \Seen/$Processed. Fire-and-forget off the dispatch path.
              markMailboxProcessed(envelope.messageId);

              // Hold markConsumed until the child durably takes up the trigger (RunStarted committed, then park/terminal): a crash before that leaves the entry in processing/ for replay re-delivery.
              waitEntered = true;
              await waitForRunTerminalOrPark(
                iter,
                cohortAbort.signal,
                runId,
                sinceGen,
              );
            } finally {
              // Same finalize-only-on-throw discipline as the signal path.
              if (!waitEntered && typeof iter.return === "function") {
                await iter.return(undefined).catch(() => {
                  /* best-effort finalisation of the watcher iterator. */
                });
              }
            }
            break;
          }
          const iter = broadcaster.source(runId)[Symbol.asyncIterator]();
          const outcome = await waitForRunTerminalOrPark(
            iter,
            cohortAbort.signal,
            runId,
            sinceGen,
          );
          if (outcome === "aborted") break;
          if (outcome === "terminal") {
            // The live run terminated while this mail waited for its next correlation; reject rather than falling through to trigger.fire.
            rejectTerminalRun();
            break;
          }
          // Continue loop: re-evaluate runInputChannels / cohortRunIds
        }
      }
    }
    if (cohortAbort.signal.aborted) {
      currentDispatchMessageId = null;
      return false;
    }
    emitDispatchTiming(messageId, "reply-produced", performance.now());
    // D2 leg: `markConsumed` is paid AFTER `reply-produced`, so its growth is invisible to the 4.7 round-trip bracket -- the leg mark makes the out-of-window cost visible.
    legMarkStart(messageId, "markconsumed");
    try {
      await inboxPrimitives.markConsumed(
        bindings.repoStore,
        inboxWritePrincipal,
        bindings.workflowRunRepoId,
        {
          address: bindings.deploymentMailAddress,
          messageId: envelope.messageId,
          runId,
          consumedAt: Date.now(),
          retentionHorizonMs: consumedRetentionMs,
          ...(rejection !== undefined ? { rejection } : {}),
        },
      );
    } catch (cause) {
      // A markConsumed failure is fatal: swallowing it would hide a mail that is neither consumed nor visibly failed. Propagate so it surfaces and stays reclaimable.
      throw new Error(`failed to markConsumed for run ${runId}`, { cause });
    }
    legMarkEnd(messageId, "markconsumed");
    maybeRepack(runId);
    currentDispatchMessageId = null;
    return true;
  }

  /** Wait for the run's terminal event or the cohort abort. The caller mints the iterator before the trigger.fire so the listener is armed when the frame arrives. */
  async function waitForRunTerminal(
    iter: AsyncIterator<TerminalRunEvent>,
    abortSignal: AbortSignal,
  ): Promise<void> {
    let onAbort: (() => void) | null = null;
    const abortPromise = new Promise<{ done: true }>((resolve) => {
      if (abortSignal.aborted) {
        resolve({ done: true });
        return;
      }
      onAbort = () => resolve({ done: true });
      abortSignal.addEventListener("abort", onAbort, { once: true });
    });
    try {
      while (true) {
        if (abortSignal.aborted) return;
        const result = await Promise.race([iter.next(), abortPromise]);
        if (result.done === true) return;
        // A terminal event for this runId arrived; stop waiting.
        return;
      }
    } finally {
      if (onAbort !== null) {
        abortSignal.removeEventListener("abort", onAbort);
      }
      if (typeof iter.return === "function") {
        await iter.return(undefined).catch(() => {
          /* swallowed: best-effort finalisation of the watcher iterator. */
        });
      }
    }
  }

  /** Wait for the run's terminal event, a park past `sinceGen`, or the cohort abort. Returns "terminal" / "parked" / "aborted"; throws when the backstop fires. sinceGen is captured by the caller before its pre-wait awaits, so a park during them is still observed and a stale channel entry cannot false-positive. */
  async function waitForRunTerminalOrPark(
    iter: AsyncIterator<TerminalRunEvent>,
    abortSignal: AbortSignal,
    runId: string,
    sinceGen: number,
  ): Promise<"terminal" | "parked" | "aborted"> {
    let onAbort: (() => void) | null = null;
    const abortPromise = new Promise<{ source: "abort" }>((resolve) => {
      if (abortSignal.aborted) {
        resolve({ source: "abort" });
        return;
      }
      onAbort = () => resolve({ source: "abort" });
      abortSignal.addEventListener("abort", onAbort, { once: true });
    });

    let parkResolve: (() => void) | null = null;
    const parkPromise = new Promise<{ source: "park" }>((resolve) => {
      parkResolve = () => resolve({ source: "park" });
      parkNotifyWaiters.set(runId, parkResolve);
    });

    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    const timeoutPromise = new Promise<{ source: "timeout" }>((resolve) => {
      timeoutHandle = setTimeout(
        () => resolve({ source: "timeout" }),
        TERMINAL_OR_PARK_BACKSTOP_MS,
      );
    });

    try {
      if (abortSignal.aborted) return "aborted";
      // Check-after-register: synchronously read the generation now that the waiter is armed. Past sinceGen means the run parked during the caller's pre-wait awaits (the waiter no-op'd), so catch it here rather than hanging to the backstop.
      if ((parkGenerations.get(runId) ?? 0) > sinceGen) return "parked";
      const result = await Promise.race([
        iter.next().then((r) => ({ source: "iter" as const, r })),
        abortPromise,
        parkPromise,
        timeoutPromise,
      ]);
      if (result.source === "abort") return "aborted";
      if (result.source === "park") return "parked";
      if (result.source === "timeout") {
        // Backstop: the run neither parked, terminated, nor aborted. Throw so the dispatch fails and the mail stays reclaimable rather than consumed on a false assumption.
        logger.error`waitForRunTerminalOrPark backstop fired for run ${runId} after ${TERMINAL_OR_PARK_BACKSTOP_MS}ms; failing the dispatch so the mail stays reclaimable`;
        throw new Error(
          `waitForRunTerminalOrPark backstop: run ${runId} did not park or terminate within ${TERMINAL_OR_PARK_BACKSTOP_MS}ms`,
        );
      }
      if (result.r.done === true) return "aborted";
      // A terminal event for this runId arrived; stop waiting.
      return "terminal";
    } finally {
      if (timeoutHandle !== undefined) {
        clearTimeout(timeoutHandle);
      }
      if (parkResolve !== null) {
        parkNotifyWaiters.delete(runId);
      }
      if (onAbort !== null) {
        abortSignal.removeEventListener("abort", onAbort);
      }
      if (typeof iter.return === "function") {
        await iter.return(undefined).catch(() => {
          /* swallowed: best-effort finalisation of the watcher iterator. */
        });
      }
    }
  }

  /** Dispatch loop body: drains one FIFO entry per iteration until the cohort aborts. replayGate is the spawn-time replay promise, awaited before the first dequeue so fresh mail cannot ship ahead of an orphaned processing/ entry; null on the recycle restart (triggerRecycle already awaited its replay). */
  async function runDispatchLoop(
    sender: ControlChannelSender,
    cohortAbort: AbortController,
    broadcaster: TerminalBroadcaster,
    replayGate: Promise<void> | null,
  ): Promise<void> {
    if (replayGate !== null) {
      await replayGate;
      if (cohortAbort.signal.aborted) return;
    }
    while (!cohortAbort.signal.aborted) {
      // Capture the wake before the iteration: wakeDispatch resolves the current promise and swaps a fresh one, so a mail enqueued during dispatchOne resolves this capture. Capturing after would strand it.
      const wake = dispatchWake.promise;
      let dispatched: boolean;
      try {
        dispatched = await dispatchOne(sender, cohortAbort, broadcaster);
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`dispatch loop iteration failed: ${message}`;
        // Surface the failure but keep looping; pausing on the wake avoids busy-spinning against a persistent fault.
        dispatched = false;
      }
      if (dispatched) continue;
      if (cohortAbort.signal.aborted) return;
      const abortPromise = new Promise<void>((resolve) => {
        if (cohortAbort.signal.aborted) {
          resolve();
          return;
        }
        cohortAbort.signal.addEventListener("abort", () => resolve(), {
          once: true,
        });
      });
      await Promise.race([wake, abortPromise]);
    }
  }

  async function requestCancel(
    opts: CancelRequestOpts,
  ): Promise<CancelCommitInfo> {
    if (
      state.phase === "stopping" ||
      state.phase === "stopped" ||
      state.phase === "crash-looping"
    ) {
      throw new Error("Cannot cancel a stopped workflow supervisor");
    }
    const commitCancellation = async () => {
      const committing = commitCancelRequested({
        substrate: bindings.repoStore,
        repoId: bindings.workflowRunRepoId,
        ref: bindings.workflowRunRef,
        anchorRunId: bindings.anchorRunId,
        runId: opts.runId,
        origin: opts.origin,
        reason: opts.reason,
        at: opts.at,
        signAsPrincipal: bindings.signAsPrincipal,
      });
      cancellationCommits.add(committing);
      try {
        const result = await committing;
        return { commitSha: result.commitSha, seq: result.seq };
      } finally {
        cancellationCommits.delete(committing);
      }
    };
    const cohort = state;
    if (
      cohort.phase !== "starting" &&
      cohort.phase !== "running" &&
      cohort.phase !== "recycling"
    )
      return commitCancellation();
    const sender = cohort.controlSender;

    const requestId = `cancel-${String((cancellationSeq += 1))}`;
    const prepared = Promise.withResolvers<string | undefined>();
    pendingCancellations.set(requestId, {
      broadcaster: cohort.terminalBroadcaster,
      resolve: prepared.resolve,
    });
    try {
      await sender.send({
        type: "cancel.prepare",
        data: { requestId, runId: opts.runId, reason: opts.reason },
      });
      const error = await prepared.promise;
      if (error !== undefined) throw new Error(error);
      cohort.terminalCohortAbort.signal.throwIfAborted();
      if (activeControlSender() !== sender)
        throw new Error("Cancellation's workflow child was replaced");
      const result = await commitCancellation();
      await sender
        .send({ type: "cancel.committed", data: { requestId } })
        .catch((cause: unknown) => {
          logger.warn`CancelRequested committed for ${opts.runId}, but the child wakeup failed: ${cause instanceof Error ? cause.message : String(cause)}`;
        });
      return result;
    } catch (cause) {
      await sender
        .send({
          type: "cancel.committed",
          data: {
            requestId,
            error: cause instanceof Error ? cause.message : String(cause),
          },
        })
        .catch(() => undefined);
      throw cause;
    } finally {
      pendingCancellations.delete(requestId);
    }
  }

  async function shutdown(): Promise<void> {
    await shutdownInternal({ reason: "shutdown requested" });
  }

  type ShutdownOptions = {
    reason: string;
    // Terminal phase the teardown lands in: stopped (clean) or crash-looping (latch).
    terminalPhase?: "stopped" | "crash-looping";
    // True when the supervisor drives itself to a terminal phase (latch, channel crash, recycle failure) rather than the host requesting shutdown; the phase alone cannot carry this.
    selfTerminated?: boolean;
    // Durable record of the teardown, written after the child exits and owned writers settle, before shutdown resolves or reports self-termination.
    terminalCommit?: () => Promise<void>;
  };

  function shutdownInternal(opts: ShutdownOptions): Promise<void> {
    if (shutdownPromise !== null) return shutdownPromise;
    if (
      state.phase === "idle" ||
      state.phase === "stopped" ||
      state.phase === "crash-looping"
    )
      return Promise.resolve();
    // Publish the completion before teardown can call back into the supervisor.
    const completion = Promise.withResolvers<undefined>();
    shutdownPromise = completion.promise;
    void performShutdown(opts).then(
      () => completion.resolve(undefined),
      completion.reject,
    );
    return shutdownPromise;
  }

  async function performShutdown(opts: ShutdownOptions): Promise<void> {
    const prior = state;
    state = { phase: "stopping" };
    // Teardown is TOTAL: one try/finally guarantees the child is killed and the terminal transition lands whatever throws. This is the documented carve-out to fail-loud: leaking the child or wedging in `stopping` is worse than logging and continuing.
    const accumulatorsToDispose = [...drainAccumulators.values()];
    const childrenToStop = new Set(uninstalledChildren);
    if (
      prior.phase === "starting" ||
      prior.phase === "running" ||
      prior.phase === "recycling"
    )
      childrenToStop.add(prior.handle);
    const childTerminations: Promise<void>[] = [];
    let killRequested = false;
    function killChildren(): void {
      if (killRequested) return;
      killRequested = true;
      // Escalate to SIGKILL: workflow code can trap SIGTERM and hold the forced stop open.
      for (const handle of childrenToStop) {
        childTerminations.push(
          killChildHandle(handle, DEFAULT_KILL_TIMEOUT_MS, {
            setTimer: readySetTimer,
            clearTimer: readyClearTimer,
            logger,
          }).catch((cause: unknown) => {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.warn`child kill threw during shutdown: ${message}`;
          }),
        );
      }
    }
    try {
      // Stop every armed accumulator before the child dies; guard each stop so one throw does not leave the rest armed.
      for (const accumulator of accumulatorsToDispose) {
        try {
          accumulator.stop();
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`drain accumulator stop threw during shutdown: ${message}`;
        }
      }
      drainAccumulators.clear();
      cohortRunIds.clear();
      runInputChannels.clear();
      parkNotifyWaiters.clear();
      parkGenerations.clear();
      if (
        prior.phase === "starting" ||
        prior.phase === "running" ||
        prior.phase === "recycling"
      ) {
        prior.terminalCohortAbort.abort();
        // Reject pending round-trips and waiters so handler closures cannot sit on resolvers the dying channel never invokes.
        rejectCohortAwaiters("shutdown");
        // Dispose the broadcaster so minted iterators settle done:true, unblocking waiters through the same path the abort drives.
        prior.terminalBroadcaster.dispose();
        // Wake the loop so its dispatchWake await settles and it notices the abort.
        wakeDispatch();
      }
      // Kill before awaiting the loop: a dispatch blocked on an unresponsive child releases its pipe.
      killChildren();
      // Own any started signed append through teardown before the host releases its bindings.
      await Promise.allSettled([...cancellationCommits]);
      // Own detached writes so every requested write lands before shutdown resolves; new merges fail once stopping.
      while (detachedWrites.size > 0) await Promise.all([...detachedWrites]);
      // Await each accumulator's disposed() so no escalation or watcher coroutine outlives the supervisor.
      await Promise.all(
        accumulatorsToDispose.map((a) =>
          a.disposed().catch(() => {
            /* swallowed: each accumulator already logs its own failure. */
          }),
        ),
      );
      // Includes loops a recycle retired from state, so their last writes land before shutdown resolves.
      await Promise.allSettled([...dispatchLoops]);
      if (
        (prior.phase === "starting" ||
          prior.phase === "running" ||
          prior.phase === "recycling") &&
        prior.replayDone !== null
      ) {
        // Await the spawn-time replay so its substrate write settles before exit; otherwise a later boot can observe a partially-applied replay.
        await prior.replayDone.catch(() => {
          /* swallowed: the replay's own catch already surfaces the
             failure to the supervisor's warn channel; the shutdown
             path only waits for the substrate write to settle. */
        });
      }
      if (
        (prior.phase === "starting" ||
          prior.phase === "running" ||
          prior.phase === "recycling") &&
        prior.sweepDone !== null
      ) {
        // Await the spawn-time sweep so an in-flight fold's commit does not interleave with the next boot.
        await prior.sweepDone.catch(() => {
          /* swallowed: the sweep's own catch already surfaces failures to
             the supervisor's warn channel; the shutdown path only waits for
             the in-flight fold's substrate commit to settle. */
        });
      }
      if (recyclePolicy !== null) {
        try {
          recyclePolicy.stop();
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`recycle policy stop threw during shutdown: ${message}`;
        }
        recyclePolicy = null;
      }
      // Disarm the stable-run timer and drop any pending child exit; a kill in the finally may re-record one, but the terminal phase makes it a no-op and spawn() requires idle.
      clearStableRunResetTimer();
      // Cancel every armed backoff wait; the coroutines re-check the phase and bail without respawning.
      cancelRespawnBackoffWaits();
      pendingChildExit = null;
      spawnContext = null;
      if (
        prior.phase === "starting" ||
        prior.phase === "running" ||
        prior.phase === "recycling"
      ) {
        if (prior.mailUnsubscribe !== null) {
          try {
            prior.mailUnsubscribe();
          } catch (cause) {
            const message =
              cause instanceof Error ? cause.message : String(cause);
            logger.warn`mail unsubscribe threw during shutdown: ${message}`;
          }
        }
        try {
          bindings.mailBus.unregisterAddress(bindings.deploymentMailAddress);
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : String(cause);
          logger.warn`mail bus unregisterAddress threw: ${message}`;
        }
      }
    } finally {
      // Load-bearing: the kill and terminal transition run whatever happened above, so a throwing step can neither leak the child nor wedge in `stopping`.
      killChildren();
      await Promise.all(childTerminations);
      await Promise.all(
        [...childrenToStop].map((handle) =>
          handle.exited.catch(() => {
            /* A non-zero child exit is expected during shutdown. */
          }),
        ),
      );
      if (
        prior.phase === "starting" ||
        prior.phase === "running" ||
        prior.phase === "recycling"
      ) {
        await prior.eventPump.catch(() => {
          /* swallowed for the same reason as above. */
        });
      }
      state = { phase: opts.terminalPhase ?? "stopped" };
    }
    if (opts.terminalCommit !== undefined) {
      try {
        await opts.terminalCommit();
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.error`terminal commit failed; the deployment has no durable record of why it stopped (${opts.reason}): ${message}`;
      }
    }
    // Surface a self-termination to the host after the terminal transition is
    // committed. Concurrent shutdown callers share this teardown. Catch sink
    // failures so they cannot escape a completed shutdown.
    if (opts.selfTerminated === true) {
      try {
        bindings.onSelfTerminate?.({
          phase: opts.terminalPhase ?? "stopped",
          reason: opts.reason,
        });
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : String(cause);
        logger.warn`onSelfTerminate sink threw: ${message}`;
      }
    }
    logger.info`supervisor shutdown complete (${opts.reason})`;
  }

  async function drain(opts: DrainOpts): Promise<void> {
    await drainImpl(opts, { fromRecycle: false });
  }

  /** Internal drain. fromRecycle admits `recycling` for the recycle drain step (runs before abortPriorCohort + kill); external callers leave it false so a stray drain during the kill/respawn gap is dropped, never written into a sender about to be torn down. drain() is a documented best-effort no-op off-running so the host can call it unconditionally. */
  async function drainImpl(
    opts: DrainOpts,
    ctx: { fromRecycle: boolean },
  ): Promise<void> {
    // No-op off running/starting (and off recycling unless fromRecycle), so host shutdown can call drain unconditionally.
    if (
      state.phase !== "running" &&
      state.phase !== "starting" &&
      !(ctx.fromRecycle && state.phase === "recycling")
    ) {
      return;
    }
    // Forward the drain mail; the child's DrainController flips its signal and the runtime picks it up on the next tick. The supervisor never blocks on the child's ack -- the accumulator is the deadline-keeper.
    await state.controlSender.send({
      type: "drain",
      data: { deadlineMs: opts.deadlineMs },
    });
    // Arm one accumulator per in-flight run; expiry escalates to a signed CancelRequested{origin:"supervisor-drain"}. Already-parked runs need no escalation -- the cohort abort eventually tears them down.
    const cohortSource = perCohortTerminalSource(
      state.terminalCohortAbort,
      state.terminalBroadcaster,
    );
    for (const runId of cohortRunIds) {
      if (drainAccumulators.has(runId)) continue;
      if (runInputChannels.has(runId)) continue;
      const accumulator = accumulatorFactory({
        substrate: bindings.repoStore,
        repoId: bindings.workflowRunRepoId,
        ref: bindings.workflowRunRef,
        anchorRunId: bindings.anchorRunId,
        runId,
        signAsPrincipal: bindings.signAsPrincipal,
        drainTimeoutMs,
        now: drainNow,
        setTimer: drainSetTimer,
        clearTimer: drainClearTimer,
        ...(cohortSource !== null ? { terminalEventSource: cohortSource } : {}),
      });
      drainAccumulators.set(runId, accumulator);
      accumulator.start();
    }
  }

  async function recycle(opts: RecycleOpts): Promise<RecycleAttempt> {
    if (respawnInProgress) {
      throw new Error("supervisor: recycle already in progress");
    }
    if (state.phase !== "running") {
      throw new Error(
        `supervisor: recycle called in phase ${state.phase}; expected running`,
      );
    }
    if (spawnContext === null) {
      throw new Error(
        "supervisor: recycle called without a spawn context; spawn() must complete first",
      );
    }
    // Keep the contention read in the caller's precondition zone: a double recycle must throw, and runRespawn sets the latch synchronously so nothing interleaves.
    return runRespawn({
      origin: opts.origin ?? "operator",
      reason: opts.reason,
      prior: state,
      priorContext: spawnContext,
      drain: async (deadlineMs) => {
        // Bypass the public surface's recycling no-op so the still-live sender (this runs before abortPriorCohort + kill) receives the frame.
        await drainImpl({ deadlineMs }, { fromRecycle: true });
      },
    });
  }

  /** Shared kill/replay/respawn/install driver: transitions to recycling, runs the triggerRecycle sequence with the caller's drain step, and swaps in the new cohort via the inline installNewChild. Crash-respawn calls it with a no-op drain. */
  async function runRespawn(args: {
    origin: RecycleOrigin;
    reason: string;
    prior: ActiveState;
    priorContext: SpawnContext;
    drain: (deadlineMs: number) => Promise<void>;
  }): Promise<RecycleAttempt> {
    // Set synchronously at entry so the caller's contention read and this set cannot be separated by a turn.
    respawnInProgress = true;
    const { origin, reason, prior, priorContext, drain } = args;
    // The cohort abort fires mid-sequence (between replay and kill), not up-front: aborting early would starve drain accumulators of live terminal events, aborting late would race the loop against the dying sender.
    const priorDispatchLoop = prior.dispatchLoop;
    // Transition to recycling: inbound mail keeps flowing, the prior loop stays alive for the drain window and exits on the abort before the kill lands.
    state = {
      phase: "recycling",
      handle: prior.handle,
      controlSender: prior.controlSender,
      channelId: prior.channelId,
      eventPump: prior.eventPump,
      onInferenceEvent: prior.onInferenceEvent,
      mailUnsubscribe: prior.mailUnsubscribe,
      credentialsSnapshot: prior.credentialsSnapshot,
      terminalCohortAbort: prior.terminalCohortAbort,
      terminalBroadcaster: prior.terminalBroadcaster,
      dispatchLoop: null,
      replayDone: null,
      sweepDone: prior.sweepDone,
    };
    let attempt: RecycleAttempt;
    try {
      attempt = await triggerRecycle(
        {
          bindings: {
            ...bindings,
            subprocessSpawner: (spawnArgs) => {
              if (state.phase !== "recycling") {
                throw new Error(
                  `Cannot spawn a replacement in supervisor phase ${state.phase}`,
                );
              }
              const handle = bindings.subprocessSpawner(spawnArgs);
              uninstalledChildren.add(handle);
              const forgetExitedChild = () =>
                uninstalledChildren.delete(handle);
              void handle.exited.then(forgetExitedChild, forgetExitedChild);
              return handle;
            },
          },
          stepOrder: priorContext.stepOrder,
          definitionHash: priorContext.definitionHash,
          warmKeep: priorContext.warmKeep,
          onInferenceEvent: priorContext.onInferenceEvent,
          current: {
            handle: prior.handle,
            controlSender: prior.controlSender,
            channelId: prior.channelId,
            eventPump: prior.eventPump,
          },
          drain,
          replayProcessingToInbox: async () => {
            await inboxPrimitives.replayProcessingToInbox(
              bindings.repoStore,
              inboxWritePrincipal,
              bindings.workflowRunRepoId,
              bindings.deploymentMailAddress,
            );
          },
          abortPriorCohort: () => {
            // Fired between drain/replay and kill; the prior loop notices the abort on its next wake and exits before the kill.
            prior.terminalCohortAbort.abort();
            wakeDispatch();
          },
          // Kept inline: the swap closes over the supervisor's full mutable cohort state; a helper would take it all as parameters for zero reuse.
          installNewChild: ({
            wiring,
            credentialsSnapshot,
            controlIncoming,
          }) => {
            // Shutdown owns the uninstalled child; a late ready frame drains its IPC resources, never installs it.
            if (state.phase !== "recycling") {
              void wiring.eventPump.catch((cause: unknown) => {
                const message =
                  cause instanceof Error ? cause.message : String(cause);
                logger.warn`orphan-cohort eventPump failed during phase-guard teardown: ${message}`;
              });
              void controlIncoming.return(undefined).catch((cause: unknown) => {
                const message =
                  cause instanceof Error ? cause.message : String(cause);
                logger.warn`orphan-cohort controlIncoming.return failed during phase-guard teardown: ${message}`;
              });
              return;
            }
            // Stop every armed accumulator (they tracked runs in the killed child); the resumed child re-discovers survivors and the next drain mints fresh ones.
            for (const accumulator of drainAccumulators.values()) {
              accumulator.stop();
            }
            drainAccumulators.clear();
            cohortRunIds.clear();
            runInputChannels.clear();
            parkNotifyWaiters.clear();
            parkGenerations.clear();
            // Reject pending round-trips and waiters so handler closures cannot survive the kill/respawn gap.
            rejectCohortAwaiters("recycle");
            // Dispose the prior broadcaster so held iterators settle done:true; the new cohort wires a fresh one.
            prior.terminalBroadcaster.dispose();
            // Mint a fresh cohort abort and start a new dispatch loop against the new wiring.
            const newCohortAbort = new AbortController();
            const newBroadcaster = createTerminalBroadcaster();
            const newDispatchLoop = runDispatchLoop(
              wiring.controlSender,
              newCohortAbort,
              newBroadcaster,
              null,
            );
            ownDispatchLoop(newDispatchLoop);
            void newDispatchLoop.catch((cause) => {
              const message =
                cause instanceof Error ? cause.message : String(cause);
              logger.error`dispatch loop (post-recycle) terminated with error: ${message}`;
            });
            // Transition back to running with the new wiring; the mail subscription is unchanged.
            state = {
              phase: "running",
              handle: wiring.handle,
              controlSender: wiring.controlSender,
              channelId: wiring.channelId,
              eventPump: wiring.eventPump,
              onInferenceEvent: priorContext.onInferenceEvent,
              mailUnsubscribe: prior.mailUnsubscribe,
              credentialsSnapshot,
              terminalCohortAbort: newCohortAbort,
              terminalBroadcaster: newBroadcaster,
              dispatchLoop: newDispatchLoop,
              replayDone: null,
              sweepDone: prior.sweepDone,
            };
            uninstalledChildren.delete(wiring.handle);
            // Bump the generation and arm the exit-watcher atomically; the predecessor's stale watcher can never drive a spurious respawn.
            armChildForRunning(wiring.handle);
            // Cache fresh spawn context so the policy timer's uptime check resets on recycle.
            const now = bindings.recyclePolicyNow ?? defaultNow;
            spawnContext = {
              stepOrder: priorContext.stepOrder,
              definitionHash: priorContext.definitionHash,
              warmKeep: priorContext.warmKeep,
              onInferenceEvent: priorContext.onInferenceEvent,
              spawnedAt: now(),
            };
            // Re-arm the pump on the new iterator; it closes over the new cohort's broadcaster, and the prior pump keeps its own.
            void pumpUpstreamControl(controlIncoming, newBroadcaster).catch(
              (cause) => {
                const message =
                  cause instanceof Error ? cause.message : String(cause);
                logger.error`upstream control pump (post-recycle) failed: ${message}`;
              },
            );
            // Kick the new loop so it picks up entries the replay just moved back.
            wakeDispatch();

            // Trigger A on the recycle seam too: a respawned child re-parks without re-emitting, and the hub link is untouched, so re-drive the re-registration here.
            void reEmitParkedCorrelations().catch((cause) => {
              const message =
                cause instanceof Error ? cause.message : String(cause);
              logger.warn`re-emit of parked correlations on re-establishment failed: ${message}`;
            });
          },
          onCrash: onChildCrash,
          // Edge-resolved once at the supervisor factory; recycle bounds
          // the respawn handshake with the same value the spawn path uses.
          readyTimeoutMs,
          ...(bindings.recyclePolicySetTimer !== undefined
            ? { setTimer: bindings.recyclePolicySetTimer }
            : {}),
          ...(bindings.recyclePolicyClearTimer !== undefined
            ? { clearTimer: bindings.recyclePolicyClearTimer }
            : {}),
        },
        { origin, reason },
      );
      // Await the previous cohort's loop so no teardown coroutine survives past the recycle's return.
      if (priorDispatchLoop !== null) {
        await priorDispatchLoop.catch(() => {
          /* swallowed: dispatch-loop failures are surfaced by the
             loop's own logger. */
        });
      }
    } catch (cause) {
      // triggerRecycle failed in `recycling`: tear the prior cohort down through the real shutdown path and re-throw. A failed crash-origin respawn is a broken deploy, not a flap: it must NOT feed the crash-loop counter and lands in `stopped`, leaving no RunFailed tombstone.
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`recycle failed; tearing supervisor down: ${message}`;
      await shutdownInternal({
        reason: `recycle failed: ${message}`,
        selfTerminated: true,
      }).catch((shutdownCause) => {
        const inner =
          shutdownCause instanceof Error
            ? shutdownCause.message
            : String(shutdownCause);
        logger.error`shutdown after recycle failure also threw: ${inner}`;
      });
      throw cause;
    } finally {
      respawnInProgress = false;
      // Handle any child exit deferred during the respawn now that the latch is clear; superseded generations drop as stale.
      maybeHandleChildExit();
    }
    return attempt;
  }

  async function deliverSignal(opts: DeliverSignalOpts): Promise<void> {
    // The supervisor is the single producer of signal.deliver frames, keeping the child the single writer of runs/<runId>/events/. `recycling` is rejected: the sender points at the dying child, so a frame could be silently lost; rejecting lets the caller retry after the recycle.
    if (state.phase !== "running" && state.phase !== "starting") {
      throw new Error(
        `supervisor: deliverSignal called in phase ${state.phase}; expected starting/running`,
      );
    }
    // Refresh the grant floor on the same channel before the signal so a standing approval's lowered floor applies to the resumed run; both frames ride the same FIFO, so grants-updated lands first. Best-effort and non-fatal.
    await deliverGrants(opts.runId);
    // deliverGrants yields the event loop; a recycle can swap state in that window, so re-assert the phase before sending into the (possibly dying) sender.
    const phaseAfterRefresh: SupervisorState["phase"] = state.phase;
    if (phaseAfterRefresh !== "running" && phaseAfterRefresh !== "starting") {
      throw new Error(
        `supervisor: deliverSignal raced a recycle in phase ${phaseAfterRefresh}; expected starting/running`,
      );
    }
    await state.controlSender.send({
      type: "signal.deliver",
      data: {
        runId: opts.runId,
        signalName: opts.signalName,
        signalId: opts.signalId,
        payload: opts.payload,
      },
    });
  }

  async function deliverSources(opts: DeliverSourcesOpts): Promise<void> {
    // Single producer of sources-updated frames; recycling is rejected for the same reason as deliverSignal.
    if (state.phase !== "running" && state.phase !== "starting") {
      throw new Error(
        `supervisor: deliverSources called in phase ${state.phase}; expected starting/running`,
      );
    }
    await state.controlSender.send({
      type: "sources-updated",
      data: {
        sources: opts.sources,
        defaultSource: opts.defaultSource,
      },
    });
  }

  async function deliverCredentials(
    opts: DeliverCredentialsOpts,
  ): Promise<void> {
    // Compute the next mirror first: it must advance regardless of phase so a revocation stays gone when the child (re)starts, not resurrected from the frozen deploy delivery.
    const next = mergeCredentialDelivery(
      currentCredentialDelivery,
      opts.delivery,
      opts.revoke,
    );
    // Send only in starting/running; otherwise just advance the mirror so the next spawned child is seeded from it.
    if (state.phase === "running" || state.phase === "starting") {
      await state.controlSender.send({
        type: "credentials-updated",
        data: {
          delivery: opts.delivery,
          ...(opts.revoke !== undefined ? { revoke: opts.revoke } : {}),
        },
      });
    }
    // Advance the mirror (a send throw skips this so the mirror still matches the child); whole-object swap via the same merge the child applies.
    currentCredentialDelivery = next;
  }

  /** Refresh a live run's grant floor mid-run: re-read its durable grants.json and push a grants-updated frame so a lowered floor applies to an executing or resuming run. Unlike pushRunGrants it NEVER synthesizes a RunFailed (a non-live child is skipped) and pushes only the file's contents, never caller-supplied grants. */
  async function deliverGrants(runId: string): Promise<"pushed" | "skipped"> {
    if (bindings.onRunStart === undefined) return "skipped";
    if (state.phase !== "running" && state.phase !== "starting") {
      return "skipped";
    }
    try {
      const snapshot = await bindings.onRunStart({
        runId,
        anchorRunId: bindings.anchorRunId,
      });
      await state.controlSender.send({
        type: "grants-updated",
        data: {
          snapshot: {
            steps: snapshot.steps.map((s) => ({
              stepId: s.stepId,
              address: s.address,
              grants: [...s.grants],
              contentHash: s.contentHash,
            })),
          },
        },
      });
      return "pushed";
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      logger.error`deliverGrants refresh failed for run ${runId}; the durable grants file still governs the next barrier/respawn: ${message}`;
      return "skipped";
    }
  }

  function getCredentialsSnapshot(): CredentialsSnapshot | null {
    if (state.phase === "starting" || state.phase === "running") {
      return state.credentialsSnapshot;
    }
    return null;
  }

  return {
    spawn,
    requestCancel,
    shutdown,
    drain,
    recycle,
    deliverSignal,
    deliverSources,
    deliverCredentials,
    deliverGrants,
    reEmitParkedCorrelations,
    getCredentialsSnapshot,
  };
}

type SupervisorState =
  | { phase: "idle" }
  | { phase: "stopping" }
  | { phase: "stopped" }
  // Terminal: the crash-loop guard latched. Distinct from `stopped` so the supervisor does not respawn and a re-entrant shutdown is idempotent.
  | { phase: "crash-looping" }
  | ({ phase: "starting" } & ActiveState)
  | ({ phase: "running" } & ActiveState)
  | ({ phase: "recycling" } & ActiveState);

type ActiveState = {
  handle: SubprocessHandle;
  controlSender: ControlChannelSender;
  channelId: string;
  eventPump: Promise<void>;
  onInferenceEvent: (event: EventPayload) => void;
  mailUnsubscribe: (() => void) | null;
  credentialsSnapshot: CredentialsSnapshot | null;
  /** Per-cohort abort controller shared by terminal-event watchers and the dispatch loop; shutdown aborts it, installNewChild mints a fresh one so nothing from the previous cohort survives. */
  terminalCohortAbort: AbortController;
  /** Per-cohort terminal-run broadcaster the control pump fans terminal.event into; disposed on shutdown/recycle so minted iterators settle done:true. */
  terminalBroadcaster: TerminalBroadcaster;
  /** The dispatch loop's exit promise. null in `starting` (the loop starts on ready); the replacing recycle awaits it, and shutdownInternal waits on every loop through `dispatchLoops`. */
  dispatchLoop: Promise<void> | null;
  /** Settles when the spawn-time processing replay resolves; shutdown awaits its substrate write, and the dispatch loop borrows it as the first-iteration gate. null on the recycle path (triggerRecycle awaited its own replay inline). */
  replayDone: Promise<void> | null;
  /** Settles when the spawn-time compaction sweep resolves; shutdown awaits it. NOT borrowed by the dispatch loop (housekeeping must not gate the first dequeue); carried forward on recycle. A fold abandoned at shutdown is re-proposed on the next boot. */
  sweepDone: Promise<void> | null;
};

type SpawnContext = {
  stepOrder: readonly string[];
  definitionHash: string;
  /** Warm-keep flag carried on respawn env (unchanged across recycle). */
  warmKeep: boolean;
  onInferenceEvent: (event: EventPayload) => void;
  spawnedAt: number;
};

/** Iterate the control-channel iterator until the child's ready frame; post-ready frames are consumed by pumpUpstreamControl on the same iterator. */
async function waitForReady(
  iter: AsyncGenerator<ControlPayload, void, void>,
): Promise<{ childPid: number }> {
  // Use explicit next() so ready does not finalize the generator via iter.return(); the upstream pump continues on the same iterator.
  while (true) {
    const next = await iter.next();
    if (next.done === true) {
      throw new Error(
        "workflow-host supervisor: control channel ended before child emitted ready",
      );
    }
    const payload = next.value;
    if (payload.type === "ready") {
      return { childPid: payload.data.childPid };
    }
    // Drop stray pre-ready variants; the receiver validated them, so one here is a bug worth warning on, not crashing over.
  }
}

function defaultNow(): number {
  return Date.now();
}

/** Drain the event-channel iterator into the sink; resolves when the iterator ends (child exit or crash callback). */
async function pumpEvents(
  iter: AsyncGenerator<EventPayload, void, void>,
  onInferenceEvent: (event: EventPayload) => void,
): Promise<void> {
  for await (const event of iter) {
    onInferenceEvent(event);
  }
}

/** Default mail-audit ref: an in-process store keyed by messageId, keeping library tests independent of audit-store wiring. */
function defaultInProcessMailAuditRef(
  messageId: string,
  _rawMessage: Uint8Array,
): MailAuditRef {
  return { store: "in-process", path: messageId };
}

/** Project a terminal.event frame into the TerminalRunEvent union downstream consumers use; the IPC validator already narrowed kind/error upstream. */
function terminalEventFromPayload(
  data: Extract<ControlPayload, { type: "terminal.event" }>["data"],
): TerminalRunEvent {
  if (data.kind === "RunCompleted") {
    return { kind: "RunCompleted", seq: data.seq, at: data.at };
  }
  if (data.kind === "RunCancelled") {
    return { kind: "RunCancelled", seq: data.seq, at: data.at };
  }
  // error.message is required for RunFailed on the wire; a missing one means the upstream validator was bypassed, so throw loudly rather than coercing to "".
  if (data.error === undefined || typeof data.error.message !== "string") {
    throw new Error(
      `terminalEventFromPayload: RunFailed payload missing required error.message (runId=${data.runId}, seq=${String(data.seq)})`,
    );
  }
  return {
    kind: "RunFailed",
    seq: data.seq,
    at: data.at,
    error: { message: data.error.message },
  };
}

/** Reconstruct an OutboundMessage from its wire projection: attachments are base64-encoded, optional fields stay omitted (exactOptionalPropertyTypes), and type is already narrowed by the wire validator. */
function outboundMessageFromPayload(
  payload: OutboundMessagePayload,
): OutboundMessage {
  const message: OutboundMessage = {
    to: payload.to,
    type: payload.type,
  };
  if (payload.cc !== undefined) message.cc = payload.cc;
  if (payload.subject !== undefined) message.subject = payload.subject;
  if (payload.content !== undefined) message.content = payload.content;
  if (payload.payload !== undefined) message.payload = payload.payload;
  if (payload.summary !== undefined) message.summary = payload.summary;
  if (payload.inReplyTo !== undefined) message.inReplyTo = payload.inReplyTo;
  if (payload.references !== undefined) message.references = payload.references;
  if (payload.correlationId !== undefined) {
    message.correlationId = payload.correlationId;
  }
  if (payload.sessionId !== undefined) message.sessionId = payload.sessionId;
  if (payload.tenantId !== undefined) message.tenantId = payload.tenantId;
  if (payload.attachments !== undefined) {
    message.attachments = payload.attachments.map((a) => ({
      name: a.name,
      contentType: a.contentType,
      data: base64ToBytes(a.dataBase64),
    }));
  }
  return message;
}

function bytesToBase64(bytes: Uint8Array): string {
  return base64Encode(bytes);
}

function base64ToBytes(value: string): Uint8Array {
  return base64Decode(value);
}
