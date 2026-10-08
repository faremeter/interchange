// The db-free substrate layer of this package. `@intx/hub-sessions` holds two
// layers: the git-backed repo-store substrate (repo-store, workflow-run
// primitives, kind handlers) and the db-backed control-plane services (sessions,
// credential push, the websocket layer). This entry exposes only the former, so
// a consumer with no database can import it without dragging `@intx/db` into
// its boot graph; the package barrel re-exports both layers.

export {
  classifyTerminalEvent,
  DEFAULT_CONSUMED_RETENTION_MS,
  dequeueToProcessing,
  enqueueInbox,
  markConsumed,
  parseEventSeq,
  scanRunsForBoot,
  readCommittedWorkflowRunLifecycle,
  readWorkflowRunLifecycle,
  readProcessingEntry,
  replayProcessingToInbox,
  requireEventSeq,
  StaleInboxEnqueueError,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_PARTS_DIR,
  WORKFLOW_RUN_EVENTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
  MAX_MAIL_PART_PATH_COMPONENT_BYTES,
} from "./workflow-run-kind";
export type {
  WorkflowRunSupervisorPrincipal,
  WorkflowRunWorkflowProcessPrincipal,
  DequeueToProcessingResult,
  EnqueueAlreadyPresentReason,
  EnqueueInboxArgs,
  EnqueueInboxOutcome,
  EnqueueInboxResult,
  MarkConsumedArgs,
  MarkConsumedResult,
  WorkflowRunLifecycle,
  ReplayProcessingToInboxOpts,
  ReplayProcessingToInboxResult,
} from "./workflow-run-kind";

export {
  encodeCombinedEventLog,
  splitCombinedEventLog,
  WORKFLOW_RUN_EVENTS_FILE,
} from "./workflow-run-event-log";

export { workflowDefinitionEnvelopeSchema } from "./workflow-kind";

export { subscribeKind } from "./repo-store/subscribe-kind";
export type { SubscribeKindEntry } from "./repo-store/subscribe-kind";

export { createAgentRepoStore } from "./agent-repo";

export type {
  CommittedReads,
  CommittedTreeEntry,
  Principal,
  RepoId,
  RepoStore,
  WriteResult,
  InitRepoOpts,
  NewlyTerminalRun,
} from "./repo-store/types";

export { WORKFLOW_RUN_RESTORE_REFS } from "./workflow-run-restore";
