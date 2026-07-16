// The db-free substrate layer of this package.
//
// `@intx/hub-sessions` holds two layers: the git-backed repo-store substrate
// (the `RepoStore`, the workflow-run claim-check and event-log primitives, the
// kind-handler protocol) and the db-backed control-plane services (sessions,
// credential push, the websocket layer). The substrate layer never imports
// `@intx/db`; the services layer does. The package barrel (`index.ts`)
// re-exports both, so importing any symbol through it pulls drizzle and the
// rest of the hub data layer into module-evaluation.
//
// This entry exposes only the substrate layer, so any consumer that needs
// repo/substrate access without the control-plane services -- for example a
// process that reads and writes the agent-state and workflow-run repos but
// runs nowhere near a database -- can import it without dragging `@intx/db`
// into its boot graph. Every symbol re-exported here is defined in a db-free
// module. The barrel still re-exports all of these for hub-side consumers.

export {
  DEFAULT_CONSUMED_RETENTION_MS,
  dequeueToProcessing,
  enqueueInbox,
  markConsumed,
  readOwnedMessageIds,
  readProcessingEntry,
  replayProcessingToInbox,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
} from "./workflow-run-kind";
export type {
  WorkflowRunSupervisorPrincipal,
  WorkflowRunWorkflowProcessPrincipal,
  DequeueToProcessingResult,
  EnqueueInboxArgs,
  EnqueueInboxResult,
  MarkConsumedArgs,
  MarkConsumedResult,
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
