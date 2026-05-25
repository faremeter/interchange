export {
  createClock,
  ClockOverrunError,
  ClockWallClockOverrunError,
} from "./clock";
export type { Clock, AdvanceOpts, RunOpts } from "./clock";

export { setupHarness } from "./harness";
export type { Harness, SetupHarnessOpts } from "./harness";

export { createSimulatedStream, toStreamId } from "./simulated-stream";
export type {
  SimulatedStream,
  SimulatedStreamHandle,
  StreamId,
  CreateSimulatedStreamOpts,
  ChunkFiredEvent,
  EnqueueAllOpts,
} from "./simulated-stream";

export type {
  Scenario,
  ReplyOnceOpts,
  ReplyOnceToolCall,
  RequestPredicate,
  BodyAwareRequestPredicate,
  StallHandle,
  StallOpts,
  WhenRequestMatchesOpts,
  WireEventPredicate,
} from "./scenario";

export type {
  ToolHandler,
  ToolHandlerReturn,
  DispatchToolResult,
} from "./tool-handler";

export {
  WrongHarnessError,
  UnmatchedFetchError,
  AmbiguousRequestError,
} from "./errors";
export type { UnmatchedFetchInfo, AmbiguousFetchInfo } from "./errors";

export * as wire from "./wire";

export {
  expectEvents,
  expectMediaBlock,
  expectToolCalls,
  expectToolCall,
} from "./matchers";
export type {
  EventAssertion,
  EventPartial,
  ToolCallsAssertion,
  ToolCallPartial,
  CollectedToolCall,
  MediaBlock,
  MediaBlockAssertion,
  ExpectMediaBlockOpts,
  SingleToolCallAssertion,
} from "./matchers";

export { INVARIANTS, formatEventBrief } from "./invariants";
export type {
  Invariant,
  InvariantViolation,
  ReplayContext,
} from "./invariants";

export { runCompatReplay } from "./compat-replay";
export type {
  CompatReplayOpts,
  CompatReplayResult,
  CompatReplaySkipReason,
} from "./compat-replay";

export {
  SessionManifest,
  loadSessionManifest,
  writeSessionManifest,
} from "./session-manifest";
