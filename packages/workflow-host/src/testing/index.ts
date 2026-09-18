// @intx/workflow-host/testing -- in-memory doubles for the IPC transports.
//
// These exist so a test can drive a supervisor or a workflow-process child
// over the real channel code without spawning a process. They hold their
// frames in an array and have no durability, no backpressure, and no
// framing beyond the newline terminator the event channel expects, so a
// production caller reaching for this subpath is almost certainly looking
// for the real transports in the package root instead.
export {
  createMemoryFrameStream,
  createMemoryNdjsonStream,
  type MemoryFrameStream,
  type MemoryNdjsonStream,
} from "./memory-streams";
export {
  createSupervisorReaper,
  type ReapableSupervisor,
  type SupervisorReaper,
} from "./supervisor-reaper";
export { createMockMailBus, type MockMailBus } from "./mail-bus";
export { createSpawnObserver, type SpawnObserver } from "./spawn-observer";
export {
  parseTriggerFireRunIds,
  readPayloadsOfType,
  waitForTriggerFireRunIds,
  waitForUpstreamPayload,
  waitForUpstreamPayloads,
  type UpstreamFrameSource,
} from "./upstream-frames";
export { createChangeNotifier, type ChangeNotifier } from "./change-notifier";
export {
  createLogCapture,
  type CapturedLogRecord,
  type LogCapture,
} from "./log-capture";
export { createStubRepoStore, type StubRepoStoreOpts } from "./stub-repo-store";
