// @intx/workflow-host/testing -- in-memory doubles for the IPC transports,
// so a test can drive a supervisor or a workflow-process child over the real
// channel code without spawning a process. No durability or backpressure; a
// production caller should use the package root's real transports.
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
