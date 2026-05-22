// @intx/agent — in-process agent runtime.
//
// Sits on top of `createReactorAssembly` from `@intx/inference` to
// provide a code-driven agent surface: send a message, stream events,
// project history, hot-swap inference sources. Peer to `@intx/harness`;
// the harness drives the reactor from a mail transport (INBOX watch,
// connector threads, outbound replies via MessageTransport) while the
// agent drives it from in-process calls.

export { AgentInUseError } from "./lock";
export {
  type AgentTool,
  type AgentToolRunner,
  type StringToolHandler,
  type ToolHandler,
  DuplicateToolError,
  createToolRunner,
  fromToolRunner,
  stringTool,
  tool,
} from "./tool";
export {
  type SourceRegistry,
  InvalidInferenceSourceError,
  SourceNotFoundError,
  createSourceRegistry,
} from "./source";
export {
  type Agent,
  type AgentConfig,
  type SendOptions,
  type SendResult,
  AgentClosedError,
  AgentConfigError,
  createAgent,
} from "./agent";
export { SendQueueFullError } from "./send-queue";
export { StreamBackpressureError } from "./stream";
