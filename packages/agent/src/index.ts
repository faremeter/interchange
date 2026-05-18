// @interchange/agent — in-process agent runtime.
//
// Sits on top of `createReactorAssembly` from `@interchange/inference` to
// provide a code-driven agent surface: send a message, stream events, project
// history, hot-swap providers. Peer to `@interchange/harness`; the harness
// drives the reactor from a mail transport (INBOX watch, connector threads,
// outbound replies via MessageTransport) while the agent drives it from
// in-process calls.

export { AgentInUseError } from "./lock";
export {
  type AgentTool,
  type AgentToolRunner,
  type StringToolHandler,
  type ToolHandler,
  DuplicateToolError,
  createToolRunner,
  stringTool,
  tool,
} from "./tool";
export {
  type ProviderRegistry,
  InvalidProviderConfigError,
  ProviderNotFoundError,
  createProviderRegistry,
} from "./provider";
