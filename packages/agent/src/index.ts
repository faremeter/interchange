// @intx/agent — in-process agent runtime.
//
// Sits on top of `createReactorAssembly` from `@intx/inference` to
// provide a code-driven agent surface: send a message, stream events,
// project history, hot-swap inference sources. Peer to `@intx/harness`;
// the harness drives the reactor from a mail transport (INBOX watch,
// connector threads, outbound replies via MessageTransport) while the
// agent drives it from in-process calls.

export { AgentContextLockError } from "./lock";
export {
  type AgentTool,
  type AgentToolRunner,
  type AnnotatedToolFactory,
  type StringToolHandler,
  type ToolBundle,
  type ToolFactory,
  type ToolFactoryMeta,
  type ToolHandler,
  DuplicateToolError,
  createToolRunner,
  defineTool,
  fromToolRunner,
  stringTool,
  tool,
} from "./tool";
export {
  type AuthorizeFn,
  type BaseEnv,
  type Dependencies,
  AgentEnvError,
} from "./env";
export {
  type AnnotatedDirectorFactory,
  type DirectorAgentContext,
  type DirectorConfigSchema,
  type DirectorFactory,
  type DirectorFactoryMeta,
  type DirectorRef,
  type DirectorRegistry,
} from "./director-types";
export { validateNamespacedId } from "./namespace";
export { CanonicalizationError, canonicalizeForHash } from "./canonicalize";
export { type DefinedDirector, defineDirector } from "./director";
export {
  createDefaultDirectorRegistry,
  createDirectorRegistry,
  UnknownDirectorIdError,
} from "./director-registry";
export {
  type DefaultDirectorConfig,
  buildDefaultDirectorRef,
  defaultDirectorFactory,
} from "./default-director";
export {
  type SourceRegistry,
  InvalidInferenceSourceError,
  SourceNotFoundError,
  createSourceRegistry,
} from "./source";
export {
  type Agent,
  type SendOptions,
  type SendResult,
  AgentClosedError,
  createAgent,
} from "./agent";
export {
  type AgentDefinition,
  type DefineAgentConfig,
  type EnvRequiredByAll,
  type InferencePreference,
  defineAgent,
} from "./definition";
export {
  effectiveDirectorRef,
  getRequiredEnvKeys,
  validateEnv,
} from "./env-validation";
export type { RequiredEnvKeys } from "./env-validation";
export { SendQueueFullError } from "./send-queue";
export { StreamBackpressureError } from "./stream";
