export { createHarness } from "./harness";
export type { Harness } from "./harness";

export type { HarnessConfig } from "./config";
export { validateConfig } from "./config";
export type { BeforeToolExtension } from "@intx/types/runtime";

export {
  createDefaultDirector,
  DefaultDirector,
  type DefaultDirectorPolicy,
} from "@intx/inference";

export { mergeToolRunners } from "./merge-tool-runners";

export { createHarnessRuntimeCapabilities } from "./runtime-capabilities";
export type { HarnessRuntimeCapabilitiesOptions } from "./runtime-capabilities";

export {
  createConnectorRouter,
  NoActiveConnectorThreadError,
} from "./connector-router";
export type {
  ConnectorRouter,
  ConnectorReplyParts,
  ConnectorRouterOptions,
  RouteDecision,
} from "./connector-router";
