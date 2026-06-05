export {
  createHarness,
  defineMailTools,
  type Harness,
  type MailEnv,
  type MailToolWrapper,
} from "./harness";

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
