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
  createCredentialProviderRegistry,
  createHttpCredentialProvider,
  builtinCredentialProviders,
} from "./credential-providers";
export type {
  CredentialProviderRegistry,
  FetchLike,
  HttpCredentialProviderOptions,
} from "./credential-providers";

export { createCredentialCapability } from "./credential-capability";
export type {
  CredentialCapabilityDeps,
  HostCredentialCapability,
  ResolvedCredentialBinding,
} from "./credential-capability";

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
