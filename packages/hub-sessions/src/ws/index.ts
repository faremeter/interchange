export {
  createSidecarRouter,
  type SidecarRouter,
  type SidecarRouterConfig,
  type SidecarConnection,
  type SidecarAuthIdentity,
  type SidecarAuthenticator,
  type AllocatedSidecarTarget,
  type SidecarAllocationRouter,
  type SendPackOptions,
  type WsHandle,
} from "./sidecar-handler";
export {
  createSidecarCredentialResolver,
  createSidecarTokenAuthenticator,
  type CreateSidecarTokenAuthenticatorDeps,
} from "./sidecar-token-authenticator";
export {
  createSidecarEmitter,
  type SidecarEventEmitter,
  type SidecarEventMap,
  type SidecarEventType,
  type SidecarEventListener,
  type SidecarLookups,
  type SidecarMailPersistedRow,
  type SidecarMailPersistedPayload,
  type MailTriggeredRunGrantsResult,
  type WorkflowRunPackSource,
} from "./sidecar-events";
