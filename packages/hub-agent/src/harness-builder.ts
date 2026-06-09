// HarnessBuilder seam.
//
// The package declares the shape of the per-agent harness construction
// step; the host (apps/sidecar today, any custom sidecar tomorrow)
// supplies the concrete implementation. This keeps @intx/hub-agent
// free of dependencies on the concrete tool, storage, authz, and
// inference packages the harness is wired up against.

import type { Harness } from "@intx/harness";
import type { MailAuditStore } from "@intx/storage-isogit";
import type { GrantRule } from "@intx/types/authz";
import type {
  ConnectorThreadState,
  CryptoProvider,
  HarnessConfig as AgentConfig,
  InferenceEvent,
  InferenceSource,
  MessageTransport,
} from "@intx/types/runtime";
import type { DeployApplyErrorFrame } from "@intx/types/sidecar";

/**
 * Per-attempt context the builder uses to emit a deploy-apply error
 * frame back to the hub. The host (typically the sidecar app's wiring
 * to hub-link) translates this into the wire-level frame.
 */
export type DeployApplyErrorEmitter = (
  payload: Omit<DeployApplyErrorFrame, "type" | "agentAddress">,
) => void;

/**
 * Inputs the builder receives for one agent's session-start. The package
 * pre-resolves every value that depends on per-agent disk layout, transport
 * registration, or crypto bootstrap so the builder itself only handles
 * harness construction.
 */
export type BuildHarnessArgs = {
  agentAddress: string;
  agentConfig: AgentConfig;
  source: InferenceSource;
  /** Absolute path to the per-agent directory, from AgentRepoStore. */
  storeDir: string;
  /** Per-agent view of the host's message transport. */
  agentTransport: MessageTransport;
  /** Per-agent crypto provider, from the host's createAgentCrypto. */
  crypto: CryptoProvider;
  onEvent: (event: InferenceEvent) => void;
  onConnectorStateChanged: (state: ConnectorThreadState | null) => void;
  /**
   * Emit a `deploy.apply.error` frame back to the hub when the tool-
   * package loader rejects an apply. The builder calls this just
   * before throwing to abort harness construction; the host wires it
   * to the sidecar's hub-link. Hosts that do not yet support
   * tool-package distribution can omit this — a builder that
   * encounters a manifest while this is undefined throws with no
   * dedicated frame, which lands as a generic `agent.error` via the
   * existing session-start error handler.
   */
  emitDeployApplyError?: DeployApplyErrorEmitter;
};

/**
 * What the builder returns. The harness drives inference; the mailStore
 * is owned by SessionManager for inbound/outbound audit; updateGrants
 * mutates the live grant ref the harness's authz closure reads;
 * disposers tear down builder-allocated resources on session end.
 *
 * The builder is responsible for invoking `dispose` on any resource it
 * allocates that survives a build failure. The bundle's disposers are
 * the contract for the *success* path; mid-construction failure must
 * be cleaned up by the builder before the throw propagates.
 */
export type HarnessBundle = {
  harness: Harness;
  mailStore: MailAuditStore;
  updateGrants(grants: GrantRule[]): void;
  disposers: (() => Promise<void>)[];
};

export type HarnessBuilder = {
  /**
   * Throws if the supplied source cannot be built by this host. Called
   * before `build` at session-start, and standalone at update-source
   * time so the operator sees rejection on the control plane rather
   * than during the next inference call.
   */
  canBuildSource(source: InferenceSource): void;
  build(args: BuildHarnessArgs): Promise<HarnessBundle>;
};
