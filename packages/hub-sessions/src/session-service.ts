import { getLogger } from "@interchange/log";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@interchange/mime";
import type { CryptoProvider, HarnessConfig } from "@interchange/types/runtime";
import type { AgentRepoStore, DeployContent } from "./agent-repo";
import type { SidecarRouter } from "./ws/sidecar-handler";

const logger = getLogger(["interchange", "hub", "session-service"]);

export class SessionLaunchError extends Error {
  /** Which phase failed: "write", "provision", "pack", or "start". */
  readonly phase: string;
  /** True if the sidecar has a provisioned agent that could not be cleaned up. */
  readonly leakedAgent: boolean;

  constructor(phase: string, cause: unknown, leakedAgent: boolean) {
    const msg =
      cause instanceof Error ? cause.message : "Session launch failed";
    super(msg, { cause });
    this.name = "SessionLaunchError";
    this.phase = phase;
    this.leakedAgent = leakedAgent;
  }
}

export type SessionService = {
  /**
   * Orchestrate the full deploy lifecycle:
   *   1. Write deploy tree to hub repo and produce packfile
   *   2. Provision agent on sidecar (sendAgentDeploy)
   *   3. Deliver packfile (sendPack)
   *   4. Start session (sendSessionStart)
   *
   * On partial failure after provision, attempts cleanup via
   * sendAgentUndeploy before re-throwing.
   */
  launchSession(params: {
    agentAddress: string;
    agentId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
  }): Promise<void>;

  /**
   * Compose a signed RFC 2822 message from the user and deliver it to the
   * agent via the mail transport. Throws if the agent is unreachable.
   * Returns the raw MIME bytes of the assembled message.
   */
  sendUserMessage(params: UserMessageParams): Promise<Uint8Array>;

  /**
   * Undeploy an agent and wait for the sidecar to acknowledge.
   */
  endSession(agentAddress: string, reason: string): Promise<void>;
};

export type UserMessageParams = {
  agentAddress: string;
  from: string;
  messageId: string;
  date: Date;
  content: string;
  inReplyTo?: string;
  references?: string[];
  sessionId: string;
  tenantId: string;
  cryptoProvider: CryptoProvider;
};

export function createSessionService(deps: {
  sidecarRouter: SidecarRouter;
  agentRepoStore: AgentRepoStore;
}): SessionService {
  const { sidecarRouter, agentRepoStore } = deps;

  async function launchSession(params: {
    agentAddress: string;
    agentId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
  }): Promise<void> {
    const { agentAddress, agentId, config, deployContent } = params;

    // Phase 0: Write deploy tree and produce packfile (hub-local, no
    // sidecar state to clean up if this fails).
    let pack: Uint8Array;
    let commitSha: string;
    let ref: string;
    try {
      await agentRepoStore.writeDeployTree(agentId, deployContent);
      ({ pack, commitSha, ref } =
        await agentRepoStore.createDeployPack(agentId));
    } catch (err) {
      throw new SessionLaunchError("write", err, false);
    }

    // Phase 1: Provision on sidecar.
    try {
      await sidecarRouter.sendAgentDeploy(agentAddress, config);
    } catch (err) {
      throw new SessionLaunchError("provision", err, false);
    }

    // Phases 2-3: Pack delivery and session start. If either fails,
    // attempt cleanup so the sidecar doesn't retain a zombie agent.
    try {
      await sidecarRouter.sendPack(agentAddress, pack, ref, commitSha);
    } catch (err) {
      await attemptCleanup(agentAddress, "pack", err);
      throw new SessionLaunchError("pack", err, false);
    }

    try {
      await sidecarRouter.sendSessionStart(agentAddress);
    } catch (err) {
      await attemptCleanup(agentAddress, "start", err);
      throw new SessionLaunchError("start", err, false);
    }
  }

  async function attemptCleanup(
    agentAddress: string,
    failedPhase: string,
    originalErr: unknown,
  ): Promise<void> {
    try {
      await sidecarRouter.sendAgentUndeploy(agentAddress, failedPhase);
    } catch (cleanupErr) {
      logger.error`Failed to clean up agent ${agentAddress} after ${failedPhase} failure: ${String(cleanupErr)}`;
      // Preserve the original error as cause so the root cause is not
      // lost when the cleanup also fails.
      throw new SessionLaunchError(failedPhase, originalErr, true);
    }
  }

  async function sendUserMessage(
    params: UserMessageParams,
  ): Promise<Uint8Array> {
    const {
      agentAddress,
      from,
      messageId,
      date,
      content,
      inReplyTo,
      references,
      sessionId,
      tenantId,
      cryptoProvider,
    } = params;

    const headers: MessageHeaders = {
      from,
      to: [agentAddress],
      cc: undefined,
      date,
      messageId,
      subject: undefined,
      inReplyTo,
      references,
      mimeVersion: "1.0",
      interchangeType: "conversation.message",
      interchangeCorrelationId: undefined,
      interchangeTenantId: tenantId,
      interchangeAgentId: undefined,
      interchangeSessionId: sessionId,
      interchangeOfferingId: undefined,
      interchangeSchemaVersion: undefined,
      traceparent: undefined,
      tracestate: undefined,
    };

    const signedContent = assembleSignedContent({
      kind: "conversation",
      text: content,
    });
    const signature = await createDetachedSignatureFromProvider(
      signedContent,
      cryptoProvider,
    );
    const rawMessage = assembleMessage(headers, signedContent, signature);
    const base64 = Buffer.from(rawMessage).toString("base64");

    const delivered = sidecarRouter.routeMail(agentAddress, base64);
    if (!delivered) {
      throw new Error(
        `Failed to deliver message to ${agentAddress}: agent is unreachable`,
      );
    }

    return rawMessage;
  }

  async function endSession(
    agentAddress: string,
    reason: string,
  ): Promise<void> {
    await sidecarRouter.sendAgentUndeploy(agentAddress, reason);
  }

  return { launchSession, sendUserMessage, endSession };
}
