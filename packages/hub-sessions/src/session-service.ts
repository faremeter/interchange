import { createHash } from "node:crypto";

import { getLogger } from "@intx/log";
import {
  assembleMessage,
  assembleSignedContent,
  createDetachedSignatureFromProvider,
  type MessageHeaders,
} from "@intx/mime";
import type { DB } from "@intx/db";
import { sessionAsset as sessionAssetTable } from "@intx/db/schema";
import type { CryptoProvider, HarnessConfig } from "@intx/types/runtime";
import type { AgentRepoStore, DeployContent } from "./agent-repo";
import type { AgentAssetWithAsset, AssetService } from "./asset-service";
import type { SidecarRouter } from "./ws/sidecar-handler";
import type { Principal, RepoId } from "./repo-store";

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
   *   3. Deliver deploy packfile (sendPack)
   *   4. Fan-out attached asset packs: for each row returned by
   *      `assetService.listAgentAssets(agentId)`, resolve the
   *      mountPath, resolve the ref to a source commit SHA, build a
   *      pack, insert a `session_asset` row, and send the pack to the
   *      sidecar with `mountPath` set on the `repo.pack.done` frame.
   *      The manifest insert MUST precede the pack send.
   *   5. Start session (sendSessionStart)
   *
   * On partial failure after provision, attempts cleanup via
   * sendAgentUndeploy before re-throwing.
   */
  launchSession(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
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

export type SessionServiceDeps = {
  sidecarRouter: SidecarRouter;
  agentRepoStore: AgentRepoStore;
  /**
   * Optional asset attachment integration. When set, `launchSession`
   * fans out per-attachment packs after the deploy pack lands and
   * inserts a `session_asset` row per attachment. When unset, only
   * the deploy pack is sent — the legacy single-pack path is
   * preserved bit-for-bit.
   */
  assetService?: AssetService;
  /** DB handle used for `session_asset` manifest inserts. Required
   * iff `assetService` is set. */
  db?: DB["db"];
};

// Hub-side principal for reading skill repos. Skills are signed by the
// hub itself, and listAgentAssets is being called on the hub to assemble
// packs for delivery to a sidecar — so the hub principal is correct.
const HUB_PRINCIPAL: Principal = { kind: "hub" };

type ResolvedAttachment = {
  agentAssetId: string;
  mountPath: string;
  sourceCommitSha: string;
  repoId: RepoId;
  pack: Uint8Array;
  ref: string;
};

function createPackSha(pack: Uint8Array): string {
  return createHash("sha256").update(pack).digest("hex");
}

/**
 * Compute the materialization path for an attachment from the asset's
 * kind and name. v1 does not let users override the path — the path is
 * a function of the asset, full stop. Today only `skill` has a defined
 * mapping (`skills/<asset.name>/`); other kinds reach this code path
 * via the `never` branch and throw, per the defensive-coding rule that
 * we never silently invent a default for an unhandled kind.
 *
 * Asset names are validated lowercase-kebab at `createAsset`, which is
 * the only entry path into this function, so the resulting path is
 * safe under `applyAssetPack`'s per-segment validator.
 */
function resolveMountPath(row: AgentAssetWithAsset): string {
  switch (row.asset.kind) {
    case "skill":
      return `skills/${row.asset.name}/`;
    case "agent-state":
      throw new Error(
        `mount_path_required: agent_asset row ${row.id} references agent-state asset ${row.asset.id}; agent-state attachments are not supported`,
      );
    default: {
      const exhaustive: never = row.asset.kind;
      throw new Error(
        `mount_path_required: no default mountPath for asset kind ${String(exhaustive)} on row ${row.id}`,
      );
    }
  }
}

export function createSessionService(deps: SessionServiceDeps): SessionService {
  const { sidecarRouter, agentRepoStore, assetService, db } = deps;

  if (assetService !== undefined && db === undefined) {
    throw new Error(
      "createSessionService: db is required when assetService is set",
    );
  }

  async function launchSession(params: {
    agentAddress: string;
    agentId: string;
    instanceId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
  }): Promise<void> {
    const { agentAddress, agentId, instanceId, config, deployContent } = params;

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

    // Phase 0b: Resolve attached assets before contacting the sidecar.
    // Failures at this stage are hub-local — no sidecar state to clean up.
    let attachments: ResolvedAttachment[] = [];
    if (assetService !== undefined) {
      try {
        attachments = await resolveAttachments(assetService, agentId);
      } catch (err) {
        throw new SessionLaunchError("write", err, false);
      }
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

    // Phase 2b: Asset-pack fan-out. For each attached asset, build a
    // pack, insert the manifest row, then send the pack. The manifest
    // insert MUST happen before the pack send: if the sidecar acks
    // but the row is missing, the session has materialization without
    // a recorded manifest. If the row insert fails, the pack send
    // must not happen.
    if (assetService !== undefined && attachments.length > 0) {
      for (const att of attachments) {
        try {
          await sendAttachmentPack(instanceId, agentAddress, att);
        } catch (err) {
          await attemptCleanup(agentAddress, "pack", err);
          throw new SessionLaunchError("pack", err, false);
        }
      }
    }

    try {
      await sidecarRouter.sendSessionStart(agentAddress);
    } catch (err) {
      await attemptCleanup(agentAddress, "start", err);
      throw new SessionLaunchError("start", err, false);
    }
  }

  async function sendAttachmentPack(
    instanceId: string,
    agentAddress: string,
    attachment: ResolvedAttachment,
  ): Promise<void> {
    if (db === undefined) {
      // Guarded at construction; reassert defensively so the
      // narrowing is visible to readers and a future refactor cannot
      // accidentally invoke this without a db.
      throw new Error("sendAttachmentPack invoked without a db handle");
    }

    const { agentAssetId, mountPath, sourceCommitSha, repoId, pack, ref } =
      attachment;

    const assetPackSha = createPackSha(pack);

    // Insert manifest row before the pack send so we never end up in
    // the materialized-without-manifest state.
    await db.insert(sessionAssetTable).values({
      instanceId,
      agentAssetId,
      mountPath,
      assetPackSha,
      sourceCommitSha,
      materializedAt: new Date(),
    });

    const result = await sidecarRouter.sendPack(
      agentAddress,
      pack,
      ref,
      sourceCommitSha,
      { mountPath, repoId },
    );

    if (result.assetPackSha !== assetPackSha) {
      throw new Error(
        `attachment_pack_sha_mismatch: producer hash ${assetPackSha} != router hash ${result.assetPackSha}`,
      );
    }
  }

  async function resolveAttachments(
    service: AssetService,
    agentId: string,
  ): Promise<ResolvedAttachment[]> {
    const rows = await service.listAgentAssets(agentId);
    const resolved: ResolvedAttachment[] = [];
    for (const row of rows) {
      resolved.push(await resolveAttachment(row));
    }
    return resolved;
  }

  async function resolveAttachment(
    row: AgentAssetWithAsset,
  ): Promise<ResolvedAttachment> {
    const mountPath = resolveMountPath(row);
    const repoId: RepoId = { kind: row.asset.kind, id: row.asset.id };

    const sourceCommitSha = await agentRepoStore.repoStore.resolveRef(
      HUB_PRINCIPAL,
      repoId,
      row.ref,
    );
    if (sourceCommitSha === null) {
      throw new Error(
        `attachment_ref_unresolved: ${row.asset.kind}/${row.asset.id} has no commit on ${row.ref}`,
      );
    }

    const { pack, ref: returnedRef } =
      await agentRepoStore.repoStore.createPack(HUB_PRINCIPAL, repoId, row.ref);

    return {
      agentAssetId: row.id,
      mountPath,
      sourceCommitSha,
      repoId,
      pack,
      ref: returnedRef,
    };
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
