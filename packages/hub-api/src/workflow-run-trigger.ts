// The shared workflow-run trigger: deliver a fresh signed conversation message
// to a deployment's stable top-level run, firing it (or resuming its live
// onTrigger input) through the run's grant materialization and the sidecar's
// dispatch transport. Both the deployment Trigger route and the run mail-send
// route drive this one path so the two surfaces cannot drift on how a run is
// fired, authorized, or delivered.
//
// The function owns everything from attachment validation and the deployment's
// anchor resolution through grant staging/commit and delivery. It returns a
// discriminated result rather than an HTTP response so each route maps the same
// outcome onto its own surface with one identical `c.json(result.body,
// result.status)` line.

import { and, eq, isNotNull } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { type } from "arktype";

import type { MailSendAuthorizer } from "@intx/authz";
import {
  asset,
  hubConsumersOf,
  isLiveWorkflowRunStatus,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type {
  DB,
  DBExecutor,
  GrantStoreFactory,
  PrincipalKeyStore,
  RunTokenStore,
} from "@intx/db";
import {
  loadDeploymentGrantSnapshot,
  resolveFrameSenderKey,
  WorkflowRunDispatchPayloadConflictError,
  WorkflowRunNotExecutableError,
} from "@intx/db";
import {
  assembleSignedContent,
  assembleMessage,
  type MessageHeaders,
} from "@intx/mime";
import { createDetachedSignatureWithSigner } from "@intx/crypto";
import {
  base64Encode,
  deriveWorkflowRunId,
  isSidecarAllocationDispatchable,
  SendMailRequest,
  validateAttachments,
  type AttachmentError,
  type GrantWalkSnapshot,
} from "@intx/types";
import type { MessageAttachment } from "@intx/types/runtime";
import type { RunGrantsFrame } from "@intx/types/sidecar";
import type {
  RepoStore,
  SidecarRouter,
  WorkflowDispatchService,
} from "@intx/hub-sessions";
import { deriveRunPrincipalId, generateId } from "@intx/hub-common";
import { deriveRunAddress } from "@intx/workflow-deploy";

import {
  admitCallerSignedMail,
  type AdmittedCallerSignedMail,
  type CallerSignedMailRejection,
} from "./caller-signed-mail";
import type { PrincipalRow, TenantRow } from "./context";
import {
  commitRunGrants,
  loadCommittedRunGrants,
  grantRowsWithLockedCredentialResolution,
  lockAnchorCredentialResolution,
  lockDispatchableAllocation,
  lockWorkflowRunState,
  credentialUseBindings,
  inactiveRunPrincipalRejection,
  loadCommittedRunGrantsFromExecutor,
  loadRunPrincipalBeforeFirstCommit,
  lockTenantRunDelegatedGrants,
  runInvokerExpiresAt,
  stageFirstTriggerRunGrants,
} from "./run-grant-materialization";
import type { GrantMaterializationRejection } from "./grant-materialization";
import { readDurableWorkflowRunLifecycle } from "./workflow-run-lifecycle";

// DoS guard on the trigger route body. Sized identically to the agent
// mail route: above the legitimate ceiling (the 30 MB per-message
// attachment cap is ~40 MB once base64-encoded, plus JSON and text
// overhead) so over-business-cap requests are rejected by the handler
// with a structured error, while genuine garbage is rejected before the
// JSON parser allocates a giant string. Shared so both routes' 413
// boundaries cannot drift.
export const MAX_MAIL_BODY_BYTES = 44 * 1024 * 1024;

// Response for the run-trigger path. The trigger fires a mail at the
// deployment address; the run id is minted by the supervisor on the
// sidecar side and is not known synchronously here, so the caller
// correlates the downstream RunStarted via the returned messageId.
export const WorkflowRunTriggerResponse = type({
  runId: "string",
  address: "string",
  messageId: "string",
});

/** The error code of a run mail the caller's `mail.send` grants refuse. */
export const MAIL_SEND_DENIED_CODE = "mail_send_denied";

export type TriggerWorkflowRunDeps = {
  db: DB["db"];
  principalKeyStore: PrincipalKeyStore;
  runTokenStore: RunTokenStore;
  grantStoreFor: GrantStoreFactory;
  // Decides whether the caller may send the mail that starts the run.
  authorizeMailSend: MailSendAuthorizer;
  sidecarRouter: SidecarRouter;
  workflowDispatchService?: WorkflowDispatchService;
  repoStore: RepoStore;
};

export type TriggerWorkflowRunArgs = {
  tenant: TenantRow;
  principal: PrincipalRow;
  userName: string | null;
  anchorRunId: string;
  message: SendMailRequest;
};

type TriggerErrorBody =
  | { error: AttachmentError }
  | { error: { code: string; message: string } };

export type TriggerWorkflowRunResult =
  | {
      ok: true;
      status: 202;
      body: { runId: string; address: string; messageId: string };
    }
  | {
      ok: false;
      status: 400 | 403 | 404 | 409 | 503;
      body: TriggerErrorBody;
      headers?: Record<string, string>;
    };

function callerSignedRejection(
  rejection: CallerSignedMailRejection,
): TriggerWorkflowRunResult {
  const { status, code, message, retryAfterSeconds } = rejection;
  return {
    ok: false,
    status,
    body: { error: { code, message } },
    ...(retryAfterSeconds !== undefined
      ? { headers: { "Retry-After": String(retryAfterSeconds) } }
      : {}),
  };
}

function runUnavailable(
  runId: string,
  state: "stopping" | "terminal",
): TriggerWorkflowRunResult {
  return {
    ok: false,
    status: 409,
    body: {
      error: {
        code:
          state === "stopping"
            ? "workflow_run_stopping"
            : "workflow_run_terminal",
        message: `Workflow run ${runId} is ${state} and cannot receive more mail`,
      },
    },
  };
}

function grantRejectionResponse(
  rejection: GrantMaterializationRejection,
): TriggerWorkflowRunResult {
  const { status, code, message } = rejection;
  return { ok: false, status, body: { error: { code, message } } };
}

/**
 * Build the workflow-run trigger over its stable per-app dependencies. The
 * returned function fires one trigger occurrence per call: it validates the
 * message, resolves the deployment's anchor run, materializes the run's grants,
 * assembles the signed message, and delivers it either durably (provisioned
 * allocation) or over ordinary sidecar capacity, returning a discriminated
 * result the caller maps onto its route surface.
 */
export function createWorkflowRunTrigger(deps: TriggerWorkflowRunDeps) {
  const {
    db,
    principalKeyStore,
    runTokenStore,
    grantStoreFor,
    authorizeMailSend,
    sidecarRouter,
    workflowDispatchService,
    repoStore,
  } = deps;

  async function readRunLifecycle(
    anchorRunId: string,
    tenantDomain: string,
    runId: string,
  ) {
    const deploymentAddress = deriveRunAddress({
      runId: anchorRunId,
      domain: tenantDomain,
    });
    return readDurableWorkflowRunLifecycle(repoStore, deploymentAddress, runId);
  }

  return async function triggerWorkflowRun(
    args: TriggerWorkflowRunArgs,
  ): Promise<TriggerWorkflowRunResult> {
    const { tenant, principal, anchorRunId, message: body } = args;

    const address = deriveRunAddress({
      runId: anchorRunId,
      domain: tenant.domain,
    });
    const runId = deriveWorkflowRunId(address);
    const topLevelRun = alias(workflowRun, "mail_top_level_run");
    // The address the caller's mail is signed and stamped under.
    const fromAddr = `${principal.refId}@${tenant.domain}`;

    // Custody of the signing key decides the form. The hub signs a
    // structured message only with a key it holds: a user principal's. Any
    // other caller -- a workflow run, whose key stays in its sidecar -- posts
    // a message it signed itself.
    let callerSigned: AdmittedCallerSignedMail | null = null;
    let messageAttachments: MessageAttachment[] = [];
    let content = "";
    if (typeof body.rawMessage === "string") {
      const admitted = await admitCallerSignedMail(db, principalKeyStore, {
        rawMessageBase64: body.rawMessage,
        callerAddress: fromAddr,
        targetAddress: address,
        tenantId: tenant.id,
      });
      if ("status" in admitted) return callerSignedRejection(admitted);
      callerSigned = admitted;
    } else {
      if (principal.kind !== "user") {
        return {
          ok: false,
          status: 400,
          body: {
            error: {
              code: "signed_mail_required",
              message:
                "This principal signs its own mail; post a signed message as rawMessage",
            },
          },
        };
      }
      // Decode and validate attachments at the boundary, emitting
      // ordered, per-index structured errors, exactly as the agent mail
      // route does.
      const attachmentResult = validateAttachments(body.attachments ?? []);
      if (!attachmentResult.ok) {
        return {
          ok: false,
          status: 400,
          body: { error: attachmentResult.error },
        };
      }
      messageAttachments = attachmentResult.attachments;
      content = body.content;
    }

    // Resolve the deployment's anchor run and, through its definition, the
    // definition asset the trigger's grants derive from. The inner join to
    // the definition yields the asset id and the definition id in one read,
    // off the run rather than the deployment projection.
    const [anchor] = await db
      .select({
        definitionId: workflowRun.definitionId,
        definitionAssetId: workflowDefinition.assetId,
        allocationId: sidecarAllocation.id,
        allocationStatus: sidecarAllocation.status,
        anchorStatus: workflowRun.status,
        runStatus: topLevelRun.status,
        credentialRefs: workflowRun.credentialRefs,
        legacyMailAccept: workflowRun.legacyMailAccept,
      })
      .from(workflowRun)
      .innerJoin(
        workflowDefinition,
        eq(workflowRun.definitionId, workflowDefinition.id),
      )
      .leftJoin(
        sidecarAllocation,
        eq(sidecarAllocation.anchorRunId, workflowRun.id),
      )
      .leftJoin(
        topLevelRun,
        and(
          eq(topLevelRun.id, runId),
          eq(topLevelRun.anchorRunId, workflowRun.id),
        ),
      )
      .where(
        and(
          eq(workflowRun.id, anchorRunId),
          eq(workflowRun.tenantId, tenant.id),
          isNotNull(workflowRun.anchorRunId),
        ),
      )
      .limit(1);
    if (anchor === undefined) {
      return {
        ok: false,
        status: 404,
        body: {
          error: {
            code: "not_found",
            message: "Workflow deployment not found",
          },
        },
      };
    }
    // The caller's `mail.send` grants decide whether it may mail this run.
    // The check runs before the trigger links the run principal, commits
    // grants or dispatches, so a refused first mail leaves the deployment as
    // it was. The route's `manage` check decides who may start a run; this
    // one decides what the caller may send.
    const sendAuthorization = await authorizeMailSend({
      sender: {
        kind: "principal",
        principalId: principal.id,
        tenantId: tenant.id,
      },
      recipients: [address],
    });
    // A fault while resolving the run's coordinates is not a refusal: it
    // fails the request as a fault while resolving the sender does.
    if (
      sendAuthorization.ok &&
      sendAuthorization.decisions[0]?.outcome === "unresolved"
    ) {
      throw new Error(
        `the send check could not resolve the coordinates of ${address}`,
      );
    }
    if (
      !sendAuthorization.ok ||
      sendAuthorization.decisions[0]?.allowed !== true
    ) {
      return {
        ok: false,
        status: 403,
        body: {
          error: {
            code: MAIL_SEND_DENIED_CODE,
            message: "No mail.send grant allows mail to this workflow run",
          },
        },
      };
    }
    const durableLifecycle = await readRunLifecycle(
      anchorRunId,
      tenant.domain,
      runId,
    );
    // A "deployed" anchor is live: mail-triggering it IS its first trigger.
    // The durable check here only rejects "terminal" (an absent durable log is
    // the valid pre-start state), so the status axis must accept "deployed" or
    // the first mail trigger of a freshly-deployed run would 409 as terminal.
    if (
      !isLiveWorkflowRunStatus(anchor.anchorStatus) ||
      (anchor.runStatus !== null &&
        !isLiveWorkflowRunStatus(anchor.runStatus)) ||
      durableLifecycle === "terminal"
    ) {
      return runUnavailable(runId, "terminal");
    }
    if (
      anchor.allocationStatus !== null &&
      !isSidecarAllocationDispatchable(anchor.allocationStatus)
    ) {
      return {
        ok: false,
        status: 409,
        body: {
          error: {
            code: "deployment_unreachable",
            message: `Workflow deployment allocation is ${anchor.allocationStatus}`,
          },
        },
      };
    }
    const definitionAssetId = anchor.definitionAssetId;
    if (definitionAssetId === null) {
      // A native workflow definition names its asset; a null here is a
      // corrupt definition the trigger cannot hydrate from.
      return {
        ok: false,
        status: 409,
        body: {
          error: {
            code: "invalid_workflow",
            message: "Workflow definition has no asset",
          },
        },
      };
    }

    const messageId =
      callerSigned?.messageId ??
      `<${generateId("sessionMail")}@${tenant.domain}>`;

    const now = new Date();
    let runPrincipalId: string;
    // The deployment's grant snapshot, read only when no run grants are
    // committed yet; the first commit stages the run's rows from it.
    let firstTriggerSnapshot: GrantWalkSnapshot | null = null;
    const committedRunGrants = await loadCommittedRunGrants(
      db,
      tenant.id,
      runId,
    );
    if (committedRunGrants !== null) {
      // This is another trigger occurrence for the one live top-level run.
      // Reuse its committed grants; recomputing from the new caller would let
      // one run change authority between sections.
      runPrincipalId = committedRunGrants.runPrincipalId;
    } else {
      // Read this deployment's immutable grant shape; a missing snapshot
      // cannot authorize an empty replacement.
      const snapshot = await loadDeploymentGrantSnapshot(db, anchorRunId);
      if (snapshot === null) {
        return {
          ok: false,
          status: 409,
          body: {
            error: {
              code: "invalid_workflow",
              message: `Workflow deployment ${anchorRunId} has no approved grant snapshot`,
            },
          },
        };
      }

      const assetRow = await db.query.asset.findFirst({
        where: and(
          eq(asset.id, definitionAssetId),
          eq(asset.tenantId, tenant.id),
        ),
      });
      if (!assetRow) {
        return {
          ok: false,
          status: 409,
          body: {
            error: {
              code: "invalid_workflow",
              message: `Definition asset ${definitionAssetId} not found`,
            },
          },
        };
      }

      runPrincipalId = await deriveRunPrincipalId(tenant.id, runId);
      firstTriggerSnapshot = snapshot;
    }

    // A caller-signed message is relayed as the bytes the hub verified. A
    // structured one is assembled here and signed with the caller's
    // hub-held key.
    let rawMessage: Uint8Array;
    if (callerSigned !== null) {
      rawMessage = callerSigned.rawMessage;
    } else {
      const from = args.userName
        ? `"${args.userName}" <${fromAddr}>`
        : fromAddr;
      // A trigger occurrence is threading-less at the mail boundary, so no
      // inReplyTo or references are stamped, and it carries no agent session, so
      // the interchangeSessionId/agentId headers are left unset. The supervisor
      // decides whether this first-fires the absent top-level log or resumes a
      // live onTrigger input. This is the same fresh-signed-message shape the
      // deploy-flow fixture's mail trigger assembles.
      const headers: MessageHeaders = {
        from,
        to: [address],
        cc: undefined,
        date: new Date(),
        messageId,
        subject: undefined,
        inReplyTo: undefined,
        references: undefined,
        mimeVersion: "1.0",
        interchangeType: "conversation.message",
        interchangeCorrelationId: undefined,
        interchangeTenantId: tenant.id,
        interchangeAgentId: undefined,
        interchangeSessionId: undefined,
        interchangeOfferingId: undefined,
        interchangeSchemaVersion: undefined,
        traceparent: undefined,
        tracestate: undefined,
      };
      const signedContent = assembleSignedContent({
        kind: "conversation",
        text: content,
        ...(messageAttachments.length > 0
          ? { attachments: messageAttachments }
          : {}),
      });
      const signature = await createDetachedSignatureWithSigner(
        signedContent,
        (input) => principalKeyStore.sign(principal.id, input),
      );
      rawMessage = assembleMessage(headers, signedContent, signature);
    }
    const base64 = base64Encode(rawMessage);

    // Called with the anchor row locked. A first commit stages the run's rows
    // under that lock: the delegators are read after a delegator change that
    // holds the lock commits, and a change that waits on the lock finds the
    // committed rows. The pre-transaction binding read races initialization,
    // so the commit also re-reads the anchor's credential resolution under the
    // lock and stamps from it, or inserts nothing while it is unrecorded.
    const commitFirstOrCanonical = async (
      tx: DBExecutor,
    ): Promise<
      | RunGrantsFrame["stepGrants"]
      | "not-ready"
      | { rejected: GrantMaterializationRejection }
    > => {
      const grantArgs = {
        db,
        runTokenStore,
        tenantId: tenant.id,
        anchorRunId,
        definitionId: anchor.definitionId,
        runId,
        runPrincipalId,
        now,
      };
      if (firstTriggerSnapshot === null) {
        return commitRunGrants(
          { ...grantArgs, hubConsumers: [], grantRows: [] },
          tx,
        );
      }
      // A trigger that lost the first-fire race finds the winner's rows here
      // and must not stage again: the delegators can have changed since.
      const raced = await loadCommittedRunGrantsFromExecutor(
        tx,
        tenant.id,
        runId,
      );
      if (raced !== null) return raced.stepGrants;
      const runPrincipal = await loadRunPrincipalBeforeFirstCommit(
        tx,
        tenant.id,
        runId,
      );
      const inactive = inactiveRunPrincipalRejection(runId, runPrincipal);
      if (inactive !== null) return { rejected: inactive };
      // Invoker-sourced requirements resolve against the deployment's
      // deployer, not this caller, so the trigger path cannot change whose
      // authority a run delegates. The deployer and the asset behind the
      // definition also resolve the run's `mail.accept` relation markers, so
      // an HTTP-launched run still carries them for later mail delivery. An
      // asset deleted since the check above leaves no creator, so a creator
      // requirement refuses as `insufficient_grants`.
      const staged = await stageFirstTriggerRunGrants(tx, grantStoreFor(tx), {
        snapshot: firstTriggerSnapshot,
        tenantId: tenant.id,
        anchorRunId,
        runId,
        runPrincipalId,
        definitionAssetId,
        legacyMailAccept: anchor.legacyMailAccept,
        credentialBindings: credentialUseBindings(anchor.credentialRefs),
        now,
        invokerExpiresAt: runInvokerExpiresAt(runPrincipal, now),
      });
      if (!staged.ok) return { rejected: staged.rejection };
      const resolution = await lockAnchorCredentialResolution(tx, anchorRunId);
      if (resolution === null) {
        throw new Error(
          `trigger run ${runId}: anchor run ${anchorRunId} disappeared while locking credential resolution`,
        );
      }
      const grantRows = grantRowsWithLockedCredentialResolution(
        staged.grantRows,
        resolution,
        { tenantId: tenant.id, runPrincipalId, now },
      );
      if (grantRows === "not-ready") return grantRows;
      return commitRunGrants(
        {
          ...grantArgs,
          hubConsumers: hubConsumersOf(resolution.credentialRefs),
          grantRows,
        },
        tx,
      );
    };

    const allocationId = anchor.allocationId;
    if (allocationId !== null) {
      if (workflowDispatchService === undefined) {
        return {
          ok: false,
          status: 503,
          body: {
            error: {
              code: "workflow_dispatch_unavailable",
              message:
                "Durable workflow dispatch is unavailable for this provisioned deployment",
            },
          },
        };
      }
      let committed;
      try {
        committed = await db.transaction(async (tx) => {
          if (firstTriggerSnapshot !== null) {
            await lockTenantRunDelegatedGrants(tx, tenant.id);
          }
          if (
            !(await lockDispatchableAllocation(tx, allocationId, anchorRunId))
          ) {
            return "allocation-unavailable" as const;
          }
          // The allocation lock serializes this read with authoritative pack
          // advancement: a receive moves the Git ref only while it holds this
          // lock. An absent run is still valid for its first mail.
          if (
            (await readRunLifecycle(anchorRunId, tenant.domain, runId)) ===
            "terminal"
          ) {
            return "run-terminal" as const;
          }
          const anchorState = await lockWorkflowRunState(
            tx,
            anchorRunId,
            anchorRunId,
          );
          if (anchorState === "stopping") return "run-stopping" as const;
          if (anchorState !== "running") return "run-terminal" as const;
          const canonicalStepGrants = await commitFirstOrCanonical(tx);
          if (canonicalStepGrants === "not-ready") return "not-ready" as const;
          if (!Array.isArray(canonicalStepGrants)) return canonicalStepGrants;
          await workflowDispatchService.enqueue(
            {
              anchorRunId: anchorRunId,
              messageId,
              senderAddress: fromAddr,
              rawMessage,
              stepGrants: canonicalStepGrants,
              now,
            },
            tx,
          );
          return "committed" as const;
        });
      } catch (error) {
        // A caller chooses the Message-ID of the mail it signs. The store
        // keys a run's queued mail by its sender and Message-ID: the same
        // sender's id with the same payload enqueues once, and with another
        // payload -- other bytes, or step grants the run's committed grants
        // changed since -- is refused.
        if (error instanceof WorkflowRunDispatchPayloadConflictError) {
          return {
            ok: false,
            status: 409,
            body: {
              error: {
                code: "message_id_conflict",
                message: `Message-ID ${messageId} is already used for this run: this sender queued it with a different payload (other bytes, or the run's grants changed since it was queued), or a signal for the run has it as its id`,
              },
            },
          };
        }
        throw error;
      }
      if (typeof committed === "object") {
        return grantRejectionResponse(committed.rejected);
      }
      if (committed === "not-ready") {
        return {
          ok: false,
          status: 409,
          body: {
            error: {
              code: "deployment_not_ready",
              message:
                "Workflow deployment has not recorded credential resolution",
            },
          },
        };
      }
      if (committed === "run-terminal")
        return runUnavailable(runId, "terminal");
      if (committed === "run-stopping")
        return runUnavailable(runId, "stopping");
      if (committed !== "committed") {
        return {
          ok: false,
          status: 409,
          body: {
            error: {
              code: "deployment_unreachable",
              message: "Workflow deployment allocation is no longer active",
            },
          },
        };
      }
      // enqueue may wake before the transaction commits.
      workflowDispatchService.wake();
      return {
        ok: true,
        status: 202,
        body: { runId: anchorRunId, address, messageId },
      };
    }

    const reserved = await db.transaction(async (tx) => {
      if (firstTriggerSnapshot !== null) {
        await lockTenantRunDelegatedGrants(tx, tenant.id);
      }
      const anchorState = await lockWorkflowRunState(
        tx,
        anchorRunId,
        anchorRunId,
      );
      if (anchorState !== "running")
        return anchorState === "stopping" ? anchorState : "terminal";
      return commitFirstOrCanonical(tx);
    });
    if (typeof reserved === "object" && !Array.isArray(reserved)) {
      return grantRejectionResponse(reserved.rejected);
    }
    if (reserved === "not-ready") {
      return {
        ok: false,
        status: 409,
        body: {
          error: {
            code: "deployment_not_ready",
            message:
              "Workflow deployment has not recorded credential resolution",
          },
        },
      };
    }
    if (reserved === "stopping" || reserved === "terminal")
      return runUnavailable(runId, reserved);
    const stepGrants = reserved;

    // Stamp the hub-verified principal address (fromAddr, the address the
    // message is signed and addressed under) as the authenticated sender --
    // never the message's own MIME From. Resolve its hub-held key so the
    // recipient can verify the signature locally against the key the hub
    // vouches for, and co-deliver it on the run's grants barrier below so the
    // sender's key rides the same push as the grant. Best-effort: a resolution
    // fault degrades to a null key (logged) rather than blocking the trigger.
    // A null key is omitted from the co-delivery, so the recipient resolves the
    // sender to `unknown`, which its admission policy rejects by default.
    // A caller-signed message co-delivers the key its signature was checked
    // against, so the recipient verifies with the same key the hub did.
    const authenticatedSenderPublicKey =
      callerSigned?.publicKey ??
      (await resolveFrameSenderKey(db, principalKeyStore, fromAddr));
    const senderIdentities =
      authenticatedSenderPublicKey !== null
        ? [{ address: fromAddr, publicKey: authenticatedSenderPublicKey }]
        : undefined;

    // Admit grants and their mail together so a concurrent delivery cannot
    // replace the sender's key between the two frames.
    let delivered: boolean;
    try {
      delivered = await sidecarRouter.routeMail(
        address,
        base64,
        fromAddr,
        messageId,
        {
          runId,
          stepGrants,
          ...(senderIdentities !== undefined ? { senderIdentities } : {}),
        },
      );
    } catch (error) {
      if (!(error instanceof WorkflowRunNotExecutableError)) throw error;
      return runUnavailable(runId, error.reason);
    }
    if (!delivered) {
      return {
        ok: false,
        status: 409,
        body: {
          error: {
            code: "deployment_unreachable",
            message: `Deployment address ${address} is not routable`,
          },
        },
      };
    }

    return {
      ok: true,
      status: 202,
      body: { runId: anchorRunId, address, messageId },
    };
  };
}
