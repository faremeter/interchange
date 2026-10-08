// The shared workflow-run trigger: deliver a fresh signed conversation message
// to a deployment's stable top-level run, firing it (or resuming its live
// onTrigger input) through the run's grant materialization and the sidecar's
// dispatch transport. Both the deployment Trigger route and the run mail-send
// route drive this one path so the two surfaces cannot drift on how a run is
// fired, authorized, or delivered. Returns a discriminated result rather than
// an HTTP response so each route maps the same outcome onto its own surface.

import { and, eq, isNotNull } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { type } from "arktype";

import {
  asset,
  isLiveWorkflowRunStatus,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type { DB, PrincipalKeyStore } from "@intx/db";
import {
  loadFrozenGrantSnapshot,
  resolveFrameSenderKey,
  WorkflowRunNotExecutableError,
} from "@intx/db";
import type { GrantStore } from "@intx/types/authz";
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
  SendMessage,
  validateAttachments,
  type AttachmentError,
} from "@intx/types";
import type { RunGrantsFrame } from "@intx/types/sidecar";
import type {
  RepoStore,
  SidecarRouter,
  WorkflowDispatchService,
} from "@intx/hub-sessions";
import { deriveRunPrincipalId, generateId } from "@intx/hub-common";
import { deriveRunAddress } from "@intx/workflow-deploy";

import type { PrincipalRow, TenantRow } from "./context";
import {
  collectCreatorGrants,
  commitRunGrants,
  loadCommittedRunGrants,
  lockDispatchableAllocation,
  lockWorkflowRunState,
  stageRunGrantsFromSnapshot,
} from "./run-grant-materialization";
import type { MaterializedGrantRow } from "./grant-materialization";
import { readDurableWorkflowRunLifecycle } from "./workflow-run-lifecycle";

// DoS guard on the trigger route body, sized identically to the agent mail
// route: above the legitimate ceiling (~40 MB base64 for the 30 MB attachment
// cap) so over-business-cap requests get a structured error, while garbage is
// rejected before the JSON parser allocates a giant string. Shared so both
// routes' 413 boundaries cannot drift.
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

export type TriggerWorkflowRunDeps = {
  db: DB["db"];
  principalKeyStore: PrincipalKeyStore;
  grantStore: GrantStore;
  sidecarRouter: SidecarRouter;
  workflowDispatchService?: WorkflowDispatchService;
  repoStore: RepoStore;
};

export type TriggerWorkflowRunArgs = {
  tenant: TenantRow;
  principal: PrincipalRow;
  userName: string | null;
  anchorRunId: string;
  message: typeof SendMessage.infer;
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
  | { ok: false; status: 400 | 403 | 404 | 409 | 503; body: TriggerErrorBody };

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
    grantStore,
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

    // The outbound mail is signed with the caller's durable hub principal key,
    // and its From address must resolve back to that same key when verified.
    // Only a user principal's address resolves to its hub key; a run or agent
    // principal would guarantee a verification mismatch. The route layer only
    // admits user principals here, so this is an invariant assertion, not a
    // client-facing rejection.
    if (principal.kind !== "user") {
      throw new Error(
        `triggerWorkflowRun: signing principal ${principal.id} has kind ` +
          `${principal.kind}; only a user principal may originate hub-signed mail`,
      );
    }

    const address = deriveRunAddress({
      runId: anchorRunId,
      domain: tenant.domain,
    });
    const runId = deriveWorkflowRunId(address);
    const topLevelRun = alias(workflowRun, "mail_top_level_run");

    // Decode and validate attachments at the boundary, emitting ordered
    // per-index structured errors, as the agent mail route does.
    const attachmentResult = validateAttachments(body.attachments ?? []);
    if (!attachmentResult.ok) {
      return {
        ok: false,
        status: 400,
        body: { error: attachmentResult.error },
      };
    }
    const messageAttachments = attachmentResult.attachments;

    // Resolve the deployment's anchor run and, through its definition, the
    // workflow asset the trigger's grants derive from, in one join off the
    // run.
    const [anchor] = await db
      .select({
        definitionId: workflowRun.definitionId,
        definitionAssetId: workflowDefinition.assetId,
        allocationId: sidecarAllocation.id,
        allocationStatus: sidecarAllocation.status,
        anchorStatus: workflowRun.status,
        runStatus: topLevelRun.status,
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
    const durableLifecycle = await readRunLifecycle(
      anchorRunId,
      tenant.domain,
      runId,
    );
    // A "deployed" anchor is live: mail-triggering it IS its first trigger.
    // The durable check here rejects only "terminal" (an absent durable log is
    // the valid pre-start state), so the status axis must accept "deployed" or
    // the first mail trigger of a fresh deployment would 409 as terminal.
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

    const messageId = `<${generateId("sessionMail")}@${tenant.domain}>`;
    const fromAddr = `${principal.refId}@${tenant.domain}`;
    const from = args.userName ? `"${args.userName}" <${fromAddr}>` : fromAddr;

    const now = new Date();
    let runPrincipalId: string;
    let stagedGrantRows: MaterializedGrantRow[];
    let stepGrants: RunGrantsFrame["stepGrants"];
    const committedRunGrants = await loadCommittedRunGrants(
      db,
      tenant.id,
      runId,
    );
    if (committedRunGrants !== null) {
      // Another trigger occurrence for the one live top-level run: reuse its
      // immutable authorization snapshot; recomputing would let one run change
      // authority between sections.
      runPrincipalId = committedRunGrants.runPrincipalId;
      stagedGrantRows = [];
      stepGrants = committedRunGrants.stepGrants;
    } else {
      // Read the deploy-approved grant-walk snapshot frozen at approval,
      // keyed by the deployment's definition id. A null snapshot is the "not
      // yet approved" state; fail closed rather than derive an empty grant set.
      const snapshot = await loadFrozenGrantSnapshot(db, anchor.definitionId);
      if (snapshot === null) {
        return {
          ok: false,
          status: 409,
          body: {
            error: {
              code: "invalid_workflow",
              message: `Workflow definition ${anchor.definitionId} has no approved grant snapshot`,
            },
          },
        };
      }

      const assetRow = await db.query.asset.findFirst({
        where: and(
          eq(asset.id, definitionAssetId),
          eq(asset.tenantId, tenant.id),
          eq(asset.kind, "workflow"),
        ),
      });
      if (!assetRow) {
        return {
          ok: false,
          status: 409,
          body: {
            error: {
              code: "invalid_workflow",
              message: `Workflow asset ${definitionAssetId} not found`,
            },
          },
        };
      }

      runPrincipalId = await deriveRunPrincipalId(tenant.id, runId);
      // The external route resolves invoker grants live and passes the
      // snapshot's full requirement list unfiltered, keeping
      // `resolveGrantMaterialization`'s reject-on-insufficient-invoker
      // contract.
      const declaredGrantRequirements = snapshot.grantRequirements;
      const invokerGrants = await grantStore.collectGrants(
        principal.id,
        tenant.id,
      );
      const creatorGrants = await collectCreatorGrants(
        grantStore,
        tenant.id,
        assetRow.creatorPrincipalId,
        declaredGrantRequirements,
      );
      const staged = await stageRunGrantsFromSnapshot({
        snapshot,
        tenantId: tenant.id,
        runPrincipalId,
        now,
        invokerGrants,
        creatorGrants,
        grantRequirements: declaredGrantRequirements,
      });
      if (!staged.ok) {
        const { status, code, message } = staged.rejection;
        return { ok: false, status, body: { error: { code, message } } };
      }
      stagedGrantRows = staged.grantRows;
      stepGrants = staged.stepGrants;
    }

    // A trigger occurrence is threading-less at the mail boundary (no
    // inReplyTo/references) and carries no agent session (no agent headers
    // stamped). The supervisor decides whether this first-fires the absent
    // top-level log or resumes a live onTrigger input.
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
      text: body.content,
      ...(messageAttachments.length > 0
        ? { attachments: messageAttachments }
        : {}),
    });
    const signature = await createDetachedSignatureWithSigner(
      signedContent,
      (input) => principalKeyStore.sign(principal.id, input),
    );
    const rawMessage = assembleMessage(headers, signedContent, signature);
    const base64 = base64Encode(rawMessage);

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
      const committed = await db.transaction(async (tx) => {
        if (
          !(await lockDispatchableAllocation(tx, allocationId, anchorRunId))
        ) {
          return "allocation-unavailable" as const;
        }
        // The allocation lock serializes this read with authoritative pack
        // advancement; an absent run is still valid for its first mail.
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
        const canonicalStepGrants = await commitRunGrants(
          {
            db,
            principalKeyStore,
            tenantId: tenant.id,
            anchorRunId,
            definitionId: anchor.definitionId,
            runId,
            runPrincipalId,
            now,
            grantRows: stagedGrantRows,
          },
          tx,
        );
        await workflowDispatchService.enqueue(
          {
            id: `dispatch:${anchorRunId}:${messageId}`,
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
      const anchorState = await lockWorkflowRunState(
        tx,
        anchorRunId,
        anchorRunId,
      );
      if (anchorState !== "running")
        return anchorState === "stopping" ? anchorState : "terminal";
      return commitRunGrants(
        {
          db,
          principalKeyStore,
          tenantId: tenant.id,
          anchorRunId,
          definitionId: anchor.definitionId,
          runId,
          runPrincipalId,
          now,
          grantRows: stagedGrantRows,
        },
        tx,
      );
    });
    if (reserved === "stopping" || reserved === "terminal")
      return runUnavailable(runId, reserved);
    stepGrants = reserved;

    // Stamp the hub-verified principal address as the authenticated sender --
    // never the message's own MIME From -- and co-deliver its hub-held key on
    // the run's grants barrier so the recipient can verify locally. Best-effort:
    // a resolution fault degrades to a null key (logged), which the recipient's
    // admission policy rejects by default.
    const authenticatedSenderPublicKey = await resolveFrameSenderKey(
      db,
      principalKeyStore,
      fromAddr,
    );
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
