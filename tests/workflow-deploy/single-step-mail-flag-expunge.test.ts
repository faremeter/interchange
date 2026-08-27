// Single-step warm-agent flag + expunge round-trip (INTR-480).
//
// Proves that the warm single-step agent consumes a message end-to-end: it
// flags an inbound mail \Deleted through `mail_flag`, then removes it with
// `mail_expunge`, and the change lands in the deployment's durable substrate
// mailbox on the hub. Both tools route through the supervisor -- the sole
// mailbox writer -- so this exercises the whole chain: agent tool -> child
// mailbox-mutation bridge -> supervisor apply + flush -> pack-push to the hub.
//
// The test carries TWO inbound messages so it can prove the expunge sweep is
// \Deleted-SELECTIVE, not a blanket wipe -- which is what makes the routed
// read-your-writes claim unconditional. The scripted mock provider drives the
// loop deterministically inside ONE run:
//   turn 0: mail_wait for mail 2's sender -- blocks the run.
//   turn 1 (after mail 2 arrives): mail_flag mail 1 (uid 1) \Deleted.
//   turn 2: mail_expunge -- sweeps every \Deleted message from INBOX.
//   turn 3: a closing text turn, which ends the run.
// Mail 1 is the first message, so the supervisor's eager-commit assigns it
// uid 1 -- the static ref the flag turn targets. Mail 2, delivered while the
// run blocks in mail_wait, eager-commits as uid 2 and is never flagged.
//
// Assertions, read off the hub's replicated workflow-run repo:
//   * The committed `mailbox/INBOX/index.json` holds exactly the UNFLAGGED
//     uid 2: the expunge removed uid 1 (\Deleted) and left uid 2. This only
//     holds if the sweep observed the flag mail_flag wrote AND honored it
//     selectively -- the routed read-your-writes. uidNext stayed at 3, so the
//     mailbox was never reset.
//   * The `1.eml` blob is ABSENT from the tip tree (physically expunged) but
//     still reachable through an ancestor commit: the audit bytes survive in
//     git history, not the live tree. `2.eml` remains in the tip tree.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import fs from "node:fs";
import git from "isomorphic-git";
import { type } from "arktype";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type { RepoId } from "@intx/hub-sessions";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepMailInboxEntry } from "./fixtures/single-step-mail-inbox-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_ma11500dba5eba11cafef00dba5eba55";
const STEP_ID = "step1";
const AGENT_ID = "agent-mail-flag-expunge";
const WORKFLOW_RUN_REF = "refs/heads/main";

// Mail 1 opens the run and eager-commits as uid 1; the agent flags it \Deleted.
const MAIL1_MESSAGE_ID = "<mail-fx-m1-7c4e21@integration.interchange>";
const MAIL1_FROM = "sender-7c4e21@integration.interchange";
const MAIL1_BODY = "Consume me: flag then expunge marker mail-fx-7c4e21.";

// Mail 2 arrives mid-run (delivered while the agent blocks in mail_wait) and
// eager-commits as uid 2. It is never flagged, so the expunge must leave it --
// that is what proves the sweep honors \Deleted rather than wiping everything.
const MAIL2_MESSAGE_ID = "<mail-fx-m2-7c4e21@integration.interchange>";
const MAIL2_FROM = "bystander-7c4e21@integration.interchange";
const MAIL2_BODY = "An unflagged bystander that must survive the expunge.";

const RESULT_MARKER = "FLAG-EXPUNGE-DONE";
// mail_wait's own timeout, kept far above a healthy notify-wake so that a run
// completing inside the shorter waitForWorkflowRunComplete budget proves the
// wait resolved from mail 2's arrival, not from a wait expiry.
const MAIL_WAIT_TIMEOUT_S = 90;

// The agent waits for mail 2, then flags mail 1 (uid 1) \Deleted and expunges.
const TOOL_NAMES = ["mail_wait", "mail_flag", "mail_expunge"] as const;
const TOOL_GRANTS: WireGrantRule[] = TOOL_NAMES.map((name) => ({
  id: `grant-tool-${name}`,
  resource: `tool:${name}`,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
}));

const TENANT_ID = "tnt_single_step_mail_fx";
const CALLER_PRINCIPAL_ID = "prn_single_step_mail_fx";
const DEFINITION_ASSET_ID = "ast_single_step_mail_fx_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: TENANT_ID,
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "single-step-mail-fx-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceScriptedTurns: [
      // turn 0: block until mail 2 arrives, so it is committed as uid 2 before
      // the expunge runs -- an unflagged message the sweep must leave alone.
      {
        toolUse: {
          name: "mail_wait",
          input: { query: { from: MAIL2_FROM }, timeout: MAIL_WAIT_TIMEOUT_S },
        },
      },
      // turn 1: flag mail 1 (uid 1) \Deleted.
      {
        toolUse: {
          name: "mail_flag",
          input: { ref: { uid: 1, mailbox: "INBOX" }, set: ["\\Deleted"] },
        },
      },
      // turn 2: sweep every \Deleted message out of the live INBOX.
      { toolUse: { name: "mail_expunge", input: {} } },
      // turn 3: a closing text turn ends the run.
      { text: RESULT_MARKER },
    ],
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/** The committed `mailbox/INBOX/index.json` fields this test inspects. */
const MailboxIndex = type({
  uidNext: "number",
  messages: type({ uid: "number", "+": "ignore" }).array(),
  "+": "ignore",
});
type MailboxIndex = typeof MailboxIndex.infer;

/**
 * Read the committed `mailbox/INBOX/` subtree from the hub's replicated
 * workflow-run repo: the parsed `index.json` and the set of `<uid>.eml` blob
 * names present in the tip tree. Returns `null` when the subtree does not
 * exist yet.
 */
async function readMailboxInbox(
  workflowRunRepoId: RepoId,
): Promise<{ index: MailboxIndex; emlNames: Set<string> } | null> {
  const repoDir =
    env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
  const oid = await git.resolveRef({ fs, dir: repoDir, ref: WORKFLOW_RUN_REF });
  let tree: Awaited<ReturnType<typeof git.readTree>>;
  try {
    tree = await git.readTree({
      fs,
      dir: repoDir,
      oid,
      filepath: "mailbox/INBOX",
    });
  } catch (cause) {
    if (cause instanceof git.Errors.NotFoundError) return null;
    throw cause;
  }
  let index: MailboxIndex | null = null;
  const emlNames = new Set<string>();
  for (const entry of tree.tree) {
    if (entry.type !== "blob") continue;
    if (entry.path === "index.json") {
      const { blob } = await git.readBlob({ fs, dir: repoDir, oid: entry.oid });
      const parsed: unknown = JSON.parse(new TextDecoder().decode(blob));
      const validated = MailboxIndex(parsed);
      if (validated instanceof type.errors) {
        throw new Error(
          `mailbox index has an unexpected shape: ${validated.summary}`,
        );
      }
      index = validated;
    } else if (entry.path.endsWith(".eml")) {
      emlNames.add(entry.path);
    }
  }
  if (index === null) throw new Error("mailbox subtree carried no index.json");
  return { index, emlNames };
}

/**
 * True when the given `<uid>.eml` blob is reachable through some commit in the
 * workflow-run ref's history -- i.e. the expunged message's bytes survive in
 * git history even after they left the tip tree.
 */
async function emlInHistory(
  workflowRunRepoId: RepoId,
  emlName: string,
): Promise<boolean> {
  const repoDir =
    env.hub.agentRepoStore.repoStore.getRepoDir(workflowRunRepoId);
  const commits = await git.log({ fs, dir: repoDir, ref: WORKFLOW_RUN_REF });
  for (const commit of commits) {
    try {
      await git.readBlob({
        fs,
        dir: repoDir,
        oid: commit.oid,
        filepath: `mailbox/INBOX/${emlName}`,
      });
      return true;
    } catch (cause) {
      if (cause instanceof git.Errors.NotFoundError) continue;
      throw cause;
    }
  }
  return false;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "warm single-step agent flags a message \\Deleted and expunges it",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("mail_expunge removes the \\Deleted message, leaves the unflagged one, and keeps the expunged bytes in history", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const entryModule = singleStepMailInboxEntry({
        stepId: STEP_ID,
        systemPrompt: "You are the single-step mail-inbox agent.",
        address: deploymentMailAddress,
        agentId: AGENT_ID,
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: deploymentMailAddress,
        approvals: "approve-probed",
        config,
        sources: { [STEP_ID]: [inferenceSource] },
      });
      expect(handle.publicKey).toBeTruthy();

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Fire mail 1: it opens the run and eager-commits as uid 1. The agent's
      // first scripted turn is mail_wait, which blocks the run.
      const mail1 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL1_MESSAGE_ID,
        from: MAIL1_FROM,
        content: MAIL1_BODY,
        grants: TOOL_GRANTS,
      });
      expect(mail1.messageId).toBe(MAIL1_MESSAGE_ID);

      const runId = await waitForFirstRunId(env, handle.workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      // Wait for the mail_wait turn to issue (the run's first inference), then
      // settle so its watch is registered before mail 2 arrives.
      await waitFor(() => env.inference.requests.length >= 1, {
        timeoutMs: 20_000,
        diagnostics: env.sidecarDiagnostics,
      });
      await new Promise((r) => setTimeout(r, 300));

      // Fire mail 2 while the run blocks in mail_wait: it eager-commits as uid 2
      // and resolves the wait, so the agent proceeds to flag uid 1 and expunge
      // with uid 2 already in the mailbox -- unflagged.
      const mail2 = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: MAIL2_MESSAGE_ID,
        from: MAIL2_FROM,
        content: MAIL2_BODY,
        grants: TOOL_GRANTS,
      });
      expect(mail2.messageId).toBe(MAIL2_MESSAGE_ID);

      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        const failed = events.find(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      // The committed mailbox holds exactly the unflagged uid 2: the expunge
      // removed uid 1 (\Deleted) and honored the flag selectively, leaving uid 2
      // alone. That the sweep dropped uid 1 but not uid 2 is the read-your-writes
      // proof -- the supervisor's sweep observed the flag mail_flag wrote.
      // uidNext stays 3 (mail 2 got uid 2, never a reused 1), so the mailbox was
      // not reset.
      const mailbox = await readMailboxInbox(handle.workflowRunRepoId);
      if (mailbox === null) {
        throw new Error("mailbox/INBOX subtree was never committed");
      }
      expect(mailbox.index.messages.map((m) => m.uid)).toEqual([2]);
      expect(mailbox.index.uidNext).toBe(3);
      expect(mailbox.emlNames.has("1.eml")).toBe(false);
      expect(mailbox.emlNames.has("2.eml")).toBe(true);

      // The expunged bytes are not lost: `1.eml` is still reachable through an
      // ancestor commit (a workflow-run repo's objects are never GC'd), so the
      // audit trail survives in history rather than the live tree.
      expect(await emlInHistory(handle.workflowRunRepoId, "1.eml")).toBe(true);
    });
  },
);
