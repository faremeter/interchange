// End-to-end acceptance for mediated credentials at action steps.
//
// A source-ref workflow from `fixtures/action-credential-workflow.ts` binds one
// tenant credential to two action handlers: `push` at top level, and
// `pushChild` inside a childWorkflow body, and `pushLoop` inside a top-level
// loop body. Deploy resolves the root bindings against the DB-seeded provider
// and credential and stamps each binding with its consumer, `action:<handler>`.
// Each handler resolves the handle through the credentials `perform` gives its
// `run`, and fetches a path on the credential's pinned origin, an in-process
// mock server that records the Authorization header it sees.
//
// The proofs:
//   - Positive: the exact seeded secret reaches the origin as a bearer from the
//     top-level handler, the childWorkflow-body handler, and the loop-body
//     handler. Each handler's output
//     is the origin's response. The top-level case runs through the real
//     sidecar substrate factory, so it covers the production
//     `actionCredentials` binding.
//   - Consumer: deploy stores each binding with consumer `action:<handler>`,
//     and each body run's grants file keeps only the use grant for its own
//     handler, which proves the grant passed the child cap.
//   - Scoping: a handler that no binding names cannot resolve the handle, and
//     the origin sees nothing from it.
//   - Negative: a run without the use grants fails closed, and the origin sees
//     nothing. A run that grants only the top-level handler fails the body
//     handlers closed, so each body run's capability enforces its own grant.
//   - Refusal: deploy refuses a workflow whose declared handles are unbound.
//
// Harness justification: SPAWN-REAL. A real hub server, a real sidecar
// subprocess, and a real workflow-process child evaluating the deployed source.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";
import { eq } from "drizzle-orm";

import { actionConsumer } from "@intx/authz";
import { createNoopCredentialCipher } from "@intx/crypto";
import { tenant as tenantTable, workflowRun } from "@intx/db/schema";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { loopBodyRunId } from "@intx/workflow";
import { deriveRunAddress } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedPrincipal,
  seedProvider,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type WorkflowRunEvent,
} from "../hub-agent/lib/deploy-flow-env";
import {
  ACTION_CREDENTIAL_HANDLE,
  CHILD_HANDLER,
  CHILD_PATH,
  LOOP_HANDLER,
  LOOP_PATH,
  LOOP_STEP_ID,
  SCOPED_OUT_HANDLER,
  SCOPED_OUT_PATH,
  TOP_HANDLER,
  TOP_PATH,
  actionCredentialWorkflowEntry,
} from "./fixtures/action-credential-workflow";
import {
  deriveWireRunGrants,
  failureMessages,
} from "./nested-tool-invoke-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const TENANT_ID = "tnt_action_credential";
const CALLER_PRINCIPAL_ID = "prn_action_credential";
const PROVIDER_ID = "prv_action_credential";
const PROVIDER_NAME = "action-credential-provider";
const CREDENTIAL_ID = "cred_action_credential";
const CREDENTIAL_NAME = "action-credential-cred";
const SECRET = "sk-action-charlie-3e8b";

// The envelope `grants.json` carries: `{ grants: [...] }`.
const GrantsFile = type({ grants: "unknown[]" }).onUndeclaredKey("ignore");

let env: DeployFlowEnv;
let h: TestDb;

// In-process mock origin the mediated http credential authenticates to.
let origin: ReturnType<typeof Bun.serve>;
const originRequests: { path: string; authorization: string | null }[] = [];

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  origin = Bun.serve({
    port: 0,
    fetch(req) {
      const url = new URL(req.url);
      originRequests.push({
        path: url.pathname,
        authorization: req.headers.get("authorization"),
      });
      return Response.json({ ok: true, seenPath: url.pathname });
    },
  });

  h = await createTestDb();
  await h.db.insert(tenantTable).values({
    id: TENANT_ID,
    name: TENANT_ID,
    slug: tenantSlugFromId(TENANT_ID),
    domain: DEPLOYMENT_DOMAIN,
    parentId: null,
  });
  await seedPrincipal(h.db, {
    id: CALLER_PRINCIPAL_ID,
    tenantId: TENANT_ID,
    kind: "user",
  });
  await seedProvider(h.db, {
    id: PROVIDER_ID,
    tenantId: TENANT_ID,
    name: PROVIDER_NAME,
    plugin: "http",
    apiBaseUrl: origin.url.origin,
  });
  await seedCredential(h.db, {
    id: CREDENTIAL_ID,
    tenantId: TENANT_ID,
    providerId: PROVIDER_ID,
    name: CREDENTIAL_NAME,
    secret: SECRET,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (origin !== undefined) await origin.stop(true);
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/** The run grant that lets `consumer` use the credential (Gate 2). */
function useGrant(consumer: string): WireGrantRule {
  return {
    id: `grant-use-${consumer}`,
    resource: `credential:${CREDENTIAL_ID}`,
    action: "use",
    effect: "allow",
    origin: "system",
    conditions: { tool: consumer },
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function stepOutput(
  events: readonly WorkflowRunEvent[],
  stepId: string,
): unknown {
  const completed = events.find(
    (event) =>
      event.type === "StepCompleted" && event.body["stepId"] === stepId,
  );
  if (completed === undefined) {
    throw new Error(`no StepCompleted event for step ${stepId}`);
  }
  const output = completed.body["output"];
  if (!isRecord(output)) {
    throw new Error(
      `step ${stepId} output is not an object: ${JSON.stringify(output)}`,
    );
  }
  const ref = output["ref"];
  if (typeof ref !== "string" || !ref.startsWith("inline:")) {
    throw new Error(
      `step ${stepId} output is not an inline ref: ${JSON.stringify(output)}`,
    );
  }
  return JSON.parse(ref.slice("inline:".length));
}

// Read a run's grants.json off the sidecar's on-disk workflow-run repo, or
// null when the file is absent.
function readRunGrantsFile(repoId: string, runId: string): unknown[] | null {
  const filePath = path.join(
    env.sidecar.dataDir,
    "workflow-runs",
    repoId,
    "runs",
    runId,
    "grants.json",
  );
  if (!fs.existsSync(filePath)) return null;
  const parsed: unknown = JSON.parse(fs.readFileSync(filePath, "utf-8"));
  return GrantsFile.assert(parsed).grants;
}

// The credential grants in a grants file, or none when the file is absent.
function credentialGrants(grants: unknown[] | null): unknown[] {
  return (grants ?? []).filter(
    (grant) =>
      isRecord(grant) &&
      typeof grant["resource"] === "string" &&
      grant["resource"].startsWith("credential:"),
  );
}

type Deployed = {
  anchorRunId: string;
  mailAddress: string;
  workflowRunRepoId: RepoId;
  runGrants: WireGrantRule[];
};

/** Deploy the fixture by source-ref under a fresh anchor and asset. */
async function deploy(opts: {
  name: string;
  withBinding: boolean;
}): Promise<Deployed> {
  const anchorRunId = `run_action-credential-${opts.name}`;
  const definitionAssetId = `ast_action_credential_${opts.name}`;
  await seedAsset(h.db, {
    id: definitionAssetId,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: definitionAssetId,
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  const mailAddress = deriveRunAddress({
    runId: anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });
  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: anchorRunId,
    tenantId: "tenant-1",
    principalId: `prin_${anchorRunId}`,
    agentAddress: mailAddress,
    systemPrompt: "unused (no agent step)",
    tools: [],
    grants: [],
    sources: [
      {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      },
    ],
    defaultSource: "anthropic:mock-model",
  };

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule: actionCredentialWorkflowEntry({
      address: mailAddress,
      workflowId: `wf_action_credential_${opts.name}`,
      childWorkflowId: `wf_action_credential_${opts.name}_child`,
      ...(opts.withBinding
        ? { binding: { provider: PROVIDER_NAME, name: CREDENTIAL_NAME } }
        : {}),
    }),
    actions: "./workflow.mjs",
    loops: "./workflow.mjs",
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: mailAddress,
    approvals: "approve-probed",
    config,
    credentialCipher: createNoopCredentialCipher(),
  });
  const snapshot = handle.approved.grantSnapshot;
  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(mailAddress),
    { diagnostics: env.sidecarDiagnostics },
  );
  return {
    anchorRunId,
    mailAddress,
    workflowRunRepoId: handle.workflowRunRepoId,
    runGrants: deriveWireRunGrants(snapshot),
  };
}

/** Fire one run and wait for its terminal event. */
async function runOnce(
  deployed: Deployed,
  grants: WireGrantRule[],
  messageId: string,
) {
  await fireMailTrigger(env, deployed.mailAddress, { messageId, grants });
  const runId = await waitForFirstRunId(env, deployed.workflowRunRepoId, {
    diagnostics: env.sidecarDiagnostics,
  });
  const terminal = await waitForWorkflowRunComplete(
    env,
    deployed.anchorRunId,
    runId,
    { diagnostics: env.sidecarDiagnostics },
  );
  const events = await readWorkflowRunEvents(env, deployed.anchorRunId, runId);
  return { runId, terminal, events };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "mediated credentials at action steps",
  () => {
    test("top-level and child-run actions authenticate with the bound credential", async () => {
      const deployed = await deploy({ name: "pos", withBinding: true });

      // Deploy stamped each binding with its handler's consumer.
      const [anchor] = await h.db
        .select({ credentialRefs: workflowRun.credentialRefs })
        .from(workflowRun)
        .where(eq(workflowRun.id, deployed.anchorRunId));
      expect(anchor?.credentialRefs?.bindings).toEqual([
        {
          handle: ACTION_CREDENTIAL_HANDLE,
          credentialId: CREDENTIAL_ID,
          consumer: actionConsumer(TOP_HANDLER),
        },
        {
          handle: ACTION_CREDENTIAL_HANDLE,
          credentialId: CREDENTIAL_ID,
          consumer: actionConsumer(CHILD_HANDLER),
        },
        {
          handle: ACTION_CREDENTIAL_HANDLE,
          credentialId: CREDENTIAL_ID,
          consumer: actionConsumer(LOOP_HANDLER),
        },
      ]);

      const topGrant = useGrant(actionConsumer(TOP_HANDLER));
      const childGrant = useGrant(actionConsumer(CHILD_HANDLER));
      const loopGrant = useGrant(actionConsumer(LOOP_HANDLER));
      const before = originRequests.length;
      const { runId, terminal, events } = await runOnce(
        deployed,
        [...deployed.runGrants, topGrant, childGrant, loopGrant],
        "<action-credential-pos-1@integration.interchange>",
      );
      if (terminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failureMessages(events))}\n${env.sidecarDiagnostics()}`,
        );
      }

      // Each handler returned the origin's response to its own fetch.
      expect(stepOutput(events, TOP_HANDLER)).toEqual({
        status: 200,
        body: { ok: true, seenPath: TOP_PATH },
      });
      const spawned = events.find((e) => e.type === "ChildSpawned");
      const childRunId = spawned?.body["childRunId"];
      if (typeof childRunId !== "string") {
        throw new Error("expected a ChildSpawned event with a childRunId");
      }
      const childEvents = await readWorkflowRunEvents(
        env,
        deployed.anchorRunId,
        childRunId,
      );
      expect(stepOutput(childEvents, CHILD_HANDLER)).toEqual({
        status: 200,
        body: { ok: true, seenPath: CHILD_PATH },
      });
      const loopRunId = loopBodyRunId(runId, LOOP_STEP_ID, 0);
      const loopEvents = await readWorkflowRunEvents(
        env,
        deployed.anchorRunId,
        loopRunId,
      );
      expect(stepOutput(loopEvents, LOOP_HANDLER)).toEqual({
        status: 200,
        body: { ok: true, seenPath: LOOP_PATH },
      });

      // The exact seeded secret reached the origin from every bound handler,
      // and the handler that no binding names sent nothing.
      const fresh = originRequests.slice(before);
      expect(fresh).toHaveLength(3);
      expect(fresh).toContainEqual({
        path: TOP_PATH,
        authorization: `Bearer ${SECRET}`,
      });
      expect(fresh).toContainEqual({
        path: CHILD_PATH,
        authorization: `Bearer ${SECRET}`,
      });
      expect(fresh).toContainEqual({
        path: LOOP_PATH,
        authorization: `Bearer ${SECRET}`,
      });
      expect(stepOutput(events, SCOPED_OUT_HANDLER)).toEqual({
        refused: `no credential is bound to handle "${ACTION_CREDENTIAL_HANDLE}" for consumer ${actionConsumer(SCOPED_OUT_HANDLER)}`,
      });
      expect(fresh.map((r) => r.path)).not.toContain(SCOPED_OUT_PATH);

      // The parent's file holds every use grant. Each body run's inherited
      // file keeps only the grant for that body's own handler.
      const repoId = deployed.workflowRunRepoId.id;
      expect(readRunGrantsFile(repoId, runId)).toEqual(
        expect.arrayContaining([topGrant, childGrant, loopGrant]),
      );
      expect(credentialGrants(readRunGrantsFile(repoId, childRunId))).toEqual([
        childGrant,
      ]);
      expect(credentialGrants(readRunGrantsFile(repoId, loopRunId))).toEqual([
        loopGrant,
      ]);
    });

    test("fails closed when no grant authorizes the credential's use", async () => {
      const deployed = await deploy({ name: "neg", withBinding: true });
      const before = originRequests.length;
      const { terminal, events } = await runOnce(
        deployed,
        deployed.runGrants,
        "<action-credential-neg-1@integration.interchange>",
      );
      expect(terminal.type).toBe("RunFailed");
      expect(failureMessages(events).join("\n")).toContain(
        `consumer ${actionConsumer(TOP_HANDLER)} is not authorized to use credential ${CREDENTIAL_ID}`,
      );
      expect(originRequests.length).toBe(before);
    });

    test("a child-run action fails closed when only the top-level handler holds a grant", async () => {
      const deployed = await deploy({ name: "neg-child", withBinding: true });
      const before = originRequests.length;
      const { terminal, events } = await runOnce(
        deployed,
        [...deployed.runGrants, useGrant(actionConsumer(TOP_HANDLER))],
        "<action-credential-neg-child-1@integration.interchange>",
      );
      expect(terminal.type).toBe("RunFailed");

      // The child run's own capability enforced Gate 2: the top-level grant
      // names another consumer, so the child handler's resolve was refused.
      const spawned = events.find((e) => e.type === "ChildSpawned");
      const childRunId = spawned?.body["childRunId"];
      if (typeof childRunId !== "string") {
        throw new Error("expected a ChildSpawned event with a childRunId");
      }
      const childEvents = await readWorkflowRunEvents(
        env,
        deployed.anchorRunId,
        childRunId,
      );
      expect(failureMessages(childEvents).join("\n")).toContain(
        `consumer ${actionConsumer(CHILD_HANDLER)} is not authorized to use credential ${CREDENTIAL_ID}`,
      );
      expect(originRequests.slice(before)).toEqual([
        { path: TOP_PATH, authorization: `Bearer ${SECRET}` },
      ]);
    });

    test("deploy refuses a workflow whose declared handles are unbound", async () => {
      await expect(
        deploy({ name: "unbound", withBinding: false }),
      ).rejects.toThrow("credential_bindings_invalid");
    });
  },
);
