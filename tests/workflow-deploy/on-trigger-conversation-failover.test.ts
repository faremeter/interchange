// An onTrigger conversation fails over to a new host and continues with no
// lost turns, and a one-step conversation moves into an onTrigger body on a
// new deployment.
//
// The Hub runs as a subprocess with the local-process sidecar provisioner the
// admin UI e2e suite uses, so every sidecar is provisioned, lost, and replaced
// by the production allocation reconciler. The failover workflow has one
// onTrigger section on mail whose body has the agent step `reply`. Each
// inbound mail is a new body run, and the body step continues its
// conversation at its place key, `section.reply`. Two events run on the first
// sidecar. Once the Hub's copy of the run log holds the section's input park
// after each of them, the sidecar's process tree is killed, and its local disk
// goes with it. The deployment's disconnect limit is one second, so the
// reconciler soon finds the worker lost, classifies the run as parked, and
// replaces the worker: the Hub's copy of the workflow-run history is restored
// onto a new sidecar and the same frozen definition is deployed there. The
// third mail is the section's next event, and its body step continues the
// conversation.
//
// A conversation also moves to a new deployment through the routes a client
// uses. A one-step deployment whose agent step is `reply` answers one mail.
// `reply`'s state is read from the export route and passed, without its reply
// thread, to the deploy route of a new deployment as its `stepState` at the
// key `section.reply`. The new deployment puts the agent step `reply` in the
// body of its onTrigger section `section`. The Hub files the state as a seed
// in the new deployment's history, the reconciler restores that history onto
// the new sidecar, and the first event of the section continues the
// conversation. The new deployment's mailbox numbers its first message past
// the one the first deployment received, so a mailbox UID the imported turns
// name cannot reach one of its messages.
//
// The proof is what the model sees. The mock inference server records every
// request and echoes each turn's inbound text, so the request of the
// continued turn must carry the request of the turn before it plus its reply,
// verbatim, before the new mail.
//
// Harness justification: SPAWN-REAL. A real Hub subprocess with a real
// provisioner, real sidecar subprocesses, real workflow-process children, and
// mock inference. The host loss is a kill of the first sidecar's process tree.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";
import { and, eq } from "drizzle-orm";

import { createEnvKeyCredentialCipher, generateKeyPair } from "@intx/crypto";
import { createDB, loadRunPrincipalActiveKey } from "@intx/db";
import {
  principal as principalTable,
  sidecarAllocation as sidecarAllocationTable,
  tenant as tenantTable,
} from "@intx/db/schema";
import { createAgentRepoStore, DEFAULT_ASSET_REF } from "@intx/hub-sessions";
import { sectionBodyRunId } from "@intx/workflow";
import {
  credentialAad,
  deriveWorkflowRunId,
  hexDecode,
  StepStateSnapshot,
  type StepStateImport,
} from "@intx/types";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { loadEnvFile, REPO_ROOT, requireKey } from "@intx/test-harness/env";
import {
  harnessHubEnvAvailable,
  loadHarnessDbConfig,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedModel,
  seedModelOffering,
  seedModelProvider,
  seedProvider,
} from "@intx/test-harness/seed";

import { startHub, type HubHandle } from "../hub-api/lib/git-harness";
import {
  apiCall,
  createTenant,
  signUpUser,
} from "../hub-api/lib/git-asset-fixtures";
import {
  currentWaitMark,
  listProcessSubtree,
  startMockInference,
  stopOutstandingWaits,
  waitFor,
  type MockInference,
} from "../hub-agent/lib/deploy-flow-env";
import { bundleWorkflowEntry } from "../hub-agent/lib/bundle-workflow-entry";
import {
  messageText,
  conversationRequestFor,
  withoutCacheMarkers,
} from "./fixtures/conversation-requests";
import { onTriggerBodyEntry } from "./fixtures/on-trigger-body";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const PROVISIONED_HUB_ENTRY = path.join(
  "tests",
  "admin-ui-e2e",
  "harness",
  "hub.ts",
);

const MARKER_ONE = "failover-fa11a7-one";
const MARKER_TWO = "failover-fa11a7-two";
const MARKER_THREE = "failover-fa11a7-three";

const IMPORT_ONE = "route-import-2b9057-one";
const IMPORT_TWO = "route-import-2b9057-two";

const SECTION_ID = "section";
const BODY_STEP_ID = "reply";
const BODY_STEP_KEY = `${SECTION_ID}.${BODY_STEP_ID}`;

const DeployResponse = type({ id: "string" });
const RunEvents = type({
  events: type({
    seq: "number",
    type: "string",
    body: "Record<string, unknown>",
  }).array(),
});
type RunEvent = (typeof RunEvents.infer)["events"][number];

let hub: HubHandle | undefined;
let inference: MockInference | undefined;
let dbHandle: ReturnType<typeof createDB> | undefined;
const scratchDirs: string[] = [];
const waitMark = currentWaitMark();

function killProcessTree(rootPid: number): void {
  for (const pid of [rootPid, ...listProcessSubtree(rootPid)]) {
    try {
      process.kill(pid, "SIGKILL");
    } catch {
      // The process already exited.
    }
  }
}

// Each live process's start time, keyed by pid, so a pid the system reused
// after its process exited is not mistaken for that process.
function processStartTimes(): Map<number, string> {
  const result = Bun.spawnSync(["ps", "-A", "-o", "pid=,lstart="]);
  const started = new Map<number, string>();
  for (const line of new TextDecoder().decode(result.stdout).split("\n")) {
    const match = /^\s*(\d+)\s+(\S.*?)\s*$/.exec(line);
    if (match?.[1] === undefined || match[2] === undefined) continue;
    started.set(Number.parseInt(match[1], 10), match[2]);
  }
  return started;
}

function requireSetup() {
  if (hub === undefined || inference === undefined || !dbHandle) {
    throw new Error("setup did not run");
  }
  return { h: hub, mock: inference, db: dbHandle.db };
}

type Tenant = {
  user: Awaited<ReturnType<typeof signUpUser>>;
  tenantId: string;
  domain: string;
  callerPrincipalId: string;
  offeringId: string;
};

// A tenant with one offering for `mock-model`, backed by a tenant credential
// sealed under the spawned Hub's key. `key` keeps each tenant's seeded ids
// apart.
async function provisionTenant(key: string): Promise<Tenant> {
  const { h, mock, db } = requireSetup();
  const mockURL = `http://localhost:${String(mock.server.port)}`;
  const user = await signUpUser(h.url);
  const { tenantId } = await createTenant(h.url, user);
  const tenantRow = await db.query.tenant.findFirst({
    where: eq(tenantTable.id, tenantId),
  });
  if (tenantRow === undefined) throw new Error("tenant row missing");
  const callerPrincipal = await db.query.principal.findFirst({
    where: and(
      eq(principalTable.tenantId, tenantId),
      eq(principalTable.kind, "user"),
      eq(principalTable.refId, user.userId),
    ),
  });
  if (callerPrincipal === undefined) throw new Error("no caller principal");

  const hubEnv = await loadEnvFile(path.join(REPO_ROOT, ".env.hub"));
  const cipher = createEnvKeyCredentialCipher(
    hexDecode(requireKey(hubEnv, "CREDENTIAL_ENCRYPTION_KEY", ".env.hub")),
  );
  const credentialId = `cred_${key}`;
  await seedProvider(db, {
    id: `prov_${key}`,
    tenantId,
    name: "mock-anthropic",
    plugin: "anthropic",
    apiBaseUrl: mockURL,
  });
  await seedCredential(db, {
    id: credentialId,
    tenantId,
    providerId: `prov_${key}`,
    name: "mock-key",
    secret: await cipher.encrypt(
      "sk-mock",
      credentialAad(credentialId, "secret"),
    ),
  });
  await seedModel(db, {
    id: `mdl_${key}`,
    tenantId,
    canonicalName: "mock-model",
  });
  await seedModelProvider(db, {
    id: `mp_${key}`,
    tenantId,
    name: "mock-anthropic",
    plugin: "anthropic",
    baseURL: mockURL,
    credentialId,
  });
  const offeringId = `off_${key}`;
  await seedModelOffering(db, {
    id: offeringId,
    tenantId,
    modelId: `mdl_${key}`,
    providerId: `mp_${key}`,
  });
  return {
    user,
    tenantId,
    domain: tenantRow.domain,
    callerPrincipalId: callerPrincipal.id,
    offeringId,
  };
}

// The route mints the anchor id, so a definition cannot name the deployment
// address; mail goes to the address the route derives.
function placeholderAddress(tenant: Tenant): string {
  return `placeholder@${tenant.domain}`;
}

// A workflow with one onTrigger section on mail whose body has the agent step
// `reply`.
function onTriggerConversation(tenant: Tenant): string {
  return onTriggerBodyEntry({
    address: placeholderAddress(tenant),
    sectionId: SECTION_ID,
    body: {
      variant: "agent",
      stepId: BODY_STEP_ID,
      systemPrompt: "You are the conversational step.",
    },
  });
}

// A workflow with the one agent step `reply`, which answers one mail.
function oneStepConversation(tenant: Tenant): string {
  return singleStepAgentEntry({
    stepId: BODY_STEP_ID,
    systemPrompt: "You are the conversational step.",
    address: placeholderAddress(tenant),
  });
}

// Publish a conversation workflow as a workflow asset and return the commit
// its deploys name.
async function publishConversation(
  tenant: Tenant,
  assetId: string,
  entryModule: string,
): Promise<string> {
  const { h, db } = requireSetup();
  await seedAsset(db, {
    id: assetId,
    tenantId: tenant.tenantId,
    kind: "workflow",
    name: assetId,
    creatorPrincipalId: tenant.callerPrincipalId,
  });
  const scratch = await fs.mkdtemp(path.join(os.tmpdir(), "failover-"));
  scratchDirs.push(scratch);
  const workflowJs = await bundleWorkflowEntry(scratch, entryModule);
  const repoStore = createAgentRepoStore({
    dataDir: h.dataDir,
    signingKey: await generateKeyPair(),
  }).repoStore;
  const repoId = { kind: "workflow" as const, id: assetId };
  await repoStore.initRepo(repoId);
  const { commitSha } = await repoStore.writeTree(
    { kind: "hub" },
    repoId,
    DEFAULT_ASSET_REF,
    {
      files: {
        "package.json": JSON.stringify({
          name: "@wf/conversation-failover",
          version: "1.0.0",
          interchange: { workflow: "./workflow.mjs" },
        }),
        "workflow.mjs": workflowJs,
      },
      message: "conversation failover source",
    },
  );
  return commitSha;
}

async function deployConversation(
  tenant: Tenant,
  assetId: string,
  commitSha: string,
  stepState?: StepStateImport,
) {
  const { h, db } = requireSetup();
  const deployed = await apiCall(
    h.url,
    "POST",
    `/api/tenants/${tenant.tenantId}/workflows/deployments`,
    {
      source: {
        kind: "asset",
        assetId,
        package: { format: "source", commitSha },
      },
      entry: "./workflow.mjs",
      sourceOfferingIds: [tenant.offeringId],
      defaultSourceOfferingId: tenant.offeringId,
      ...(stepState !== undefined ? { stepState } : {}),
    },
    tenant.user.cookies,
  );
  const deployment = DeployResponse(deployed.data);
  if (deployed.status !== 201 || deployment instanceof type.errors) {
    throw new Error(
      `deploy failed: ${String(deployed.status)} ${JSON.stringify(deployed.data)}\n${h.output()}`,
    );
  }
  const anchorRunId = deployment.id;
  const runId = deriveWorkflowRunId(
    deriveRunAddress({ runId: anchorRunId, domain: tenant.domain }),
  );

  // The deployment's key, which commits when an initialization completes.
  const readRunKey = () =>
    loadRunPrincipalActiveKey(db, tenant.tenantId, anchorRunId);
  const readAllocation = () =>
    db.query.sidecarAllocation.findFirst({
      where: eq(sidecarAllocationTable.anchorRunId, anchorRunId),
    });
  const readEvents = async (): Promise<RunEvent[]> => {
    const res = await apiCall(
      h.url,
      "GET",
      `/api/tenants/${tenant.tenantId}/workflows/${anchorRunId}/runs/${runId}/events`,
      undefined,
      tenant.user.cookies,
    );
    const parsed = RunEvents(res.data);
    if (res.status !== 200 || parsed instanceof type.errors) {
      throw new Error(
        `events read failed: ${String(res.status)} ${JSON.stringify(res.data)}`,
      );
    }
    return parsed.events;
  };
  const inputParks = async () =>
    (await readEvents()).filter(
      (e) =>
        e.type === "SignalAwaited" &&
        e.body["stepId"] === SECTION_ID &&
        e.body["parkKind"] === "input",
    ).length;
  // A terminal event is the log's last, so a later read finds it too.
  const readTerminal = async () =>
    (await readEvents()).find(
      (e) =>
        e.type === "RunCompleted" ||
        e.type === "RunFailed" ||
        e.type === "RunCancelled",
    );
  // The body run of the section's event at `eventIndex`, once it ended:
  // its `ChildCompleted` terminal status, or `undefined` while it runs.
  const bodyTerminalStatus = async (eventIndex: number) =>
    (await readEvents()).find(
      (e) =>
        e.type === "ChildCompleted" &&
        e.body["childRunId"] ===
          sectionBodyRunId(runId, SECTION_ID, eventIndex),
    )?.body["terminalStatus"];
  const sendMail = async (content: string) => {
    const res = await apiCall(
      h.url,
      "POST",
      `/api/tenants/${tenant.tenantId}/workflows/${anchorRunId}/mail`,
      { content },
      tenant.user.cookies,
    );
    if (res.status !== 202) {
      throw new Error(
        `mail failed: ${String(res.status)} ${JSON.stringify(res.data)}`,
      );
    }
  };
  const exportStep = async (stepKey: string): Promise<StepStateSnapshot> => {
    const res = await apiCall(
      h.url,
      "GET",
      `/api/tenants/${tenant.tenantId}/workflows/runs/${runId}/steps/${stepKey}/state`,
      undefined,
      tenant.user.cookies,
    );
    const snapshot = StepStateSnapshot(res.data);
    if (res.status !== 200 || snapshot instanceof type.errors) {
      throw new Error(
        `export failed: ${String(res.status)} ${JSON.stringify(res.data)}`,
      );
    }
    return snapshot;
  };
  return {
    readRunKey,
    readAllocation,
    inputParks,
    readTerminal,
    bodyTerminalStatus,
    sendMail,
    exportStep,
  };
}

beforeAll(async () => {
  if (!harnessHubEnvAvailable()) return;
  inference = startMockInference({ echoUserMessage: true });
  hub = await startHub({
    entry: PROVISIONED_HUB_ENTRY,
    // How long a deployed worker may stay away before the reconciler decides
    // it is lost; 15 minutes by default.
    env: { WORKFLOW_DEFAULT_MAX_DISCONNECTED: "1s" },
  });
  dbHandle = createDB({ ...loadHarnessDbConfig(), schema: hub.schema });
});

afterAll(async () => {
  // A wait still registered here is one the lane timeout abandoned.
  const outstanding = stopOutstandingWaits(waitMark);
  if (outstanding !== null) {
    process.stderr.write(`\n${outstanding}\n${hub?.output() ?? ""}\n`);
  }
  if (hub !== undefined) {
    // The provisioner stops its sidecars on a clean Hub shutdown; anything a
    // failed test left running is killed here.
    const leftovers = listProcessSubtree(hub.pid);
    const startedBefore = processStartTimes();
    await hub.stop();
    const startedAfter = processStartTimes();
    for (const pid of leftovers) {
      const started = startedBefore.get(pid);
      if (started !== undefined && startedAfter.get(pid) === started) {
        killProcessTree(pid);
      }
    }
  }
  if (dbHandle !== undefined) await dbHandle.close();
  await inference?.server.stop(true);
  for (const dir of scratchDirs) {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

describe.skipIf(!harnessHubEnvAvailable())(
  "onTrigger conversation failover",
  () => {
    test("the conversation continues on a replacement host with every earlier turn", async () => {
      const { mock } = requireSetup();
      const tenant = await provisionTenant("conversation_failover");
      const assetId = "ast_conversation_failover_wf";
      const commitSha = await publishConversation(
        tenant,
        assetId,
        onTriggerConversation(tenant),
      );
      const {
        readRunKey,
        readAllocation,
        inputParks,
        readTerminal,
        bodyTerminalStatus,
        sendMail,
      } = await deployConversation(tenant, assetId, commitSha);

      // --- Two events on the first host ----------------------------------
      // No signal reports these states, so they are polled. The first deploy
      // is complete once the run principal's key commits.
      await waitFor(async () => (await readRunKey()) !== null);
      const first = await readAllocation();
      if (first?.externalRef == null) {
        throw new Error("the first sidecar has no process id");
      }
      await sendMail(`Opening turn ${MARKER_ONE}.`);
      await waitFor(async () => (await inputParks()) >= 1);
      await sendMail(`Second turn ${MARKER_TWO}.`);
      await waitFor(async () => (await inputParks()) >= 2);

      // --- Lose the first host --------------------------------------------
      killProcessTree(Number(first.externalRef));

      // --- The reconciler replaces the worker -----------------------------
      // Only a run classified as parked is replaced; any other verdict
      // releases the allocation and fails the deployment.
      await waitFor(async () => {
        const row = await readAllocation();
        const runKey = await readRunKey();
        return (
          row !== undefined &&
          row.generation > first.generation &&
          row.status === "allocated" &&
          row.externalRef !== null &&
          row.externalRef !== first.externalRef &&
          runKey !== null
        );
      });

      // --- The third event runs on the replacement host -------------------
      await sendMail(`Third turn ${MARKER_THREE}.`);
      await waitFor(async () => (await bodyTerminalStatus(2)) !== undefined);
      // The event's body run completed, and the section stays live after
      // it: the run has no terminal.
      expect(await bodyTerminalStatus(2)).toBe("completed");
      expect(await readTerminal()).toBeUndefined();

      // --- The model saw the whole conversation ---------------------------
      const secondTurn = withoutCacheMarkers(
        conversationRequestFor(mock.requests, MARKER_ONE, MARKER_TWO)
          .messages ?? [],
      );
      const thirdTurnRequest = conversationRequestFor(
        mock.requests,
        MARKER_ONE,
        MARKER_THREE,
      );
      const thirdTurn = withoutCacheMarkers(thirdTurnRequest.messages ?? []);
      expect(thirdTurn.slice(0, secondTurn.length)).toEqual(secondTurn);
      const carriedReply = thirdTurnRequest.messages?.[secondTurn.length];
      expect(carriedReply?.role).toBe("assistant");
      expect(messageText(carriedReply)).toContain(MARKER_TWO);
      expect(thirdTurn).toHaveLength(secondTurn.length + 2);
    });

    test("an onTrigger body step continues a one-step conversation exported through the routes", async () => {
      const { mock } = requireSetup();
      const tenant = await provisionTenant("conversation_import");

      // --- One turn on the exporting one-step deployment ------------------
      const assetA = "ast_conversation_import_a";
      const a = await deployConversation(
        tenant,
        assetA,
        await publishConversation(tenant, assetA, oneStepConversation(tenant)),
      );
      await waitFor(async () => (await a.readRunKey()) !== null);
      await a.sendMail(`Opening turn ${IMPORT_ONE}.`);
      await waitFor(async () => (await a.readTerminal()) !== undefined);
      expect((await a.readTerminal())?.type).toBe("RunCompleted");

      // --- Export it through the route ------------------------------------
      // The run is terminal on the Hub, so the state its turn committed
      // first is too.
      const exported = await a.exportStep(BODY_STEP_ID);
      expect(exported.turns.map((turn) => turn.role)).toEqual([
        "user",
        "assistant",
      ]);
      expect(exported.mailboxUidNext).toBe(2);
      // The step keeps a reply thread for the mail its turn answered. An
      // import carries no thread, so the client drops it.
      expect(exported.connectorState).not.toBeNull();
      const imported = { ...exported, connectorState: null };

      // --- A new deployment imports it into its onTrigger body ------------
      const assetB = "ast_conversation_import_b";
      const b = await deployConversation(
        tenant,
        assetB,
        await publishConversation(
          tenant,
          assetB,
          onTriggerConversation(tenant),
        ),
        { [BODY_STEP_KEY]: imported },
      );
      await waitFor(async () => (await b.readRunKey()) !== null);
      expect(await b.exportStep(BODY_STEP_KEY)).toEqual(imported);
      await b.sendMail(`Second turn ${IMPORT_TWO}.`);
      await waitFor(async () => (await b.inputParks()) >= 1);

      // --- The model saw the exporting deployment's whole conversation ----
      const lastTurnOnA = withoutCacheMarkers(
        conversationRequestFor(mock.requests, IMPORT_ONE, IMPORT_ONE)
          .messages ?? [],
      );
      const firstTurnOnB = conversationRequestFor(
        mock.requests,
        IMPORT_ONE,
        IMPORT_TWO,
      );
      const continued = withoutCacheMarkers(firstTurnOnB.messages ?? []);
      expect(continued.slice(0, lastTurnOnA.length)).toEqual(lastTurnOnA);
      const carriedReply = firstTurnOnB.messages?.[lastTurnOnA.length];
      expect(carriedReply?.role).toBe("assistant");
      expect(messageText(carriedReply)).toContain(IMPORT_ONE);
      expect(continued).toHaveLength(lastTurnOnA.length + 2);

      // --- The new deployment's own state carries the imported turns ------
      // Its park is on the Hub, so the state of its body step is there too.
      const continuedState = await b.exportStep(BODY_STEP_KEY);
      expect(continuedState.turns.slice(0, exported.turns.length)).toEqual(
        exported.turns,
      );
      expect(continuedState.turns).toHaveLength(exported.turns.length + 2);
      expect(continuedState.mailboxUidNext).toBe(3);
    });
  },
);
