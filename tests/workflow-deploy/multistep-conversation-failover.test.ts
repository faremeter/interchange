// A multi-step workflow fails over to a new host mid-conversation and the
// conversation continues with no lost turns.
//
// The Hub runs as a subprocess with the local-process sidecar provisioner the
// admin UI e2e suite uses, so every sidecar is provisioned, lost, and replaced
// by the production allocation reconciler. The workflow's first step is a
// conversation: its trigger budget is three, so every inbound mail is its next
// turn, and `step2` runs once the third turn is done. Two turns run on the
// first sidecar. Once the Hub's copy of the run log holds both input parks, the
// sidecar's process tree is killed, and its local disk goes with it. The
// deployment's disconnect limit is one second, so the reconciler soon finds the
// worker lost, classifies the run as parked, and replaces the worker: the Hub's
// copy of the workflow-run history is restored onto a new sidecar and the same
// frozen definition is deployed there. The third mail is the step's next turn.
//
// The proof is what the model sees. The mock inference server records every
// request and echoes each turn's inbound text, so the third turn's request
// must carry the second turn's request plus its reply, verbatim, before the
// new mail. The run then completes through `step2` on the replacement host.
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
import { createDB } from "@intx/db";
import {
  principal as principalTable,
  sidecarAllocation as sidecarAllocationTable,
  tenant as tenantTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import { createAgentRepoStore, DEFAULT_ASSET_REF } from "@intx/hub-sessions";
import { credentialAad, deriveWorkflowRunId, hexDecode } from "@intx/types";
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
  step1RequestFor,
  withoutCacheMarkers,
} from "./fixtures/conversation-requests";
import { twoStepEntry } from "./fixtures/two-step";

const PROVISIONED_HUB_ENTRY = path.join(
  "tests",
  "admin-ui-e2e",
  "harness",
  "hub.ts",
);

const MARKER_ONE = "failover-fa11a7-one";
const MARKER_TWO = "failover-fa11a7-two";
const MARKER_THREE = "failover-fa11a7-three";

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

// Publish the two-step conversation workflow as a workflow asset and return
// the commit its deploys name.
async function publishConversation(
  tenant: Tenant,
  assetId: string,
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
  const workflowJs = await bundleWorkflowEntry(
    scratch,
    twoStepEntry({
      // The route mints the anchor id, so the definition cannot name the
      // deployment address; mail goes to the address the route derives.
      address: `placeholder@${tenant.domain}`,
      systemPrompt1: "You are the conversational step.",
      systemPrompt2: "You summarize the conversation.",
      step1Triggers: 3,
    }),
  );
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

  const readAnchor = () =>
    db.query.workflowRun.findFirst({
      where: eq(workflowRunTable.id, anchorRunId),
    });
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
        e.body["stepId"] === "step1" &&
        e.body["parkKind"] === "input",
    ).length;
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
  return {
    readAnchor,
    readAllocation,
    readEvents,
    inputParks,
    sendMail,
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
  "multi-step conversation failover",
  () => {
    test("the conversation continues on a replacement host with every earlier turn", async () => {
      const { mock } = requireSetup();
      const tenant = await provisionTenant("conversation_failover");
      const assetId = "ast_conversation_failover_wf";
      const commitSha = await publishConversation(tenant, assetId);
      const { readAnchor, readAllocation, readEvents, inputParks, sendMail } =
        await deployConversation(tenant, assetId, commitSha);

      // --- Two turns on the first host -----------------------------------
      // No signal reports these states, so they are polled. The first deploy
      // is complete once the anchor key commits.
      await waitFor(async () => (await readAnchor())?.publicKey != null);
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
        const anchor = await readAnchor();
        return (
          row !== undefined &&
          row.generation > first.generation &&
          row.status === "allocated" &&
          row.externalRef !== null &&
          row.externalRef !== first.externalRef &&
          anchor?.publicKey != null
        );
      });

      // --- The third turn runs on the replacement host --------------------
      await sendMail(`Third turn ${MARKER_THREE}.`);
      // A terminal event is the log's last, so a later read finds it too.
      const readTerminal = async () =>
        (await readEvents()).find(
          (e) => e.type === "RunCompleted" || e.type === "RunFailed",
        );
      await waitFor(async () => (await readTerminal()) !== undefined);
      expect((await readTerminal())?.type).toBe("RunCompleted");

      // --- The model saw the whole conversation ---------------------------
      const secondTurn = withoutCacheMarkers(
        step1RequestFor(mock.requests, MARKER_ONE, MARKER_TWO).messages ?? [],
      );
      const thirdTurnRequest = step1RequestFor(
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
  },
);
