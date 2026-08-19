// Parent -> child grants-file inheritance through the real subprocess.
//
// When a mail-triggered parent run spawns a child, the child runs under
// the parent's authority: the spawn adapter reads the parent's
// `runs/<parentRunId>/grants.json` and writes the same flat grant set to
// the child's own `runs/<childRunId>/grants.json`. This test drives that
// composition end to end -- a real parent workflow deployed BY SOURCE-REF
// (bundle a source entry module into a hub asset, probe it, approve+freeze
// it against a real DB, deploy the source-ref frame) whose spawn step fires a
// child through the real sidecar subprocess -- and asserts BOTH files land on
// the sidecar's on-disk workflow-run repo, carrying the grants the trigger
// delivered.
//
// What this ADDS over the unit coverage. The spawn adapter's behavior in
// isolation -- inheritance, grandchild multi-hop, and fail-closed-at-spawn
// when the parent grants file is absent -- is proven directly against a
// real on-disk substrate in
// `apps/sidecar/src/workflow-substrate-factory-child-grants.test.ts`, which
// calls `createSidecarRunChild` with hand-seeded grants. This test proves
// the WIRING composes: that a real mail trigger's delivered grants reach
// `runs/<parentRunId>/grants.json` through the supervisor, and that the
// real child-spawn adapter is actually invoked with those grants during an
// honest parent->child spawn, writing the child's inherited file. The
// fail-closed negative is not reproducible here -- every mail-triggered run
// materializes a grants file, so an absent parent file cannot arise through
// the trigger path -- and stays covered at the unit level.
//
// SCOPE. The child's step body does not execute: `childWorkflow` per-step
// execution is not implemented (INTR-310), so the child fails its step
// after the spawn. That is expected and irrelevant here -- the grants-file
// inheritance write happens at spawn time, BEFORE the child step runs, so
// grant CONSUMPTION by the child is out of reach and not asserted. Only the
// spawn-time inheritance write is under test.

import fs from "node:fs";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { type } from "arktype";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { childWorkflowEntry } from "./fixtures/child-workflow";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const PARENT_DEPLOYMENT_ID = "run_child-inherited-grants-parent-1";
const CHILD_DEPLOYMENT_ID = "run_child-inherited-grants-child-1";
const PARENT_WORKFLOW_ID = `wf_${PARENT_DEPLOYMENT_ID}`;
const CHILD_WORKFLOW_ID = `wf_${CHILD_DEPLOYMENT_ID}`;

const TENANT_ID = "tnt_child_inherited_grants";
const CALLER_PRINCIPAL_ID = "prn_child_inherited_grants";
const DEFINITION_ASSET_ID = "ast_child_inherited_grants_wf";

// The parent run's grant, delivered per run via the `run.grants` frame.
// The child must inherit exactly this set into its own grants file.
const PARENT_GRANT: WireGrantRule = {
  id: "grant-parent-inherited",
  resource: "effect:fs:write",
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

// The envelope `grants.json` carries: `{ grants: [...] }`.
const GrantsFile = type({ grants: "unknown[]" }).onUndeclaredKey("ignore");

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
    name: "child-inherited-grants-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// Read a run's grants.json off the sidecar's on-disk workflow-run repo.
// Returns null when the file is absent. The sidecar roots each workflow-run
// repo at `<dataDir>/workflow-runs/<repoId>` (the workflow-run kind
// handler's directoryPrefix), and each run's grants live at
// `runs/<runId>/grants.json` inside it.
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

describe.skipIf(!harnessDbEnvAvailable())(
  "parent -> child inherited grants round-trip",
  () => {
    test("the child inherits the parent's delivered grants file at spawn", async () => {
      const parentMailAddress = deriveRunAddress({
        runId: PARENT_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const childMailAddress = deriveRunAddress({
        runId: CHILD_DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${PARENT_DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: parentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step).",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${parentMailAddress}`,
        `mail.address:${childMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
      ]);

      const entryModule = childWorkflowEntry({
        workflowId: PARENT_WORKFLOW_ID,
        address: parentMailAddress,
        steps: [
          {
            stepId: "step1",
            agentId: "agent-inherited-parent-step",
            systemPrompt: "You are the parent workflow's first step agent.",
          },
        ],
        spawns: [
          {
            stepId: "spawn",
            after: ["step1"],
            child: {
              workflowId: CHILD_WORKFLOW_ID,
              address: childMailAddress,
              steps: [
                {
                  stepId: "childStep",
                  agentId: "agent-inherited-child-step",
                  systemPrompt: "You are the child workflow's step agent.",
                },
              ],
            },
          },
        ],
      });

      const handle = await deployWorkflowSourceForTest(env, {
        entryModule,
        db: h.db,
        tenantId: TENANT_ID,
        definitionAssetId: DEFINITION_ASSET_ID,
        anchorRunId: PARENT_DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        agentAddress: parentMailAddress,
        approvals: operatorApprovals,
        config,
        sources: {
          step1: [inferenceSource],
          spawn: [inferenceSource],
        },
      });
      expect(handle.publicKey).toBeTruthy();

      const parentWorkflowRunRepoId: RepoId = handle.workflowRunRepoId;
      const parentRepoId = parentWorkflowRunRepoId.id;

      await waitFor(
        () => env.hub.router.getRoutableAddresses().includes(parentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Fire the parent trigger carrying the parent grant per run.
      await fireMailTrigger(env, parentMailAddress, {
        messageId: "<child-inherited-grants-1@integration.interchange>",
        grants: [PARENT_GRANT],
      });

      const parentRunId = await waitForFirstRunId(
        env,
        parentWorkflowRunRepoId,
        {
          diagnostics: env.sidecarDiagnostics,
          timeoutMs: 20_000,
        },
      );

      // Wait for the spawn to fire; capture the child's runId.
      await waitFor(
        async () => {
          const events = await readWorkflowRunEvents(
            env,
            PARENT_DEPLOYMENT_ID,
            parentRunId,
          );
          return events.some((e) => e.type === "ChildSpawned");
        },
        { diagnostics: env.sidecarDiagnostics, timeoutMs: 20_000 },
      );

      const parentEvents = await readWorkflowRunEvents(
        env,
        PARENT_DEPLOYMENT_ID,
        parentRunId,
      );
      const spawned = parentEvents.find((e) => e.type === "ChildSpawned");
      if (spawned === undefined)
        throw new Error("unreachable: no ChildSpawned");
      const childRunId = spawned.body["childRunId"];
      if (typeof childRunId !== "string") {
        throw new Error(
          `ChildSpawned missing string childRunId; got ${typeof childRunId}`,
        );
      }

      // The parent's delivered grants landed in its own grants file.
      const parentGrantsOnDisk = readRunGrantsFile(parentRepoId, parentRunId);
      if (parentGrantsOnDisk === null) {
        throw new Error(
          `parent grants.json absent for run ${parentRunId}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(parentGrantsOnDisk).toEqual([PARENT_GRANT]);

      // The child's inherited grants file was written at spawn, carrying the
      // parent's grant set verbatim. The spawn writes it before the child's
      // (INTR-310-failing) step runs; poll to let the write land.
      await waitFor(
        () => readRunGrantsFile(parentRepoId, childRunId) !== null,
        {
          diagnostics: env.sidecarDiagnostics,
          timeoutMs: 20_000,
        },
      );
      const childGrantsOnDisk = readRunGrantsFile(parentRepoId, childRunId);
      expect(childGrantsOnDisk).toEqual([PARENT_GRANT]);
    });
  },
);
