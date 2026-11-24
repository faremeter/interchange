// A deployment whose saved lifecycle policy sets `standbyAfter` is put in
// standby by the Hub's lifecycle sweep once every run has waited that long.
//
// The deployment first has no run at all, which also counts as every run
// waiting. Its sidecar then reconnects, and its hello reports the standby the
// Hub already recorded, which stays in standby with no extra command. A mail
// trigger then wakes it, its run parks on an author signal, and the Hub sweep
// puts it in standby again. The signal finally wakes it and completes the run.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import { tenant as tenantTable, workflowRun } from "@intx/db/schema";
import { type RepoId, type SidecarEventMap } from "@intx/hub-sessions";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedPrincipal,
  tenantSlugFromId,
} from "@intx/test-harness/seed";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  dropHubLink,
  fireMailTrigger,
  injectSignal,
  listRunIds,
  listWorkflowHostChildren,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { startStandbySweep } from "./standby-helpers";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_standby-after-idle";
const TENANT_ID = "tnt_standby_after_idle";
const CALLER_PRINCIPAL_ID = "prn_standby_after_idle";
const DEFINITION_ASSET_ID = "ast_standby_after_idle_wf";
const SIGNAL_NAME = "go";
const IDLE_REASON =
  "Every run of the deployment waited on input from outside long enough for the Hub to put it in standby";

type StandbyReport = SidecarEventMap["deployment.standby"];

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
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
  await seedAsset(h.db, {
    id: DEFINITION_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "standby-after-idle-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

function entry(address: string): string {
  return `
import { awaitSignal, defineWorkflow } from "@intx/workflow/definition";

export const workflow = defineWorkflow({
  id: ${JSON.stringify(`wf_${DEPLOYMENT_ID}`)},
  trigger: { type: "mail", to: ${JSON.stringify(address)} },
  steps: {
    hold: awaitSignal({ name: ${JSON.stringify(SIGNAL_NAME)} }),
  },
});
`;
}

async function findRunOnGate(
  workflowRunRepoId: RepoId,
): Promise<string | undefined> {
  for (const id of await listRunIds(env, workflowRunRepoId)) {
    const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, id);
    if (
      events.some(
        (e) =>
          e.type === "SignalAwaited" &&
          e.body["stepId"] === "hold" &&
          e.body["signalName"] === SIGNAL_NAME,
      )
    )
      return id;
  }
  return undefined;
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a deployment whose policy sets standbyAfter",
  () => {
    test("is put in standby by the Hub sweep each time its runs have waited, and input still wakes it", async () => {
      const address = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });
      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        credentialId: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentAddress: address,
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      const early: StandbyReport[] = [];
      let takeUp: ((report: StandbyReport) => Promise<void>) | null = null;
      const unsubscribe = env.hub.router.events.on(
        "deployment.standby",
        (report) => {
          if (takeUp === null) {
            early.push(report);
            return;
          }
          return takeUp(report);
        },
      );
      let noted: Promise<void> = Promise.resolve();
      let reportCount = 0;
      let reportFailure: Error | undefined;
      try {
        const handle = await deployWorkflowSourceForTest(env, {
          entryModule: entry(address),
          db: h.db,
          tenantId: TENANT_ID,
          definitionAssetId: DEFINITION_ASSET_ID,
          anchorRunId: DEPLOYMENT_ID,
          deploymentDomain: DEPLOYMENT_DOMAIN,
          agentAddress: address,
          approvals: "approve-probed",
          config,
        });
        // The shortest the policy allows, so the test spends little time idle.
        // Every wait below is for a report or a recorded state, never for this
        // duration.
        await h.db
          .update(workflowRun)
          .set({ lifecyclePolicy: { standbyAfter: "10s" } })
          .where(eq(workflowRun.id, DEPLOYMENT_ID));
        const target = env.hub.prepareAllocationIdentity(
          DEPLOYMENT_ID,
          address,
          SIDECAR_ID,
        );
        const { lifecycle, standby, drainInFlight, commands } =
          await startStandbySweep(env, h.db, {
            tenantId: TENANT_ID,
            deploymentId: DEPLOYMENT_ID,
            target,
            realClock: true,
          });

        const enqueue = (report: StandbyReport): Promise<void> => {
          noted = noted
            .then(async () => {
              await lifecycle.noteDeploymentInStandby(report);
              reportCount += 1;
            })
            .catch((cause: unknown) => {
              reportFailure ??=
                cause instanceof Error ? cause : new Error(String(cause));
            });
          return noted;
        };
        for (const report of early.splice(0)) void enqueue(report);
        takeUp = enqueue;

        const driveUntilInStandby = async () => {
          let current: Awaited<ReturnType<typeof standby>>;
          await waitFor(
            async () => {
              if (reportFailure !== undefined) throw reportFailure;
              await lifecycle.reconcileNext();
              await drainInFlight();
              current = await standby();
              return current?.state === "standby";
            },
            { diagnostics: env.sidecarDiagnostics },
          );
          return current;
        };

        // ---- no run yet: the Hub puts the child in standby ----
        const confirmed = await driveUntilInStandby();
        expect(confirmed).toEqual({
          state: "standby",
          changedAt: expect.any(String),
          wakeAt: null,
          reason: IDLE_REASON,
        });
        expect(listWorkflowHostChildren(env)).toEqual([]);
        const sentBeforeReconnect = commands();
        expect(sentBeforeReconnect.length).toBeGreaterThan(0);
        expect(new Set(sentBeforeReconnect)).toEqual(
          new Set(["idle-check", "idle-standby"]),
        );

        // ---- a reconnect reports standby, and no command follows ----
        const reportsBeforeDrop = reportCount;
        dropHubLink(env);
        await waitFor(() => reportCount > reportsBeforeDrop, {
          diagnostics: env.sidecarDiagnostics,
        });
        await noted;
        if (reportFailure !== undefined) throw reportFailure;
        // A sweep pass sends nothing either, when one is due by now.
        await lifecycle.reconcileNext();
        await drainInFlight();
        expect(await standby()).toEqual(confirmed);
        expect(commands()).toEqual(sentBeforeReconnect);
        expect(listWorkflowHostChildren(env)).toEqual([]);

        // ---- mail wakes it, and its run parks on the gate ----
        await fireMailTrigger(env, address, {
          messageId: "<standby-after-idle@integration.interchange>",
        });
        let found: string | undefined;
        await waitFor(
          async () => {
            found = await findRunOnGate(handle.workflowRunRepoId);
            return found !== undefined;
          },
          { diagnostics: env.sidecarDiagnostics },
        );
        if (found === undefined) throw new Error("no run on the gate");
        const runId = found;
        // Wait for the wake report to flip the Hub record to awake before
        // driving the next automatic standby.
        await waitFor(async () => (await standby())?.state === "awake", {
          diagnostics: env.sidecarDiagnostics,
        });

        // ---- the run only waits again: standby again ----
        expect(await driveUntilInStandby()).toMatchObject({
          state: "standby",
          reason: IDLE_REASON,
        });
        expect(listWorkflowHostChildren(env)).toEqual([]);

        // ---- the signal wakes it, and the run completes ----
        await injectSignal(env, DEPLOYMENT_ID, runId, SIGNAL_NAME, {
          resumed: true,
        });
        const terminal = await waitForWorkflowRunComplete(
          env,
          DEPLOYMENT_ID,
          runId,
          { diagnostics: env.sidecarDiagnostics },
        );
        expect(terminal.type).toBe("RunCompleted");
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        expect(events.filter((e) => e.type === "RunStarted")).toHaveLength(1);

        unsubscribe();
        await noted;
        if (reportFailure !== undefined) throw reportFailure;
        await drainInFlight();
      } finally {
        unsubscribe();
        await noted;
      }
    }, 180_000);
  },
);
