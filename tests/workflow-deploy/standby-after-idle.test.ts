// A deployment deployed with a `standbyAfter` wait is put in standby by its
// sidecar, with no standby request, once every run has waited that long. The
// wait is passed to the deploy directly, so a saved lifecycle policy is not
// part of what this covers.
//
// The deployment first has no run at all, which also counts as every run
// waiting. Its sidecar then reconnects, and its hello reports the standby the
// Hub already confirmed, which the Hub does not confirm again. A mail trigger
// then wakes it, its run parks on an author signal, and the sidecar puts it in
// standby again. Each time the Hub records the standby from the sidecar's
// report, and the signal finally completes the run.
//
// Harness justification: SPAWN-REAL: real hub, real sidecar subprocess, real
// workflow-process child, mock inference.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { tenant as tenantTable } from "@intx/db/schema";
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
// Short, so the test spends little time idle. Every wait below is for a
// report or a recorded state, never for this duration.
const STANDBY_AFTER_MS = 1_000;
const IDLE_REASON =
  "Every run of the deployment waited on input from outside long enough for its sidecar to put it in standby";

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
  "a deployment deployed with a standbyAfter wait",
  () => {
    test("is put in standby by its sidecar each time its runs have waited, and input still wakes it", async () => {
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

      // The sidecar can report its first standby before the Hub's lifecycle
      // service of this test exists, so reports are held until it does and
      // then taken up in the order they arrived, as the Hub takes them up.
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
      // The Hub's handling of every report taken up so far. It never rejects:
      // a report the Hub could not take up fails the test where it waits.
      let noted: Promise<void> = Promise.resolve();
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
          standbyAfterMs: STANDBY_AFTER_MS,
        });
        const target = env.hub.prepareAllocationIdentity(
          DEPLOYMENT_ID,
          address,
          SIDECAR_ID,
        );
        const { lifecycle, settled, commands } = await startStandbySweep(
          env,
          h.db,
          {
            tenantId: TENANT_ID,
            deploymentId: DEPLOYMENT_ID,
            target,
          },
        );

        // The standby each idle report taken up so far named.
        const idleStandbys: string[] = [];
        let reportFailure: Error | undefined;
        const idleWaiters: (() => void)[] = [];
        const enqueue = (report: StandbyReport): Promise<void> => {
          noted = noted
            .then(async () => {
              await lifecycle.noteDeploymentInStandby(report);
              if (report.inStandby && report.idleStandby !== undefined)
                idleStandbys.push(report.idleStandby);
            })
            .catch((cause: unknown) => {
              reportFailure ??=
                cause instanceof Error ? cause : new Error(String(cause));
            })
            .then(() => {
              for (const wake of idleWaiters.splice(0)) wake();
            });
          return noted;
        };
        for (const report of early.splice(0)) void enqueue(report);
        takeUp = enqueue;

        // The Hub's record once it has taken up the sidecar's `count`th
        // report of an idle standby and confirmed it.
        const untilIdleStandby = async (count: number) => {
          while (idleStandbys.length < count) {
            if (reportFailure !== undefined) throw reportFailure;
            await new Promise<void>((resolve) => idleWaiters.push(resolve));
          }
          return settled();
        };

        // ---- no run yet: nothing works, so the child is stopped ----
        const confirmed = await untilIdleStandby(1);
        expect(confirmed).toEqual({
          state: "standby",
          changedAt: expect.any(String),
          wakeAt: null,
          reason: IDLE_REASON,
        });
        expect(listWorkflowHostChildren(env)).toEqual([]);
        const confirms = commands();
        expect(confirms.length).toBeGreaterThan(0);
        expect(new Set(confirms)).toEqual(new Set(["confirm"]));

        // ---- a reconnect reports the same standby, which stays confirmed ----
        dropHubLink(env);
        expect(await untilIdleStandby(2)).toEqual(confirmed);
        expect(idleStandbys[1]).toBe(idleStandbys[0]);
        expect(commands()).toEqual(confirms);
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

        // ---- the run only waits again, so the child is stopped again ----
        expect(await untilIdleStandby(3)).toMatchObject({
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

        // The deployment waits again now, so its sidecar can report another
        // standby. No further report is taken up, and the Hub finishes
        // confirming one it already has before the Hub and database go.
        unsubscribe();
        await noted;
        if (reportFailure !== undefined) throw reportFailure;
        await settled();
      } finally {
        unsubscribe();
        await noted;
      }
    }, 180_000);
  },
);
