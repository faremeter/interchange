import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { signalName } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";
import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import {
  createInMemoryRepoStore,
  type RepoStore as RuntimeRepoStore,
  type WorkflowEvent,
} from "@intx/workflow";

import {
  collectParkedApprovalCorrelations,
  type LoadParkedApproval,
} from "./parked-correlations";

const at = new Date().toISOString();
const repoId: RepoId = { kind: "workflow-run", id: "dep-1" };

const snapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

/**
 * A durable log that reduces to a single step parked on `signalName`. Mirrors
 * the post-flush crash window a real park leaves behind: RunStarted, the
 * step's StepStarted, then the SignalAwaited that reduces the step to
 * `awaiting-signal`.
 */
function parkedSeed(
  runId: string,
  stepId: string,
  name: string,
): WorkflowEvent[] {
  return parkedSeedSteps(runId, [{ stepId, name }]);
}

/**
 * A durable log that reduces to several concurrently-parked steps in one run:
 * RunStarted, a StepStarted per step, then a SignalAwaited per step.
 */
function parkedSeedSteps(
  runId: string,
  steps: { stepId: string; name: string }[],
): WorkflowEvent[] {
  const events: WorkflowEvent[] = [
    {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: undefined },
    },
  ];
  let seq = 2;
  for (const step of steps) {
    events.push({
      kind: "StepStarted",
      seq: seq++,
      at,
      stepId: step.stepId,
      attempt: 1,
      input: { ref: "inline:null" },
    });
  }
  for (const step of steps) {
    events.push({
      kind: "SignalAwaited",
      seq: seq++,
      at,
      stepId: step.stepId,
      signalName: step.name,
    });
  }
  return events;
}

/**
 * A substrate stub that resolves `getRepoDir` the way the real substrate does
 * -- `<baseDir>/<kind>/<id>` -- and surfaces any other method as a precise
 * failure. `getRepoDir` is the only method the enumeration exercises.
 */
function createStubSubstrate(baseDir: string): SubstrateRepoStore {
  const stub: Partial<SubstrateRepoStore> = {
    getRepoDir(id: RepoId): string {
      return path.join(baseDir, id.kind, id.id);
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; missing methods surface as a precise failure via the proxy
  return new Proxy(stub as SubstrateRepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub substrate: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
}

/**
 * Build a substrate stub and a runtime repo store seeded with the given parked
 * runs. `discoverInFlightRuns` reads run ids from the substrate repo dir's
 * `runs/` listing and events from the runtime repo store, so both are seeded
 * for the same run ids.
 */
async function setup(
  runs: { runId: string; stepId: string; name: string }[],
): Promise<{
  substrate: SubstrateRepoStore;
  runtimeRepoStore: RuntimeRepoStore;
}> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "parked-corr-"));
  const runsDir = path.join(baseDir, repoId.kind, repoId.id, "runs");
  const runtimeRepoStore = createInMemoryRepoStore();
  for (const run of runs) {
    await fs.mkdir(path.join(runsDir, run.runId), { recursive: true });
    for (const event of parkedSeed(run.runId, run.stepId, run.name)) {
      await runtimeRepoStore.append(run.runId, event);
    }
  }
  return { substrate: createStubSubstrate(baseDir), runtimeRepoStore };
}

describe("collectParkedApprovalCorrelations", () => {
  test("enumerates a parked control-plane correlation and loads its snapshot", async () => {
    const { substrate, runtimeRepoStore } = await setup([
      { runId: "run-1", stepId: "s", name: signalName("corr-1") },
    ]);
    const calls: unknown[] = [];
    const loadParkedApproval: LoadParkedApproval = async (args) => {
      calls.push(args);
      return snapshot;
    };

    const result = await collectParkedApprovalCorrelations({
      substrate,
      repoId,
      runtimeRepoStore,
      loadParkedApproval,
    });

    expect(result).toEqual([
      { runId: "run-1", correlationId: "corr-1", kind: "approval", snapshot },
    ]);
    expect(calls).toEqual([
      { runId: "run-1", stepId: "s", attempt: 1, correlationId: "corr-1" },
    ]);
  });

  test("enumerates parked correlations across multiple runs and steps", async () => {
    const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "parked-corr-"));
    const runsDir = path.join(baseDir, repoId.kind, repoId.id, "runs");
    const runtimeRepoStore = createInMemoryRepoStore();
    // run-a parks two steps concurrently; run-b parks one. The enumeration
    // must report every park across both the run loop and the step loop.
    await fs.mkdir(path.join(runsDir, "run-a"), { recursive: true });
    for (const event of parkedSeedSteps("run-a", [
      { stepId: "s1", name: signalName("corr-a1") },
      { stepId: "s2", name: signalName("corr-a2") },
    ])) {
      await runtimeRepoStore.append("run-a", event);
    }
    await fs.mkdir(path.join(runsDir, "run-b"), { recursive: true });
    for (const event of parkedSeedSteps("run-b", [
      { stepId: "s1", name: signalName("corr-b1") },
    ])) {
      await runtimeRepoStore.append("run-b", event);
    }
    const substrate = createStubSubstrate(baseDir);
    const calls: string[] = [];
    const loadParkedApproval: LoadParkedApproval = async (args) => {
      calls.push(args.correlationId);
      return snapshot;
    };

    const result = await collectParkedApprovalCorrelations({
      substrate,
      repoId,
      runtimeRepoStore,
      loadParkedApproval,
    });

    // Enumeration order follows directory and map iteration, so assert as a
    // set: every parked correlation is reported exactly once, each with its
    // snapshot.
    expect(result.map((r) => r.correlationId).sort()).toEqual([
      "corr-a1",
      "corr-a2",
      "corr-b1",
    ]);
    expect(result.every((r) => r.kind === "approval")).toBe(true);
    expect(result.every((r) => r.snapshot === snapshot)).toBe(true);
    expect(calls.sort()).toEqual(["corr-a1", "corr-a2", "corr-b1"]);
  });

  test("skips a step parked on an author-chosen signal name", async () => {
    const { substrate, runtimeRepoStore } = await setup([
      { runId: "run-2", stepId: "g", name: "human-approval" },
    ]);
    let called = false;
    const loadParkedApproval: LoadParkedApproval = async () => {
      called = true;
      return snapshot;
    };

    const result = await collectParkedApprovalCorrelations({
      substrate,
      repoId,
      runtimeRepoStore,
      loadParkedApproval,
    });

    expect(result).toEqual([]);
    expect(called).toBe(false);
  });

  test("throws when a control-plane park is found but no binding is wired", async () => {
    const { substrate, runtimeRepoStore } = await setup([
      { runId: "run-3", stepId: "s", name: signalName("corr-3") },
    ]);

    await expect(
      collectParkedApprovalCorrelations({
        substrate,
        repoId,
        runtimeRepoStore,
      }),
    ).rejects.toThrow(/no loadParkedApproval binding is wired/);
  });

  test("throws when the binding has no snapshot for an enumerated park", async () => {
    const { substrate, runtimeRepoStore } = await setup([
      { runId: "run-4", stepId: "s", name: signalName("corr-4") },
    ]);
    const loadParkedApproval: LoadParkedApproval = async () => undefined;

    await expect(
      collectParkedApprovalCorrelations({
        substrate,
        repoId,
        runtimeRepoStore,
        loadParkedApproval,
      }),
    ).rejects.toThrow(/the run log and the step store disagree/);
  });
});
