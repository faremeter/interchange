import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { signalName } from "@intx/types";
import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import {
  createInMemoryRepoStore,
  type RepoStore as RuntimeRepoStore,
  type WorkflowEvent,
} from "@intx/workflow";

import { discoverInFlightRuns } from "./self-discovery";

const at = new Date().toISOString();
const repoId: RepoId = { kind: "workflow-run", id: "dep-1" };

/**
 * A substrate stub that resolves `getRepoDir` the way the real substrate does
 * (`<baseDir>/<kind>/<id>`) and surfaces any other method as a precise failure.
 * `getRepoDir` is the only method the enumeration exercises.
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

// A non-terminal single-step run: RunStarted, its StepStarted, and a
// SignalAwaited that reduces the step to `awaiting-signal`.
function parkedRun(
  runId: string,
  stepId: string,
  corr: string,
): WorkflowEvent[] {
  return [
    {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: undefined },
    },
    {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId,
      attempt: 1,
      input: { ref: "inline:null" },
    },
    {
      kind: "SignalAwaited",
      seq: 3,
      at,
      stepId,
      signalName: signalName(corr),
    },
  ];
}

// A non-terminal onTrigger container that has spawned one body child: the
// container StepStarted, a ChildSpawned naming the body run, and the approval
// SignalAwaited the container proxied up.
function sectionRunWithChild(
  runId: string,
  childRunId: string,
  corr: string,
): WorkflowEvent[] {
  return [
    {
      kind: "RunStarted",
      seq: 1,
      at,
      runId,
      definitionHash: "x",
      trigger: { type: "manual", payload: undefined },
    },
    {
      kind: "StepStarted",
      seq: 2,
      at,
      stepId: "section",
      attempt: 1,
      input: { ref: "inline:null" },
    },
    {
      kind: "ChildSpawned",
      seq: 3,
      at,
      stepId: "section",
      childRunId,
      childDefinitionRef: "body-ref",
    },
    {
      kind: "SignalAwaited",
      seq: 4,
      at,
      stepId: "section",
      signalName: signalName(corr),
      parkKind: "approval",
    },
  ];
}

async function setup(
  runs: { runId: string; events: WorkflowEvent[] }[],
): Promise<{
  substrate: SubstrateRepoStore;
  runtimeRepoStore: RuntimeRepoStore;
}> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "self-discovery-"));
  const runsDir = path.join(baseDir, repoId.kind, repoId.id, "runs");
  const runtimeRepoStore = createInMemoryRepoStore();
  for (const run of runs) {
    await fs.mkdir(path.join(runsDir, run.runId), { recursive: true });
    await runtimeRepoStore.appendBatch(run.runId, run.events);
  }
  return { substrate: createStubSubstrate(baseDir), runtimeRepoStore };
}

describe("discoverInFlightRuns", () => {
  test("excludes a run that another run spawned as a child", async () => {
    const { substrate, runtimeRepoStore } = await setup([
      {
        runId: "dep-run",
        events: sectionRunWithChild("dep-run", "section__0", "corr-parent"),
      },
      {
        runId: "section__0",
        events: parkedRun("section__0", "s", "corr-parent"),
      },
      {
        runId: "sibling-run",
        events: parkedRun("sibling-run", "s", "corr-sibling"),
      },
    ]);

    const discovered = await discoverInFlightRuns({
      substrate,
      repoId,
      runtimeRepoStore,
    });

    // The body child (`section__0`) is driven by its parent, so it is not a
    // top-level in-flight run; the parent and the unrelated sibling are.
    expect(discovered.map((r) => r.runId).sort()).toEqual([
      "dep-run",
      "sibling-run",
    ]);
  });
});
