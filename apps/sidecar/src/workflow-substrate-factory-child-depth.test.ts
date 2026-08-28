// The childWorkflow spawn-depth ceiling on the REAL in-process spawn path.
//
// `createSidecarRunChild` recurses on itself: a childWorkflow runs its child in
// the same process against the same shared workflow-run repo, threading `depth`
// by value through each rung (no serialization, no reset). So this harness --
// a real on-disk substrate plus the real `createSidecarRunChild` /
// `createInMemorySpawnChild` / `runtimeRun` seam -- exercises the identical spawn
// path a deployed chain runs; the hub deploy-frame and the workflow-host
// subprocess wrapper are the only things a full roundtrip would add, and neither
// is where the depth guard lives.
//
// A nested chain lets depth ACCUMULATE rung over rung until the guard fires,
// rather than pre-loading a single `depth + 1 > max` arithmetic check (that is
// covered by the runLocal and child-depth unit tests). With the ceiling lowered
// to 2, the parent (depth 0) spawns outer (1) and outer spawns mid (2) for real
// -- proving depth climbs past 0 -- and only mid spawning leaf (depth 3) trips
// the guard, landing a clean `StepFailed` that names the offending depth.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { defineAgent } from "@intx/agent";
import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  RepoId,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import { createWorkflowRunRepoStore } from "@intx/workflow-host";
import {
  createInMemoryScheduler,
  createInMemoryRepoStore,
  childWorkflow,
  defineWorkflow,
  step,
  type WorkflowDefinition,
  type WorkflowEvent,
} from "@intx/workflow";

import {
  createSidecarRunChild,
  type SidecarChildStepInvoker,
} from "./workflow-substrate-factory";
import { runGrantsPath } from "./run-grants";

const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-child-depth";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: DEPLOYMENT_ID,
};
const allowAll: AuthorizeFn = () => ({ allowed: true });
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: DEPLOYMENT_ID,
};

const tempDirs: string[] = [];
let signingKey: KeyPair;
// `buildChildRunEnv` reads each spawned rung's `sources.json` eagerly, keyed by
// the rung's rewritten ref -- the `<enclosingId>__<stepId>` handle the deploy
// mints, which accumulates as the chain descends. The intermediate rungs carry
// no agent step, but the read still happens, so stage a minimal file for every
// rung that runs its env: the top (`depth-parent`), outer
// (`depth-parent__spawn`), and mid (`depth-parent__spawn__spawn`). The leaf rung
// is never reached because the guard fires before mid spawns it.
const RUNG_SOURCE_REFS = [
  "depth-parent",
  "depth-parent__spawn",
  "depth-parent__spawn__spawn",
] as const;
let sourcesDataDir: string;

// The guard fires before any step runs, so no invoker is ever called.
const noopInvoker: SidecarChildStepInvoker = () => {
  throw new Error("child-depth: no step should run before the guard fires");
};
const noopOnEvent = (): void => {
  /* the event sink is not asserted in this test */
};

beforeAll(async () => {
  signingKey = await generateKeyPair();
  sourcesDataDir = await makeTempDir("child-depth-assets-");
  for (const ref of RUNG_SOURCE_REFS) {
    await stageSources(ref);
  }
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

async function stageSources(ref: string): Promise<void> {
  const dir = path.join(sourcesDataDir, "assets", "workflow", ref);
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.writeFile(
    path.join(dir, "sources.json"),
    JSON.stringify({
      work: [
        {
          id: "anthropic:m",
          provider: "anthropic",
          baseURL: "http://localhost:1",
          apiKey: "sk-x",
          model: "m",
        },
      ],
    }),
  );
}

function grant(resource: string, action: string): GrantRule {
  return {
    id: `grant-${resource}-${action}`,
    resource,
    action,
    effect: "allow",
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

const evaluateGrantsAdapter: Parameters<
  typeof createSidecarRunChild
>[0]["evaluateGrants"] = async ({ resource, action, grants }) => {
  const result = await evaluateGrants(
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the snapshot's grants are typed unknown[] at the workflow-host boundary; the sidecar owns the GrantRule grammar, so the seeded rows narrow here
    [...(grants as readonly GrantRule[])],
    resource,
    action,
  );
  return { effect: result.effect, matchingGrants: [], resolvedBy: null };
};

// parent -> outer -> mid -> leaf, each spawning the next inline child. Depths:
// parent 0, outer 1, mid 2, leaf 3. Only the leaf carries an agent step; the
// guard fires before it ever runs.
function nestedChain(): WorkflowDefinition {
  const leaf = defineWorkflow({
    id: "depth-leaf",
    trigger: { type: "manual" },
    steps: {
      work: step({
        agent: defineAgent({
          id: "depth-leaf-agent",
          systemPrompt: "s",
          tools: [],
          capabilities: [],
          inference: { sources: [{ provider: "anthropic", model: "m" }] },
        }),
      }),
    },
  });
  const mid = defineWorkflow({
    id: "depth-mid",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: leaf }) },
  });
  const outer = defineWorkflow({
    id: "depth-outer",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: mid }) },
  });
  return defineWorkflow({
    id: "depth-parent",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: outer }) },
  });
}

async function makeSubstrate(): Promise<ReturnType<typeof createRepoStore>> {
  const dataDir = await makeTempDir("child-depth-substrate-");
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: allowAll,
  });
  await substrate.writeTree({ kind: "hub" }, WORKFLOW_RUN_REPO_ID, REF, {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "genesis",
  });
  return substrate;
}

async function seedRunGrants(
  substrate: ReturnType<typeof createRepoStore>,
  runId: string,
  grants: readonly GrantRule[],
): Promise<void> {
  await substrate.writeTree({ kind: "hub" }, WORKFLOW_RUN_REPO_ID, REF, {
    files: {
      [runGrantsPath(runId)]: JSON.stringify({ grants }, null, 2),
    },
    message: `seed grants for ${runId}`,
  });
}

function reader(substrate: ReturnType<typeof createRepoStore>) {
  return createWorkflowRunRepoStore({
    substrate,
    repoId: WORKFLOW_RUN_REPO_ID,
    principal: PRINCIPAL,
    ref: REF,
  });
}

// The runId of the single child a rung spawned (its `ChildSpawned`).
async function spawnedChildRunId(
  substrate: ReturnType<typeof createRepoStore>,
  runId: string,
): Promise<string> {
  const events: readonly WorkflowEvent[] = await reader(substrate).read(runId);
  const spawned = events.find((e) => e.kind === "ChildSpawned");
  if (spawned === undefined || spawned.kind !== "ChildSpawned") {
    throw new Error(`run ${runId} has no ChildSpawned event`);
  }
  return spawned.childRunId;
}

describe("createSidecarRunChild spawn-depth ceiling", () => {
  test("a chain past the ceiling fails loud naming the offending depth", async () => {
    const substrate = await makeSubstrate();
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant("inference.source:anthropic:m", "invoke"),
    ]);

    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: REF,
      principal: PRINCIPAL,
      scheduler: createInMemoryScheduler({
        repoStore: createInMemoryRepoStore(),
        clock: () => new Date(),
      }),
      invokeStep: noopInvoker,
      evaluateGrants: evaluateGrantsAdapter,
      dataDir: sourcesDataDir,
    });

    // Run the top of the chain at depth 0 with the ceiling lowered to 2.
    const topRunId = "run-top";
    const result = await runChild(
      {
        definition: nestedChain(),
        definitionRef: REF,
        childRunId: topRunId,
        input: null,
        parentRunId,
        parentStepId: "s",
        signal: new AbortController().signal,
        depth: 0,
        maxChildSpawnDepth: 2,
      },
      noopOnEvent,
    );

    // The top run settled failed: the deep spawn was rejected and the failure
    // propagated up every rung's spawn step.
    expect(result.terminalStatus).toBe("failed");

    // Depth climbed for real: the parent spawned outer (depth 1) and outer
    // spawned mid (depth 2) before the guard fired -- two successful in-process
    // spawns, not a single pre-loaded arithmetic trip.
    const outerRunId = await spawnedChildRunId(substrate, topRunId);
    const midRunId = await spawnedChildRunId(substrate, outerRunId);

    // mid (depth 2) spawning leaf (depth 3) tripped the guard: its spawn step
    // failed loud, and the error names the offending depth and ceiling verbatim.
    // mid never committed a ChildSpawned (the guard fires before it).
    const midEvents: readonly WorkflowEvent[] =
      await reader(substrate).read(midRunId);
    expect(midEvents.some((e) => e.kind === "ChildSpawned")).toBe(false);
    const midStepFailed = midEvents.find((e) => e.kind === "StepFailed");
    if (midStepFailed === undefined || midStepFailed.kind !== "StepFailed") {
      throw new Error(`mid run ${midRunId} has no StepFailed event`);
    }
    expect(midStepFailed.error.message).toContain(
      'childWorkflow spawn depth 3 exceeds the maximum 2 at step "spawn"',
    );
  });
});
