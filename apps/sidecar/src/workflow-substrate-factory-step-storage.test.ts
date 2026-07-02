// Per-step scratch keying for `createSidecarStepBuildEnv` (#3 leak fix).
//
// `stepStorageRoot` rooted every step invocation's workspace + tool
// scratch under the per-message `runId`, and nothing ever reclaimed it,
// so a long-lived deployment's `workflow-step-state/` grew without
// bound. The fix keys the warm single-step agent's scratch STABLY per
// agent (so the cached agent reuses one workspace across runs and the
// warm case is bounded to one dir per agent) while the cold/multi-step
// path keeps its per-run keying (reclaimed at run completion / undeploy
// elsewhere).
//
// These tests pin the keying directly off the production `buildEnv` the
// substrate factory wires:
//   - WARM (`durableConversation` present): two different runIds produce
//     the SAME `env.workdir`, and a file written for run-1 is visible in
//     the env built for run-2 -- the workspace-continuity the stable key
//     buys.
//   - COLD (no `durableConversation`): two different runIds produce
//     DIFFERENT `env.workdir`s, each under that run's subtree -- the
//     per-run keying the run-completion cleanup reclaims.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type {
  AuditStore,
  ContextStore,
  InferenceSource,
} from "@intx/types/runtime";
import type { RepoId } from "@intx/hub-sessions";
import { createBuiltinRegistry } from "@intx/inference/providers";
import type { ChildOutboundMailBridge, StepEnvBase } from "@intx/workflow-host";
import type { StepInvokeRequest } from "@intx/workflow";

import {
  createSidecarStepBuildEnv,
  type SidecarStepBuildEnvDeps,
} from "./workflow-substrate-factory";
import type { DurableConversationRegistry } from "./conversation-state";

const STEP_ID = "step-1";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: "deployment-keying",
};

const SOURCE: InferenceSource = {
  id: STEP_ID,
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-keying",
  model: "claude-keying",
};

function stubOutboundMailBridge(): ChildOutboundMailBridge {
  return {
    submit: () => Promise.reject(new Error("unused in buildEnv keying test")),
    handleResult: () => undefined,
    cancelAll: () => undefined,
    pendingCount: 0,
  };
}

// A warm `durableConversation` whose `acquire(stepId)` returns a storage
// the env builder files into `env.storage`. `buildEnv` never invokes the
// store's methods, so a structural double-cast stub is sufficient and is
// the documented test-stub escape hatch for a wide library interface.
function stubDurableConversationRegistry(): DurableConversationRegistry {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- buildEnv only files `.storage` into the env; it never calls the store's methods, so a structural stub cannot be satisfied field-by-field
  const storage = {} as ContextStore & AuditStore;
  const store = {
    storage,
    mirrorToSubstrate: () => Promise.resolve(),
    restoreFromSubstrate: () => Promise.resolve(),
  };
  return {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the registry's full DurableConversationStore surface is not exercised by buildEnv
    acquire: () => Promise.resolve(store as never),
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- same: only `.storage` is read
    get: () => store as never,
  };
}

function buildDeps(opts: {
  dataDir: string;
  durableConversation?: DurableConversationRegistry;
}): SidecarStepBuildEnvDeps {
  return {
    table: { [STEP_ID]: SOURCE },
    dataDir: opts.dataDir,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    signer: (payload: string) => Promise.resolve(`sig:${payload.length}`),
    mailboxAddress: "ins_deployment-keying@example.com",
    stepCount: 1,
    outboundMailBridge: stubOutboundMailBridge(),
    cache: { cacheMaxBytes: 1_000_000, registryMaxTarballBytes: 1_000_000 },
    adapters: createBuiltinRegistry(),
    ...(opts.durableConversation !== undefined
      ? { durableConversation: opts.durableConversation }
      : {}),
  };
}

function requestForRun(runId: string): StepInvokeRequest {
  return {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- buildEnv reads only authzContext; the agent definition is never consulted here
    agent: {} as StepInvokeRequest["agent"],
    input: null,
    authzContext: { stepId: STEP_ID, runId, attempt: 1 },
    signal: new AbortController().signal,
  };
}

async function makeTempDir(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "sidecar-step-keying-"));
}

describe("createSidecarStepBuildEnv per-step scratch keying", () => {
  test("warm path keys one stable workdir across runs and preserves files", async () => {
    const dataDir = await makeTempDir();
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({
        dataDir,
        durableConversation: stubDurableConversationRegistry(),
      }),
    );

    const env1: StepEnvBase = await buildEnv(requestForRun("run-1"));
    // A file the agent would write during run-1's turn.
    const marker = path.join(env1.workdir, "turn-1.txt");
    await fs.writeFile(marker, "carried");

    const env2: StepEnvBase = await buildEnv(requestForRun("run-2"));

    // Stable keying: a different runId resolves to the SAME workdir, so
    // the warm case is bounded to one dir per agent (not one-per-message)
    // and the workspace survives across runs/respawn.
    expect(env2.workdir).toBe(env1.workdir);
    // The warm workdir lives under the stable `warm/<stepId>/` sub-root,
    // never under any run's `runs/<runId>/` subtree.
    expect(env1.workdir).toContain(
      path.join("workflow-step-state", WORKFLOW_RUN_REPO_ID.id, "warm"),
    );
    expect(env1.workdir).not.toContain(path.join("runs", "run-1"));
    // Continuity: the file written in run-1 is visible in run-2's env.
    expect(
      await fs.readFile(path.join(env2.workdir, "turn-1.txt"), "utf8"),
    ).toBe("carried");
  });

  test("cold path keys a distinct per-run workdir under that run's subtree", async () => {
    const dataDir = await makeTempDir();
    const buildEnv = createSidecarStepBuildEnv(buildDeps({ dataDir }));

    const env1: StepEnvBase = await buildEnv(requestForRun("run-1"));
    const env2: StepEnvBase = await buildEnv(requestForRun("run-2"));

    // Per-run keying: each run gets its own workdir, rooted under that
    // run's `runs/<runId>/` subtree -- exactly what the run-completion
    // cleanup reclaims at run granularity.
    expect(env2.workdir).not.toBe(env1.workdir);
    expect(env1.workdir).toContain(
      path.join(
        "workflow-step-state",
        WORKFLOW_RUN_REPO_ID.id,
        "runs",
        "run-1",
      ),
    );
    expect(env2.workdir).toContain(path.join("runs", "run-2"));
    // The cold path never parks scratch under the warm sub-root.
    expect(env1.workdir).not.toContain(path.join("warm", STEP_ID));
  });
});
