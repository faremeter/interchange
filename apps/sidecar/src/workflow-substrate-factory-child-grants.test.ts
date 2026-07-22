// A spawned child inherits the grants of the run that spawned it.
//
// `createSidecarRunChild` reads the parent run's
// `runs/<parentRunId>/grants.json`, binds the child's `env.authorize` to
// that flat grant set, and persists the same grants under the child's own
// `runs/<childRunId>/grants.json` so a grandchild can inherit them in
// turn. A child whose parent has no grants file fails closed at spawn.
//
// The substrate here is a real on-disk workflow-run repo. The child's
// injected `invokeStep` calls the credentials-backed `authorize` the
// runtime env carries, so a granted resource resolves `allow` and an
// ungranted one does not -- exercising the binding end to end.

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
import {
  createInMemoryScheduler,
  createInMemoryRepoStore,
  defineWorkflow,
  step,
  type StepInvokeResult,
  type WorkflowDefinition,
} from "@intx/workflow";

import {
  createSidecarRunChild,
  type SidecarChildStepInvoker,
} from "./workflow-substrate-factory";
import { readRunGrants, runGrantsPath } from "./run-grants";

const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-child-grants";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: DEPLOYMENT_ID,
};
const allowAll: AuthorizeFn = () => ({ allowed: true });
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  deploymentId: DEPLOYMENT_ID,
};

const tempDirs: string[] = [];
let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
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

// The one-step child definition every spawn in these tests runs. Its
// single step's agent id doubles as the `tool:<id>` resource the injected
// invoker authorizes, so a granted resource maps to this step.
const CHILD_STEP_AGENT_ID = "wallet-spend";
function childDefinition(id: string): WorkflowDefinition {
  const agent = defineAgent({
    id: CHILD_STEP_AGENT_ID,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: { s: step({ agent }) },
  });
}

// Grant evaluator that delegates the decision to `@intx/authz` against
// the credentials snapshot's grants alone. Unlike the production adapter
// it does NOT merge any per-step tool-mark floor grants, so a decision
// here reflects only the inherited grant set the test seeded.
const evaluateGrantsAdapter: SidecarRunChildDepsEvaluator = async ({
  resource,
  action,
  grants,
}) => {
  const result = await evaluateGrants(
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the snapshot's grants are typed unknown[] at the workflow-host boundary; the sidecar owns the GrantRule grammar, so the seeded rows narrow here
    [...(grants as readonly GrantRule[])],
    resource,
    action,
  );
  return { effect: result.effect, matchingGrants: [], resolvedBy: null };
};
type SidecarRunChildDepsEvaluator = Parameters<
  typeof createSidecarRunChild
>[0]["evaluateGrants"];

// Build a real on-disk workflow-run substrate and seed its genesis tree.
async function makeSubstrate(
  prefix: string,
): Promise<ReturnType<typeof createRepoStore>> {
  const dataDir = await makeTempDir(prefix);
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

// Seed a run's grants file the way the hub's `run.grants` delivery does:
// a single `runs/<runId>/grants.json` under the workflow-run repo.
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

// An invoker that authorizes `tool:<agentId>` against the child's env
// authorize and records the decision, so a test can assert the child saw
// the inherited grant.
function recordingInvoker(record: {
  decisions: { resource: string; effect: string | null }[];
}): SidecarChildStepInvoker {
  return async (req, authorize): Promise<StepInvokeResult> => {
    const resource = `tool:${req.agent.id}`;
    const decision = await authorize(resource, "invoke", req.authzContext);
    record.decisions.push({ resource, effect: decision.effect });
    return { output: null };
  };
}

// An invoker that authorizes the step's own tool AND a second probe
// resource against the child's env authorize, recording both decisions.
// The probe lets a test prove the inherited grant set was actually
// delivered (the covered resource resolves `allow`) alongside the
// uncovered resource resolving a non-allow effect.
function recordingInvokerWithProbe(
  record: {
    decisions: { resource: string; effect: string | null }[];
  },
  probeResource: string,
): SidecarChildStepInvoker {
  return async (req, authorize): Promise<StepInvokeResult> => {
    const resource = `tool:${req.agent.id}`;
    const decision = await authorize(resource, "invoke", req.authzContext);
    record.decisions.push({ resource, effect: decision.effect });
    const probe = await authorize(probeResource, "invoke", req.authzContext);
    record.decisions.push({ resource: probeResource, effect: probe.effect });
    return { output: null };
  };
}

describe("createSidecarRunChild grant inheritance", () => {
  test("a child inherits the parent run's grants and authorizes against them", async () => {
    const substrate = await makeSubstrate("child-grants-inherit-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant(`tool:${CHILD_STEP_AGENT_ID}`, "invoke"),
    ]);

    const record = {
      decisions: [] as { resource: string; effect: string | null }[],
    };
    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: REF,
      workflowDefinitionRef: "refs/heads/main",
      principal: PRINCIPAL,
      scheduler: createInMemoryScheduler({
        repoStore: createInMemoryRepoStore(),
        clock: () => new Date(),
      }),
      invokeStep: recordingInvoker(record),
      evaluateGrants: evaluateGrantsAdapter,
    });

    const childRunId = "run-child";
    const result = await runChild({
      definition: childDefinition("child-wf"),
      definitionRef: "refs/heads/main",
      childRunId,
      input: null,
      parentRunId,
      parentStepId: "s",
      signal: new AbortController().signal,
    });

    expect(result.terminalStatus).toBe("completed");
    // The child authorized the granted tool and saw `allow`.
    expect(record.decisions).toEqual([
      { resource: `tool:${CHILD_STEP_AGENT_ID}`, effect: "allow" },
    ]);
    // The child persisted its own inherited grants file for a grandchild.
    const childGrants = await readRunGrants({
      repoStore: substrate,
      deploymentId: DEPLOYMENT_ID,
      runId: childRunId,
    });
    expect(childGrants).toBeDefined();
    expect(childGrants).toEqual([
      grant(`tool:${CHILD_STEP_AGENT_ID}`, "invoke"),
    ]);
  });

  test("an ungranted tool resolves to a non-allow decision", async () => {
    const substrate = await makeSubstrate("child-grants-ungranted-");
    const parentRunId = "run-parent";
    // Parent holds a DIFFERENT grant, so `tool:wallet-spend` is not covered.
    await seedRunGrants(substrate, parentRunId, [
      grant("tool:other", "invoke"),
    ]);

    const record = {
      decisions: [] as { resource: string; effect: string | null }[],
    };
    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: REF,
      workflowDefinitionRef: "refs/heads/main",
      principal: PRINCIPAL,
      scheduler: createInMemoryScheduler({
        repoStore: createInMemoryRepoStore(),
        clock: () => new Date(),
      }),
      invokeStep: recordingInvokerWithProbe(record, "tool:other"),
      evaluateGrants: evaluateGrantsAdapter,
    });

    await runChild({
      definition: childDefinition("child-wf"),
      definitionRef: "refs/heads/main",
      childRunId: "run-child",
      input: null,
      parentRunId,
      parentStepId: "s",
      signal: new AbortController().signal,
    });

    // The uncovered tool resolves a null (non-allow) effect -- the child
    // stays fail-closed on a tool its inherited grants do not cover -- while
    // the covered `tool:other` resolves `allow`. The positive control proves
    // the null is genuine fail-closed on the inherited set, not an empty
    // grant view that would deny everything regardless.
    expect(record.decisions).toEqual([
      { resource: `tool:${CHILD_STEP_AGENT_ID}`, effect: null },
      { resource: "tool:other", effect: "allow" },
    ]);
  });

  test("a grandchild inherits the child's persisted grants (multi-hop)", async () => {
    const substrate = await makeSubstrate("child-grants-multihop-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant(`tool:${CHILD_STEP_AGENT_ID}`, "invoke"),
    ]);

    const record = {
      decisions: [] as { resource: string; effect: string | null }[],
    };
    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: REF,
      workflowDefinitionRef: "refs/heads/main",
      principal: PRINCIPAL,
      scheduler: createInMemoryScheduler({
        repoStore: createInMemoryRepoStore(),
        clock: () => new Date(),
      }),
      invokeStep: recordingInvoker(record),
      evaluateGrants: evaluateGrantsAdapter,
    });

    // Hop 1: parent -> child. Writes runs/run-child/grants.json.
    const childRunId = "run-child";
    await runChild({
      definition: childDefinition("child-wf"),
      definitionRef: "refs/heads/main",
      childRunId,
      input: null,
      parentRunId,
      parentStepId: "s",
      signal: new AbortController().signal,
    });

    // Hop 2: child -> grandchild. The grandchild's parent is the child,
    // so it reads the child's persisted grants file.
    const grandchildRunId = "run-grandchild";
    const grandResult = await runChild({
      definition: childDefinition("grandchild-wf"),
      definitionRef: "refs/heads/main",
      childRunId: grandchildRunId,
      input: null,
      parentRunId: childRunId,
      parentStepId: "s",
      signal: new AbortController().signal,
    });

    expect(grandResult.terminalStatus).toBe("completed");
    // Both hops authorized `allow` against the inherited grant.
    expect(record.decisions).toEqual([
      { resource: `tool:${CHILD_STEP_AGENT_ID}`, effect: "allow" },
      { resource: `tool:${CHILD_STEP_AGENT_ID}`, effect: "allow" },
    ]);
    const grandchildGrants = await readRunGrants({
      repoStore: substrate,
      deploymentId: DEPLOYMENT_ID,
      runId: grandchildRunId,
    });
    expect(grandchildGrants).toEqual([
      grant(`tool:${CHILD_STEP_AGENT_ID}`, "invoke"),
    ]);
  });

  test("a child whose parent has no grants file fails closed at spawn", async () => {
    const substrate = await makeSubstrate("child-grants-absent-");
    // No grants file seeded for the parent run.
    const parentRunId = "run-parent-ungranted";

    const record = {
      decisions: [] as { resource: string; effect: string | null }[],
    };
    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: REF,
      workflowDefinitionRef: "refs/heads/main",
      principal: PRINCIPAL,
      scheduler: createInMemoryScheduler({
        repoStore: createInMemoryRepoStore(),
        clock: () => new Date(),
      }),
      invokeStep: recordingInvoker(record),
      evaluateGrants: evaluateGrantsAdapter,
    });

    await expect(
      runChild({
        definition: childDefinition("child-wf"),
        definitionRef: "refs/heads/main",
        childRunId: "run-child",
        input: null,
        parentRunId,
        parentStepId: "s",
        signal: new AbortController().signal,
      }),
    ).rejects.toThrow(/has no grants file/);
    // The child never ran a step, so no authorize decision was recorded.
    expect(record.decisions).toEqual([]);
  });
});
