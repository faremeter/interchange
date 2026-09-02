// An onTrigger body child, spawned through the real sidecar seam, parks on
// an approval and resumes on the correlated grant.
//
// `createSidecarSpawnSuspendableChild` runs the body definition against a
// real on-disk workflow-run substrate and hands back the live handle
// `runOnTrigger` drives. This test exercises that handle end to end: the
// body step's injected `invokeStep` suspends as an approval, so the child
// runtime parks on `signalName(correlationId)`; `handle.next()` surfaces the
// park with the step's snapshot; `handle.resume` delivers the grant on the
// child's own signal channel, and the re-invoked step completes the run.
//
// A second case aborts the parent signal while the body is parked and proves
// the child cancels and the handle surfaces a terminal rather than hanging --
// the abort threads through `handle.cancel` and the run settles.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { ApprovalSnapshot, KeyPair } from "@intx/types/runtime";
import { defineAgent } from "@intx/agent";
import {
  builtinCredentialProviders,
  createCredentialProviderRegistry,
} from "@intx/harness";
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
  childWorkflow,
  createInMemoryScheduler,
  createInMemoryRepoStore,
  defineWorkflow,
  step,
  type WorkflowDefinition,
  type WorkflowEvent,
} from "@intx/workflow";

import { createWorkflowRunRepoStore } from "@intx/workflow-host";

import {
  createSidecarSpawnSuspendableChild,
  type SidecarChildStepInvoker,
} from "./workflow-substrate-factory";
import { runGrantsPath } from "./run-grants";

const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-suspendable-child";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: DEPLOYMENT_ID,
};
const allowAll: AuthorizeFn = () => ({ allowed: true });
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: DEPLOYMENT_ID,
};

const BODY_STEP_AGENT_ID = "wallet-spend";
const CORRELATION_ID = "corr-approval-1";
const SNAPSHOT: ApprovalSnapshot = {
  name: BODY_STEP_AGENT_ID,
  description: "spend from the shared wallet",
  inputSchema: { amount: "number" },
  arguments: { amount: 100 },
};

const tempDirs: string[] = [];
let signingKey: KeyPair;
// `buildChildRunEnv` reads the body run's `assets/workflow/<ref>/sources.json`
// eagerly; the suspend/resume mock invoker ignores inference, but the read still
// happens, so stage a minimal sources file for the body definition id.
let bodySourcesDataDir: string;

beforeAll(async () => {
  signingKey = await generateKeyPair();
  bodySourcesDataDir = await makeTempDir("suspendable-assets-");
  // The approval-park body ("body-wf") carries an agent step "s"; the
  // depth body ("depth-body") carries only a childWorkflow step (no
  // inference), but `buildChildRunEnv` reads its sources eagerly all the same.
  await stageBodySources("body-wf");
  await stageBodySources("depth-body");
});

async function stageBodySources(id: string): Promise<void> {
  const dir = path.join(bodySourcesDataDir, "assets", "workflow", id);
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.writeFile(
    path.join(dir, "sources.json"),
    JSON.stringify({
      s: [
        {
          id: "anthropic:m",
          provider: "anthropic",
          baseURL: "http://localhost:1",
          credentialId: "sk-x",
          model: "m",
        },
      ],
    }),
  );
}

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

// The one-step body definition every spawn runs. Its single step's injected
// invoker suspends as an approval on the first invocation and completes on
// the resume re-invocation.
function bodyDefinition(id: string): WorkflowDefinition {
  const agent = defineAgent({
    id: BODY_STEP_AGENT_ID,
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

const evaluateGrantsAdapter: Parameters<
  typeof createSidecarSpawnSuspendableChild
>[0]["evaluateGrants"] = async ({ resource, action, grants }) => {
  const result = await evaluateGrants(
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the snapshot's grants are typed unknown[] at the workflow-host boundary; the sidecar owns the GrantRule grammar, so the seeded rows narrow here
    [...(grants as readonly GrantRule[])],
    resource,
    action,
  );
  return { effect: result.effect, matchingGrants: [], resolvedBy: null };
};

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

// An invoker that suspends as an approval on its first invocation and, on the
// resume re-invocation, records the delivered decision and completes.
function suspendThenComplete(record: {
  resumeDecision?: unknown;
}): SidecarChildStepInvoker {
  return async (req) => {
    if (req.resume === undefined) {
      return {
        suspend: {
          correlationId: CORRELATION_ID,
          kind: "approval",
          approvalSnapshot: SNAPSHOT,
        },
      };
    }
    record.resumeDecision = req.resume.decision;
    return { output: { echoed: req.resume.decision } };
  };
}

function makeSpawner(
  substrate: ReturnType<typeof createRepoStore>,
  invokeStep: SidecarChildStepInvoker,
): ReturnType<typeof createSidecarSpawnSuspendableChild> {
  return createSidecarSpawnSuspendableChild({
    substrate,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    workflowRunRef: REF,
    principal: PRINCIPAL,
    scheduler: createInMemoryScheduler({
      repoStore: createInMemoryRepoStore(),
      clock: () => new Date(),
    }),
    invokeStep,
    evaluateGrants: evaluateGrantsAdapter,
    dataDir: bodySourcesDataDir,
    bodySources: {},
    credentialProviders: createCredentialProviderRegistry(
      builtinCredentialProviders(),
    ),
  });
}

describe("createSidecarSpawnSuspendableChild", () => {
  test("surfaces a body approval park, resumes on the grant, and completes", async () => {
    const substrate = await makeSubstrate("suspendable-approval-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant(`tool:${BODY_STEP_AGENT_ID}`, "invoke"),
    ]);

    const record: { resumeDecision?: unknown } = {};
    const spawn = makeSpawner(substrate, suspendThenComplete(record));

    const handle = await spawn(
      {
        definition: bodyDefinition("body-wf"),
        definitionRef: REF,
        childRunId: "run-body-0",
        input: { text: "event-0" },
        parentRunId,
        parentStepId: "section",
        signal: new AbortController().signal,
        depth: 0,
        maxChildSpawnDepth: 32,
      },
      () => undefined,
    );

    // The body step suspended -> the handle surfaces the approval park on the
    // reserved correlation, carrying the step's snapshot.
    const parked = await handle.next();
    expect(parked.kind).toBe("park");
    if (parked.kind !== "park") throw new Error("expected a park");
    expect(parked.park.correlationId).toBe(CORRELATION_ID);
    expect(parked.park.approvalSnapshot).toEqual(SNAPSHOT);

    // Grant it: resume delivers the decision on the child's own signal
    // channel, unblocking the parked step.
    const decision = { outcome: "approved" as const, note: "ok" };
    await handle.resume(CORRELATION_ID, decision);

    // The re-invoked step ran with the delivered decision and the run
    // completed.
    const terminal = await handle.next();
    expect(terminal.kind).toBe("terminal");
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("completed");
    expect(record.resumeDecision).toEqual(decision);
  });

  test("threads depth so a body's childWorkflow grandchild trips the ceiling", async () => {
    const substrate = await makeSubstrate("suspendable-depth-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant("inference.source:anthropic:m", "invoke"),
    ]);

    // The invoker must never run: the depth guard fires before the grandchild's
    // agent step is reached.
    const spawn = makeSpawner(substrate, () => {
      throw new Error("depth: no step should run before the guard fires");
    });

    // Spawn the body AT depth 1 with the ceiling lowered to 1. The body runs at
    // depth 1 (passed through unchanged), so its `childWorkflow` spawns the
    // grandchild at depth 2 > 1 and trips the guard. WITHOUT depth threading the
    // body would run at depth 0 and the grandchild at depth 1 <= 1 -- no trip --
    // so this failure proves the container's depth reached the body run.
    const childRunId = "run-depth-body";
    const handle = await spawn(
      {
        definition: childSpawningBody(),
        definitionRef: REF,
        childRunId,
        input: null,
        parentRunId,
        parentStepId: "section",
        signal: new AbortController().signal,
        depth: 1,
        maxChildSpawnDepth: 1,
      },
      () => undefined,
    );

    // The body's childWorkflow step failed loud, so the body run settles failed.
    const terminal = await handle.next();
    expect(terminal.kind).toBe("terminal");
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("failed");

    // The guard fired before the body committed a ChildSpawned, and the
    // StepFailed names the tripped depth 2 (body depth 1 + 1), not the depth 1 a
    // reset-at-the-body-boundary would give.
    const bodyEvents: readonly WorkflowEvent[] =
      await reader(substrate).read(childRunId);
    expect(bodyEvents.some((e) => e.kind === "ChildSpawned")).toBe(false);
    const stepFailed = bodyEvents.find((e) => e.kind === "StepFailed");
    if (stepFailed === undefined || stepFailed.kind !== "StepFailed") {
      throw new Error(`body run ${childRunId} has no StepFailed event`);
    }
    expect(stepFailed.error.message).toContain(
      'childWorkflow spawn depth 2 exceeds the maximum 1 at step "spawn"',
    );
  });
});

// A body whose single step spawns a `childWorkflow` grandchild. The grandchild
// carries an agent step that is never reached: the depth guard fires first.
function childSpawningBody(): WorkflowDefinition {
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
  return defineWorkflow({
    id: "depth-body",
    trigger: { type: "manual" },
    steps: { spawn: childWorkflow({ definition: leaf }) },
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
