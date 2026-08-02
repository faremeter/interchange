// A parent abort of an in-process child settles the child CANCELLED, not
// FAILED, driving the full cancel cascade to a clean RunCancelled terminal.
// The child's control-plane cancel commits a supervisor-signed
// `CancelRequested`, and the run body's workflow-process cascade then commits
// `CancelPropagated`/`RunCancelled` -- which carries the supervisor-signed
// cancel forward, accepted because the kind handler checks a cancel's origin
// only on the authoring write.
//
// Both in-process spawners are covered: the terminal-only `createSidecarRunChild`
// and the suspendable `createSidecarSpawnSuspendableChild`. Each abort is driven
// while the child is live and parked on an approval -- the reachable drain-window
// path -- so the test exercises the real cancel cascade, not a child that
// already settled.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { ApprovalSnapshot, KeyPair } from "@intx/types/runtime";
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
  defineWorkflow,
  step,
  type WorkflowDefinition,
  type WorkflowEvent,
} from "@intx/workflow";

import {
  createSidecarRunChild,
  createSidecarSpawnSuspendableChild,
  type SidecarChildStepInvoker,
} from "./workflow-substrate-factory";
import { runGrantsPath } from "./run-grants";

const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-abort";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: DEPLOYMENT_ID,
};
const allowAll: AuthorizeFn = () => ({ allowed: true });
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  deploymentId: DEPLOYMENT_ID,
};
const BODY_STEP_AGENT_ID = "wallet-spend";
const CORRELATION_ID = "corr-abort-1";
const SNAPSHOT: ApprovalSnapshot = {
  name: BODY_STEP_AGENT_ID,
  description: "spend from the shared wallet",
  inputSchema: { amount: "number" },
  arguments: { amount: 100 },
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

// An invoker that parks the body step on an approval and never returns an
// output: the child stays parked until the parent abort cancels it.
const parkForever: SidecarChildStepInvoker = async () => ({
  suspend: {
    correlationId: CORRELATION_ID,
    kind: "approval",
    approvalSnapshot: SNAPSHOT,
  },
});

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

function sharedDeps(substrate: ReturnType<typeof createRepoStore>) {
  return {
    substrate,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    workflowRunRef: REF,
    workflowDefinitionRef: REF,
    principal: PRINCIPAL,
    scheduler: createInMemoryScheduler({
      repoStore: createInMemoryRepoStore(),
      clock: () => new Date(),
    }),
    invokeStep: parkForever,
    evaluateGrants: evaluateGrantsAdapter,
  };
}

function reader(substrate: ReturnType<typeof createRepoStore>) {
  return createWorkflowRunRepoStore({
    substrate,
    repoId: WORKFLOW_RUN_REPO_ID,
    principal: PRINCIPAL,
    ref: REF,
  });
}

// Poll the child's durable log until its step parks (a SignalAwaited is
// committed), so the abort lands while the child is genuinely in flight.
async function waitForChildPark(
  substrate: ReturnType<typeof createRepoStore>,
  childRunId: string,
): Promise<void> {
  const r = reader(substrate);
  for (let i = 0; i < 300; i += 1) {
    const events = await r.read(childRunId);
    if (events.some((e) => e.kind === "SignalAwaited")) return;
    await new Promise((res) => setTimeout(res, 10));
  }
  throw new Error(`timed out waiting for child ${childRunId} to park`);
}

// Assert the child's durable log shows the full cancel cascade landed: the
// supervisor-signed CancelRequested and the terminal RunCancelled.
async function expectCleanCancelCascade(
  substrate: ReturnType<typeof createRepoStore>,
  childRunId: string,
): Promise<void> {
  const events: readonly WorkflowEvent[] =
    await reader(substrate).read(childRunId);
  expect(events.some((e) => e.kind === "CancelRequested")).toBe(true);
  expect(events.some((e) => e.kind === "RunCancelled")).toBe(true);
}

describe("in-process child parent-abort", () => {
  test("createSidecarRunChild settles cancelled, not failed", async () => {
    const substrate = await makeSubstrate("abort-terminal-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant(`tool:${BODY_STEP_AGENT_ID}`, "invoke"),
    ]);

    const runChild = createSidecarRunChild(sharedDeps(substrate));
    const abort = new AbortController();
    const childRunId = "run-body-terminal";
    const settled = runChild({
      definition: bodyDefinition("body"),
      definitionRef: REF,
      childRunId,
      input: null,
      parentRunId,
      parentStepId: "s",
      signal: abort.signal,
    });

    await waitForChildPark(substrate, childRunId);
    abort.abort();

    const result = await settled;
    expect(result.terminalStatus).toBe("cancelled");
    await expectCleanCancelCascade(substrate, childRunId);
  });

  test("createSidecarSpawnSuspendableChild settles cancelled, not failed", async () => {
    const substrate = await makeSubstrate("abort-suspendable-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant(`tool:${BODY_STEP_AGENT_ID}`, "invoke"),
    ]);

    const spawn = createSidecarSpawnSuspendableChild(sharedDeps(substrate));
    const abort = new AbortController();
    const childRunId = "run-body-suspendable";
    const handle = await spawn({
      definition: bodyDefinition("body"),
      definitionRef: REF,
      childRunId,
      input: { text: "event-0" },
      parentRunId,
      parentStepId: "section",
      signal: abort.signal,
    });

    // The body parked on an approval and the handle surfaced it: the child is
    // live. Abort the parent now.
    const parked = await handle.next();
    expect(parked.kind).toBe("park");
    abort.abort();

    const terminal = await handle.next();
    expect(terminal.kind).toBe("terminal");
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("cancelled");
    await expectCleanCancelCascade(substrate, childRunId);
  });
});
