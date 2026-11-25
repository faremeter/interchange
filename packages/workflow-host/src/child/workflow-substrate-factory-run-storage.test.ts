// A spawned run reclaims its local storage once its terminal event is
// durable, and keeps it when its `complete` rejects with no terminal.
//
// Every spawned run gets its handle from `createSuspendableChildHandle`, and
// the substrate factory gives each handle a teardown that stops the run's
// signal channel and calls `cleanupRunStorage` with the run's id. These
// tests drive the three factory sites through the real seams against a real
// on-disk workflow-run substrate: a `childWorkflow` child
// (`createSidecarRunChild`), an onTrigger body
// (`createSidecarSpawnSuspendableChild`) and a loop iteration inside a body
// (`buildChildRunEnv`). The injected step invoker writes a file into the
// run's `workflow-step-state` directory, as the real invoker's local store
// does, and the deps carry the real `createRunStorageCleanup` behind a
// recorder. So each test sees both the call and the directory on disk.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { ApprovalSnapshot, KeyPair } from "@intx/types/runtime";
import { defineAgent, createDefaultDirectorRegistry } from "@intx/agent";
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
  createInMemoryScheduler,
  createInMemoryRepoStore,
  defineWorkflow,
  escalation,
  loop,
  loopBodyRunId,
  step,
  type SuspendableChildHandle,
  type WorkflowDefinition,
  unavailableMailSigner,
} from "@intx/workflow";

import { createWorkflowRunRepoStore, runGrantsPath } from "@intx/workflow-host";

import {
  createRunStorageCleanup,
  createSidecarRunChild,
  createSidecarSpawnSuspendableChild,
  type SidecarChildStepInvoker,
} from "./substrate-factory";

const noWake = (): void => undefined;

const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-run-storage";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: DEPLOYMENT_ID,
};
const allowAll: AuthorizeFn = () => ({ allowed: true });
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: DEPLOYMENT_ID,
};

const TOP_LEVEL_RUN_ID = "run-parent";
const BODY_RUN_ID = "run-parent__section__0";
const AGENT_ID = "storage-agent";
const BODY_ID = "storage-body";
const LOOP_BODY_ID = "storage-loop-body";
const LOOP_ITERATION_ID = "storage-loop-iteration";
const CORRELATION_ID = "corr-storage-approval";
const SNAPSHOT: ApprovalSnapshot = {
  name: AGENT_ID,
  description: "approve the step",
  inputSchema: {},
  arguments: {},
};

const LOOPS_SOURCE = `
export function once() {
  return false;
}

export function next(_output, currentInput) {
  return currentInput;
}
`;

const tempDirs: string[] = [];
let signingKey: KeyPair;
let closurePackageDir: string;

beforeAll(async () => {
  signingKey = await generateKeyPair();
  closurePackageDir = await makeTempDir("run-storage-closure-");
  await fs.promises.writeFile(
    path.join(closurePackageDir, "package.json"),
    JSON.stringify({
      name: "@fixture/run-storage",
      version: "1.0.0",
      interchange: { loops: "./loops.js" },
    }),
  );
  await fs.promises.writeFile(
    path.join(closurePackageDir, "loops.js"),
    LOOPS_SOURCE,
  );
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

function agentStep() {
  return step({
    agent: defineAgent({
      id: AGENT_ID,
      systemPrompt: "s",
      tools: [],
      capabilities: [],
      inference: { sources: [{ provider: "anthropic", model: "m" }] },
    }),
  });
}

function agentBody(): WorkflowDefinition {
  return defineWorkflow({
    id: BODY_ID,
    trigger: { type: "manual" },
    steps: { s: agentStep() },
  });
}

// A body whose loop runs its agent body once.
function loopBody(): WorkflowDefinition {
  return defineWorkflow({
    id: LOOP_BODY_ID,
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body: defineWorkflow({
          id: LOOP_ITERATION_ID,
          trigger: { type: "manual" },
          steps: { s: agentStep() },
        }),
        while: "once",
        carry: "next",
        input: { literal: null },
        maxIterations: 2,
        onExhausted: "exhausted",
      }),
      exhausted: escalation({ to: "operator", after: ["rework"] }),
    },
  });
}

function stepStateRuns(dataDir: string): string {
  return path.join(
    dataDir,
    "workflow-step-state",
    WORKFLOW_RUN_REPO_ID.id,
    "runs",
  );
}

async function exists(p: string): Promise<boolean> {
  return fs.promises
    .access(p)
    .then(() => true)
    .catch(() => false);
}

const evaluateGrantsAdapter: Parameters<
  typeof createSidecarRunChild
>[0]["evaluateGrants"] = async ({ resource, action, grants }) => {
  const result = await evaluateGrants([...grants], resource, action, {
    conditions: "pessimistic",
  });
  return { effect: result.effect, matchingGrants: [], resolvedBy: null };
};

async function makeSubstrate(): Promise<ReturnType<typeof createRepoStore>> {
  const substrate = createRepoStore({
    dataDir: await makeTempDir("run-storage-repo-"),
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: allowAll,
  });
  await substrate.writeTree({ kind: "hub" }, WORKFLOW_RUN_REPO_ID, REF, {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "genesis",
  });
  const grants: GrantRule[] = [
    {
      id: "grant-agent",
      resource: `tool:${AGENT_ID}`,
      action: "invoke",
      effect: "allow",
      origin: "creator",
      conditions: null,
      expiresAt: null,
      roleId: null,
      principalId: null,
    },
  ];
  await substrate.writeTree({ kind: "hub" }, WORKFLOW_RUN_REPO_ID, REF, {
    files: {
      [runGrantsPath(TOP_LEVEL_RUN_ID)]: JSON.stringify({ grants }, null, 2),
    },
    message: "seed top-level grants",
  });
  return substrate;
}

function reader(substrate: ReturnType<typeof createRepoStore>) {
  return createWorkflowRunRepoStore({
    substrate,
    repoId: WORKFLOW_RUN_REPO_ID,
    principal: PRINCIPAL,
    ref: REF,
  });
}

// How the step invoker answers. Each invocation first writes into its run's
// step-state directory, as the real invoker's per-attempt local store does.
type StepBehavior = "suspend-then-complete" | "fail";

function storageWritingInvoker(
  dataDir: string,
  behavior: StepBehavior,
): SidecarChildStepInvoker {
  return async (req) => {
    const runId = req.authzContext.runId;
    if (runId === undefined) throw new Error("step invoked without a runId");
    const dir = path.join(stepStateRuns(dataDir), runId, "store");
    await fs.promises.mkdir(dir, { recursive: true });
    await fs.promises.writeFile(path.join(dir, "turn.json"), "{}");
    if (behavior === "fail") throw new Error("the step failed");
    if (req.resume === undefined) {
      return {
        suspend: {
          correlationId: CORRELATION_ID,
          kind: "approval",
          approvalSnapshot: SNAPSHOT,
        },
      };
    }
    return { output: { done: true } };
  };
}

type Reclaim = { runId: string; lastEventKind: string | undefined };

// Wraps the real cleanup. Each call records the run's last durable event at
// the time of the call, so a test can check that the reclaim came after the
// run's terminal commit.
function recordingCleanup(
  substrate: ReturnType<typeof createRepoStore>,
  dataDir: string,
): {
  cleanupRunStorage: (runId: string) => Promise<void>;
  reclaims: Reclaim[];
  until: (runIds: readonly string[]) => Promise<void>;
} {
  const real = createRunStorageCleanup({
    dataDir,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    topLevelRunId: TOP_LEVEL_RUN_ID,
  });
  const reclaims: Reclaim[] = [];
  const waiters: (() => void)[] = [];
  const done = (runIds: readonly string[]): boolean =>
    runIds.every((id) => reclaims.some((r) => r.runId === id));
  return {
    reclaims,
    cleanupRunStorage: async (runId) => {
      const log = await reader(substrate).read(runId);
      await real(runId);
      reclaims.push({ runId, lastEventKind: log.at(-1)?.kind });
      for (const wake of waiters.splice(0)) wake();
    },
    until: async (runIds) => {
      while (!done(runIds)) {
        await new Promise<void>((resolve) => waiters.push(resolve));
      }
    },
  };
}

function makeDeps(
  substrate: ReturnType<typeof createRepoStore>,
  dataDir: string,
  invokeStep: SidecarChildStepInvoker,
  cleanupRunStorage: (runId: string) => Promise<void>,
): Parameters<typeof createSidecarRunChild>[0] {
  return {
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
    dataDir,
    bodySources: {
      [BODY_ID]: {},
      [LOOP_BODY_ID]: {},
      [LOOP_ITERATION_ID]: {},
    },
    credentialProviders: createCredentialProviderRegistry(
      builtinCredentialProviders(),
    ),
    collectDeclaredResources: () => new Set<string>(),
    collectDeclaredCredentialConsumers: () => new Set<string>(),
    filterGrantsToDeclaredResources: (parentGrants: readonly unknown[]) =>
      parentGrants,
    closurePackageDir,
    directors: createDefaultDirectorRegistry(),
    mailSigner: unavailableMailSigner,
    cleanupRunStorage,
  };
}

type Site = {
  name: string;
  spawn: (
    deps: Parameters<typeof createSidecarRunChild>[0],
    signal: AbortSignal,
  ) => Promise<SuspendableChildHandle>;
  // The spawned runs whose storage the site must reclaim. The run that
  // parks comes first.
  runIds: readonly string[];
};

const spawnInput = (definition: WorkflowDefinition, signal: AbortSignal) => ({
  definition,
  definitionRef: definition.id,
  childRunId: BODY_RUN_ID,
  input: null,
  parentRunId: TOP_LEVEL_RUN_ID,
  parentStepId: "section",
  signal,
  depth: 0,
  maxChildSpawnDepth: 32,
  stepPathPrefix: ["section"],
});

const SITES: Site[] = [
  {
    name: "a childWorkflow child",
    spawn: (deps, signal) =>
      createSidecarRunChild(deps)(spawnInput(agentBody(), signal), () => {
        /* no live events */
      }),
    runIds: [BODY_RUN_ID],
  },
  {
    name: "an onTrigger body",
    spawn: (deps, signal) =>
      createSidecarSpawnSuspendableChild(deps)(
        spawnInput(agentBody(), signal),
        () => {
          /* no live events */
        },
      ),
    runIds: [BODY_RUN_ID],
  },
  {
    name: "a loop iteration in a body",
    spawn: (deps, signal) =>
      createSidecarSpawnSuspendableChild(deps)(
        spawnInput(loopBody(), signal),
        () => {
          /* no live events */
        },
      ),
    runIds: [loopBodyRunId(BODY_RUN_ID, "rework", 0), BODY_RUN_ID],
  },
];

const TERMINAL_EVENT_KINDS = new Set([
  "RunCompleted",
  "RunFailed",
  "RunCancelled",
]);

describe("a spawned run reclaims its local storage once its terminal is durable", () => {
  test.each(SITES)(
    "$name: keeps the storage while parked and reclaims it after completion",
    async (site) => {
      const substrate = await makeSubstrate();
      const dataDir = await makeTempDir("run-storage-data-");
      const recorder = recordingCleanup(substrate, dataDir);
      const deps = makeDeps(
        substrate,
        dataDir,
        storageWritingInvoker(dataDir, "suspend-then-complete"),
        recorder.cleanupRunStorage,
      );
      const handle = await site.spawn(deps, new AbortController().signal);

      const parked = await handle.next(noWake);
      if (parked.kind !== "park") {
        throw new Error(`expected a park, got ${parked.kind}`);
      }
      // The parking run's storage is in use while the park is open.
      const [parkingRunId] = site.runIds;
      if (parkingRunId === undefined) throw new Error("site names no run");
      expect(
        await exists(path.join(stepStateRuns(dataDir), parkingRunId)),
      ).toBe(true);
      expect(recorder.reclaims).toEqual([]);

      await handle.resume(parked.park.correlationId, { outcome: "approved" });
      const terminal = await handle.next(noWake);
      if (terminal.kind !== "terminal") {
        throw new Error(`expected a terminal, got ${terminal.kind}`);
      }
      expect(terminal.terminalStatus).toBe("completed");

      await recorder.until(site.runIds);
      expect([...recorder.reclaims.map((r) => r.runId)].sort()).toEqual(
        [...site.runIds].sort(),
      );
      for (const reclaim of recorder.reclaims) {
        expect(TERMINAL_EVENT_KINDS.has(reclaim.lastEventKind ?? "")).toBe(
          true,
        );
        expect(
          await exists(path.join(stepStateRuns(dataDir), reclaim.runId)),
        ).toBe(false);
      }
      expect(recorder.reclaims.some((r) => r.runId === TOP_LEVEL_RUN_ID)).toBe(
        false,
      );
    },
  );

  test.each(SITES)(
    "$name: reclaims the storage of a run that failed",
    async (site) => {
      const substrate = await makeSubstrate();
      const dataDir = await makeTempDir("run-storage-data-");
      const recorder = recordingCleanup(substrate, dataDir);
      const deps = makeDeps(
        substrate,
        dataDir,
        storageWritingInvoker(dataDir, "fail"),
        recorder.cleanupRunStorage,
      );
      const handle = await site.spawn(deps, new AbortController().signal);

      const terminal = await handle.next(noWake);
      if (terminal.kind !== "terminal") {
        throw new Error(`expected a terminal, got ${terminal.kind}`);
      }
      expect(terminal.terminalStatus).toBe("failed");

      await recorder.until(site.runIds);
      for (const runId of site.runIds) {
        const reclaim = recorder.reclaims.filter((r) => r.runId === runId);
        expect(reclaim).toHaveLength(1);
        expect(reclaim[0]?.lastEventKind).toBe("RunFailed");
        expect(await exists(path.join(stepStateRuns(dataDir), runId))).toBe(
          false,
        );
      }
    },
  );

  test.each(SITES)(
    "$name: reclaims the storage of a parked run its parent aborts, after its RunFailed commits",
    async (site) => {
      const substrate = await makeSubstrate();
      const dataDir = await makeTempDir("run-storage-data-");
      const recorder = recordingCleanup(substrate, dataDir);
      const deps = makeDeps(
        substrate,
        dataDir,
        storageWritingInvoker(dataDir, "suspend-then-complete"),
        recorder.cleanupRunStorage,
      );
      const parent = new AbortController();
      const handle = await site.spawn(deps, parent.signal);

      const parked = await handle.next(noWake);
      expect(parked.kind).toBe("park");
      expect(recorder.reclaims).toEqual([]);

      parent.abort();
      const terminal = await handle.next(noWake);
      if (terminal.kind !== "terminal") {
        throw new Error(`expected a terminal, got ${terminal.kind}`);
      }
      expect(terminal.terminalStatus).toBe("failed");

      await recorder.until(site.runIds);
      for (const runId of site.runIds) {
        const reclaim = recorder.reclaims.filter((r) => r.runId === runId);
        expect(reclaim).toHaveLength(1);
        // The abort fails the parked step locally, and the run commits its
        // own RunFailed. The reclaim comes only after that commit.
        expect(reclaim[0]?.lastEventKind).toBe("RunFailed");
        expect(await exists(path.join(stepStateRuns(dataDir), runId))).toBe(
          false,
        );
      }
    },
  );

  test("a reclaim that fails still lets the terminal reach the container, with no unhandled rejection", async () => {
    const substrate = await makeSubstrate();
    const dataDir = await makeTempDir("run-storage-data-");
    // A file where the `runs` directory goes makes every removal under it
    // fail with ENOTDIR. The step invoker writes nothing here.
    await fs.promises.mkdir(path.dirname(stepStateRuns(dataDir)), {
      recursive: true,
    });
    await fs.promises.writeFile(stepStateRuns(dataDir), "not a directory");
    const real = createRunStorageCleanup({
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      topLevelRunId: TOP_LEVEL_RUN_ID,
    });
    const settled: string[] = [];
    let wake: (() => void) | undefined;
    const reclaimed = new Promise<void>((resolve) => {
      wake = resolve;
    });
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", onUnhandled);
    try {
      const deps = makeDeps(
        substrate,
        dataDir,
        async () => ({ output: { done: true } }),
        async (runId) => {
          await real(runId);
          settled.push(runId);
          wake?.();
        },
      );
      const handle = await createSidecarSpawnSuspendableChild(deps)(
        spawnInput(agentBody(), new AbortController().signal),
        () => {
          /* no live events */
        },
      );
      const terminal = await handle.next(noWake);
      if (terminal.kind !== "terminal") {
        throw new Error(`expected a terminal, got ${terminal.kind}`);
      }
      expect(terminal.terminalStatus).toBe("completed");
      await reclaimed;
      expect(settled).toEqual([BODY_RUN_ID]);
      // Let a rejection that nothing handled reach the process hook.
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });
});

// A body whose log write fails after its step parked in the local store.
// `complete` rejects with a repo write failure and the log has no terminal,
// so a restart recovers the body from the durable prefix of its log. That
// recovery reads the parked step's local store, so the directory must stay.
describe("a spawned run whose terminal never commits", () => {
  // The bound on the wait for a reclaim that must not come. On a regression
  // the reclaim comes within milliseconds of the settlement.
  const RECLAIM_WAIT_MS = 1000;

  test("keeps its local storage when the run's log write fails", async () => {
    const base = await makeSubstrate();
    const dataDir = await makeTempDir("run-storage-data-");
    let failWrites = false;
    const failWrite = (): Promise<never> =>
      Promise.reject(new Error("substrate unavailable"));
    const substrate: ReturnType<typeof createRepoStore> = {
      ...base,
      writeTree: (...args) =>
        failWrites ? failWrite() : base.writeTree(...args),
      writeTreePreservingPrefix: (...args) =>
        failWrites ? failWrite() : base.writeTreePreservingPrefix(...args),
      writeTreeDelta: (...args) =>
        failWrites ? failWrite() : base.writeTreeDelta(...args),
    };
    const real = createRunStorageCleanup({
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      topLevelRunId: TOP_LEVEL_RUN_ID,
    });
    const reclaimed: string[] = [];
    const reclaim = Promise.withResolvers<undefined>();
    const invokeStep: SidecarChildStepInvoker = async (req) => {
      const runId = req.authzContext.runId;
      if (runId === undefined) throw new Error("step invoked without a runId");
      const dir = path.join(stepStateRuns(dataDir), runId, "store");
      await fs.promises.mkdir(dir, { recursive: true });
      await fs.promises.writeFile(path.join(dir, "turn.json"), "{}");
      // The substrate goes away before the run records the park.
      failWrites = true;
      return {
        suspend: {
          correlationId: CORRELATION_ID,
          kind: "approval",
          approvalSnapshot: SNAPSHOT,
        },
      };
    };
    const deps = makeDeps(substrate, dataDir, invokeStep, async (runId) => {
      await real(runId);
      reclaimed.push(runId);
      reclaim.resolve(undefined);
    });
    const handle = await createSidecarSpawnSuspendableChild(deps)(
      spawnInput(agentBody(), new AbortController().signal),
      () => {
        /* no live events */
      },
    );

    // The handle surfaces the write failure, not a park or a terminal.
    await expect(handle.next(noWake)).rejects.toThrow("substrate unavailable");
    await Promise.race([
      reclaim.promise,
      new Promise<void>((resolve) => setTimeout(resolve, RECLAIM_WAIT_MS)),
    ]);

    const log = await reader(base).read(BODY_RUN_ID);
    expect(TERMINAL_EVENT_KINDS.has(log.at(-1)?.kind ?? "")).toBe(false);
    expect(reclaimed).toEqual([]);
    expect(await exists(path.join(stepStateRuns(dataDir), BODY_RUN_ID))).toBe(
      true,
    );
  });
});

describe("createRunStorageCleanup", () => {
  test("removes a spawned run's directory and keeps the top-level run's", async () => {
    const dataDir = await makeTempDir("run-storage-data-");
    const cleanup = createRunStorageCleanup({
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      topLevelRunId: TOP_LEVEL_RUN_ID,
    });
    for (const runId of [TOP_LEVEL_RUN_ID, BODY_RUN_ID]) {
      await fs.promises.mkdir(path.join(stepStateRuns(dataDir), runId), {
        recursive: true,
      });
    }

    await cleanup(TOP_LEVEL_RUN_ID);
    await cleanup(BODY_RUN_ID);

    expect(await fs.promises.readdir(stepStateRuns(dataDir))).toEqual([
      TOP_LEVEL_RUN_ID,
    ]);
  });

  test("resolves when the removal fails", async () => {
    const dataDir = await makeTempDir("run-storage-data-");
    await fs.promises.mkdir(path.dirname(stepStateRuns(dataDir)), {
      recursive: true,
    });
    await fs.promises.writeFile(stepStateRuns(dataDir), "not a directory");
    const cleanup = createRunStorageCleanup({
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      topLevelRunId: TOP_LEVEL_RUN_ID,
    });

    expect(await cleanup(BODY_RUN_ID)).toBeUndefined();
  });
});
