import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type { AuthorizeFn, Principal, RepoId } from "@intx/hub-sessions";
import type {
  SpawnSuspendableChild,
  SuspendableChildHandle,
  WorkflowDefinition,
  WorkflowEvent,
} from "@intx/workflow";

import { createWorkflowRunRepoStore } from "./repo-store";
import {
  createInMemorySpawnChild,
  createInMemorySpawnSuspendableChild,
  type RunChildWorkflow,
} from "./spawn-child";

const tempDirs: string[] = [];

// These tests exercise the adapter's resolution/abort/forwarding, not event
// emission, so the terminal `HostSpawnChild` sink is a no-op.
const noopOnEvent = (): void => {
  /* the event sink is not asserted in these tests */
};

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

// This adapter is the TERMINAL childWorkflow resolver: an owned inline child is
// lifted to an internal ref at child boot and resolved from the parent's
// in-memory closure map with no on-disk read and no separate per-child
// re-verify (the parent's re-verify already covers the inline child). It drives
// the child terminal-only; the onTrigger-body re-verify gate lives on the
// suspendable adapter and is covered in `reverify.test.ts`.

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

const REF = "refs/heads/main";
const allowAll: AuthorizeFn = () => ({ allowed: true });

/**
 * Minimum-valid child definition: the terminal resolver copies the definition
 * by reference and hands it to the `runChild` stub, so the runtime body's
 * primitive narrow is not exercised here.
 */
function childDefinition(id: string): WorkflowDefinition {
  const definition = {
    id,
    triggers: [{ type: "manual" as const }],
    steps: {
      first: { kind: "step", id: "first" } as unknown,
    },
    stepOrder: ["first"],
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test fixture; only the spawn-child adapter's map resolution is exercised, not the runtime body's primitive narrow
  return definition as unknown as WorkflowDefinition;
}

describe("workflow-host SpawnChildWorkflow adapter - in-memory resolution", () => {
  test("resolves the child from the in-memory map and hands it to runChild", async () => {
    const definition = childDefinition("wf-resolve");
    const calls: Parameters<RunChildWorkflow>[0][] = [];
    const runChild: RunChildWorkflow = async (input) => {
      calls.push(input);
      return { terminalStatus: "completed" };
    };

    const spawn = createInMemorySpawnChild({
      bodies: new Map([["wf-resolve", definition]]),
      runChild,
    });

    const ctrl = new AbortController();
    const result = await spawn(
      {
        definitionRef: "wf-resolve",
        childRunId: "child-1",
        input: { goal: "resolve" },
        parentRunId: "parent-1",
        parentStepId: "step-a",
        signal: ctrl.signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      },
      noopOnEvent,
    );

    expect(result.terminalStatus).toBe("completed");
    expect(calls).toHaveLength(1);
    const settled = calls[0];
    if (settled === undefined) throw new Error("expected one runChild call");
    expect(settled.definitionRef).toBe("wf-resolve");
    expect(settled.childRunId).toBe("child-1");
    expect(settled.parentRunId).toBe("parent-1");
    expect(settled.parentStepId).toBe("step-a");
    expect(settled.input).toEqual({ goal: "resolve" });
    expect(settled.definition).toBe(definition);
  });

  test("fails loud when the ref is not in the map", async () => {
    const runChild: RunChildWorkflow = async () => ({
      terminalStatus: "completed",
    });
    const spawn = createInMemorySpawnChild({ bodies: new Map(), runChild });

    const ctrl = new AbortController();
    await expect(
      spawn(
        {
          definitionRef: "wf-missing",
          childRunId: "child-2",
          input: null,
          parentRunId: "parent-2",
          parentStepId: "step-a",
          signal: ctrl.signal,
          depth: 1,
          maxChildSpawnDepth: 32,
        },
        noopOnEvent,
      ),
    ).rejects.toThrow(/no in-memory childWorkflow definition/);
  });
});

describe("workflow-host SpawnChildWorkflow adapter - child execution", () => {
  test("returns the child's terminal status to the parent runtime", async () => {
    const runChild: RunChildWorkflow = async () => ({
      terminalStatus: "failed",
    });
    const spawn = createInMemorySpawnChild({
      bodies: new Map([["wf-terminal", childDefinition("wf-terminal")]]),
      runChild,
    });

    const ctrl = new AbortController();
    const result = await spawn(
      {
        definitionRef: "wf-terminal",
        childRunId: "child-4",
        input: null,
        parentRunId: "parent-4",
        parentStepId: "step-a",
        signal: ctrl.signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      },
      noopOnEvent,
    );
    expect(result.terminalStatus).toBe("failed");
  });

  test("propagates a thrown error from the runChild callback", async () => {
    const cause = new Error("child runtime exploded");
    const runChild: RunChildWorkflow = async () => {
      throw cause;
    };
    const spawn = createInMemorySpawnChild({
      bodies: new Map([["wf-throws", childDefinition("wf-throws")]]),
      runChild,
    });

    const ctrl = new AbortController();
    await expect(
      spawn(
        {
          definitionRef: "wf-throws",
          childRunId: "child-5",
          input: null,
          parentRunId: "parent-5",
          parentStepId: "step-a",
          signal: ctrl.signal,
          depth: 1,
          maxChildSpawnDepth: 32,
        },
        noopOnEvent,
      ),
    ).rejects.toBe(cause);
  });
});

describe("workflow-host SpawnChildWorkflow adapter - abort handling", () => {
  test("short-circuits a pre-aborted signal without invoking runChild", async () => {
    let runCalls = 0;
    const runChild: RunChildWorkflow = async () => {
      runCalls += 1;
      return { terminalStatus: "completed" };
    };
    const spawn = createInMemorySpawnChild({
      bodies: new Map([["wf-pre-abort", childDefinition("wf-pre-abort")]]),
      runChild,
    });

    const ctrl = new AbortController();
    ctrl.abort();
    await expect(
      spawn(
        {
          definitionRef: "wf-pre-abort",
          childRunId: "child-6",
          input: null,
          parentRunId: "parent-6",
          parentStepId: "step-a",
          signal: ctrl.signal,
          depth: 1,
          maxChildSpawnDepth: 32,
        },
        noopOnEvent,
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(runCalls).toBe(0);
  });

  test("propagates the parent signal to the runChild callback", async () => {
    const observedSignals: AbortSignal[] = [];
    const runChild: RunChildWorkflow = async (input) => {
      observedSignals.push(input.signal);
      return new Promise((_resolve, reject) => {
        if (input.signal.aborted) {
          reject(abortDOMException(input.signal));
          return;
        }
        input.signal.addEventListener(
          "abort",
          () => {
            reject(abortDOMException(input.signal));
          },
          { once: true },
        );
      });
    };
    const spawn = createInMemorySpawnChild({
      bodies: new Map([["wf-propagate", childDefinition("wf-propagate")]]),
      runChild,
    });

    const ctrl = new AbortController();
    const settled = spawn(
      {
        definitionRef: "wf-propagate",
        childRunId: "child-7",
        input: null,
        parentRunId: "parent-7",
        parentStepId: "step-a",
        signal: ctrl.signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      },
      noopOnEvent,
    );
    while (observedSignals.length === 0) {
      await new Promise<void>((r) => setTimeout(r, 1));
    }
    expect(observedSignals[0]).toBe(ctrl.signal);
    ctrl.abort();
    await expect(settled).rejects.toMatchObject({ name: "AbortError" });
  });
});

describe("workflow-host SpawnChildWorkflow adapter - sub-namespace scoping", () => {
  test("a runChild that writes through the workflow-run RepoStore against the child's runId lands events under runs/<childRunId>/events/", async () => {
    const runDataDir = await makeTempDir("spawn-child-scope-run-");
    const runSubstrate = createRepoStore({
      dataDir: runDataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: allowAll,
    });
    const runRepoId: RepoId = { kind: "workflow-run", id: "dep-scope" };
    // workflow-run kind handler accepts the gitignore-only genesis;
    // seed it so the first append has a coherent prior tree to extend.
    await runSubstrate.writeTree({ kind: "hub" }, runRepoId, REF, {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    });

    const workflowRunPrincipalShape = {
      kind: "workflow-process",
      anchorRunId: "test-deployment",
    };
    const workflowRunPrincipal: Principal = workflowRunPrincipalShape;
    // First, seed the parent's RunStarted under runs/<parentRunId>/.
    const parentAdapter = createWorkflowRunRepoStore({
      substrate: runSubstrate,
      repoId: runRepoId,
      principal: workflowRunPrincipal,
      ref: REF,
    });
    const parentRunStarted: WorkflowEvent = {
      kind: "RunStarted",
      seq: 1,
      at: new Date(0).toISOString(),
      runId: "parent-scope",
      definitionHash: "parent-hash",
      trigger: { type: "manual", payload: {} },
    };
    await parentAdapter.append("parent-scope", parentRunStarted);

    // Now wire the spawn-child adapter. The runChild callback uses the
    // same workflow-run substrate but appends through a sibling
    // adapter scoped to the child's runId at the call boundary.
    const childAdapter = createWorkflowRunRepoStore({
      substrate: runSubstrate,
      repoId: runRepoId,
      principal: workflowRunPrincipal,
      ref: REF,
    });
    const runChild: RunChildWorkflow = async ({ childRunId }) => {
      const event: WorkflowEvent = {
        kind: "RunStarted",
        seq: 1,
        at: new Date(0).toISOString(),
        runId: childRunId,
        definitionHash: "child-hash",
        trigger: { type: "manual", payload: {} },
      };
      await childAdapter.append(childRunId, event);
      return { terminalStatus: "completed" };
    };

    const spawn = createInMemorySpawnChild({
      bodies: new Map([["wf-scope", childDefinition("wf-scope")]]),
      runChild,
    });

    const ctrl = new AbortController();
    const result = await spawn(
      {
        definitionRef: "wf-scope",
        childRunId: "child-scope",
        input: { goal: "scope" },
        parentRunId: "parent-scope",
        parentStepId: "step-a",
        signal: ctrl.signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      },
      noopOnEvent,
    );
    expect(result.terminalStatus).toBe("completed");

    // Inspect the workflow-run repo on disk: both runs should have
    // their own events subtree. The child's events MUST live under
    // runs/<childRunId>/events/, sibling to the parent's tree, not
    // commingled.
    const runDir = runSubstrate.getRepoDir(runRepoId);
    const childEventsDir = path.join(runDir, "runs", "child-scope", "events");
    const parentEventsDir = path.join(runDir, "runs", "parent-scope", "events");
    const childEntries = await fs.promises.readdir(childEventsDir);
    expect(childEntries).toContain("1.json");
    const parentEntries = await fs.promises.readdir(parentEventsDir);
    expect(parentEntries).toContain("1.json");
    // The two trees must not point at the same file: a structural
    // accident in which the child's path computation collapses onto
    // the parent's would corrupt the parent's log on the next append.
    expect(childEventsDir).not.toBe(parentEventsDir);

    // The child's events read back through the runtime-shape adapter
    // when keyed on the child's runId. The parent's read does not see
    // the child's RunStarted, and vice versa.
    const childEvents = await childAdapter.read("child-scope");
    expect(childEvents).toHaveLength(1);
    expect(childEvents[0]?.kind).toBe("RunStarted");
    const childFirst = childEvents[0];
    if (childFirst === undefined || childFirst.kind !== "RunStarted") {
      throw new Error("expected RunStarted at seq 1");
    }
    expect(childFirst.runId).toBe("child-scope");

    const parentEvents = await parentAdapter.read("parent-scope");
    expect(parentEvents).toHaveLength(1);
    const parentFirst = parentEvents[0];
    if (parentFirst === undefined || parentFirst.kind !== "RunStarted") {
      throw new Error("expected RunStarted at seq 1");
    }
    expect(parentFirst.runId).toBe("parent-scope");
  });
});

function abortDOMException(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException("aborted", "AbortError");
}

// The source-ref in-memory suspendable-child resolver: the parent already
// re-evaluated + re-verified the whole closure, so onTrigger bodies resolve
// from the in-hand map with no disk read and no separate per-body re-verify.
describe("createInMemorySpawnSuspendableChild - in-memory body resolution", () => {
  /* eslint-disable @typescript-eslint/no-unsafe-type-assertion -- test stubs: the resolver copies the definition by reference and identity-compares the handle; neither is structurally inspected */
  const bodyDef = {
    id: "wf__sect",
    triggers: [],
    stepOrder: [],
    steps: {},
  } as unknown as WorkflowDefinition;
  const fakeHandle = {} as unknown as SuspendableChildHandle;
  /* eslint-enable @typescript-eslint/no-unsafe-type-assertion */

  // Body inference events are not asserted in these unit tests.
  const noopOnEvent = () => {
    /* no-op event sink */
  };

  function makeInput(
    ref: string,
    signal: AbortSignal,
  ): Parameters<SpawnSuspendableChild>[0] {
    return {
      definitionRef: ref,
      childRunId: "child-run",
      input: undefined,
      parentRunId: "parent-run",
      parentStepId: "sect",
      signal,
      depth: 0,
      maxChildSpawnDepth: 32,
    };
  }

  test("resolves the body from the in-memory map and delegates to the executor", async () => {
    let seen: WorkflowDefinition | undefined;
    const spawn = createInMemorySpawnSuspendableChild({
      bodies: new Map([["wf__sect", bodyDef]]),
      runSuspendableChild: async (args) => {
        seen = args.definition;
        return fakeHandle;
      },
    });

    const handle = await spawn(
      makeInput("wf__sect", new AbortController().signal),
      noopOnEvent,
    );

    expect(seen).toBe(bodyDef);
    expect(handle).toBe(fakeHandle);
  });

  test("fails loud when the ref is not in the map", async () => {
    const spawn = createInMemorySpawnSuspendableChild({
      bodies: new Map(),
      runSuspendableChild: async () => fakeHandle,
    });

    await expect(
      spawn(makeInput("missing", new AbortController().signal), noopOnEvent),
    ).rejects.toThrow(/no in-memory onTrigger body/);
  });

  test("short-circuits a pre-aborted signal without resolving or executing", async () => {
    let executorCalled = false;
    const spawn = createInMemorySpawnSuspendableChild({
      bodies: new Map([["wf__sect", bodyDef]]),
      runSuspendableChild: async () => {
        executorCalled = true;
        return fakeHandle;
      },
    });
    const ctrl = new AbortController();
    ctrl.abort();

    await expect(
      spawn(makeInput("wf__sect", ctrl.signal), noopOnEvent),
    ).rejects.toThrow();
    expect(executorCalled).toBe(false);
  });
});
