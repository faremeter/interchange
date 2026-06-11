import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";
import type { KeyPair } from "@intx/types/runtime";
import {
  createRepoStore,
  workflowKindHandler,
  WORKFLOW_JSON_PATH,
} from "@intx/hub-sessions";
import type { AuthorizeFn, Principal, RepoId } from "@intx/hub-sessions";
import type { WorkflowDefinition } from "@intx/workflow";

import { createWorkflowSpawnChild, type RunChildWorkflow } from "./spawn-child";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

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

const REF = "refs/heads/main";
const allowAll: AuthorizeFn = () => ({ allowed: true });

const HUB_PRINCIPAL: Principal = { kind: "hub" };
// The adapter never invokes the substrate's authorize gate (the
// resolution path goes through `getRepoDir`, documented as ungated),
// so the bare workflow-sidecar shape is sufficient. The principal
// is still held in closure so the adapter is symmetric with the
// sibling production adapters.
const SIDECAR_PRINCIPAL: Principal = { kind: "sidecar" };

/**
 * Minimum-valid workflow envelope: matches
 * `workflowDefinitionEnvelopeSchema` and the runtime body's expected
 * shape closely enough that downstream `runtimeRun` callers do not
 * need the primitive narrow exercised here.
 */
function validWorkflowEnvelope(id: string): WorkflowDefinition {
  // The envelope schema accepts the `step` primitive's structural
  // shape; the actual `runtimeRun` body is not exercised in these
  // tests (the `runChild` callback is a controllable stub).
  const definition = {
    id,
    triggers: [{ type: "manual" as const }],
    steps: {
      first: { kind: "step", id: "first" } as unknown,
    },
    stepOrder: ["first"],
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test fixture; the runtime body's primitive narrow is not exercised here, only the spawn-child adapter's envelope read
  return definition as unknown as WorkflowDefinition;
}

async function seedWorkflowAsset(
  substrate: ReturnType<typeof createRepoStore>,
  repoId: RepoId,
  body: string,
): Promise<void> {
  await substrate.writeTree(HUB_PRINCIPAL, repoId, REF, {
    files: { [WORKFLOW_JSON_PATH]: body },
    message: "seed workflow asset",
  });
}

describe("workflow-host SpawnChildWorkflow adapter - definitionRef resolution", () => {
  test("resolves the workflow.json envelope from the deploy ref and hands it to runChild", async () => {
    const dataDir = await makeTempDir("spawn-child-resolve-");
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-resolve" };
    const envelope = validWorkflowEnvelope("wf-resolve");
    await seedWorkflowAsset(substrate, repoId, JSON.stringify(envelope));

    const calls: Parameters<RunChildWorkflow>[0][] = [];
    const runChild: RunChildWorkflow = async (input) => {
      calls.push(input);
      return { terminalStatus: "completed" };
    };

    const spawn = createWorkflowSpawnChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runChild,
    });

    const ctrl = new AbortController();
    const result = await spawn({
      definitionRef: "wf-resolve",
      childRunId: "child-1",
      input: { goal: "resolve" },
      parentRunId: "parent-1",
      parentStepId: "step-a",
      signal: ctrl.signal,
    });

    expect(result.terminalStatus).toBe("completed");
    expect(calls).toHaveLength(1);
    const settled = calls[0];
    if (settled === undefined) throw new Error("expected one runChild call");
    expect(settled.definitionRef).toBe("wf-resolve");
    expect(settled.childRunId).toBe("child-1");
    expect(settled.parentRunId).toBe("parent-1");
    expect(settled.parentStepId).toBe("step-a");
    expect(settled.input).toEqual({ goal: "resolve" });
    expect(settled.definition.id).toBe("wf-resolve");
    expect(settled.definition.stepOrder).toEqual(["first"]);
  });

  test("rejects when the workflow asset has no workflow.json on the deploy ref", async () => {
    const dataDir = await makeTempDir("spawn-child-missing-");
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });

    const runChild: RunChildWorkflow = async () => ({
      terminalStatus: "completed",
    });
    const spawn = createWorkflowSpawnChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runChild,
    });

    const ctrl = new AbortController();
    await expect(
      spawn({
        definitionRef: "wf-missing",
        childRunId: "child-2",
        input: null,
        parentRunId: "parent-2",
        parentStepId: "step-a",
        signal: ctrl.signal,
      }),
    ).rejects.toThrow(/workflow.json not present/);
  });

  test("rejects when the workflow.json fails envelope validation", async () => {
    const dataDir = await makeTempDir("spawn-child-invalid-");
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-invalid" };
    // Bypass the workflow-kind handler's push-time validatePush by
    // overwriting the on-disk file after a valid seed. The substrate's
    // working tree carries loose files alongside the git index; a
    // post-commit overwrite lets the test exercise the adapter's
    // envelope-validation path without the push gate refusing the
    // seed in the first place.
    await seedWorkflowAsset(
      substrate,
      repoId,
      JSON.stringify(validWorkflowEnvelope("wf-invalid")),
    );
    const dir = substrate.getRepoDir(repoId);
    await fs.promises.writeFile(
      path.join(dir, WORKFLOW_JSON_PATH),
      JSON.stringify({ id: "" }),
      "utf8",
    );

    const runChild: RunChildWorkflow = async () => ({
      terminalStatus: "completed",
    });
    const spawn = createWorkflowSpawnChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runChild,
    });

    const ctrl = new AbortController();
    await expect(
      spawn({
        definitionRef: "wf-invalid",
        childRunId: "child-3",
        input: null,
        parentRunId: "parent-3",
        parentStepId: "step-a",
        signal: ctrl.signal,
      }),
    ).rejects.toThrow(/failed envelope validation/);
  });
});

describe("workflow-host SpawnChildWorkflow adapter - child execution", () => {
  test("returns the child's terminal status to the parent runtime", async () => {
    const dataDir = await makeTempDir("spawn-child-terminal-");
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-terminal" };
    await seedWorkflowAsset(
      substrate,
      repoId,
      JSON.stringify(validWorkflowEnvelope("wf-terminal")),
    );

    const runChild: RunChildWorkflow = async () => ({
      terminalStatus: "failed",
    });
    const spawn = createWorkflowSpawnChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runChild,
    });

    const ctrl = new AbortController();
    const result = await spawn({
      definitionRef: "wf-terminal",
      childRunId: "child-4",
      input: null,
      parentRunId: "parent-4",
      parentStepId: "step-a",
      signal: ctrl.signal,
    });
    expect(result.terminalStatus).toBe("failed");
  });

  test("propagates a thrown error from the runChild callback", async () => {
    const dataDir = await makeTempDir("spawn-child-throws-");
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-throws" };
    await seedWorkflowAsset(
      substrate,
      repoId,
      JSON.stringify(validWorkflowEnvelope("wf-throws")),
    );

    const cause = new Error("child runtime exploded");
    const runChild: RunChildWorkflow = async () => {
      throw cause;
    };
    const spawn = createWorkflowSpawnChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runChild,
    });

    const ctrl = new AbortController();
    await expect(
      spawn({
        definitionRef: "wf-throws",
        childRunId: "child-5",
        input: null,
        parentRunId: "parent-5",
        parentStepId: "step-a",
        signal: ctrl.signal,
      }),
    ).rejects.toBe(cause);
  });
});

describe("workflow-host SpawnChildWorkflow adapter - abort handling", () => {
  test("short-circuits a pre-aborted signal without invoking runChild", async () => {
    const dataDir = await makeTempDir("spawn-child-pre-abort-");
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-pre-abort" };
    await seedWorkflowAsset(
      substrate,
      repoId,
      JSON.stringify(validWorkflowEnvelope("wf-pre-abort")),
    );

    let runCalls = 0;
    const runChild: RunChildWorkflow = async () => {
      runCalls += 1;
      return { terminalStatus: "completed" };
    };
    const spawn = createWorkflowSpawnChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runChild,
    });

    const ctrl = new AbortController();
    ctrl.abort();
    await expect(
      spawn({
        definitionRef: "wf-pre-abort",
        childRunId: "child-6",
        input: null,
        parentRunId: "parent-6",
        parentStepId: "step-a",
        signal: ctrl.signal,
      }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(runCalls).toBe(0);
  });

  test("propagates the parent signal to the runChild callback", async () => {
    const dataDir = await makeTempDir("spawn-child-propagate-");
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-propagate" };
    await seedWorkflowAsset(
      substrate,
      repoId,
      JSON.stringify(validWorkflowEnvelope("wf-propagate")),
    );

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
    const spawn = createWorkflowSpawnChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runChild,
    });

    const ctrl = new AbortController();
    const settled = spawn({
      definitionRef: "wf-propagate",
      childRunId: "child-7",
      input: null,
      parentRunId: "parent-7",
      parentStepId: "step-a",
      signal: ctrl.signal,
    });
    // The adapter resolves the definition via an `fs.readFile` await
    // before invoking the callback; poll the observation slot so the
    // assertion fires once runChild has the signal in hand, then abort
    // and let the propagated signal reject the child's pending
    // promise.
    while (observedSignals.length === 0) {
      await new Promise<void>((r) => setTimeout(r, 1));
    }
    expect(observedSignals[0]).toBe(ctrl.signal);
    ctrl.abort();
    await expect(settled).rejects.toMatchObject({ name: "AbortError" });
  });
});

function abortDOMException(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException("aborted", "AbortError");
}
