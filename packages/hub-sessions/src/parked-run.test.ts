import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { loopBodyRunId, sectionBodyRunId } from "@intx/workflow";

import { createParkedRunClassifier } from "./parked-run";
import type { RepoStore } from "./repo-store/types";
import { createWorkflowRunReader } from "./workflow-run-reader";
import { workflowRunRepoIdForAddress } from "./workflow-run-kind";

const ANCHOR = "run-anchor";
const ADDRESS = "run_anchor@workers.example";
const AT = "2026-10-01T00:00:00.000Z";

// The reader only consults `getRepoDir`. Every other method rejects so a
// drift onto a write path fails loudly.
function repoStoreFor(root: string): RepoStore {
  const unused = () =>
    Promise.reject(new Error("parked-run test: method not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    writeTreeDelta: unused,
    receivePack: unused,
    createPack: unused,
    commitPackedTip: () => {
      throw new Error("parked-run test: method not wired");
    },
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: (repoId) => path.join(root, repoId.id),
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
    subscribe: () => {
      throw new Error("parked-run test: subscribe not wired");
    },
  };
}

async function commitFiles(
  dir: string,
  files: Record<string, string>,
): Promise<void> {
  for (const [rel, content] of Object.entries(files)) {
    const full = path.join(dir, rel);
    await fs.promises.mkdir(path.dirname(full), { recursive: true });
    await fs.promises.writeFile(full, content);
    await git.add({ fs, dir, filepath: rel });
  }
  await git.commit({
    fs,
    dir,
    message: "seed run events",
    author: { name: "test", email: "test@example.com" },
  });
}

// On-disk events carry `type` and `seq` and no `kind`. The classifier has
// to translate that envelope; feeding it a runtime event would hide a
// translator that never runs.
function eventFiles(
  runId: string,
  events: readonly Record<string, unknown>[],
): Record<string, string> {
  const files: Record<string, string> = {};
  for (let index = 0; index < events.length; index += 1) {
    const seq = index + 1;
    const event = events[index];
    if (event === undefined) continue;
    files[`runs/${runId}/events/${String(seq)}.json`] = JSON.stringify({
      seq,
      at: AT,
      ...event,
    });
  }
  return files;
}

function runStarted(runId: string): Record<string, unknown> {
  return {
    type: "RunStarted",
    runId,
    definitionHash: "hash",
    trigger: { type: "manual", payload: null },
  };
}

function stepStarted(stepId: string): Record<string, unknown> {
  return {
    type: "StepStarted",
    stepId,
    attempt: 1,
    input: { ref: "inline:null" },
  };
}

describe("createParkedRunClassifier", () => {
  let root: string;
  let dir: string;

  beforeEach(async () => {
    root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "parked-run-"));
    dir = path.join(root, workflowRunRepoIdForAddress(ADDRESS).id);
    await git.init({ fs, dir, defaultBranch: "main" });
  });

  afterEach(async () => {
    await fs.promises.rm(root, { recursive: true, force: true });
  });

  function classify(projection: unknown, address: string | null = ADDRESS) {
    return createParkedRunClassifier({
      runReader: createWorkflowRunReader(repoStoreFor(root)),
      addressForAnchor: async () => address,
      projectionForAnchor: async () => projection,
    });
  }

  test("a committed SignalAwaited on an awaitSignal step is parked", async () => {
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
      ]),
    });

    const verdict = await classify({
      steps: { hold: { kind: "awaitSignal" } },
    })({ anchorRunId: ANCHOR });

    expect(verdict).toEqual({
      verdict: "parked",
      reason: "resume guard would re-attach a park",
    });
  });

  test("a synthetic map park is not parked even beside a real signal park", async () => {
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("items[0]"),
        { type: "SignalAwaited", stepId: "items[0]", signalName: "go" },
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "later" },
      ]),
    });

    const verdict = await classify({
      steps: {
        items: { kind: "map" },
        hold: { kind: "awaitSignal" },
      },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("not-parked");
    expect(verdict.reason).toContain("items[0]");
  });

  test("a crashed agent step beside a committed signal park stays parked", async () => {
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("think"),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
      ]),
    });

    const verdict = await classify({
      steps: {
        think: { kind: "step" },
        hold: { kind: "awaitSignal" },
      },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("parked");
  });

  test("sleep with no signal park is not parked", async () => {
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("nap"),
        {
          type: "TimerSet",
          timerId: "timer-1",
          fireAt: "2026-10-01T00:00:30.000Z",
          stepId: "nap",
        },
      ]),
    });

    const verdict = await classify({
      steps: { nap: { kind: "sleep" } },
    })({ anchorRunId: ANCHOR });

    expect(verdict).toEqual({
      verdict: "not-parked",
      reason: "resume guard found no re-attachable park",
    });
  });

  test("a received signal that already moved the gate in-flight is not parked", async () => {
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
        {
          type: "SignalReceived",
          signalName: "go",
          signalId: "sig-1",
          payload: { done: true },
        },
      ]),
    });

    const verdict = await classify({
      steps: { hold: { kind: "awaitSignal" } },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("not-parked");
  });

  test("a loop proxy park on the committed ref is parked", async () => {
    const childRunId = loopBodyRunId(ANCHOR, "again", 0);
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("again"),
        {
          type: "ChildSpawned",
          stepId: "again",
          childRunId,
          childDefinitionRef: "body",
        },
        {
          type: "SignalAwaited",
          stepId: "again",
          signalName: "go",
          parkKind: "signal-relay",
        },
      ]),
    });

    const verdict = await classify({
      steps: {
        again: {
          kind: "loop",
          maxIterations: 3,
          body: { steps: { hold: { kind: "awaitSignal" } } },
        },
      },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("parked");
  });

  test("a tolerated iteration followed by one unrelayed author signal is parked", async () => {
    const failedChild = loopBodyRunId(ANCHOR, "again", 0);
    const parkedChild = loopBodyRunId(ANCHOR, "again", 1);
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("again"),
        {
          type: "ChildSpawned",
          stepId: "again",
          childRunId: failedChild,
          childDefinitionRef: "body",
        },
        {
          type: "ChildCompleted",
          childRunId: failedChild,
          terminalStatus: "failed",
        },
        {
          type: "ChildSpawned",
          stepId: "again",
          childRunId: parkedChild,
          childDefinitionRef: "body",
        },
      ]),
      ...eventFiles(parkedChild, [
        runStarted(parkedChild),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
      ]),
    });

    const verdict = await classify({
      steps: {
        again: {
          kind: "loop",
          maxIterations: 3,
          onIterationFailure: "tolerate",
          body: { steps: { hold: { kind: "awaitSignal" } } },
        },
      },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("parked");
  });

  test("two drive-fresh loops are parked together", async () => {
    const left = loopBodyRunId(ANCHOR, "left", 0);
    const right = loopBodyRunId(ANCHOR, "right", 0);
    const child = (runId: string) =>
      eventFiles(runId, [
        runStarted(runId),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
      ]);
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("left"),
        {
          type: "ChildSpawned",
          stepId: "left",
          childRunId: left,
          childDefinitionRef: "body",
        },
        stepStarted("right"),
        {
          type: "ChildSpawned",
          stepId: "right",
          childRunId: right,
          childDefinitionRef: "body",
        },
      ]),
      ...child(left),
      ...child(right),
    });
    const loop = {
      kind: "loop",
      maxIterations: 2,
      body: { steps: { hold: { kind: "awaitSignal" } } },
    };

    const verdict = await classify({
      steps: { left: loop, right: loop },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("parked");
  });

  test("an unrelayed onTrigger body is not parked", async () => {
    const childRunId = sectionBodyRunId(ANCHOR, "inbox", 0);
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("inbox"),
        {
          type: "ChildSpawned",
          stepId: "inbox",
          childRunId,
          childDefinitionRef: "body",
        },
      ]),
      ...eventFiles(childRunId, [
        runStarted(childRunId),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
      ]),
    });

    const verdict = await classify({
      steps: {
        inbox: {
          kind: "onTrigger",
          body: { inline: { steps: { hold: { kind: "awaitSignal" } } } },
        },
      },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("not-parked");
  });

  test("a legacy onTrigger body id with an unsupported inner step is not parked", async () => {
    const childRunId = "inbox__0";
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("inbox"),
        {
          type: "ChildSpawned",
          stepId: "inbox",
          childRunId,
          childDefinitionRef: "body",
        },
        {
          type: "SignalAwaited",
          stepId: "inbox",
          signalName: "go",
          parkKind: "signal-relay",
        },
      ]),
      ...eventFiles(childRunId, [
        runStarted(childRunId),
        stepStarted("items[0]"),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
      ]),
    });

    const verdict = await classify({
      steps: {
        inbox: {
          kind: "onTrigger",
          body: {
            inline: {
              steps: {
                items: { kind: "map" },
                hold: { kind: "awaitSignal" },
              },
            },
          },
        },
      },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("not-parked");
    expect(verdict.reason).toContain("items[0]");
  });

  test("a legacy onTrigger body id parked on its author signal is parked", async () => {
    const childRunId = "inbox__0";
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [
        runStarted(ANCHOR),
        stepStarted("inbox"),
        {
          type: "ChildSpawned",
          stepId: "inbox",
          childRunId,
          childDefinitionRef: "body",
        },
        {
          type: "SignalAwaited",
          stepId: "inbox",
          signalName: "go",
          parkKind: "signal-relay",
        },
      ]),
      ...eventFiles(childRunId, [
        runStarted(childRunId),
        stepStarted("hold"),
        { type: "SignalAwaited", stepId: "hold", signalName: "go" },
      ]),
    });

    const verdict = await classify({
      steps: {
        inbox: {
          kind: "onTrigger",
          body: { inline: { steps: { hold: { kind: "awaitSignal" } } } },
        },
      },
    })({ anchorRunId: ANCHOR });

    expect(verdict).toEqual({
      verdict: "parked",
      reason: "resume guard would re-attach a park",
    });
  });

  test("an empty committed log is not parked", async () => {
    await commitFiles(dir, { README: "present ref, no run\n" });

    const verdict = await classify({
      steps: { hold: { kind: "awaitSignal" } },
    })({ anchorRunId: ANCHOR });

    expect(verdict).toEqual({
      verdict: "not-parked",
      reason: "committed log is empty",
    });
  });

  test("a missing repository is unknown", async () => {
    const verdict = await classify(
      { steps: { hold: { kind: "awaitSignal" } } },
      "other@workers.example",
    )({ anchorRunId: ANCHOR });

    expect(verdict).toEqual({
      verdict: "unknown",
      reason: "workflow run repository is missing",
    });
  });

  test("a missing anchor address is unknown", async () => {
    const verdict = await classify(
      { steps: { hold: { kind: "awaitSignal" } } },
      null,
    )({ anchorRunId: ANCHOR });

    expect(verdict).toEqual({
      verdict: "unknown",
      reason: "anchor run has no address",
    });
  });

  test("a reader failure is unknown", async () => {
    const reader = createWorkflowRunReader(repoStoreFor(root));
    const classifyThrown = createParkedRunClassifier({
      runReader: {
        ...reader,
        readRunEvents: () => Promise.reject(new Error("disk unreadable")),
      },
      addressForAnchor: async () => ADDRESS,
      projectionForAnchor: async () => ({
        steps: { hold: { kind: "awaitSignal" } },
      }),
    });

    const verdict = await classifyThrown({ anchorRunId: ANCHOR });

    expect(verdict).toEqual({
      verdict: "unknown",
      reason: "disk unreadable",
    });
  });

  test("a log the state machine rejects is unknown", async () => {
    await commitFiles(dir, {
      ...eventFiles(ANCHOR, [stepStarted("hold")]),
    });

    const verdict = await classify({
      steps: { hold: { kind: "awaitSignal" } },
    })({ anchorRunId: ANCHOR });

    expect(verdict.verdict).toBe("unknown");
    expect(verdict.reason).toContain("StepStarted");
  });
});
