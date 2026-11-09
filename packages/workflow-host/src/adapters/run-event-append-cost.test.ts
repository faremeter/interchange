// The cost of one event append as a run's log grows. An append reads the
// parent's event names, not their bytes, and writes only the new event,
// so the git objects it reads and writes do not grow with the run. A
// signal delivery also reads events to drop a repeated signal, but each
// event only once across deliveries.

import { describe, test, expect, afterAll, beforeAll, spyOn } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type {
  RepoId,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";

import { emptyState } from "@intx/workflow";

import { createWorkflowRunRepoStore } from "./repo-store";
import { createWorkflowHostSignalChannel } from "../seams/signal-channel";

const REF = "refs/heads/main";
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: "cost-deployment",
};
const RUN_ID = "run-1";

// Two run lengths; a cost that grows with the log differs between them.
const SHORT = 100;
const LONG = 300;

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

type AppendCost = { readBlob: number; writeBlob: number };

async function runOfLength(length: number) {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "run-event-append-cost-"),
  );
  tempDirs.push(dataDir);
  const repoId: RepoId = { kind: "workflow-run", id: "cost-deployment" };
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: () => ({ allowed: true }),
  });
  await substrate.writeTree({ kind: "hub" }, repoId, REF, {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "genesis",
  });
  const adapter = createWorkflowRunRepoStore({
    substrate,
    repoId,
    principal: PRINCIPAL,
    ref: REF,
  });
  const costs = new Map<number, AppendCost>();
  await adapter.append(RUN_ID, {
    kind: "RunStarted",
    seq: 1,
    at: AT,
    runId: RUN_ID,
    definitionHash: "definition-hash",
    trigger: { type: "manual", payload: {} },
  });
  for (let seq = 2; seq <= length; seq++) {
    costs.set(
      seq,
      await countGitObjects(() =>
        adapter.append(RUN_ID, {
          kind: "StepStarted",
          seq,
          at: AT,
          stepId: `step-${String(seq)}`,
          attempt: 1,
          input: { ref: "inline:{}" },
        }),
      ),
    );
  }
  return { substrate, repoId, adapter, costs };
}

async function countGitObjects(
  write: () => Promise<void>,
): Promise<AppendCost> {
  const readBlob = spyOn(git, "readBlob");
  const writeBlob = spyOn(git, "writeBlob");
  try {
    await write();
    return {
      readBlob: readBlob.mock.calls.length,
      writeBlob: writeBlob.mock.calls.length,
    };
  } finally {
    readBlob.mockRestore();
    writeBlob.mockRestore();
  }
}

const AT = new Date(0).toISOString();

describe("workflow-run event append cost", () => {
  test("an append reads and writes the same git objects at any run length", async () => {
    const { adapter, costs } = await runOfLength(LONG);
    // The append writes its one event. Validation reads that event and
    // the parent's last event, whose type the terminal lock checks.
    const expected: AppendCost = { readBlob: 2, writeBlob: 1 };
    expect(costs.get(SHORT)).toEqual(expected);
    expect(costs.get(LONG)).toEqual(expected);
    expect(await adapter.read(RUN_ID)).toHaveLength(LONG);
  }, 120_000);

  test("a signal delivery reads each prior event once across deliveries", async () => {
    const { substrate, repoId, adapter } = await runOfLength(SHORT);
    let nextId = 0;
    const channel = createWorkflowHostSignalChannel({
      repoStore: substrate,
      runtimeStore: adapter,
      principal: PRINCIPAL,
      repoId,
      ref: REF,
      runId: RUN_ID,
      readState: () => emptyState(RUN_ID),
      newId: () => `sig-${String((nextId += 1))}`,
      clock: () => new Date(0),
    });
    try {
      // The first delivery reads every event once to look for a repeat.
      const first = await countGitObjects(() => channel.deliver("approve", {}));
      expect(first.readBlob).toBeGreaterThanOrEqual(SHORT);
      // A later delivery reads only the event the first one added, plus
      // the two the validation reads.
      const second = await countGitObjects(() =>
        channel.deliver("approve", {}),
      );
      expect(second).toEqual({ readBlob: 3, writeBlob: 1 });
      // A repeated signal id appends nothing.
      await channel.deliver("approve", {}, "sig-1");
      expect(await adapter.read(RUN_ID)).toHaveLength(SHORT + 2);

      // Once the run has ended, a delivery reads only the last event to
      // see that, then commits nothing new, so validation reads that
      // event again and writes no blob.
      await adapter.append(RUN_ID, {
        kind: "RunCompleted",
        seq: SHORT + 3,
        at: AT,
      });
      const ended = await countGitObjects(() => channel.deliver("approve", {}));
      expect(ended).toEqual({ readBlob: 2, writeBlob: 0 });
      expect(await adapter.read(RUN_ID)).toHaveLength(SHORT + 3);
    } finally {
      await channel.stop();
    }
  }, 120_000);
});
