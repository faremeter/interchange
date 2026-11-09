// The cost of one event append as a run's log grows. An append reads the
// parent's event names, not their bytes, and writes only the new event,
// so the git objects it reads and writes do not grow with the run.

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

import { createWorkflowRunRepoStore } from "./repo-store";

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

describe("workflow-run event append cost", () => {
  test("an append reads and writes the same git objects at any run length", async () => {
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

    const at = new Date(0).toISOString();
    await adapter.append(RUN_ID, {
      kind: "RunStarted",
      seq: 1,
      at,
      runId: RUN_ID,
      definitionHash: "definition-hash",
      trigger: { type: "manual", payload: {} },
    });
    const costs = new Map<number, AppendCost>();
    const readBlob = spyOn(git, "readBlob");
    const writeBlob = spyOn(git, "writeBlob");
    try {
      for (let seq = 2; seq <= LONG; seq++) {
        readBlob.mockClear();
        writeBlob.mockClear();
        await adapter.append(RUN_ID, {
          kind: "StepStarted",
          seq,
          at,
          stepId: `step-${String(seq)}`,
          attempt: 1,
          input: { ref: "inline:{}" },
        });
        if (seq === SHORT || seq === LONG) {
          costs.set(seq, {
            readBlob: readBlob.mock.calls.length,
            writeBlob: writeBlob.mock.calls.length,
          });
        }
      }
    } finally {
      readBlob.mockRestore();
      writeBlob.mockRestore();
    }

    // The append writes its one event. Validation reads that event and
    // the parent's last event, whose type the terminal lock checks.
    const expected: AppendCost = { readBlob: 2, writeBlob: 1 };
    expect(costs.get(SHORT)).toEqual(expected);
    expect(costs.get(LONG)).toEqual(expected);
    expect(await adapter.read(RUN_ID)).toHaveLength(LONG);
  }, 120_000);
});
