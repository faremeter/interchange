import fs from "node:fs";
import path from "node:path";

import git from "isomorphic-git";

import { WORKFLOW_RUN_RUNS_PREFIX } from "@intx/hub-sessions";
import { collectReachableObjects } from "@intx/storage-isogit/node";

/** The events one commit adds for one run. */
export type WorkflowRunPackRun = {
  runId: string;
  terminalType?: string;
  terminalAt?: string;
  signalId?: string;
  consumedMessageId?: string;
  // When true, omit the seq-0 RunStarted and emit the terminal event
  // alone at seq 1 -- the exact artifact the crash-loop guard's
  // supervisor-authored RunFailed produces on an anchor run whose event
  // log is otherwise empty.
  terminalOnlyAtSeq1?: boolean;
};

export type WorkflowRunLine = {
  genesis: string;
  /** One commit per step, in order. */
  commits: string[];
  /** A pack of every commit from the genesis up to `commitSha`. */
  packAt: (commitSha: string) => Promise<Uint8Array>;
};

function eventBody(seq: number, type: string, at?: string): string {
  return JSON.stringify({ seq, type, ...(at !== undefined ? { at } : {}) });
}

function runFiles(runs: readonly WorkflowRunPackRun[]): Record<string, string> {
  const files: Record<string, string> = {};
  for (const {
    runId,
    terminalType,
    terminalAt,
    signalId,
    consumedMessageId,
    terminalOnlyAtSeq1,
  } of runs) {
    if (terminalOnlyAtSeq1 === true) {
      if (terminalType === undefined) {
        throw new Error("terminalOnlyAtSeq1 requires a terminalType");
      }
      if (signalId !== undefined) {
        throw new Error(
          "terminalOnlyAtSeq1 cannot be combined with a signalId",
        );
      }
      files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/1.json`] = eventBody(
        1,
        terminalType,
        terminalAt,
      );
      continue;
    }
    files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/0.json`] =
      JSON.stringify({
        seq: 0,
        type: "RunStarted",
        ...(consumedMessageId === undefined ? {} : { consumedMessageId }),
      });
    let seq = 1;
    if (signalId !== undefined) {
      files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/${String(seq)}.json`] =
        JSON.stringify({ seq, type: "SignalReceived", signalId });
      seq += 1;
    }
    if (terminalType !== undefined) {
      files[`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/${String(seq)}.json`] =
        eventBody(seq, terminalType, terminalAt);
    }
  }
  return files;
}

/**
 * Build one line of workflow-run history in the empty directory `srcDir`.
 * The genesis commit carries a `.gitignore`-only tree (the kind handler's
 * accepted initial commit). Each step is one more commit that adds the
 * event logs of its runs, so the line grows the way a sidecar's history
 * does.
 */
export async function buildWorkflowRunLine(
  srcDir: string,
  ref: string,
  steps: readonly (readonly WorkflowRunPackRun[])[],
): Promise<WorkflowRunLine> {
  await git.init({ fs, dir: srcDir, defaultBranch: "main" });
  const author = { name: "probe", email: "probe@example.com" };
  await fs.promises.writeFile(path.join(srcDir, ".gitignore"), "");
  await git.add({ fs, dir: srcDir, filepath: ".gitignore" });
  const genesis = await git.commit({
    fs,
    dir: srcDir,
    message: "genesis",
    author,
    ref,
  });

  const commits: string[] = [];
  let parent = genesis;
  for (const [index, runs] of steps.entries()) {
    for (const [rel, body] of Object.entries(runFiles(runs))) {
      const full = path.join(srcDir, rel);
      await fs.promises.mkdir(path.dirname(full), { recursive: true });
      await fs.promises.writeFile(full, body);
      await git.add({ fs, dir: srcDir, filepath: rel });
    }
    parent = await git.commit({
      fs,
      dir: srcDir,
      message: `step ${String(index)}`,
      author,
      parent: [parent],
      ref,
    });
    commits.push(parent);
  }

  return {
    genesis,
    commits,
    packAt: async (commitSha) => {
      const chain = (await git.log({ fs, dir: srcDir, ref: commitSha })).map(
        (entry) => entry.oid,
      );
      const oids = Array.from(
        new Set(
          (
            await Promise.all(
              chain.map((sha) => collectReachableObjects(srcDir, sha)),
            )
          ).flat(),
        ),
      );
      const packResult = await git.packObjects({
        fs,
        dir: srcDir,
        oids,
        write: false,
      });
      if (packResult.packfile === undefined) {
        throw new Error("git.packObjects returned no packfile");
      }
      return packResult.packfile;
    },
  };
}
