import { describe, test, expect, beforeAll, afterAll } from "bun:test";
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
import type {
  AuthorizeFn,
  RepoId,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";

import { compactRunEvents } from "./run-event-compaction";
import { recoverInterruptedCompactions } from "./run-event-recovery";

// The sweep's harness is a deliberate local mirror of the helpers in
// run-event-compaction.test.ts. The sweep operates over MULTIPLE runs, so it
// seeds per-run event trees by id, where that file's `setup`/`ev` assume the
// single fixed `run-1`.

const REF = "refs/heads/main";
const allowAll: AuthorizeFn = () => ({ allowed: true });

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

function ev(runId: string, seq: number, type: string): string {
  return JSON.stringify({ seq, type, runId });
}

async function setup(anchorRunId: string) {
  const dataDir = await makeTempDir("recover-");
  const repoId: RepoId = { kind: "workflow-run", id: anchorRunId };
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: allowAll,
  });
  await substrate.writeTree({ kind: "hub" }, repoId, REF, {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "genesis",
  });
  const supervisor: WorkflowRunSupervisorPrincipal = {
    kind: "supervisor",
    anchorRunId,
  };
  const runDir = (runId: string) =>
    path.join(substrate.getRepoDir(repoId), "runs", runId);
  // Seed a run's per-event `events/<seq>.json` blobs: the on-disk shape a
  // crash leaves behind when the fold never runs.
  const seedPerEvent = (runId: string, events: [number, string][]) =>
    substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: `runs/${runId}/events/`,
      merge: async () =>
        Object.fromEntries(
          events.map(([seq, type]) => [
            `runs/${runId}/events/${seq}.json`,
            ev(runId, seq, type),
          ]),
        ),
      message: `seed per-event ${runId}`,
    });
  // Seed an already-sealed run: the combined `events.jsonl`, no `events/`.
  const seedSealed = (runId: string, events: [number, string][]) =>
    substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: `runs/${runId}/events/`,
      merge: async () => ({
        [`runs/${runId}/events.jsonl`]:
          events.map(([seq, type]) => ev(runId, seq, type)).join("\n") + "\n",
      }),
      message: `seed sealed ${runId}`,
    });
  return { repoId, substrate, anchorRunId, runDir, seedPerEvent, seedSealed };
}

describe("recoverInterruptedCompactions", () => {
  test("seals a run left terminal-but-per-event by an interrupted fold", async () => {
    const { repoId, substrate, anchorRunId, runDir, seedPerEvent } =
      await setup("dep-crash-window");
    // The exact crash window: the terminal event committed, the fold did not.
    await seedPerEvent("run-1", [
      [0, "RunStarted"],
      [1, "RunCompleted"],
    ]);
    expect(
      (await fs.promises.readdir(path.join(runDir("run-1"), "events"))).sort(),
    ).toEqual(["0.json", "1.json"]);

    const result = await recoverInterruptedCompactions({
      substrate,
      repoId,
      ref: REF,
      anchorRunId,
      pendingSealRunIds: ["run-1"],
    });
    expect(result).toEqual({ sealed: 1, failed: [] });

    // The per-event directory is gone; the combined file is the verbatim fold.
    await expect(
      fs.promises.readdir(path.join(runDir("run-1"), "events")),
    ).rejects.toThrow();
    const combined = await fs.promises.readFile(
      path.join(runDir("run-1"), "events.jsonl"),
      "utf8",
    );
    expect(combined).toBe(
      `${ev("run-1", 0, "RunStarted")}\n${ev("run-1", 1, "RunCompleted")}\n`,
    );
  });

  test("leaves live and already-sealed proposals untouched", async () => {
    const { repoId, substrate, anchorRunId, runDir, seedPerEvent, seedSealed } =
      await setup("dep-noop");
    // `live` is non-terminal (RunStarted only); `sealed` is already folded.
    // Neither should be mutated even when handed to the sweep.
    await seedPerEvent("live", [[0, "RunStarted"]]);
    await seedSealed("sealed", [
      [0, "RunStarted"],
      [1, "RunCompleted"],
    ]);

    const result = await recoverInterruptedCompactions({
      substrate,
      repoId,
      ref: REF,
      anchorRunId,
      pendingSealRunIds: ["live", "sealed"],
    });
    expect(result).toEqual({ sealed: 0, failed: [] });

    // `live` keeps its per-event directory; `sealed` keeps its combined file.
    expect(
      (await fs.promises.readdir(path.join(runDir("live"), "events"))).sort(),
    ).toEqual(["0.json"]);
    await expect(
      fs.promises.readdir(path.join(runDir("sealed"), "events")),
    ).rejects.toThrow();
    expect(
      await fs.promises.readFile(
        path.join(runDir("sealed"), "events.jsonl"),
        "utf8",
      ),
    ).toBe(
      `${ev("sealed", 0, "RunStarted")}\n${ev("sealed", 1, "RunCompleted")}\n`,
    );
  });

  test("is safe when a live fold folds the same run concurrently", async () => {
    // A sweep can race the live fire-and-forget fold for the same run. This
    // composes the two; the exactly-once guarantee itself lives in
    // compactRunEvents' own "two concurrent seals" test. Here we only assert
    // the sweep participates safely: exactly one path seals, and the combined
    // file survives intact.
    const { repoId, substrate, anchorRunId, runDir, seedPerEvent } =
      await setup("dep-concurrent");
    await seedPerEvent("run-1", [
      [0, "RunStarted"],
      [1, "RunCompleted"],
    ]);

    const [sweep, direct] = await Promise.all([
      recoverInterruptedCompactions({
        substrate,
        repoId,
        ref: REF,
        anchorRunId,
        pendingSealRunIds: ["run-1"],
      }),
      compactRunEvents({
        substrate,
        repoId,
        ref: REF,
        anchorRunId,
        runId: "run-1",
      }),
    ]);
    expect(sweep.sealed + (direct.compacted ? 1 : 0)).toBe(1);
    expect(sweep.failed).toEqual([]);

    const combined = await fs.promises.readFile(
      path.join(runDir("run-1"), "events.jsonl"),
      "utf8",
    );
    expect(combined).toBe(
      `${ev("run-1", 0, "RunStarted")}\n${ev("run-1", 1, "RunCompleted")}\n`,
    );
  });

  test("records a failed fold and still seals the remaining runs", async () => {
    const { repoId, substrate, anchorRunId, runDir, seedPerEvent } =
      await setup("dep-failure");
    // Two terminal-but-per-event runs. The write for `bad` is injected to
    // throw; `good` must still seal, and `bad` must be reported in `failed`.
    await seedPerEvent("bad", [
      [0, "RunStarted"],
      [1, "RunCompleted"],
    ]);
    await seedPerEvent("good", [
      [0, "RunStarted"],
      [1, "RunCompleted"],
    ]);

    const failing: typeof substrate = {
      ...substrate,
      writeTreePreservingPrefix: (
        ...args: Parameters<typeof substrate.writeTreePreservingPrefix>
      ) => {
        const [, , , writeOpts] = args;
        if (writeOpts.preservePrefix.includes("/bad/")) {
          return Promise.reject(new Error("injected fold failure for bad"));
        }
        return substrate.writeTreePreservingPrefix(...args);
      },
    };

    const result = await recoverInterruptedCompactions({
      substrate: failing,
      repoId,
      ref: REF,
      anchorRunId,
      pendingSealRunIds: ["bad", "good"],
    });
    expect(result).toEqual({
      sealed: 1,
      failed: [{ runId: "bad", message: "injected fold failure for bad" }],
    });

    // `good` sealed; `bad` stays per-event, ready to retry on the next boot.
    await expect(
      fs.promises.readdir(path.join(runDir("good"), "events")),
    ).rejects.toThrow();
    expect(
      (await fs.promises.readdir(path.join(runDir("bad"), "events"))).sort(),
    ).toEqual(["0.json", "1.json"]);
  });
});
