import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { createWorkflowRunReader } from "./workflow-run-reader";
import type { RepoId, RepoStore } from "./repo-store/types";

const REF = "refs/heads/main";
const REPO_ID: RepoId = { kind: "workflow-run", id: "run_test" };

// The reader only consults `getRepoDir`; the remaining RepoStore methods
// throw so any drift onto a substrate method this read path does not own
// fails loudly rather than returning a misleading empty result.
function repoStoreFor(dirById: Map<string, string>): RepoStore {
  const unused = () =>
    Promise.reject(new Error("workflow-run reader test: method not wired"));
  return {
    initRepo: unused,
    writeTree: unused,
    writeTreePreservingPrefix: unused,
    writeTreeDelta: unused,
    receivePack: unused,
    createPack: unused,
    commitPackedTip: () => {
      throw new Error("workflow-run reader test: method not wired");
    },
    resolveRef: unused,
    listRefs: unused,
    resolveHead: unused,
    getRepoDir: (repoId) => {
      const dir = dirById.get(repoId.id);
      if (dir === undefined) {
        throw new Error(`unknown repo id ${repoId.id}`);
      }
      return dir;
    },
    openCommittedReads: unused,
    openCommittedReadsAtCommit: unused,
    subscribe: () => {
      throw new Error("workflow-run reader test: subscribe not wired");
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

describe("WorkflowRunReader", () => {
  let dir: string;
  let reader: ReturnType<typeof createWorkflowRunReader>;

  beforeEach(async () => {
    dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "wf-run-reader-"));
    await git.init({ fs, dir, defaultBranch: "main" });
    reader = createWorkflowRunReader(
      repoStoreFor(new Map([[REPO_ID.id, dir]])),
    );
  });

  afterEach(async () => {
    await fs.promises.rm(dir, { recursive: true, force: true });
  });

  test("returns seq-ordered events across out-of-order filenames", async () => {
    await commitFiles(dir, {
      "runs/run-1/events/0.json": JSON.stringify({
        type: "RunStarted",
        consumedMessageId: "m1",
      }),
      "runs/run-1/events/2.json": JSON.stringify({ type: "RunCompleted" }),
      "runs/run-1/events/1.json": JSON.stringify({
        type: "SignalAwaited",
        name: "approve",
      }),
    });

    const events = await reader.readRunEvents(REPO_ID, REF, "run-1");
    expect(events.map((e) => e.seq)).toEqual([0, 1, 2]);
    expect(events.map((e) => e.type)).toEqual([
      "RunStarted",
      "SignalAwaited",
      "RunCompleted",
    ]);
    expect(events[0]?.body).toMatchObject({
      type: "RunStarted",
      consumedMessageId: "m1",
    });
  });

  test("reads a combined events.jsonl identically to per-event files", async () => {
    const events = [
      { seq: 0, type: "RunStarted", consumedMessageId: "m1" },
      { seq: 1, type: "SignalAwaited", name: "approve" },
      { seq: 2, type: "RunCompleted" },
    ];
    // run-a holds the per-event form; run-b holds the same events folded
    // into one `events.jsonl` -- each line the verbatim per-event JSON, in
    // seq order, with a trailing newline -- the shape the writer produces.
    const perEvent: Record<string, string> = {};
    for (const e of events) {
      perEvent[`runs/run-a/events/${e.seq}.json`] = JSON.stringify(e);
    }
    const combined = events.map((e) => JSON.stringify(e)).join("\n") + "\n";
    await commitFiles(dir, {
      ...perEvent,
      "runs/run-b/events.jsonl": combined,
    });

    const fromPerEvent = await reader.readRunEvents(REPO_ID, REF, "run-a");
    const fromCombined = await reader.readRunEvents(REPO_ID, REF, "run-b");

    const shape = (e: { seq: number; type: string; body: unknown }) => ({
      seq: e.seq,
      type: e.type,
      body: e.body,
    });
    expect(fromCombined.map(shape)).toEqual(fromPerEvent.map(shape));
    expect(fromCombined.map((e) => e.seq)).toEqual([0, 1, 2]);
    expect(fromCombined.map((e) => e.type)).toEqual([
      "RunStarted",
      "SignalAwaited",
      "RunCompleted",
    ]);
    expect(
      (
        await reader.readLatestRunEvents(REPO_ID, REF, (id) => id === "run-b")
      ).events.get("run-b"),
    ).toMatchObject({
      seq: 2,
      type: "RunCompleted",
    });
  });

  test("separates a repository without a ref from a missing repository", async () => {
    // `beforeEach` initializes the repository without committing, which is the
    // state an interrupted genesis leaves behind.
    expect(
      (await reader.readLatestRunEvents(REPO_ID, REF, () => true)).tip,
    ).toBeNull();
    expect(await reader.hasRepository(REPO_ID)).toBe(true);

    const missing = createWorkflowRunReader(
      repoStoreFor(new Map([[REPO_ID.id, path.join(dir, "unmounted")]])),
    );
    expect(
      (await missing.readLatestRunEvents(REPO_ID, REF, () => true)).tip,
    ).toBeNull();
    expect(await missing.hasRepository(REPO_ID)).toBe(false);
  });

  test("pins the ref and reads only the latest blob of selected runs", async () => {
    await commitFiles(dir, {
      "runs/run-1/events/0.json": "{ unreadable older event",
      "runs/run-1/events/1.json": JSON.stringify({
        seq: 1,
        type: "SignalAwaited",
      }),
      "runs/run-skipped/events/0.json": "{ unreadable unselected event",
    });
    const snapshot = await reader.readLatestRunEvents(
      REPO_ID,
      REF,
      (id) => id !== "run-skipped",
    );
    expect([...snapshot.events.keys()]).toEqual(["run-1"]);
    expect(snapshot.events.get("run-1")).toMatchObject({
      seq: 1,
      type: "SignalAwaited",
    });
    expect(snapshot.tip).toBe(await reader.resolveRefTip(REPO_ID, REF));

    await commitFiles(dir, {
      "runs/run-1/events/2.json": JSON.stringify({
        seq: 2,
        type: "RunCompleted",
      }),
    });
    expect(await reader.resolveRefTip(REPO_ID, REF)).not.toBe(snapshot.tip);
    expect(
      (
        await reader.readLatestRunEvents(REPO_ID, REF, (id) => id === "run-1")
      ).events.get("run-1"),
    ).toMatchObject({
      seq: 2,
      type: "RunCompleted",
    });
  });

  test("reads a tip without a runs tree as empty history", async () => {
    await commitFiles(dir, { README: "no runs yet" });
    const snapshot = await reader.readLatestRunEvents(REPO_ID, REF, () => true);
    expect(snapshot.tip).not.toBeNull();
    expect(snapshot.events.size).toBe(0);
  });

  test.each(["runs", "runs/run-1", "runs/run-1/events"])(
    "propagates a missing %s tree object instead of reading empty history",
    async (treePath) => {
      await commitFiles(dir, {
        "runs/run-1/events/0.json": JSON.stringify({
          seq: 0,
          type: "RunCompleted",
        }),
      });
      const tip = await git.resolveRef({ fs, dir, ref: REF });
      const { oid } = await git.readTree({
        fs,
        dir,
        oid: tip,
        filepath: treePath,
      });
      await fs.promises.rm(
        path.join(dir, ".git", "objects", oid.slice(0, 2), oid.slice(2)),
      );

      await expect(
        reader.readLatestRunEvents(REPO_ID, REF, () => true),
      ).rejects.toBeInstanceOf(git.Errors.NotFoundError);
    },
  );

  test("rejects an illegal event filename in the events dir", async () => {
    // Only `<seq>.json` may land under `events/`; validatePush enforces
    // that, so a foreign name here is corruption the reader surfaces
    // rather than silently dropping.
    await commitFiles(dir, {
      "runs/run-1/events/0.json": JSON.stringify({ type: "RunStarted" }),
      "runs/run-1/events/1.txt": "not json",
    });

    await expect(reader.readRunEvents(REPO_ID, REF, "run-1")).rejects.toThrow(
      "event_filename_invalid",
    );
  });

  test("lists run ids present under runs/", async () => {
    await commitFiles(dir, {
      "runs/run-a/events/0.json": JSON.stringify({ type: "RunStarted" }),
      "runs/run-b/events/0.json": JSON.stringify({ type: "RunStarted" }),
    });

    const runIds = await reader.listRunIds(REPO_ID, REF);
    expect(runIds.sort()).toEqual(["run-a", "run-b"]);
  });

  test("returns empty for an uninitialised repo (no ref)", async () => {
    expect(await reader.listRunIds(REPO_ID, REF)).toEqual([]);
    expect(await reader.readRunEvents(REPO_ID, REF, "run-x")).toEqual([]);
  });

  test("returns empty for an unknown run", async () => {
    await commitFiles(dir, {
      "runs/run-1/events/0.json": JSON.stringify({ type: "RunStarted" }),
    });
    expect(await reader.readRunEvents(REPO_ID, REF, "missing")).toEqual([]);
  });

  test("throws when an event blob is missing its type discriminator", async () => {
    await commitFiles(dir, {
      "runs/run-1/events/0.json": JSON.stringify({ notType: "x" }),
    });
    await expect(reader.readRunEvents(REPO_ID, REF, "run-1")).rejects.toThrow(
      /missing a string `type` field/,
    );
  });

  test("throws when a validly-named event blob holds invalid JSON", async () => {
    await commitFiles(dir, {
      "runs/run-1/events/0.json": "{ this is not json",
    });
    await expect(reader.readRunEvents(REPO_ID, REF, "run-1")).rejects.toThrow();
  });

  test("throws when a validly-named event blob holds a non-object", async () => {
    await commitFiles(dir, {
      "runs/run-1/events/0.json": JSON.stringify(["not", "an", "object"]),
    });
    await expect(reader.readRunEvents(REPO_ID, REF, "run-1")).rejects.toThrow(
      /is not a JSON object/,
    );
  });

  test("returns empty when the repo id is unknown to the store", async () => {
    expect(
      await reader.listRunIds({ kind: "workflow-run", id: "nope" }, REF),
    ).toEqual([]);
  });
});
