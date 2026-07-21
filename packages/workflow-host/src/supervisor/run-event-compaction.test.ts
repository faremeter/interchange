import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import {
  createRepoStore,
  createWorkflowRunReader,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  RepoId,
  WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";

import { compactRunEvents } from "./run-event-compaction";

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

function ev(seq: number, type: string): string {
  return JSON.stringify({ seq, type, runId: "run-1" });
}

async function setup(deploymentId: string) {
  const dataDir = await makeTempDir("compact-");
  const repoId: RepoId = { kind: "workflow-run", id: deploymentId };
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
    deploymentId,
  };
  const eventsDir = () =>
    path.join(substrate.getRepoDir(repoId), "runs", "run-1", "events");
  return { repoId, substrate, supervisor, deploymentId, eventsDir };
}

describe("compactRunEvents", () => {
  test("folds a terminated run's events into one combined file", async () => {
    const { repoId, substrate, supervisor, deploymentId, eventsDir } =
      await setup("dep-1");
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async () => ({
        "runs/run-1/events/0.json": ev(0, "RunStarted"),
        "runs/run-1/events/1.json": ev(1, "RunCompleted"),
      }),
      message: "seed events",
    });
    expect((await fs.promises.readdir(eventsDir())).sort()).toEqual([
      "0.json",
      "1.json",
    ]);

    const result = await compactRunEvents({
      substrate,
      repoId,
      ref: REF,
      deploymentId,
      runId: "run-1",
    });
    expect(result.compacted).toBe(true);

    // The per-event directory is gone; the combined file holds the verbatim
    // fold of the per-event blobs.
    await expect(fs.promises.readdir(eventsDir())).rejects.toThrow();
    const combined = await fs.promises.readFile(
      path.join(substrate.getRepoDir(repoId), "runs", "run-1", "events.jsonl"),
      "utf8",
    );
    expect(combined).toBe(`${ev(0, "RunStarted")}\n${ev(1, "RunCompleted")}\n`);

    // The committed tree still reads back the same events through the reader.
    const reader = createWorkflowRunReader(substrate);
    const events = await reader.readRunEvents(repoId, REF, "run-1");
    expect(events.map((e) => [e.seq, e.type])).toEqual([
      [0, "RunStarted"],
      [1, "RunCompleted"],
    ]);

    // Idempotent: a second seal is a no-op.
    const again = await compactRunEvents({
      substrate,
      repoId,
      ref: REF,
      deploymentId,
      runId: "run-1",
    });
    expect(again.compacted).toBe(false);
  });

  test("leaves an in-flight (non-terminal) run untouched", async () => {
    const { repoId, substrate, supervisor, deploymentId, eventsDir } =
      await setup("dep-2");
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async () => ({ "runs/run-1/events/0.json": ev(0, "RunStarted") }),
      message: "seed",
    });

    const result = await compactRunEvents({
      substrate,
      repoId,
      ref: REF,
      deploymentId,
      runId: "run-1",
    });
    expect(result.compacted).toBe(false);
    expect((await fs.promises.readdir(eventsDir())).sort()).toEqual(["0.json"]);
  });

  test("two concurrent seals do not destroy the combined file", async () => {
    const { repoId, substrate, supervisor, deploymentId } =
      await setup("dep-concurrent");
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async () => ({
        "runs/run-1/events/0.json": ev(0, "RunStarted"),
        "runs/run-1/events/1.json": ev(1, "RunCompleted"),
      }),
      message: "seed events",
    });

    const calls = {
      substrate,
      repoId,
      ref: REF,
      deploymentId,
      runId: "run-1",
    };
    const [a, b] = await Promise.all([
      compactRunEvents(calls),
      compactRunEvents(calls),
    ]);
    // Exactly one performs the fold; the loser reads an already-sealed run.
    expect([a.compacted, b.compacted].filter(Boolean).length).toBe(1);

    const combined = await fs.promises.readFile(
      path.join(substrate.getRepoDir(repoId), "runs", "run-1", "events.jsonl"),
      "utf8",
    );
    expect(combined).toBe(`${ev(0, "RunStarted")}\n${ev(1, "RunCompleted")}\n`);
    const reader = createWorkflowRunReader(substrate);
    expect(
      (await reader.readRunEvents(repoId, REF, "run-1")).map((e) => e.seq),
    ).toEqual([0, 1]);
  });

  test("a sibling blobs/ subtree survives a seal", async () => {
    const { repoId, substrate, supervisor, deploymentId } =
      await setup("dep-blobs");
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async () => ({
        "runs/run-1/events/0.json": ev(0, "RunStarted"),
        "runs/run-1/events/1.json": ev(1, "RunCompleted"),
        [`runs/run-1/blobs/${"a".repeat(64)}`]: "payload-bytes",
      }),
      message: "seed events and a blob",
    });

    expect(
      (
        await compactRunEvents({
          substrate,
          repoId,
          ref: REF,
          deploymentId,
          runId: "run-1",
        })
      ).compacted,
    ).toBe(true);

    const blob = await fs.promises.readFile(
      path.join(
        substrate.getRepoDir(repoId),
        "runs",
        "run-1",
        "blobs",
        "a".repeat(64),
      ),
      "utf8",
    );
    expect(blob).toBe("payload-bytes");
  });

  test("a sibling grants.json survives a seal", async () => {
    // A run's per-run grants live at `runs/<runId>/grants.json` -- a sibling
    // of the run's `events/` subtree, NOT under it. Compaction clears and
    // rebuilds only the `events/` prefix (`writeTreePreservingPrefix` passes
    // paths outside the prefix through unchanged), so the grants file must
    // survive the seal and remain readable alongside the folded
    // `events.jsonl`. The `runs/<runId>/` subtree is never pruned, so
    // grants.json lives and is reclaimed on exactly the same schedule as the
    // run's own retained event log.
    const { repoId, substrate, supervisor, deploymentId } =
      await setup("dep-grants");
    const grantsContents = JSON.stringify({
      grants: [
        { id: "run-grant", resource: "tool:send-mail", effect: "allow" },
      ],
    });
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async () => ({
        "runs/run-1/events/0.json": ev(0, "RunStarted"),
        "runs/run-1/events/1.json": ev(1, "RunCompleted"),
        "runs/run-1/grants.json": grantsContents,
      }),
      message: "seed events and grants",
    });

    expect(
      (
        await compactRunEvents({
          substrate,
          repoId,
          ref: REF,
          deploymentId,
          runId: "run-1",
        })
      ).compacted,
    ).toBe(true);

    // The per-event directory is gone, but the sibling grants file remains.
    const grants = await fs.promises.readFile(
      path.join(substrate.getRepoDir(repoId), "runs", "run-1", "grants.json"),
      "utf8",
    );
    expect(grants).toBe(grantsContents);
    const combined = await fs.promises.readFile(
      path.join(substrate.getRepoDir(repoId), "runs", "run-1", "events.jsonl"),
      "utf8",
    );
    expect(combined).toBe(`${ev(0, "RunStarted")}\n${ev(1, "RunCompleted")}\n`);
  });

  test("folds multi-digit seqs in numeric, not lexical, order", async () => {
    const { repoId, substrate, supervisor, deploymentId } =
      await setup("dep-multidigit");
    const seqs = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
    const typeAt = (s: number) => (s === 11 ? "RunCompleted" : "StepStarted");
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async () => {
        const out: Record<string, string> = {};
        for (const s of seqs)
          out[`runs/run-1/events/${s}.json`] = ev(s, typeAt(s));
        return out;
      },
      message: "seed multi-digit",
    });

    expect(
      (
        await compactRunEvents({
          substrate,
          repoId,
          ref: REF,
          deploymentId,
          runId: "run-1",
        })
      ).compacted,
    ).toBe(true);

    const combined = await fs.promises.readFile(
      path.join(substrate.getRepoDir(repoId), "runs", "run-1", "events.jsonl"),
      "utf8",
    );
    expect(combined).toBe(`${seqs.map((s) => ev(s, typeAt(s))).join("\n")}\n`);
  });

  test("a run skipped while in-flight is sealed once it reaches a terminal event", async () => {
    const { repoId, substrate, supervisor, deploymentId, eventsDir } =
      await setup("dep-becomes-terminal");
    const calls = {
      substrate,
      repoId,
      ref: REF,
      deploymentId,
      runId: "run-1",
    };
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async () => ({ "runs/run-1/events/0.json": ev(0, "RunStarted") }),
      message: "seed non-terminal",
    });
    expect((await compactRunEvents(calls)).compacted).toBe(false);

    // The run reaches a terminal event; the same call now seals it.
    await substrate.writeTreePreservingPrefix(supervisor, repoId, REF, {
      preservePrefix: "runs/run-1/events/",
      merge: async (existing) => {
        const files: Record<string, string | Uint8Array> = {};
        for (const [k, v] of existing) files[k] = v;
        files["runs/run-1/events/1.json"] = ev(1, "RunCompleted");
        return files;
      },
      message: "append terminal",
    });
    expect((await compactRunEvents(calls)).compacted).toBe(true);
    await expect(fs.promises.readdir(eventsDir())).rejects.toThrow();
  });
});
