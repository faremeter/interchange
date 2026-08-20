// Tests for the supervisor-authored `RunFailed` terminal commit.
//
// `commitRunFailed` writes a `RunFailed` into a run's event log under the
// supervisor principal so the deployment's `workflow_run.status` flips to
// `failed` when the crash-loop guard latches. It computes the next seq
// inside the substrate merge, bases the first event at seq 1, and no-ops
// when the run is already terminal (respecting push-validation's
// terminal-lock).
//
// The substrate is a minimal in-memory `writeTreePreservingPrefix` that
// actually runs the caller's merge against a file map, so the seq and
// terminal-lock logic is exercised end to end.

import { describe, test, expect } from "bun:test";

import type { RepoId, RepoStore } from "@intx/hub-sessions";

import { commitRunFailed } from "./terminal-commit";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function createInMemorySubstrate() {
  const files = new Map<string, Uint8Array>();
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const store: Partial<RepoStore> = {
    async writeTreePreservingPrefix(_principal, _repoId, _ref, args) {
      const existing = new Map<string, Uint8Array>();
      for (const [k, v] of files) {
        if (k.startsWith(args.preservePrefix)) existing.set(k, v);
      }
      const result = await args.merge(existing);
      for (const k of [...files.keys()]) {
        if (k.startsWith(args.preservePrefix)) files.delete(k);
      }
      for (const [k, v] of Object.entries(result)) {
        files.set(k, typeof v === "string" ? encoder.encode(v) : v);
      }
      return { commitSha: "memory", newlyTerminalRuns: [] };
    },
  };
  return {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; only writeTreePreservingPrefix is exercised, any other method surfaces as undefined-not-a-function
    store: store as RepoStore,
    seedEvent(path: string, obj: Record<string, unknown>) {
      files.set(path, encoder.encode(JSON.stringify(obj)));
    },
    readEvent(path: string): Record<string, unknown> | undefined {
      const raw = files.get(path);
      if (raw === undefined) return undefined;
      const parsed: unknown = JSON.parse(decoder.decode(raw));
      if (!isRecord(parsed)) {
        throw new Error("seeded blob is not an object");
      }
      return parsed;
    },
    listUnder(prefix: string): string[] {
      return [...files.keys()].filter((k) => k.startsWith(prefix)).sort();
    },
  };
}

const REPO_ID: RepoId = { kind: "workflow-run", id: "run_x" };
const REF = "refs/heads/main";
const RUN_ID = "run_x";
const EVENTS_PREFIX = `runs/${RUN_ID}/events/`;

describe("commitRunFailed", () => {
  test("appends RunFailed at seq 1 for a run with no events", async () => {
    const substrate = createInMemorySubstrate();
    const result = await commitRunFailed({
      substrate: substrate.store,
      repoId: REPO_ID,
      ref: REF,
      anchorRunId: RUN_ID,
      runId: RUN_ID,
      at: "2026-01-01T00:00:00.000Z",
      message: "crash-loop test",
    });

    expect(result.appended).toBe(true);
    const event = substrate.readEvent(`${EVENTS_PREFIX}1.json`);
    expect(event).toEqual({
      seq: 1,
      type: "RunFailed",
      at: "2026-01-01T00:00:00.000Z",
      error: { message: "crash-loop test" },
    });
  });

  test("appends RunFailed at maxSeq+1 for a running run", async () => {
    const substrate = createInMemorySubstrate();
    substrate.seedEvent(`${EVENTS_PREFIX}1.json`, {
      seq: 1,
      type: "RunStarted",
      at: "2026-01-01T00:00:00.000Z",
    });

    const result = await commitRunFailed({
      substrate: substrate.store,
      repoId: REPO_ID,
      ref: REF,
      anchorRunId: RUN_ID,
      runId: RUN_ID,
      at: "2026-01-01T00:00:05.000Z",
      message: "crash-loop after start",
    });

    expect(result.appended).toBe(true);
    // The RunStarted is preserved and the RunFailed lands contiguously at 2.
    expect(substrate.listUnder(EVENTS_PREFIX)).toEqual([
      `${EVENTS_PREFIX}1.json`,
      `${EVENTS_PREFIX}2.json`,
    ]);
    const runFailed = substrate.readEvent(`${EVENTS_PREFIX}2.json`);
    expect(runFailed?.["type"]).toBe("RunFailed");
    expect(runFailed?.["seq"]).toBe(2);
  });

  test("no-ops when the run is already terminal", async () => {
    const substrate = createInMemorySubstrate();
    substrate.seedEvent(`${EVENTS_PREFIX}1.json`, {
      seq: 1,
      type: "RunStarted",
      at: "2026-01-01T00:00:00.000Z",
    });
    substrate.seedEvent(`${EVENTS_PREFIX}2.json`, {
      seq: 2,
      type: "RunCompleted",
      at: "2026-01-01T00:00:03.000Z",
    });

    const result = await commitRunFailed({
      substrate: substrate.store,
      repoId: REPO_ID,
      ref: REF,
      anchorRunId: RUN_ID,
      runId: RUN_ID,
      at: "2026-01-01T00:00:05.000Z",
      message: "should not append",
    });

    // The already-terminal run is left untouched: no RunFailed, no third entry.
    expect(result.appended).toBe(false);
    expect(substrate.listUnder(EVENTS_PREFIX)).toEqual([
      `${EVENTS_PREFIX}1.json`,
      `${EVENTS_PREFIX}2.json`,
    ]);
    expect(substrate.readEvent(`${EVENTS_PREFIX}2.json`)?.["type"]).toBe(
      "RunCompleted",
    );
  });
});
