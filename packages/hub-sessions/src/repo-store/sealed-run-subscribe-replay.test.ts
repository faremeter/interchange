import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { type } from "arktype";
import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore } from "./store";
import { subscribeKind } from "./subscribe-kind";
import { encodeCombinedEventLog } from "../workflow-run-event-log";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "./types";

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

function permissive(): KindHandler {
  return {
    kind: "agent-state",
    directoryPrefix: "repos-sealed-replay",
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* this test drives subscribe replay, not ref-update side effects */
    },
  };
}

const allowAll: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };
const repoId: RepoId = { kind: "agent-state", id: "subject" };
const REF = "refs/heads/test";

const TimerFired = type({
  type: "'TimerFired'",
  seq: "number",
  data: { timerId: "string" },
});
const RunCompleted = type({ type: "'RunCompleted'", seq: "number" });
const Ev = TimerFired.or(RunCompleted);

describe("subscribeKind replay over a sealed run", () => {
  test("from seq 0 still emits every event after compaction drops events/", async () => {
    const dataDir = await makeTempDir("sealed-replay-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": permissive() },
      authorize: allowAll,
    });

    const blob0 = JSON.stringify({
      type: "TimerFired",
      seq: 0,
      data: { timerId: "t0" },
    });
    const blob1 = JSON.stringify({
      type: "TimerFired",
      seq: 1,
      data: { timerId: "t1" },
    });
    const blob2 = JSON.stringify({ type: "RunCompleted", seq: 2 });

    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": blob0 },
      message: "e0",
    });
    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/1.json": blob1 },
      message: "e1",
    });
    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/2.json": blob2 },
      message: "e2 terminal",
    });

    // Simulate compaction: clear events/ and write the combined file.
    const combined = encodeCombinedEventLog([
      new TextEncoder().encode(blob0),
      new TextEncoder().encode(blob1),
      new TextEncoder().encode(blob2),
    ]);
    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events.jsonl": combined },
      clearPrefix: "runs/r1/events/",
      message: "compact r1",
    });

    // Confirm the tip really is sealed (events/ gone, events.jsonl present).
    const dir = store.getRepoDir(repoId);
    expect(fs.existsSync(path.join(dir, "runs/r1/events"))).toBe(false);
    expect(fs.existsSync(path.join(dir, "runs/r1/events.jsonl"))).toBe(true);

    const ac = new AbortController();
    const iter = subscribeKind(store, principal, repoId, REF, Ev, {
      signal: ac.signal,
      from: { seq: 0 },
      kinds: ["TimerFired", "RunCompleted"],
    });

    const seen: number[] = [];
    const abortTimer = setTimeout(() => ac.abort(), 300);
    try {
      for await (const e of iter) {
        seen.push(e.seq);
        if (seen.length === 3) {
          ac.abort();
          break;
        }
      }
    } catch {
      /* aborted */
    } finally {
      clearTimeout(abortTimer);
    }
    // Replay walks historical commit diffs, so all three events emit even
    // though the tip carries only the combined file.
    expect(seen.sort((a, b) => a - b)).toEqual([0, 1, 2]);
  });
});
