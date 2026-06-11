import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { type } from "arktype";
import { generateKeyPair } from "@intx/crypto-node";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore } from "./store";
import { subscribeKind } from "./subscribe-kind";
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
    await fs.promises.rm(d, { recursive: true, force: true }).catch((_e) => {
      /* best effort cleanup */
    });
  }
});

function createPermissiveHandler(): KindHandler {
  return {
    kind: "agent-state",
    directoryPrefix: "repos-subscribe-kind",
    validatePush(): ValidatePushResult {
      return { ok: true };
    },
    onRefUpdated() {
      /* no-op */
    },
  };
}

const allowAll: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };
const repoId: RepoId = { kind: "agent-state", id: "subject" };
const REF = "refs/heads/test";

const TimerFired = type({
  type: "'TimerFired'",
  data: { timerId: "string" },
});
type TimerFired = typeof TimerFired.infer;

const SignalReceived = type({
  type: "'SignalReceived'",
  data: { name: "string" },
});
type SignalReceived = typeof SignalReceived.infer;

const RunEvent = TimerFired.or(SignalReceived);
type RunEvent = typeof RunEvent.infer;

describe("subscribeKind", () => {
  test("yields a committed TimerFired event when kinds includes TimerFired", async () => {
    const dataDir = await makeTempDir("subscribe-kind-timer-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": createPermissiveHandler() },
      authorize: allowAll,
    });

    // Commit a TimerFired payload at runs/r1/events/0.json — the
    // canonical workflow-run path layout the kind handler will use.
    await store.writeTree(principal, repoId, REF, {
      files: {
        "runs/r1/events/0.json": JSON.stringify({
          type: "TimerFired",
          data: { timerId: "t1" },
        }),
      },
      message: "TimerFired #1",
    });

    const ac = new AbortController();
    const iter = subscribeKind(store, principal, repoId, REF, RunEvent, {
      signal: ac.signal,
      from: { seq: 0 },
      kinds: ["TimerFired"],
    });

    const first = await iter.next();
    ac.abort();
    expect(first.done).toBe(false);
    if (first.done) throw new Error("unreachable");
    expect(first.value.seq).toBe(0);
    expect(first.value.event.type).toBe("TimerFired");
    if (first.value.event.type !== "TimerFired") {
      throw new Error("expected TimerFired");
    }
    expect(first.value.event.data.timerId).toBe("t1");
  });

  test("a SignalReceived commit is filtered out when kinds is TimerFired", async () => {
    const dataDir = await makeTempDir("subscribe-kind-signal-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": createPermissiveHandler() },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: {
        "runs/r1/events/0.json": JSON.stringify({
          type: "SignalReceived",
          data: { name: "go" },
        }),
      },
      message: "SignalReceived #0",
    });

    const ac = new AbortController();
    const iter = subscribeKind(store, principal, repoId, REF, RunEvent, {
      signal: ac.signal,
      from: { seq: 0 },
      kinds: ["TimerFired"],
    });

    // Schedule abort so a missing match resolves the iterator cleanly
    // instead of hanging.
    setTimeout(() => ac.abort(), 50);
    const result = await iter.next();
    expect(result.done).toBe(true);
  });

  test("only emits blobs added by each ref-update, not the full tree on every commit", async () => {
    const dataDir = await makeTempDir("subscribe-kind-diff-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": createPermissiveHandler() },
      authorize: allowAll,
    });

    // First commit adds events 0 and 1; second commit adds event 2.
    // The subscriber sees both ref-updates and should yield three
    // distinct events with seqs [0, 1, 2] — not [0, 1, 0, 1, 2]
    // (i.e. each commit's diff, not its full tree).
    await store.writeTree(principal, repoId, REF, {
      files: {
        "runs/r1/events/0.json": JSON.stringify({
          type: "TimerFired",
          data: { timerId: "t0" },
        }),
        "runs/r1/events/1.json": JSON.stringify({
          type: "TimerFired",
          data: { timerId: "t1" },
        }),
      },
      message: "events 0+1",
    });
    await store.writeTree(principal, repoId, REF, {
      files: {
        "runs/r1/events/2.json": JSON.stringify({
          type: "TimerFired",
          data: { timerId: "t2" },
        }),
      },
      message: "event 2",
    });

    const ac = new AbortController();
    const iter = subscribeKind(store, principal, repoId, REF, RunEvent, {
      signal: ac.signal,
      from: { seq: 0 },
      kinds: ["TimerFired"],
    });

    const collected: { seq: number; timerId: string }[] = [];
    for (let i = 0; i < 3; i++) {
      const next = await iter.next();
      if (next.done) break;
      if (next.value.event.type !== "TimerFired") {
        throw new Error("expected TimerFired");
      }
      collected.push({
        seq: next.value.seq,
        timerId: next.value.event.data.timerId,
      });
    }
    ac.abort();

    expect(collected.map((e) => e.seq)).toEqual([0, 1, 2]);
    expect(collected.map((e) => e.timerId)).toEqual(["t0", "t1", "t2"]);
  });
});
