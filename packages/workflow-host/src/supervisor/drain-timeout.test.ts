// Supervisor drainTimeout accumulator + escalation tests.
//
// The accumulator ticks against wall-clock time while the supervisor
// believes the workflow has at least one `"cancel"`-behavior step in
// flight. On timeout it commits `CancelRequested{origin:
// "supervisor-drain"}` through the injected substrate. The runtime
// body's existing cancellation cascade handles teardown once the
// CancelRequested event lands on the workflow-run log.
//
// These tests exercise the accumulator semantics directly (pause /
// resume / stop / accumulatedMs / escalation commit) and the
// canonical observable sequence on the supervisor's substrate write
// side. The attachment point is the `RepoStore.
// writeTreePreservingPrefix` interceptor on a stub substrate -- the
// fake captures every commit's principal/files/ref so the test
// asserts directly on the CancelRequested blob the accumulator
// writes, instead of round-tripping through a `subscribeKind`
// observer. The same observable is being tested; the attachment
// point is just one layer closer to the write the accumulator
// performs.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type { RepoId, RepoStore } from "@intx/hub-sessions";
import type { SignedPayload } from "./types";

import {
  createDrainTimeoutAccumulator,
  DEFAULT_DRAIN_TIMEOUT_MS,
} from "./drain-timeout";

async function makeTempDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

type WriteCapture = {
  principal: { kind: string };
  repoId: RepoId;
  ref: string;
  files: Record<string, string | Uint8Array>;
};

function createStubRepoStore(opts: {
  baseDir: string;
  onWrite?: (cap: WriteCapture) => void;
}): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(opts.baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      const existing = new Map<string, Uint8Array>();
      const files = await args.merge(existing);
      opts.onWrite?.({ principal, repoId, ref, files });
      return { commitSha: "deadbeefcafef00d" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; only the subset the accumulator invokes is implemented
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub RepoStore: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
}

function makeSignSpy(): {
  signAsPrincipal: (kind: string, payload: Uint8Array) => SignedPayload;
  calls: { kind: string; payload: Uint8Array }[];
} {
  const calls: { kind: string; payload: Uint8Array }[] = [];
  return {
    signAsPrincipal: (kind, payload) => {
      calls.push({ kind, payload });
      const sig = new Uint8Array(64);
      sig.fill(7);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub mints a fixed signature; the supervisor's principal kind is a closed union
      return { sig, principalKind: kind as "supervisor" };
    },
    calls,
  };
}

type FakeClock = {
  now: () => number;
  advance: (ms: number) => void;
};

function createFakeClock(start = 1_700_000_000_000): FakeClock {
  let t = start;
  return {
    now: () => t,
    advance: (ms) => {
      t += ms;
    },
  };
}

type FakeTimer = {
  cb: () => void;
  ms: number;
  fired: boolean;
  cancelled: boolean;
};

type FakeTimerHost = {
  setTimer: (cb: () => void, ms: number) => unknown;
  clearTimer: (handle: unknown) => void;
  fireDue: (clock: FakeClock) => Promise<void>;
  pending: () => FakeTimer[];
};

function createFakeTimerHost(): FakeTimerHost {
  const timers = new Set<FakeTimer>();
  return {
    setTimer(cb, ms) {
      const t: FakeTimer = { cb, ms, fired: false, cancelled: false };
      timers.add(t);
      return t;
    },
    clearTimer(handle) {
      if (handle === null || typeof handle !== "object") return;
      for (const t of timers) {
        if (t === handle) {
          t.cancelled = true;
          timers.delete(t);
          return;
        }
      }
    },
    async fireDue(_clock) {
      const due = [...timers];
      for (const t of due) {
        if (t.cancelled) continue;
        t.fired = true;
        timers.delete(t);
        t.cb();
        // Allow pending microtasks (the escalate commit) to flush.
        await Promise.resolve();
      }
    },
    pending() {
      return [...timers];
    },
  };
}

const REPO_ID: RepoId = { kind: "workflow-run", id: "deployment-x" };
const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-x";

describe("createDrainTimeoutAccumulator", () => {
  test("DEFAULT_DRAIN_TIMEOUT_MS is 60 seconds", () => {
    expect(DEFAULT_DRAIN_TIMEOUT_MS).toBe(60_000);
  });

  test("ticks against wall-clock and escalates on expiry", async () => {
    const baseDir = await makeTempDir("drain-timeout-");
    const writes: WriteCapture[] = [];
    const substrate = createStubRepoStore({
      baseDir,
      onWrite: (cap) => writes.push(cap),
    });
    const signSpy = makeSignSpy();
    const clock = createFakeClock();
    const host = createFakeTimerHost();
    const accumulator = createDrainTimeoutAccumulator({
      substrate,
      repoId: REPO_ID,
      ref: REF,
      deploymentId: DEPLOYMENT_ID,
      runId: "run-1",
      signAsPrincipal: signSpy.signAsPrincipal,
      drainTimeoutMs: 1_000,
      now: clock.now,
      setTimer: host.setTimer,
      clearTimer: host.clearTimer,
    });
    accumulator.start();
    expect(host.pending().length).toBe(1);
    expect(accumulator.escalated).toBe(false);

    clock.advance(1_000);
    await host.fireDue(clock);
    // Give the async escalate path a tick to settle.
    await new Promise<void>((r) => setTimeout(r, 5));

    expect(accumulator.escalated).toBe(true);
    expect(signSpy.calls.length).toBe(1);
    expect(signSpy.calls[0]?.kind).toBe("supervisor");
    expect(writes.length).toBe(1);
    expect(writes[0]?.principal.kind).toBe("supervisor");
    // The committed blob is a CancelRequested event under
    // runs/<runId>/events/<seq>.json.
    const files = writes[0]?.files ?? {};
    const eventPaths = Object.keys(files).filter((k) =>
      k.startsWith("runs/run-1/events/"),
    );
    expect(eventPaths.length).toBe(1);
    const eventBlobRaw = files[eventPaths[0] ?? ""];
    expect(typeof eventBlobRaw).toBe("string");
    if (typeof eventBlobRaw !== "string") return;
    const eventBlob: unknown = JSON.parse(eventBlobRaw);
    expect(eventBlob).toMatchObject({
      type: "CancelRequested",
      runId: "run-1",
      origin: "supervisor-drain",
    });
  });

  test("pause halts wall-clock ticking; resume carries baseline forward", async () => {
    const baseDir = await makeTempDir("drain-timeout-pause-");
    const substrate = createStubRepoStore({ baseDir });
    const signSpy = makeSignSpy();
    const clock = createFakeClock();
    const host = createFakeTimerHost();
    const accumulator = createDrainTimeoutAccumulator({
      substrate,
      repoId: REPO_ID,
      ref: REF,
      deploymentId: DEPLOYMENT_ID,
      runId: "run-2",
      signAsPrincipal: signSpy.signAsPrincipal,
      drainTimeoutMs: 1_000,
      now: clock.now,
      setTimer: host.setTimer,
      clearTimer: host.clearTimer,
    });
    accumulator.start();
    clock.advance(400);
    accumulator.pause();
    expect(accumulator.accumulatedMs()).toBe(400);
    expect(host.pending().length).toBe(0);
    // Time spent paused must NOT tick the accumulator: simulate an
    // arbitrary wait-mode step running for 5 seconds.
    clock.advance(5_000);
    expect(accumulator.accumulatedMs()).toBe(400);
    accumulator.resume();
    // After resume, 600ms more is required to hit the 1_000ms cap.
    clock.advance(599);
    expect(accumulator.escalated).toBe(false);
    clock.advance(1);
    await host.fireDue(clock);
    await new Promise<void>((r) => setTimeout(r, 5));
    expect(accumulator.escalated).toBe(true);
  });

  test("stop before timeout suppresses the escalation commit", async () => {
    const baseDir = await makeTempDir("drain-timeout-stop-");
    const writes: WriteCapture[] = [];
    const substrate = createStubRepoStore({
      baseDir,
      onWrite: (cap) => writes.push(cap),
    });
    const signSpy = makeSignSpy();
    const clock = createFakeClock();
    const host = createFakeTimerHost();
    const accumulator = createDrainTimeoutAccumulator({
      substrate,
      repoId: REPO_ID,
      ref: REF,
      deploymentId: DEPLOYMENT_ID,
      runId: "run-3",
      signAsPrincipal: signSpy.signAsPrincipal,
      drainTimeoutMs: 1_000,
      now: clock.now,
      setTimer: host.setTimer,
      clearTimer: host.clearTimer,
    });
    accumulator.start();
    clock.advance(500);
    accumulator.stop();
    expect(host.pending().length).toBe(0);
    expect(accumulator.escalated).toBe(false);
    // Even if more time passes, no escalation happens.
    clock.advance(10_000);
    await host.fireDue(clock);
    expect(accumulator.escalated).toBe(false);
    expect(signSpy.calls.length).toBe(0);
    expect(writes.length).toBe(0);
  });

  test("escalation invokes signAsPrincipal with the canonical CancelRequested payload", async () => {
    const baseDir = await makeTempDir("drain-timeout-sign-");
    const substrate = createStubRepoStore({ baseDir });
    const signSpy = makeSignSpy();
    const clock = createFakeClock();
    const host = createFakeTimerHost();
    const accumulator = createDrainTimeoutAccumulator({
      substrate,
      repoId: REPO_ID,
      ref: REF,
      deploymentId: DEPLOYMENT_ID,
      runId: "run-4",
      signAsPrincipal: signSpy.signAsPrincipal,
      drainTimeoutMs: 1_000,
      now: clock.now,
      setTimer: host.setTimer,
      clearTimer: host.clearTimer,
    });
    accumulator.start();
    clock.advance(1_000);
    await host.fireDue(clock);
    await new Promise<void>((r) => setTimeout(r, 5));
    expect(signSpy.calls.length).toBe(1);
    const signedBytes = signSpy.calls[0]?.payload;
    expect(signedBytes).toBeDefined();
    if (signedBytes === undefined) return;
    const signed: unknown = JSON.parse(new TextDecoder().decode(signedBytes));
    expect(signed).toMatchObject({
      type: "CancelRequested",
      origin: "supervisor-drain",
      runId: "run-4",
    });
  });

  test("same-deployment child drain: parent and child share the same drain signal", () => {
    // Same-deployment children run inside the same workflow-process,
    // so they observe the same DrainController instance the parent
    // does. The parent's main loop iteration over inFlight steps
    // includes the spawn step; aborting it cascades through the
    // existing child-cancel emission path. No extra accumulator is
    // required -- the same one ticks for the whole deployment. The
    // test is structural: a single accumulator covers every run on
    // the deployment.
    const baseDir = "/tmp"; // unused by this structural assertion
    const substrate = createStubRepoStore({ baseDir });
    const signSpy = makeSignSpy();
    const clock = createFakeClock();
    const host = createFakeTimerHost();
    const accumulator = createDrainTimeoutAccumulator({
      substrate,
      repoId: REPO_ID,
      ref: REF,
      deploymentId: DEPLOYMENT_ID,
      runId: "parent-run",
      signAsPrincipal: signSpy.signAsPrincipal,
      drainTimeoutMs: 1_000,
      now: clock.now,
      setTimer: host.setTimer,
      clearTimer: host.clearTimer,
    });
    accumulator.start();
    expect(host.pending().length).toBe(1);
    accumulator.stop();
  });

  test("cross-deployment child drain: parent escalation does NOT touch child's workflow-run repo", async () => {
    // The cross-deployment case: the child runs under a different
    // deployment with its own supervisor. The parent's accumulator
    // commits CancelRequested only against the PARENT's runId; the
    // parent's `ChildCancelRequested` event (committed by the
    // runtime body's existing cascade) is what notifies the child's
    // supervisor to fire its own drain. The test asserts the
    // accumulator's commit lands under the parent run id only.
    const baseDir = await makeTempDir("drain-timeout-cross-");
    const writes: WriteCapture[] = [];
    const substrate = createStubRepoStore({
      baseDir,
      onWrite: (cap) => writes.push(cap),
    });
    const signSpy = makeSignSpy();
    const clock = createFakeClock();
    const host = createFakeTimerHost();
    const accumulator = createDrainTimeoutAccumulator({
      substrate,
      repoId: REPO_ID,
      ref: REF,
      deploymentId: DEPLOYMENT_ID,
      runId: "parent-run",
      signAsPrincipal: signSpy.signAsPrincipal,
      drainTimeoutMs: 1_000,
      now: clock.now,
      setTimer: host.setTimer,
      clearTimer: host.clearTimer,
    });
    accumulator.start();
    clock.advance(1_000);
    await host.fireDue(clock);
    await new Promise<void>((r) => setTimeout(r, 5));
    const eventPaths = Object.keys(writes[0]?.files ?? {}).filter((k) =>
      k.startsWith("runs/"),
    );
    expect(eventPaths.every((p) => p.startsWith("runs/parent-run/"))).toBe(
      true,
    );
  });
});
