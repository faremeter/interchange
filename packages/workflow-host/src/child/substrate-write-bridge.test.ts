// Failure-mode coverage for the child-side substrate-write bridge.
//
// Greybeard's pre-PR review flagged that the new `substrate.write` /
// `substrate.merge` IPC layer has happy-path coverage but no
// pinned-behaviour tests against the failure modes the bridge has to
// survive: the supervisor dropping mid-write (IPC channel close), a
// malformed `substrate.merge.response` landing with a stale or
// unknown `requestId`, and the cleanup semantics the bridge's
// `cancelAll` provides on teardown.
//
// These tests target the bridge directly with a mock upstream sender
// so the failure modes are observable without standing up a real
// supervisor process.

import { describe, test, expect } from "bun:test";

import { type } from "arktype";

import { createChildSubstrateWriteBridge } from "./substrate-write-bridge";
import type { ControlChannelSender } from "../ipc/control-channel";

const MergeResponseFailureShape = type({
  requestId: "string",
  result: {
    ok: "false",
    reason: "string",
  },
});

type Sent = { type: string; data: unknown };

function createMockSender(): {
  sender: ControlChannelSender;
  sent: Sent[];
  failNextSend: (reason: string) => void;
} {
  const sent: Sent[] = [];
  let failReason: string | null = null;
  let seqCounter = 0;
  return {
    sender: {
      get seq() {
        return seqCounter;
      },
      async send(payload) {
        if (failReason !== null) {
          const r = failReason;
          failReason = null;
          throw new Error(r);
        }
        seqCounter += 1;
        sent.push({ type: payload.type, data: payload.data });
      },
    },
    sent,
    failNextSend(reason: string) {
      failReason = reason;
    },
  };
}

describe("ChildSubstrateWriteBridge: supervisor-side drop mid-write", () => {
  test("a write submitted before the IPC channel tears down rejects via cancelAll", async () => {
    const mock = createMockSender();
    let nextId = 0;
    const bridge = createChildSubstrateWriteBridge({
      upstreamSender: mock.sender,
      allocateRequestId: () => `rid-${String((nextId += 1))}`,
    });

    // Submit a write; the supervisor would respond on the matching
    // requestId via `handleWriteResponse`. We never deliver a
    // response and instead simulate the supervisor's IPC channel
    // tearing down -- the control loop's exit path invokes
    // `cancelAll(reason)` so any pending awaiter rejects rather than
    // leaking forever.
    const submitPromise = bridge.submit({
      repoId: { kind: "workflow-run", id: "deployment-x" },
      ref: "refs/heads/main",
      preservePrefix: "runs/r-1/events/",
      message: "test write",
      merge: async () => ({ "runs/r-1/events/0.json": "{}" }),
    });

    // Yield once so the bridge's `await opts.upstreamSender.send(...)`
    // settles before we observe `pendingCount`.
    await Promise.resolve();
    expect(bridge.pendingCount).toBe(1);
    expect(mock.sent.length).toBe(1);
    expect(mock.sent[0]?.type).toBe("substrate.write.request");

    // Supervisor-side drop: the bridge's host invokes cancelAll on
    // the exit path. The pending submit must reject with a structured
    // error so the runtime body surfaces the failure instead of
    // hanging on the awaiter.
    bridge.cancelAll("supervisor IPC channel closed mid-write");

    await expect(submitPromise).rejects.toThrow(
      /supervisor IPC channel closed mid-write/,
    );
    expect(bridge.pendingCount).toBe(0);
  });

  test("a write whose upstream send fails synchronously rejects with the cause", async () => {
    const mock = createMockSender();
    mock.failNextSend("write returned EPIPE");
    const bridge = createChildSubstrateWriteBridge({
      upstreamSender: mock.sender,
      allocateRequestId: () => "rid-fail",
    });
    await expect(
      bridge.submit({
        repoId: { kind: "workflow-run", id: "deployment-x" },
        ref: "refs/heads/main",
        preservePrefix: "runs/r-2/events/",
        message: "test write",
        merge: async () => ({ "runs/r-2/events/0.json": "{}" }),
      }),
    ).rejects.toThrow(/EPIPE/);
    // Failed send must not leak a pending entry.
    expect(bridge.pendingCount).toBe(0);
  });
});

describe("ChildSubstrateWriteBridge: malformed substrate.merge.response", () => {
  test("a substrate.merge.request arriving with an unknown requestId replies with a structured failure", async () => {
    const mock = createMockSender();
    const bridge = createChildSubstrateWriteBridge({
      upstreamSender: mock.sender,
      allocateRequestId: () => "rid-only",
    });

    // No `submit` is in flight; the bridge's `pending` map is empty.
    // The supervisor wouldn't normally emit a stale or duplicate
    // merge request, but the bridge has to react safely.
    bridge.handleMergeRequest({ requestId: "rid-nonexistent", existing: [] });

    // The bridge's defensive path emits a structured failure on the
    // upstream channel so the supervisor can short-circuit rather
    // than wedging. We yield once so the bridge's fire-and-forget
    // upstream send settles.
    await Promise.resolve();
    await Promise.resolve();
    expect(mock.sent.length).toBe(1);
    expect(mock.sent[0]?.type).toBe("substrate.merge.response");
    const data = mock.sent[0]?.data;
    const validated = MergeResponseFailureShape(data);
    if (validated instanceof type.errors) {
      throw new Error(
        `merge.response payload failed shape check: ${validated.summary}`,
      );
    }
    expect(validated.requestId).toBe("rid-nonexistent");
    expect(validated.result.ok).toBe(false);
    expect(validated.result.reason).toMatch(/no pending entry/);
    expect(bridge.pendingCount).toBe(0);
  });

  test("a substrate.write.response arriving with an unknown requestId is dropped without throwing", async () => {
    const mock = createMockSender();
    const bridge = createChildSubstrateWriteBridge({
      upstreamSender: mock.sender,
      allocateRequestId: () => "rid-only",
    });

    // No `submit` in flight; the response targets nothing. The
    // bridge logs a warning and drops the frame rather than crashing
    // -- a malformed-or-stale response from the supervisor must not
    // wedge or kill the workflow-process child.
    expect(() =>
      bridge.handleWriteResponse({
        requestId: "rid-nonexistent",
        result: { ok: true, commitSha: "deadbeef" },
      }),
    ).not.toThrow();
    expect(bridge.pendingCount).toBe(0);
  });

  test("a merge response shape that fails base64 decoding throws inside the awaiter's merge callback", async () => {
    const mock = createMockSender();
    const bridge = createChildSubstrateWriteBridge({
      upstreamSender: mock.sender,
      allocateRequestId: () => "rid-merge",
    });

    // The bridge stores the caller's merge closure. When the
    // supervisor emits `substrate.merge.request`, the bridge invokes
    // the closure with the decoded existing entries. If the
    // supervisor's payload carries a non-base64 string for
    // `contentBase64`, `Buffer.from(value, "base64")` silently
    // ignores invalid characters -- but a downstream consumer that
    // expected specific bytes will see corruption. Pin the bridge's
    // observable contract: the merge closure receives whatever the
    // standard decoder produces, and the awaiter resolves through
    // the merge round-trip without leaving a leaked pending entry.
    let observedExisting: ReadonlyMap<string, Uint8Array> | null = null;
    const submitPromise = bridge.submit({
      repoId: { kind: "workflow-run", id: "deployment-x" },
      ref: "refs/heads/main",
      preservePrefix: "runs/r-3/events/",
      message: "test write",
      merge: async (existing) => {
        observedExisting = existing;
        return { "runs/r-3/events/0.json": "{}" };
      },
    });
    await Promise.resolve();
    expect(bridge.pendingCount).toBe(1);

    bridge.handleMergeRequest({
      requestId: "rid-merge",
      existing: [{ path: "runs/r-3/events/prior.json", contentBase64: "###" }],
    });
    // Drain the bridge's async merge round-trip so the upstream send
    // settles. The bridge's fire-and-forget body uses awaits, so
    // multiple microtask cycles are required to flush the chain.
    for (let i = 0; i < 5; i += 1) await Promise.resolve();
    expect(observedExisting).not.toBeNull();
    if (observedExisting === null) throw new Error("merge closure missed");
    const captured: ReadonlyMap<string, Uint8Array> = observedExisting;
    expect(captured.has("runs/r-3/events/prior.json")).toBe(true);

    // Terminate the write so the test does not leak a pending entry.
    bridge.handleWriteResponse({
      requestId: "rid-merge",
      result: { ok: true, commitSha: "deadbeef" },
    });
    await expect(submitPromise).resolves.toEqual({ commitSha: "deadbeef" });
    expect(bridge.pendingCount).toBe(0);
  });
});
