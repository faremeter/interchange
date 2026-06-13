// Unit tests for the child-side pack-push IPC bridge. The bridge is
// constructed against a recording upstream sender; the tests assert
// on the emitted `pack.push.request` frame's shape and on the bridge's
// resolve/reject behaviour when its `handleResponse` is invoked with
// the matching `pack.push.response` payload.

import { describe, test, expect } from "bun:test";

import { createChildPackPushBridge } from "./run-child";
import type { ControlChannelSender, ControlPayload } from "../ipc/index";

function createRecordingSender(): ControlChannelSender & {
  sent: ControlPayload[];
} {
  let seq = 0;
  const sent: ControlPayload[] = [];
  return {
    sent,
    get seq() {
      return seq;
    },
    async send(payload: ControlPayload) {
      seq += 1;
      sent.push(payload);
    },
  };
}

describe("createChildPackPushBridge", () => {
  test("sendRequest emits a pack.push.request and resolves on ok=true", async () => {
    const sender = createRecordingSender();
    const bridge = createChildPackPushBridge({
      upstreamSender: sender,
      allocatePushId: () => "pp-test-1",
    });
    const pack = new Uint8Array([10, 11, 12, 13]);
    const promise = bridge.sendRequest({
      agentAddress: "agent-1@example.com",
      repoId: { kind: "workflow-run", id: "dep-1" },
      pack,
      ref: "refs/heads/main",
      commitSha: "abc123",
    });
    // The frame was emitted before the awaiter resolved.
    expect(sender.sent).toHaveLength(1);
    const first = sender.sent[0];
    if (first === undefined) throw new Error("no frame captured");
    expect(first.type).toBe("pack.push.request");
    if (first.type !== "pack.push.request") {
      throw new Error("unexpected payload type");
    }
    expect(first.data.pushId).toBe("pp-test-1");
    expect(first.data.agentAddress).toBe("agent-1@example.com");
    expect(first.data.repoId).toEqual({ kind: "workflow-run", id: "dep-1" });
    expect(first.data.ref).toBe("refs/heads/main");
    expect(first.data.commitSha).toBe("abc123");
    expect(first.data.packBase64).toBe(Buffer.from(pack).toString("base64"));
    expect(bridge.pendingCount).toBe(1);

    bridge.handleResponse({
      pushId: "pp-test-1",
      result: { ok: true },
    });

    await promise;
    expect(bridge.pendingCount).toBe(0);
  });

  test("a matching ok=false response rejects the awaiter with the reason", async () => {
    const sender = createRecordingSender();
    const bridge = createChildPackPushBridge({
      upstreamSender: sender,
      allocatePushId: () => "pp-test-2",
    });
    const promise = bridge.sendRequest({
      agentAddress: "agent-1@example.com",
      repoId: { kind: "workflow-run", id: "dep-1" },
      pack: new Uint8Array([1]),
      ref: "refs/heads/main",
      commitSha: "deadbeef",
    });

    bridge.handleResponse({
      pushId: "pp-test-2",
      result: { ok: false, reason: "hub rejected pack" },
    });

    await expect(promise).rejects.toThrow(/hub rejected pack/);
    expect(bridge.pendingCount).toBe(0);
  });

  test("a response for an unknown pushId is dropped without affecting pending pushes", async () => {
    const sender = createRecordingSender();
    const bridge = createChildPackPushBridge({
      upstreamSender: sender,
      allocatePushId: () => "pp-test-3",
    });
    const promise = bridge.sendRequest({
      agentAddress: "agent-1@example.com",
      repoId: { kind: "workflow-run", id: "dep-1" },
      pack: new Uint8Array([1]),
      ref: "refs/heads/main",
      commitSha: "deadbeef",
    });
    bridge.handleResponse({
      pushId: "pp-unknown",
      result: { ok: true },
    });
    expect(bridge.pendingCount).toBe(1);

    bridge.handleResponse({
      pushId: "pp-test-3",
      result: { ok: true },
    });
    await promise;
    expect(bridge.pendingCount).toBe(0);
  });

  test("cancelAll rejects every pending awaiter with the supplied reason and empties the map", async () => {
    const sender = createRecordingSender();
    let counter = 0;
    const bridge = createChildPackPushBridge({
      upstreamSender: sender,
      allocatePushId: () => {
        counter += 1;
        return `pp-cancel-${String(counter)}`;
      },
    });
    // Attach catch handlers eagerly so the rejections cancelAll
    // synthesizes do not surface as unhandled-rejection noise before
    // the assertion below awaits them.
    const firstResult: Promise<Error | undefined> = bridge
      .sendRequest({
        agentAddress: "agent-1@example.com",
        repoId: { kind: "workflow-run", id: "dep-1" },
        pack: new Uint8Array([1]),
        ref: "refs/heads/main",
        commitSha: "abc",
      })
      .then(() => undefined)
      .catch((err: unknown) =>
        err instanceof Error ? err : new Error(String(err)),
      );
    const secondResult: Promise<Error | undefined> = bridge
      .sendRequest({
        agentAddress: "agent-1@example.com",
        repoId: { kind: "workflow-run", id: "dep-1" },
        pack: new Uint8Array([2]),
        ref: "refs/heads/main",
        commitSha: "def",
      })
      .then(() => undefined)
      .catch((err: unknown) =>
        err instanceof Error ? err : new Error(String(err)),
      );
    // Let the microtask queue drain so the upstream sends resolve and
    // the entries land in the pending map before cancelAll runs.
    await new Promise((r) => setTimeout(r, 0));
    expect(bridge.pendingCount).toBe(2);

    bridge.cancelAll("supervisor crashed");

    const firstError = await firstResult;
    const secondError = await secondResult;
    expect(firstError?.message).toMatch(/cancelled: supervisor crashed/);
    expect(secondError?.message).toMatch(/cancelled: supervisor crashed/);
    expect(bridge.pendingCount).toBe(0);
  });

  test("an upstream send rejection removes the pending entry and rethrows", async () => {
    const failingSender: ControlChannelSender = {
      get seq() {
        return 0;
      },
      send: () => Promise.reject(new Error("upstream broken")),
    };
    const bridge = createChildPackPushBridge({
      upstreamSender: failingSender,
      allocatePushId: () => "pp-test-4",
    });
    await expect(
      bridge.sendRequest({
        agentAddress: "agent-1@example.com",
        repoId: { kind: "workflow-run", id: "dep-1" },
        pack: new Uint8Array([1]),
        ref: "refs/heads/main",
        commitSha: "deadbeef",
      }),
    ).rejects.toThrow(/upstream send failed/);
    expect(bridge.pendingCount).toBe(0);
  });
});
