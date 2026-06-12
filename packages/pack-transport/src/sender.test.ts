import { describe, test, expect } from "bun:test";

import { createPackSender, type PackSendFrame } from "./sender";

const REPO_ID = { kind: "workflow-run" as const, id: "deployment-1" };

describe("createPackSender", () => {
  test("send emits chunked push frames then a done frame", () => {
    const frames: PackSendFrame[] = [];
    const sender = createPackSender({ sendFrame: (f) => frames.push(f) });

    const pack = new Uint8Array(70_000); // > one PACK_CHUNK_SIZE
    pack.fill(0xab);

    void sender.send({
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-1",
      pack,
      ref: "refs/heads/main",
      commitSha: "abc123",
    });

    const types = frames.map((f) => f.type);
    expect(types).toEqual([
      "repo.pack.push",
      "repo.pack.push",
      "repo.pack.done",
    ]);
    const lastPush = frames[1];
    if (lastPush?.type !== "repo.pack.push") throw new Error("unreachable");
    expect(lastPush.seq).toBe(1);
    const done = frames[2];
    if (done?.type !== "repo.pack.done") throw new Error("unreachable");
    expect(done.commitSha).toBe("abc123");
    expect(done.ref).toBe("refs/heads/main");
  });

  test("handleAck resolves the pending Promise and returns true", async () => {
    const sender = createPackSender({ sendFrame: () => undefined });
    const promise = sender.send({
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-ack",
      pack: new Uint8Array([1, 2, 3]),
      ref: "refs/heads/main",
      commitSha: "deadbeef",
    });
    const matched = sender.handleAck({
      type: "repo.pack.ack",
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-ack",
    });
    expect(matched).toBe(true);
    await expect(promise).resolves.toBeUndefined();
    const unmatched = sender.handleAck({
      type: "repo.pack.ack",
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-ack",
    });
    expect(unmatched).toBe(false);
  });

  test("handleReject rejects the pending Promise with a structured error", async () => {
    const sender = createPackSender({ sendFrame: () => undefined });
    const promise = sender.send({
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-rej",
      pack: new Uint8Array([1, 2, 3]),
      ref: "refs/heads/main",
      commitSha: "deadbeef",
    });
    const matched = sender.handleReject({
      type: "repo.pack.reject",
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-rej",
      reason: "corrupt",
    });
    expect(matched).toBe(true);
    await expect(promise).rejects.toThrow(/corrupt/);
  });

  test("cancelAll rejects every pending transfer", async () => {
    const sender = createPackSender({ sendFrame: () => undefined });
    const a = sender.send({
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-a",
      pack: new Uint8Array([1]),
      ref: "refs/heads/main",
      commitSha: "a",
    });
    const b = sender.send({
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-b",
      pack: new Uint8Array([1]),
      ref: "refs/heads/main",
      commitSha: "b",
    });
    sender.cancelAll("connection closed");
    await expect(a).rejects.toThrow(/connection closed/);
    await expect(b).rejects.toThrow(/connection closed/);
  });

  test("send releases the pending entry when sendFrame throws synchronously", async () => {
    // A synchronous throw from `sendFrame` (closed transport, serializer
    // failure) used to leave the transferId in `pending` forever, so the
    // next `send` with the same transferId would reject at the
    // already-in-flight guard. Confirm the throw cleans up the entry and
    // a retry under the same transferId is admitted.
    let shouldThrow = true;
    const sender = createPackSender({
      sendFrame: () => {
        if (shouldThrow) throw new Error("transport closed");
      },
    });
    await expect(
      sender.send({
        agentAddress: "agent@example.com",
        repoId: REPO_ID,
        transferId: "tx-throw",
        pack: new Uint8Array([1]),
        ref: "refs/heads/main",
        commitSha: "a",
      }),
    ).rejects.toThrow(/transport closed/);

    shouldThrow = false;
    const retry = sender.send({
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-throw",
      pack: new Uint8Array([1]),
      ref: "refs/heads/main",
      commitSha: "b",
    });
    // No "already in flight" rejection means the entry was cleaned up.
    sender.handleAck({
      type: "repo.pack.ack",
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-throw",
    });
    await expect(retry).resolves.toBeUndefined();
  });

  test("send rejects when transferId is already in flight", async () => {
    const sender = createPackSender({ sendFrame: () => undefined });
    void sender.send({
      agentAddress: "agent@example.com",
      repoId: REPO_ID,
      transferId: "tx-dup",
      pack: new Uint8Array([1]),
      ref: "refs/heads/main",
      commitSha: "a",
    });
    await expect(
      sender.send({
        agentAddress: "agent@example.com",
        repoId: REPO_ID,
        transferId: "tx-dup",
        pack: new Uint8Array([1]),
        ref: "refs/heads/main",
        commitSha: "b",
      }),
    ).rejects.toThrow(/already in flight/);
  });
});
