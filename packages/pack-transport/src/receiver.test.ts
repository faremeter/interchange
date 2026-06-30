import { describe, test, expect } from "bun:test";
import { createPackReceiver } from "./receiver";
import { base64Encode } from "@intx/types";
import type { PackPushFrame, PackDoneFrame } from "@intx/types/sidecar";

function makePush(overrides: Partial<PackPushFrame> = {}): PackPushFrame {
  const agentAddress = overrides.agentAddress ?? "agent@test";
  return {
    type: "repo.pack.push",
    agentAddress,
    repoId: { kind: "agent-state", id: agentAddress },
    transferId: "t1",
    seq: 0,
    data: base64Encode(new TextEncoder().encode("chunk-data")),
    ...overrides,
  };
}

function makeDone(overrides: Partial<PackDoneFrame> = {}): PackDoneFrame {
  const agentAddress = overrides.agentAddress ?? "agent@test";
  return {
    type: "repo.pack.done",
    agentAddress,
    repoId: { kind: "agent-state", id: agentAddress },
    transferId: "t1",
    ref: "refs/heads/deploy",
    commitSha: "abc123",
    ...overrides,
  };
}

describe("PackReceiver", () => {
  test("accepts sequential push frames", () => {
    const receiver = createPackReceiver();
    expect(receiver.handlePush(makePush({ seq: 0 }))).toBeNull();
    expect(receiver.handlePush(makePush({ seq: 1 }))).toBeNull();
    expect(receiver.handlePush(makePush({ seq: 2 }))).toBeNull();
  });

  test("rejects out-of-order seq with corrupt", () => {
    const receiver = createPackReceiver();
    receiver.handlePush(makePush({ seq: 0 }));
    const reason = receiver.handlePush(makePush({ seq: 2 }));
    expect(reason).toBe("corrupt");
  });

  test("handleDone assembles chunks into a single pack", () => {
    const receiver = createPackReceiver();
    const chunk1 = base64Encode(new TextEncoder().encode("hello"));
    const chunk2 = base64Encode(new TextEncoder().encode(" world"));

    receiver.handlePush(makePush({ seq: 0, data: chunk1 }));
    receiver.handlePush(makePush({ seq: 1, data: chunk2 }));

    const result = receiver.handleDone(makeDone());
    expect(result).not.toBeNull();
    if (result === null) throw new Error("unreachable");

    const text = new TextDecoder().decode(result.pack);
    expect(text).toBe("hello world");
    expect(result.ref).toBe("refs/heads/deploy");
    expect(result.commitSha).toBe("abc123");
  });

  test("handleDone returns null for unknown transferId", () => {
    const receiver = createPackReceiver();
    const result = receiver.handleDone(makeDone({ transferId: "unknown" }));
    expect(result).toBeNull();
  });

  test("rejects concurrent transfer for same agent", () => {
    const receiver = createPackReceiver();
    receiver.handlePush(makePush({ transferId: "t1", seq: 0 }));

    const reason = receiver.handlePush(makePush({ transferId: "t2", seq: 0 }));
    expect(reason).toBe("conflict");
  });

  test("allows new transfer after previous completes", () => {
    const receiver = createPackReceiver();
    receiver.handlePush(makePush({ transferId: "t1", seq: 0 }));
    receiver.handleDone(makeDone({ transferId: "t1" }));

    const reason = receiver.handlePush(makePush({ transferId: "t2", seq: 0 }));
    expect(reason).toBeNull();
  });

  test("cancel removes in-flight transfer", () => {
    const receiver = createPackReceiver();
    receiver.handlePush(makePush({ transferId: "t1", seq: 0 }));
    expect(receiver.hasTransfer("t1")).toBe(true);

    receiver.cancel("t1");
    expect(receiver.hasTransfer("t1")).toBe(false);

    // New transfer for same agent is now allowed
    const reason = receiver.handlePush(makePush({ transferId: "t2", seq: 0 }));
    expect(reason).toBeNull();
  });

  test("returns corrupt for malformed base64 data instead of throwing", () => {
    const receiver = createPackReceiver();
    let reason: string | null = null;
    let threw = false;
    try {
      reason = receiver.handlePush(
        makePush({ seq: 0, data: "@@@not-valid@@@" }),
      );
    } catch {
      threw = true;
    }
    expect(threw).toBe(false);
    expect(reason).toBe("corrupt");

    // The malformed frame cleaned the transfer up, mirroring the seq-gap
    // path, so a fresh transfer for the same agent is accepted.
    expect(receiver.hasTransfer("t1")).toBe(false);
    expect(
      receiver.handlePush(makePush({ transferId: "t2", seq: 0 })),
    ).toBeNull();
  });

  test("cleans up after seq gap rejection", () => {
    const receiver = createPackReceiver();
    receiver.handlePush(makePush({ seq: 0 }));
    receiver.handlePush(makePush({ seq: 5 })); // gap → corrupt

    // Transfer is cleaned up, new one for same agent is allowed
    expect(receiver.hasTransfer("t1")).toBe(false);
    const reason = receiver.handlePush(makePush({ transferId: "t3", seq: 0 }));
    expect(reason).toBeNull();
  });

  test("handles multiple agents independently", () => {
    const receiver = createPackReceiver();
    const r1 = receiver.handlePush(
      makePush({ agentAddress: "a1@test", transferId: "t1", seq: 0 }),
    );
    const r2 = receiver.handlePush(
      makePush({ agentAddress: "a2@test", transferId: "t2", seq: 0 }),
    );
    expect(r1).toBeNull();
    expect(r2).toBeNull();
  });
});
