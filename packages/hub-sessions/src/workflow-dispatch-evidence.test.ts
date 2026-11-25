import { describe, expect, test } from "bun:test";

import {
  dispatchEvidenceOfPack,
  type CommitDispatchEvidence,
} from "./workflow-dispatch-evidence";

const ADDRESS = "run_dep@tnt.example";
const OTHER = "run_other@tnt.example";

function commit(
  evidence: Partial<CommitDispatchEvidence>,
): CommitDispatchEvidence {
  return { consumed: [], accepted: [], pruned: [], ...evidence };
}

const rejected = (deliveryKey: string, address = ADDRESS) => ({
  messageId: `<${deliveryKey}@x>`,
  deliveryKey,
  address,
  rejection: { code: "malformed_mail", message: "Missing MIME bytes" },
});

describe("dispatchEvidenceOfPack", () => {
  test("drops a consumed entry a later commit prunes", () => {
    expect(
      dispatchEvidenceOfPack([
        commit({ consumed: [rejected("dk1:a")] }),
        commit({ pruned: [{ address: ADDRESS, identity: "dk1:a" }] }),
      ]).consumed,
    ).toEqual([]);
  });

  test("keeps an entry filed again after a prune", () => {
    const again = {
      messageId: "<dk1:a@x>",
      deliveryKey: "dk1:a",
      address: ADDRESS,
    };
    expect(
      dispatchEvidenceOfPack([
        commit({ consumed: [rejected("dk1:a")] }),
        commit({ pruned: [{ address: ADDRESS, identity: "dk1:a" }] }),
        commit({ consumed: [again] }),
      ]).consumed,
    ).toEqual([again]);
  });

  test("a prune names an entry by address and identity", () => {
    expect(
      dispatchEvidenceOfPack([
        commit({ consumed: [rejected("dk1:a", OTHER)] }),
        commit({ pruned: [{ address: ADDRESS, identity: "dk1:a" }] }),
      ]).consumed,
    ).toEqual([rejected("dk1:a", OTHER)]);
  });

  test("keeps every accepted event", () => {
    const signal = {
      runId: "run-a",
      kind: "signal",
      id: "sig-1",
      legacy: false,
    } as const;
    expect(
      dispatchEvidenceOfPack([
        commit({ accepted: [signal] }),
        commit({ pruned: [{ address: ADDRESS, identity: "sig-1" }] }),
      ]).accepted,
    ).toEqual([signal]);
  });
});
