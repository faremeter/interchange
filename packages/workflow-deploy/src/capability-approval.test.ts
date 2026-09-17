// The contract `createApprovalSet` and `isApprovedGrantRequirement` enforce.
//
// The two kinds of approved item are tested by two different operations, and
// these cases pin both:
//
//   - A grant string is approved by exact membership in `grants`. It is never
//     consulted when a requirement is being tested, and a requirement is never
//     consulted when a grant string is.
//   - A requirement is approved by `node:util`'s `isDeepStrictEqual` over the
//     whole record against `requirements`. That is stricter than "same fields,
//     same values": a present-but-undefined optional key is NOT equal to an
//     absent one.
//
// `createApprovalSet` is where that strictness is made safe. It validates every
// requirement, and `GrantRequirement` admits an object or `null` for
// `conditions` but never `undefined`, so an approval assembled by spreading a
// partially-filled record (`{ ...base, conditions }` where `conditions` is
// undefined) is rejected at construction. Before the parse existed, such an
// approval was accepted and then silently failed to match a probe requirement
// that omits the key, refusing the deploy with
// `grant_requirements_not_approved` and nothing pointing at the cause.

import { describe, test, expect } from "bun:test";

import type { GrantRequirement } from "@intx/types";

import {
  approvalItemsFromSet,
  approvalSetFromItems,
  createApprovalSet,
  isApprovedGrantRequirement,
} from "./capability-approval";

const DECLARED: GrantRequirement = {
  resource: "wallet:w1",
  action: "spend",
  source: "creator",
};

describe("isApprovedGrantRequirement equality", () => {
  test("a structurally identical distinct object matches", () => {
    const approved: GrantRequirement = {
      resource: "wallet:w1",
      action: "spend",
      source: "creator",
    };
    expect(
      isApprovedGrantRequirement(createApprovalSet([], [approved]), DECLARED),
    ).toBe(true);
  });

  test("key order does not matter", () => {
    const reordered = {
      source: "creator",
      action: "spend",
      resource: "wallet:w1",
    };
    expect(
      isApprovedGrantRequirement(createApprovalSet([], [reordered]), DECLARED),
    ).toBe(true);
  });

  test("a JSON round-trip of the approved item still matches", () => {
    const roundTripped: unknown = JSON.parse(JSON.stringify(DECLARED));
    expect(
      isApprovedGrantRequirement(
        createApprovalSet([], [roundTripped]),
        DECLARED,
      ),
    ).toBe(true);
  });

  test("an explicitly-undefined optional key is rejected at construction", () => {
    // Not a spurious rejection three layers down at the gate: the malformed
    // approval never becomes an `ApprovalSet` at all.
    expect(() =>
      createApprovalSet(
        [],
        [
          {
            resource: "wallet:w1",
            action: "spend",
            source: "creator",
            conditions: undefined,
          },
        ],
      ),
    ).toThrow(/conditions/);
  });

  test("an omitted optional key matches a declaration that omits it too", () => {
    // The counterweight to the case above: rejecting the undefined-valued form
    // must not also reject the well-formed omission it was trying to express.
    const approved = {
      resource: "wallet:w1",
      action: "spend",
      source: "creator",
    };
    expect(
      isApprovedGrantRequirement(createApprovalSet([], [approved]), DECLARED),
    ).toBe(true);
  });

  test("grant strings are never consulted for a requirement", () => {
    const approvals = createApprovalSet(["wallet:w1", "tool:spend", "creator"]);
    expect(isApprovedGrantRequirement(approvals, DECLARED)).toBe(false);
  });

  test("an approved requirement is never consulted for a grant string", () => {
    // The mirror of the case above. A requirement whose resource reads like a
    // grant string does not approve that grant string; the two kinds live in
    // separate fields precisely so neither can stand in for the other.
    const approvals = createApprovalSet([], [DECLARED]);
    expect(approvals.grants.has("wallet:w1")).toBe(false);
    expect(approvals.grants.size).toBe(0);
  });
});

// The persisted form is a flat `ApprovalItem` list and the in-memory form is
// the partitioned struct, so one boundary converts between them. These cases
// pin that the partition is by kind, that it survives the JSON round trip a
// stored row goes through, and that the requirement parse applies on the way
// back in -- a row is data from outside the program even when this program
// wrote it.
describe("the persisted/in-memory approval boundary", () => {
  test("a flat item list partitions by kind", () => {
    const approvals = approvalSetFromItems([
      "tool:fetch",
      DECLARED,
      "effect:log",
    ]);
    expect([...approvals.grants].sort()).toEqual(["effect:log", "tool:fetch"]);
    expect(approvals.requirements).toEqual([DECLARED]);
  });

  test("flattening and re-partitioning preserves both groups", () => {
    const original = createApprovalSet(
      ["tool:fetch", "effect:log"],
      [DECLARED],
    );
    const items: unknown = JSON.parse(
      JSON.stringify(approvalItemsFromSet(original)),
    );
    const restored = approvalSetFromItems(
      Array.isArray(items) ? items : [items],
    );
    expect([...restored.grants].sort()).toEqual([...original.grants].sort());
    expect(restored.requirements).toEqual(original.requirements);
  });

  test("a malformed requirement in a stored list is rejected on rehydration", () => {
    // The same parse `createApprovalSet` applies, reached through the
    // rehydration path. A row this program wrote a version ago is still
    // outside the program.
    expect(() =>
      approvalSetFromItems([
        "tool:fetch",
        { resource: "wallet:w1", action: "spend" },
      ]),
    ).toThrow(/source/);
  });
});
