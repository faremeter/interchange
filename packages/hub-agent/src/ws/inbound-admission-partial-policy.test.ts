// Relaxing ONE key must relax exactly that one condition.
//
// A verdict can raise a finding on each axis at once, and each axis carries its
// own author judgement. Where a message raises two findings, relaxing one of
// them must leave the other enforced, and the message must be rejected BY the
// one the author did not relax. That is the property `decideInboundAdmission`
// exists for, and the rule docs/INBOUND_MAIL_POLICY.md states.
//
// Nine reachable (signature, binding) pairs raise a finding on both axes, so the
// property has eighteen (pair, relaxed-axis) directions. Ten of them are covered
// by inbound-signature.test.ts: "relaxing unknown does not admit a message that
// also names no originator", the three "relaxing untrustedFrom does not admit a
// {invalid,missing,unknown} signature behind an unparseable From" cases, and the
// six mismatched-`From` directions in its "relaxing {invalid,missing,unknown}
// does not admit a From that contradicts the stamp" and "relaxing
// mismatchedFrom alone" loops. This file covers all eighteen, so what it adds is
// breadth rather than unique detection: a decision keyed on the single reduced
// `outcomeForVerdict` headline instead of on the finding set fails those ten
// cases too. Neither file is the only thing standing between that regression and
// a green build, so do not read either as made redundant by the other.
//
// The ground that is only here:
//   - the eight (pair, relaxed-axis) directions that suite does not reach.
//     Among them: relaxing `absentFrom` and expecting the signature key to be
//     the one that rejects, which it asserts for no pair at all.
//   - admission when exactly the two raised keys are relaxed, for the six pairs
//     over an absent or unparseable originator. That suite reaches it for the
//     three mismatched-`From` pairs, and otherwise only through wider policies:
//     one four-key case for invalid + unparseable, and the relax-everything
//     table.
//   - the ordered two-element finding set for invalid + absent and for
//     missing + absent.
//   - a valid signature over an unparseable originator under
//     `{absentFrom: "admit"}`. Its twin -- no originator at all under
//     `{untrustedFrom: "admit"}` -- that suite pins six times over in its
//     blank-`From` loop.

import { describe, expect, test } from "bun:test";
import type { AuthorControllableOutcome } from "@intx/types/runtime";

import {
  decideInboundAdmission,
  resolveInboundMailPolicy,
  type InboundSignatureVerdict,
} from "./inbound-signature";

function verdict(
  signature: InboundSignatureVerdict["signature"],
  fromMatch: InboundSignatureVerdict["fromMatch"],
): InboundSignatureVerdict {
  return {
    signature,
    fromMatch,
    authenticatedSender: "alpha@test.example",
    messageFrom:
      fromMatch === "absent"
        ? null
        : fromMatch === "mismatch"
          ? "beta@test.example"
          : "alpha@test.example",
  };
}

// Every reachable pair that raises a finding on BOTH axes, with the two
// findings it raises. `notEvaluated` is only ever an `error` verdict's
// placeholder, and `error` short-circuits above the finding set, so it is not
// here.
const TWO_FINDING_PAIRS: [
  signature: InboundSignatureVerdict["signature"],
  fromMatch: InboundSignatureVerdict["fromMatch"],
  fromSignature: AuthorControllableOutcome,
  fromBinding: AuthorControllableOutcome,
][] = [
  ["invalid", "absent", "invalid", "absentFrom"],
  ["invalid", "unparseable", "invalid", "untrustedFrom"],
  ["invalid", "mismatch", "invalid", "mismatchedFrom"],
  ["missing", "absent", "missing", "absentFrom"],
  ["missing", "unparseable", "missing", "untrustedFrom"],
  ["missing", "mismatch", "missing", "mismatchedFrom"],
  ["unknown", "absent", "unknown", "absentFrom"],
  ["unknown", "unparseable", "unknown", "untrustedFrom"],
  ["unknown", "mismatch", "unknown", "mismatchedFrom"],
];

describe("relaxing one key does not admit a message that raises two findings", () => {
  for (const [
    signature,
    fromMatch,
    fromSignature,
    fromBinding,
  ] of TWO_FINDING_PAIRS) {
    test(`${signature}/${fromMatch} raises both ${fromSignature} and ${fromBinding}`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, fromMatch),
        resolveInboundMailPolicy({}),
      );
      // Order matters for the log line: the signature axis is named first.
      expect(decision.findings).toEqual([fromSignature, fromBinding]);
    });

    test(`${signature}/${fromMatch} stays rejected when only ${fromSignature} is relaxed`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, fromMatch),
        resolveInboundMailPolicy({ [fromSignature]: "admit" }),
      );
      expect(decision.rejectedBy).toBe(fromBinding);
    });

    test(`${signature}/${fromMatch} stays rejected when only ${fromBinding} is relaxed`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, fromMatch),
        resolveInboundMailPolicy({ [fromBinding]: "admit" }),
      );
      expect(decision.rejectedBy).toBe(fromSignature);
    });

    test(`${signature}/${fromMatch} is admitted only when both are relaxed`, () => {
      const decision = decideInboundAdmission(
        verdict(signature, fromMatch),
        resolveInboundMailPolicy({
          [fromSignature]: "admit",
          [fromBinding]: "admit",
        }),
      );
      expect(decision.rejectedBy).toBeNull();
    });
  }

  // The sibling keys are independent: relaxing the one that tolerates an odd
  // correspondent header must not also accept mail that names no originator.
  test("untrustedFrom and absentFrom do not stand in for each other", () => {
    const absent = decideInboundAdmission(
      verdict("valid", "absent"),
      resolveInboundMailPolicy({ untrustedFrom: "admit" }),
    );
    expect(absent.rejectedBy).toBe("absentFrom");

    const unparseable = decideInboundAdmission(
      verdict("valid", "unparseable"),
      resolveInboundMailPolicy({ absentFrom: "admit" }),
    );
    expect(unparseable.rejectedBy).toBe("untrustedFrom");
  });
});
