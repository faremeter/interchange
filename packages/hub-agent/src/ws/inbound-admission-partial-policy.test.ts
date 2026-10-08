// Relaxing ONE key must relax exactly that one condition.
//
// A verdict can raise a finding on each axis at once, and each axis carries its
// own author judgement: where a message raises two findings, relaxing one must
// leave the other enforced, and the message must be rejected BY the one the
// author did not relax. Nine reachable (signature, binding) pairs raise a
// finding on both axes, so the property has eighteen (pair, relaxed-axis)
// directions. Ten are covered by inbound-signature.test.ts; this file covers
// all eighteen, so a decision keyed on the single reduced `outcomeForVerdict`
// headline instead of the finding set fails those ten cases too. The ground
// only here: the eight directions that suite does not reach; admission when
// exactly the two raised keys are relaxed (six pairs over an absent or
// unparseable originator); the ordered two-element finding set for
// invalid/missing + absent; and a valid signature over an unparseable
// originator under `{absentFrom: "admit"}`.

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
// placeholder, and `error` short-circuits above the finding set.
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
