import { describe, expect, test } from "bun:test";

import {
  describeProbeGateRefusal,
  type ProbeGateRefusal,
} from "./workflow-probe-gate";

describe("describeProbeGateRefusal", () => {
  const cases: [ProbeGateRefusal, string][] = [
    [
      {
        ok: false,
        reason: "wire_hash_mismatch",
        shippedWireHash: "aaa",
        recomputedWireHash: "bbb",
      },
      "wire_hash_mismatch: the shipped hash aaa does not match the recomputed hash bbb",
    ],
    [
      {
        ok: false,
        reason: "grants_not_approved",
        unapprovedGrants: ["tool:read", "tool:write"],
      },
      "grants_not_approved: tool:read, tool:write",
    ],
    [
      {
        ok: false,
        reason: "grant_requirements_not_approved",
        unapprovedGrantRequirements: [
          {
            resource: "wallet:wal_1",
            action: "spend",
            source: "invoker",
            conditions: { maxAmount: 5 },
          },
        ],
      },
      "grant_requirements_not_approved: spend on wallet:wal_1 (invoker)",
    ],
    [
      {
        ok: false,
        reason: "creator_requirements_unsupported_for_package_registry_tarball",
        creatorGrantRequirements: [
          { resource: "mail:*", action: "send", source: "creator" },
          { resource: "wallet:wal_2", action: "spend", source: "creator" },
        ],
      },
      "creator_requirements_unsupported_for_package_registry_tarball: a " +
        "workflow from a package-registry tarball cannot declare " +
        "creator-sourced grant requirements, because the hub does not know " +
        "who published the package; declared: send on mail:* (creator); " +
        "spend on wallet:wal_2 (creator)",
    ],
    [
      {
        ok: false,
        reason: "unimplemented_trigger",
        unimplementedTriggerTypes: ["cron", "webhook"],
      },
      "unimplemented_trigger: cron, webhook",
    ],
    [
      {
        ok: false,
        reason: "steps_without_grant_record",
        stepsWithoutGrantRecord: [],
        message: "step b has no record",
      },
      "steps_without_grant_record: step b has no record",
    ],
    [
      {
        ok: false,
        reason: "mail_accept_grant_requirements",
        mailAcceptGrantRequirements: [
          { resource: "mail.accept:*", action: "accept", source: "creator" },
        ],
      },
      "mail_accept_grant_requirements: accept-rules are declared only " +
        "through mailAccept; declared: accept on mail.accept:* (creator)",
    ],
    [
      {
        ok: false,
        reason: "mail_accept_markers_undeclared",
        undeclaredMailAcceptMarkers: ["mail.accept:*", "mail.accept:tenant"],
      },
      "mail_accept_markers_undeclared: the projection's mailAccept does not " +
        "produce mail.accept:*, mail.accept:tenant",
    ],
    [
      {
        ok: false,
        reason: "credential_bindings_invalid",
        violations: [],
        message: 'two bindings name action push and handle "github"',
      },
      'credential_bindings_invalid: two bindings name action push and handle "github"',
    ],
  ];

  test.each(cases)("names the refusal and what it refused", (refusal, text) => {
    expect(describeProbeGateRefusal(refusal)).toBe(text);
  });

  test("leaves requirement conditions out", () => {
    const [refusal] = cases[2] ?? [];
    if (refusal === undefined) throw new Error("missing case");
    expect(describeProbeGateRefusal(refusal)).not.toContain("maxAmount");
  });
});
