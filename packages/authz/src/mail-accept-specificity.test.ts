import { describe, test, expect } from "bun:test";

import { evaluateGrants } from "./evaluate";
import { MAIL_ACCEPT_ACTION, mailAcceptResource } from "./coord";
import type { GrantRule } from "./types";

// These tests pin the grant-evaluation behavior for `mail.accept` coordinate
// resources against the unchanged comparator. The comparator sorts by a flat
// character-count specificity and breaks ties by effect priority
// (deny > ask > allow). No coordinate-specific retune exists; the ordering the
// design requires for these shapes is emergent from that existing behavior.
// These tests lock it in so a future comparator change cannot silently regress
// it. They also never place grants of two different coord types in a single
// `evaluateGrants` call, because a coord type is an anchored literal segment and
// grants of different coord types never co-occur in one matching set.

function grant(
  overrides: Partial<GrantRule> &
    Pick<GrantRule, "resource" | "action" | "effect">,
): GrantRule {
  return {
    id: `grt_${Math.random().toString(36).slice(2, 10)}`,
    origin: "role",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
    ...overrides,
  };
}

describe("mail.accept coordinate specificity", () => {
  test("floor: at equal specificity a competing ask beats allow", async () => {
    const resource = mailAcceptResource("principal", "prn_abc");
    const grants = [
      grant({ resource, action: MAIL_ACCEPT_ACTION, effect: "allow" }),
      grant({ resource, action: MAIL_ACCEPT_ACTION, effect: "ask" }),
    ];

    const result = await evaluateGrants(grants, resource, MAIL_ACCEPT_ACTION);

    expect(result.effect).toBe("ask");
  });

  test("exact coordinate allow out-ranks wildcard deny of the same coord type", async () => {
    const resource = mailAcceptResource("principal", "prn_abc");
    const wildcard = mailAcceptResource("principal", "*");
    const grants = [
      grant({ resource: wildcard, action: MAIL_ACCEPT_ACTION, effect: "deny" }),
      grant({ resource, action: MAIL_ACCEPT_ACTION, effect: "allow" }),
    ];

    const result = await evaluateGrants(grants, resource, MAIL_ACCEPT_ACTION);

    expect(result.effect).toBe("allow");
    expect(result.resolvedBy?.resource).toBe(resource);
  });

  test("effect breaks ties only at equal specificity", async () => {
    const resource = mailAcceptResource("tenant", "tnt_abc");
    const wildcard = mailAcceptResource("tenant", "*");

    // At equal specificity, effect priority decides: deny beats allow.
    const equal = [
      grant({ resource, action: MAIL_ACCEPT_ACTION, effect: "allow" }),
      grant({ resource, action: MAIL_ACCEPT_ACTION, effect: "deny" }),
    ];
    expect(
      (await evaluateGrants(equal, resource, MAIL_ACCEPT_ACTION)).effect,
    ).toBe("deny");

    // When specificity differs, it dominates: a more-specific allow beats a
    // less-specific deny. Effect never overrides a specificity difference.
    const unequal = [
      grant({ resource: wildcard, action: MAIL_ACCEPT_ACTION, effect: "deny" }),
      grant({ resource, action: MAIL_ACCEPT_ACTION, effect: "allow" }),
    ];
    const result = await evaluateGrants(unequal, resource, MAIL_ACCEPT_ACTION);
    expect(result.effect).toBe("allow");
    expect(result.resolvedBy?.resource).toBe(resource);
  });
});
