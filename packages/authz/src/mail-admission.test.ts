import { describe, test, expect } from "bun:test";

import { evaluateMailAdmission } from "./mail-admission";
import { mailAcceptResource, MAIL_ACCEPT_ACTION } from "./coord";
import type { MailAcceptCoordinate } from "./coord";
import type { ConditionRegistry, GrantRule } from "./types";

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

function acceptGrant(
  coord: MailAcceptCoordinate,
  effect: GrantRule["effect"],
  overrides: Partial<GrantRule> = {},
): GrantRule {
  return grant({
    resource: mailAcceptResource(coord.coordType, coord.id),
    action: MAIL_ACCEPT_ACTION,
    effect,
    ...overrides,
  });
}

// Each lower tier has a longer resource than the tier above it, so pattern
// specificity alone would pick the lower tier. A test where the higher tier
// wins therefore proves the tier ordering, not the id lengths.
const PRINCIPAL: MailAcceptCoordinate = {
  coordType: "principal",
  id: "prn_sender",
};
const ASSET: MailAcceptCoordinate = {
  coordType: "asset",
  id: "ast_sender_with_a_longer_id",
};
const TENANT: MailAcceptCoordinate = {
  coordType: "tenant",
  id: "tnt_sender_with_the_longest_id_of_all",
};

describe("evaluateMailAdmission", () => {
  test("null sender coordinates are not admitted", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: null,
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(TENANT, "allow"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("throws on a conditioned mail.accept grant when no registry is passed", async () => {
    // A conditioned grant with no registry would be silently skipped by
    // evaluateGrants; for a deny that fails OPEN. The helper must refuse to
    // evaluate and fail loud instead of admitting past an unevaluable deny.
    await expect(
      evaluateMailAdmission({
        senderCoordinates: [PRINCIPAL],
        recipientGrants: [
          acceptGrant(PRINCIPAL, "deny", {
            conditions: { time_window: { after: "2020-01-01T00:00:00Z" } },
          }),
        ],
      }),
    ).rejects.toThrow(/condition registry/);
  });

  test("a conditioned grant with a registry supplied does not throw", async () => {
    const registry: ConditionRegistry = { time_window: () => true };
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "deny", {
          conditions: { time_window: { after: "2020-01-01T00:00:00Z" } },
        }),
      ],
      registry,
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
  });

  test("principal coordinate matching an allow accept-grant is admitted", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL],
      recipientGrants: [acceptGrant(PRINCIPAL, "allow")],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("tenant coordinate matching a mail.accept:tenant allow is admitted", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [TENANT],
      recipientGrants: [acceptGrant(TENANT, "allow")],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("a coordinate wildcard accept-grant admits", async () => {
    // A `mail.accept:*` allow lives in-namespace and survives the namespace filter,
    // so it legitimately admits any coordinate.
    const result = await evaluateMailAdmission({
      senderCoordinates: [ASSET],
      recipientGrants: [
        grant({
          resource: "mail.accept:*",
          action: MAIL_ACCEPT_ACTION,
          effect: "allow",
        }),
      ],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("no matching accept-grant is not admitted (default-deny)", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        acceptGrant({ coordType: "principal", id: "prn_other" }, "allow"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("empty recipient grants are not admitted", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT, ASSET],
      recipientGrants: [],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("a broad `*` allow with no mail.accept grant does not admit", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [grant({ resource: "*", action: "*", effect: "allow" })],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("a broad `mail.*` allow with no mail.accept grant does not admit", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        grant({
          resource: "mail.*",
          action: MAIL_ACCEPT_ACTION,
          effect: "allow",
        }),
        grant({ resource: "*", action: MAIL_ACCEPT_ACTION, effect: "allow" }),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("a principal allow is an exception to a tenant deny", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(TENANT, "deny"),
      ],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("a principal deny overrides a tenant allow", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "deny"),
        acceptGrant(TENANT, "allow"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
  });

  test("an asset grant outranks a tenant grant", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [ASSET, TENANT],
      recipientGrants: [
        acceptGrant(ASSET, "allow"),
        acceptGrant(TENANT, "deny"),
      ],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("the ordering is independent of coordinate order", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [TENANT, ASSET, PRINCIPAL],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(ASSET, "deny"),
        acceptGrant(TENANT, "deny"),
      ],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("a broad mail.accept:* deny admits only the named exception", async () => {
    const recipientGrants = [
      grant({
        resource: "mail.accept:*",
        action: MAIL_ACCEPT_ACTION,
        effect: "deny",
      }),
      acceptGrant(PRINCIPAL, "allow"),
    ];

    const friend = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants,
    });
    expect(friend.admit).toBe(true);
    expect(friend.effect).toBe("allow");

    const stranger = await evaluateMailAdmission({
      senderCoordinates: [
        { coordType: "principal", id: "prn_stranger" },
        TENANT,
      ],
      recipientGrants,
    });
    expect(stranger.admit).toBe(false);
    expect(stranger.effect).toBe("deny");
  });

  test("a tenant deny overrides a namespace-wide allow", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        grant({
          resource: "mail.accept:*",
          action: MAIL_ACCEPT_ACTION,
          effect: "allow",
        }),
        acceptGrant(TENANT, "deny"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
  });

  test("a wildcard id does not lower the tier", async () => {
    // The exact tenant deny is far more specific than the principal wildcard,
    // so only the tier ordering lets the principal allow win.
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        grant({
          resource: "mail.accept:principal:*",
          action: MAIL_ACCEPT_ACTION,
          effect: "allow",
        }),
        acceptGrant(TENANT, "deny"),
      ],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("a wildcard in the type segment ranks as namespace-wide", async () => {
    // `mail.accept:principal*` has more literal characters than
    // `mail.accept:tenant:*`, so by specificity alone its deny would win. It
    // names no coord type, so it ranks below the tenant allow.
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        grant({
          resource: "mail.accept:principal*",
          action: MAIL_ACCEPT_ACTION,
          effect: "deny",
        }),
        grant({
          resource: "mail.accept:tenant:*",
          action: MAIL_ACCEPT_ACTION,
          effect: "allow",
        }),
      ],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("within one coordinate a more specific grant wins", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL],
      recipientGrants: [
        grant({
          resource: "mail.accept:principal:*",
          action: MAIL_ACCEPT_ACTION,
          effect: "deny",
        }),
        acceptGrant(PRINCIPAL, "allow"),
      ],
    });

    expect(result.admit).toBe(true);
  });

  test("at the same coordinate and specificity a deny beats an allow", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(PRINCIPAL, "deny"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
  });

  test("a deny overrides an accept allow only with the accept action", async () => {
    // Specificity counts the action, so a deny with action `*` ranks below
    // the `accept` allow at the same coordinate and does not override it.
    const exactDeny = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(PRINCIPAL, "deny"),
      ],
    });
    const wildcardActionDeny = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(PRINCIPAL, "deny", { action: "*" }),
      ],
    });

    expect(exactDeny.admit).toBe(false);
    expect(exactDeny.effect).toBe("deny");
    expect(wildcardActionDeny.admit).toBe(true);
    expect(wildcardActionDeny.effect).toBe("allow");
  });

  test("an exact principal allow outranks every broader deny", async () => {
    // A deny written before an exact allow for a new party exists does not
    // refuse that party: the namespace and tenant denies rank at lower tiers,
    // and the principal wildcard deny is less specific.
    const newParty: MailAcceptCoordinate = {
      coordType: "principal",
      id: "prn_new_party",
    };
    const result = await evaluateMailAdmission({
      senderCoordinates: [newParty, TENANT],
      recipientGrants: [
        grant({
          resource: "mail.accept:*",
          action: MAIL_ACCEPT_ACTION,
          effect: "deny",
        }),
        grant({
          resource: "mail.accept:principal:*",
          action: MAIL_ACCEPT_ACTION,
          effect: "deny",
        }),
        acceptGrant(TENANT, "deny"),
        acceptGrant(newParty, "allow"),
      ],
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });

  test("an ask that wins is not admitted", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "ask"),
        acceptGrant(TENANT, "allow"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("ask on the only matching coordinate is not admitted", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL],
      recipientGrants: [acceptGrant(PRINCIPAL, "ask")],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("a conditioned deny with a registry that applies it blocks admission", async () => {
    const registry: ConditionRegistry = {
      always: () => true,
    };
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "deny", { conditions: { always: true } }),
        acceptGrant(TENANT, "allow"),
      ],
      registry,
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
  });

  test("a conditioned deny whose condition does not hold does not block admission", async () => {
    const registry: ConditionRegistry = {
      never: () => false,
    };
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "deny", { conditions: { never: true } }),
        acceptGrant(TENANT, "allow"),
      ],
      registry,
    });

    expect(result.admit).toBe(true);
    expect(result.effect).toBe("allow");
  });
});
