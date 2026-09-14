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

const PRINCIPAL: MailAcceptCoordinate = {
  coordType: "principal",
  id: "prn_sender",
};
const TENANT: MailAcceptCoordinate = { coordType: "tenant", id: "tnt_sender" };
const DEFINITION: MailAcceptCoordinate = {
  coordType: "definition",
  id: "def_sender",
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
    // A `mail.accept:*` allow lives in-namespace and survives the C1 filter,
    // so it legitimately admits any coordinate.
    const result = await evaluateMailAdmission({
      senderCoordinates: [DEFINITION],
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
      senderCoordinates: [PRINCIPAL, TENANT, DEFINITION],
      recipientGrants: [],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("C1: a broad `*` allow with no mail.accept grant does not admit", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [grant({ resource: "*", action: "*", effect: "allow" })],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBeNull();
  });

  test("C1: a broad `mail.*` allow with no mail.accept grant does not admit", async () => {
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

  test("deny-wins: an allow on one coordinate and a deny on another does not admit", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [PRINCIPAL, TENANT],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(TENANT, "deny"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
  });

  test("deny-wins is independent of coordinate order", async () => {
    const result = await evaluateMailAdmission({
      senderCoordinates: [TENANT, PRINCIPAL],
      recipientGrants: [
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(TENANT, "deny"),
      ],
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
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
        acceptGrant(PRINCIPAL, "allow"),
        acceptGrant(TENANT, "deny", { conditions: { always: true } }),
      ],
      registry,
    });

    expect(result.admit).toBe(false);
    expect(result.effect).toBe("deny");
  });
});
