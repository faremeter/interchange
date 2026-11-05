import { describe, test, expect } from "bun:test";

import {
  evaluateMailSend,
  mailAddressDomain,
  mailSendResourceProblem,
} from "./mail-send";
import {
  MAIL_SEND_ACTION,
  mailSendResource,
  type MailSendCoordinate,
} from "./coord";
import { HUB_CONDITION_REGISTRY } from "./hub-condition-registry";
import type { GrantRule } from "./types";

function grant(
  resource: string,
  effect: GrantRule["effect"],
  action: string = MAIL_SEND_ACTION,
): GrantRule {
  return {
    id: `grt_${Math.random().toString(36).slice(2, 10)}`,
    resource,
    action,
    effect,
    origin: "role",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

// Each lower tier has a longer id than the tier above it, so pattern
// specificity alone would pick the lower tier. A test where the higher tier
// wins therefore proves the tier ordering, not the id lengths.
const PRINCIPAL: MailSendCoordinate = { coordType: "principal", id: "prn_r" };
const ASSET: MailSendCoordinate = { coordType: "asset", id: "ast_recipient" };
const TENANT: MailSendCoordinate = {
  coordType: "tenant",
  id: "tnt_recipient_with_a_longer_id",
};
const DOMAIN: MailSendCoordinate = {
  coordType: "domain",
  id: "recipient-domain-with-the-longest-id.example.com",
};
const LOCAL = [DOMAIN, TENANT, ASSET, PRINCIPAL];

function exact(coord: MailSendCoordinate, effect: GrantRule["effect"]) {
  return grant(mailSendResource(coord.coordType, coord.id), effect);
}

async function decide(
  recipientCoordinates: readonly MailSendCoordinate[],
  senderGrants: GrantRule[],
) {
  return evaluateMailSend({
    recipientCoordinates,
    senderGrants,
    registry: HUB_CONDITION_REGISTRY,
  });
}

describe("evaluateMailSend", () => {
  test("an allow grant on the recipient allows the send", async () => {
    const result = await decide(LOCAL, [exact(PRINCIPAL, "allow")]);
    expect(result.allowed).toBe(true);
    expect(result.effect).toBe("allow");
    expect(result.grant?.resource).toBe("mail.send:principal:prn_r");
  });

  test("a deny grant on the recipient refuses the send", async () => {
    const result = await decide(LOCAL, [exact(TENANT, "deny")]);
    expect(result).toMatchObject({ allowed: false, effect: "deny" });
  });

  test("no matching grant refuses the send", async () => {
    const result = await decide(LOCAL, [
      exact({ coordType: "principal", id: "prn_other" }, "allow"),
    ]);
    expect(result).toEqual({ allowed: false, effect: null, grant: null });
  });

  test("grants outside the mail.send namespace do not allow", async () => {
    const result = await decide(LOCAL, [
      grant("*", "allow", "*"),
      grant("mail.*", "allow", "*"),
      grant("mail.accept:*:*", "allow"),
    ]);
    expect(result.allowed).toBe(false);
    expect(result.grant).toBeNull();
  });

  test("a grant with another action does not allow", async () => {
    const result = await decide(LOCAL, [
      grant("mail.send:*:*", "allow", "accept"),
    ]);
    expect(result.allowed).toBe(false);
  });

  test("a wildcard action allows a send", async () => {
    const result = await decide(LOCAL, [grant("mail.send:*:*", "allow", "*")]);
    expect(result.allowed).toBe(true);
  });

  test("a namespace-wide grant allows a recipient named only by its domain", async () => {
    const result = await decide([DOMAIN], [grant("mail.send:*:*", "allow")]);
    expect(result.allowed).toBe(true);
  });

  test("a recipient with no coordinates is refused", async () => {
    const result = await decide([], [grant("mail.send:*:*", "allow")]);
    expect(result).toEqual({ allowed: false, effect: null, grant: null });
  });

  test("a principal deny wins over a tenant allow", async () => {
    const result = await decide(LOCAL, [
      exact(TENANT, "allow"),
      exact(PRINCIPAL, "deny"),
    ]);
    expect(result.effect).toBe("deny");
  });

  test("a principal allow wins over a namespace-wide deny", async () => {
    const result = await decide(LOCAL, [
      grant("mail.send:*:*", "deny"),
      exact(PRINCIPAL, "allow"),
    ]);
    expect(result.effect).toBe("allow");
  });

  test("an asset grant wins over a tenant grant", async () => {
    const result = await decide(LOCAL, [
      exact(TENANT, "deny"),
      exact(ASSET, "allow"),
    ]);
    expect(result.effect).toBe("allow");
  });

  test("a tenant grant of either effect wins over an exact domain grant", async () => {
    const allowed = await decide(LOCAL, [
      exact(DOMAIN, "deny"),
      exact(TENANT, "allow"),
    ]);
    const denied = await decide(LOCAL, [
      exact(DOMAIN, "allow"),
      exact(TENANT, "deny"),
    ]);
    expect(allowed.effect).toBe("allow");
    expect(denied.effect).toBe("deny");
  });

  test("a domain grant wins over a namespace-wide grant", async () => {
    const result = await decide(
      [DOMAIN],
      [grant("mail.send:*:*", "allow"), exact(DOMAIN, "deny")],
    );
    expect(result.effect).toBe("deny");
  });

  test("a wildcard domain id matches subdomains at the domain tier", async () => {
    const sub: MailSendCoordinate = {
      coordType: "domain",
      id: "a.example.com",
    };
    const apex: MailSendCoordinate = { coordType: "domain", id: "example.com" };
    const grants = [
      grant("mail.send:*:*", "allow"),
      grant("mail.send:domain:*.example.com", "deny"),
    ];

    expect((await decide([sub], grants)).effect).toBe("deny");
    expect((await decide([apex], grants)).effect).toBe("allow");
  });

  test("a wildcard type segment ranks below a domain grant", async () => {
    // Both coordinate patterns hold six literal characters, so only the tier
    // rule can pick the domain grant.
    const result = await decide(
      [DOMAIN],
      [
        grant("mail.send:*domain:*", "deny"),
        grant("mail.send:domain:*", "allow"),
      ],
    );
    expect(result.grant?.resource).toBe("mail.send:domain:*");
    expect(result.effect).toBe("allow");
  });

  test("an ask that wins does not allow", async () => {
    const result = await decide(LOCAL, [exact(PRINCIPAL, "ask")]);
    expect(result.allowed).toBe(false);
    expect(result.effect).toBeNull();
    expect(result.grant?.effect).toBe("ask");
  });
});

// A run's own send grant is `mail.send:*:*` / `send` / allow. An operator
// narrows it with a deny, whatever action or namespace-wide pattern the deny
// spells.
describe("an operator deny under a run's send grant", () => {
  const runGrant = grant("mail.send:*:*", "allow");

  test.each([
    ["mail.send:*", MAIL_SEND_ACTION],
    ["mail.send:*:*", "*"],
    ["mail.send:*", "*"],
  ])("a deny %p / %p refuses the send", async (resource, action) => {
    const result = await decide(LOCAL, [
      runGrant,
      grant(resource, "deny", action),
    ]);
    expect(result.allowed).toBe(false);
    expect(result.effect).toBe("deny");
  });

  test("a deny on one domain id refuses only that domain", async () => {
    const grants = [runGrant, grant("mail.send:*:evil.com", "deny")];
    const evil = await decide(
      [{ coordType: "domain", id: "evil.com" }],
      grants,
    );
    const other = await decide([DOMAIN], grants);
    expect(evil.allowed).toBe(false);
    expect(other.allowed).toBe(true);
  });

  test("a domain deny with action * refuses past a domain allow", async () => {
    const result = await decide(
      [DOMAIN],
      [
        grant(mailSendResource("domain", DOMAIN.id), "allow"),
        grant(mailSendResource("domain", DOMAIN.id), "deny", "*"),
      ],
    );
    expect(result.allowed).toBe(false);
  });
});

describe("mail send resources", () => {
  test("build a coordinate resource", () => {
    expect(mailSendResource("domain", "example.com")).toBe(
      "mail.send:domain:example.com",
    );
  });

  test("refuse an id with a colon or a wildcard", () => {
    expect(() => mailSendResource("domain", "a:b")).toThrow();
    expect(() => mailSendResource("domain", "*.example.com")).toThrow();
  });
});

describe("mailAddressDomain", () => {
  test("returns the lower-cased domain", () => {
    expect(mailAddressDomain("Someone@Example.COM")).toBe("example.com");
  });

  test("returns null for an address it cannot split", () => {
    expect(mailAddressDomain("no-at-sign")).toBeNull();
    expect(mailAddressDomain("@example.com")).toBeNull();
    expect(mailAddressDomain("someone@")).toBeNull();
  });

  test("returns null for an address with more than one @", () => {
    expect(mailAddressDomain("a@b@example.com")).toBeNull();
  });

  test("drops a trailing root dot", () => {
    expect(mailAddressDomain("a@Example.com.")).toBe("example.com");
  });

  test("returns null for a domain that is not a host name", () => {
    expect(mailAddressDomain("a@exa*mple.com")).toBeNull();
    expect(mailAddressDomain("a@example.com:25")).toBeNull();
    expect(mailAddressDomain("a@example.com ")).toBeNull();
    expect(mailAddressDomain("a@ ")).toBeNull();
    expect(mailAddressDomain("Bob <bob@example.com>")).toBeNull();
    expect(mailAddressDomain("a@example..com")).toBeNull();
    expect(mailAddressDomain("a@-example.com")).toBeNull();
    expect(mailAddressDomain("a@.")).toBeNull();
    // Two spellings of one internationalized domain would give two ids, and
    // a deny on one would miss the other.
    expect(mailAddressDomain("a@münchen.de")).toBeNull();
    expect(mailAddressDomain("a@x_y.com")).toBeNull();
    expect(mailAddressDomain("a@[192.0.2.1]")).toBeNull();
  });
});

// A run gets `mail.send:*:*` allow at first trigger and operators narrow it
// with domain deny rows, so a deny must decide for every spelling of its
// domain that has a domain coordinate.
describe("a domain deny under a namespace-wide allow", () => {
  const grants = [
    grant("mail.send:*:*", "allow"),
    grant("mail.send:domain:evil.com", "deny"),
  ];

  test.each(["x@evil.com", "x@EVIL.com", "x@evil.com.", "x@evil.com "])(
    "refuses %p",
    async (address) => {
      const domain = mailAddressDomain(address);
      const result = await decide(
        domain === null ? [] : [{ coordType: "domain", id: domain }],
        grants,
      );
      expect(result.allowed).toBe(false);
    },
  );
});

describe("mailSendResourceProblem", () => {
  // The send check reads only the `mail.send` namespace, so a pattern written
  // toward it from outside never decides a send.
  test.each([
    "mail.send*:domain:evil.com",
    "mail.sen*:domain:evil.com",
    "mail.*:domain:evil.com",
    "mail.send*evil.com",
    "mail.send*",
  ])("refuses the near miss %p", (resource) => {
    expect(mailSendResourceProblem(resource)).toContain(
      "outside the mail.send namespace",
    );
  });

  test.each([
    "*",
    "*:*",
    "mail.*",
    "mail.*:*",
    "m*",
    "m*:read",
    "ma*x",
    "m*:principal:prn_a",
    "wallet:*",
    "mail.accept:*",
    "mail.sender:x",
    "*:prn_a",
  ])("accepts the grant %p for other namespaces", (resource) => {
    expect(mailSendResourceProblem(resource)).toBeNull();
  });

  test("accepts usable resources and resources outside the namespace", () => {
    expect(mailSendResourceProblem("mail.send:domain:example.com")).toBeNull();
    expect(
      mailSendResourceProblem("mail.send:domain:*.example.com"),
    ).toBeNull();
    expect(mailSendResourceProblem("mail.send:*:*")).toBeNull();
    // A wildcard type also matches non-domain ids, which may hold a `.`.
    expect(mailSendResourceProblem("mail.send:*:evil.com.")).toBeNull();
    expect(mailSendResourceProblem("mail.send:*")).toBeNull();
    expect(mailSendResourceProblem("mail.send:principal:prn_ABC")).toBeNull();
    expect(mailSendResourceProblem("mail.send:tenant:*")).toBeNull();
    expect(
      mailSendResourceProblem("mail.accept:domain:Example.com"),
    ).toBeNull();
    expect(mailSendResourceProblem("mail.sender:x")).toBeNull();
    expect(mailSendResourceProblem("*")).toBeNull();
  });

  test.each([
    "mail.send",
    "mail.send:DOMAIN:evil.com",
    "mail.send:domian:evil.com",
    "mail.send:domain:",
    "mail.send:domain",
    "mail.send:domain:evil.com:x",
    "mail.send:domain:evil.com.",
    "mail.send:domain:*.evil.com.",
    "mail.send:domain:evil com",
    "mail.send:domain:evil_com",
    "mail.send:domain:-evil.com",
    "mail.send:domain:evil..com",
    "mail.send:*:a:b",
    "mail.send:*:",
  ])("reports %p, which matches no coordinate", (resource) => {
    expect(mailSendResourceProblem(resource)).not.toBeNull();
  });

  test("rejects upper case where the resource can match a domain", () => {
    expect(
      mailSendResourceProblem("mail.send:domain:Example.com"),
    ).not.toBeNull();
    expect(mailSendResourceProblem("mail.send:*:Evil.com")).not.toBeNull();
    expect(mailSendResourceProblem("mail.send:*Evil.com")).not.toBeNull();
  });
});
