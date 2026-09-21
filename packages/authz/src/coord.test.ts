import { describe, test, expect } from "bun:test";

import {
  isCoordType,
  mailAcceptResource,
  parseMailAcceptResource,
  MAIL_ACCEPT_NAMESPACE,
  MAIL_ACCEPT_ACTION,
} from "./coord";

describe("constants", () => {
  test("namespace and action are the fixed grammar tokens", () => {
    expect(MAIL_ACCEPT_NAMESPACE).toBe("mail.accept");
    expect(MAIL_ACCEPT_ACTION).toBe("accept");
  });
});

describe("mailAcceptResource", () => {
  test("builds the exact resource for each coord type", () => {
    expect(mailAcceptResource("principal", "agt_abc")).toBe(
      "mail.accept:principal:agt_abc",
    );
    expect(mailAcceptResource("definition", "def_123")).toBe(
      "mail.accept:definition:def_123",
    );
    expect(mailAcceptResource("tenant", "ten_xyz")).toBe(
      "mail.accept:tenant:ten_xyz",
    );
  });

  test("throws on an empty id", () => {
    expect(() => mailAcceptResource("principal", "")).toThrow();
  });

  test("throws on an id containing a colon", () => {
    expect(() => mailAcceptResource("principal", "agt:abc")).toThrow();
  });
});

describe("parseMailAcceptResource", () => {
  test("round-trips a built resource for each coord type", () => {
    const cases: {
      coordType: "principal" | "definition" | "tenant";
      id: string;
    }[] = [
      { coordType: "principal", id: "agt_abc" },
      { coordType: "definition", id: "def_123" },
      { coordType: "tenant", id: "ten_xyz" },
    ];

    for (const { coordType, id } of cases) {
      const resource = mailAcceptResource(coordType, id);
      expect(parseMailAcceptResource(resource)).toEqual({ coordType, id });
    }
  });

  test("returns null for a non-mail.accept resource", () => {
    expect(parseMailAcceptResource("agent:agt_abc")).toBeNull();
    expect(parseMailAcceptResource("mail.other:principal:agt_abc")).toBeNull();
  });

  test("returns null for an unknown coord type", () => {
    expect(parseMailAcceptResource("mail.accept:role:agt_abc")).toBeNull();
  });

  test("returns null for the wrong segment count", () => {
    expect(parseMailAcceptResource("mail.accept:principal")).toBeNull();
    expect(parseMailAcceptResource("mail.accept:principal:agt:abc")).toBeNull();
    expect(parseMailAcceptResource("mail.accept")).toBeNull();
  });

  test("returns null for an empty id", () => {
    expect(parseMailAcceptResource("mail.accept:principal:")).toBeNull();
  });
});

describe("isCoordType", () => {
  test("accepts the three coord types", () => {
    expect(isCoordType("principal")).toBe(true);
    expect(isCoordType("definition")).toBe(true);
    expect(isCoordType("tenant")).toBe(true);
  });

  test("rejects anything else", () => {
    expect(isCoordType("role")).toBe(false);
    expect(isCoordType("")).toBe(false);
    expect(isCoordType("Principal")).toBe(false);
    expect(isCoordType("hasOwnProperty")).toBe(false);
  });
});
