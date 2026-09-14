import { describe, test, expect } from "bun:test";

import {
  mailAcceptRelationToken,
  resolveMailAcceptRelations,
  type MailAcceptRelation,
} from "./mail-accept-relation";

describe("mailAcceptRelationToken", () => {
  test("builds the exact marker for each relation", () => {
    const cases: { relation: MailAcceptRelation; token: string }[] = [
      { relation: "invoker", token: "mail.accept:invoker" },
      { relation: "self", token: "mail.accept:self" },
      { relation: "tenant", token: "mail.accept:tenant" },
      { relation: "correspondent", token: "mail.accept:correspondent" },
    ];

    for (const { relation, token } of cases) {
      expect(mailAcceptRelationToken(relation)).toBe(token);
    }
  });
});

describe("resolveMailAcceptRelations", () => {
  test("yields the empty set when the authored value is absent", () => {
    // Every relation is opt-in: an absent policy accepts nothing on the
    // relational axis, matching the admission gate's default-deny.
    expect(resolveMailAcceptRelations(undefined)).toEqual(
      new Set<MailAcceptRelation>(),
    );
    expect(resolveMailAcceptRelations(null)).toEqual(
      new Set<MailAcceptRelation>(),
    );
    expect(resolveMailAcceptRelations({})).toEqual(
      new Set<MailAcceptRelation>(),
    );
  });

  test("enables only the relations authored true", () => {
    expect(resolveMailAcceptRelations({ invoker: true })).toEqual(
      new Set<MailAcceptRelation>(["invoker"]),
    );
    expect(
      resolveMailAcceptRelations({ tenant: true, correspondent: true }),
    ).toEqual(new Set<MailAcceptRelation>(["tenant", "correspondent"]));
  });

  test("an explicit false is the same as absent (off)", () => {
    expect(resolveMailAcceptRelations({ invoker: false })).toEqual(
      new Set<MailAcceptRelation>(),
    );
  });

  test("enables all relations when every relation is authored true", () => {
    expect(
      resolveMailAcceptRelations({
        invoker: true,
        self: true,
        tenant: true,
        correspondent: true,
      }),
    ).toEqual(
      new Set<MailAcceptRelation>([
        "invoker",
        "self",
        "tenant",
        "correspondent",
      ]),
    );
  });
});
