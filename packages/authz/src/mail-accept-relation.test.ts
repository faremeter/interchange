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
      { relation: "parent", token: "mail.accept:parent" },
      { relation: "child", token: "mail.accept:child" },
    ];

    for (const { relation, token } of cases) {
      expect(mailAcceptRelationToken(relation)).toBe(token);
    }
  });
});

describe("resolveMailAcceptRelations", () => {
  test("defaults parent and child on when the authored value is absent", () => {
    expect(resolveMailAcceptRelations(undefined)).toEqual(
      new Set<MailAcceptRelation>(["parent", "child"]),
    );
    expect(resolveMailAcceptRelations(null)).toEqual(
      new Set<MailAcceptRelation>(["parent", "child"]),
    );
    expect(resolveMailAcceptRelations({})).toEqual(
      new Set<MailAcceptRelation>(["parent", "child"]),
    );
  });

  test("an explicit false disables a default-on relation", () => {
    expect(resolveMailAcceptRelations({ parent: false })).toEqual(
      new Set<MailAcceptRelation>(["child"]),
    );
  });

  test("mixes an explicit enable with an explicit disable", () => {
    expect(resolveMailAcceptRelations({ invoker: true, child: false })).toEqual(
      new Set<MailAcceptRelation>(["invoker", "parent"]),
    );
  });

  test("enables all six relations when every relation is authored true", () => {
    expect(
      resolveMailAcceptRelations({
        invoker: true,
        self: true,
        tenant: true,
        correspondent: true,
        parent: true,
        child: true,
      }),
    ).toEqual(
      new Set<MailAcceptRelation>([
        "invoker",
        "self",
        "tenant",
        "correspondent",
        "parent",
        "child",
      ]),
    );
  });

  test("leaves the default-off relations off when they are absent", () => {
    expect(resolveMailAcceptRelations({ tenant: true })).toEqual(
      new Set<MailAcceptRelation>(["tenant", "parent", "child"]),
    );
  });
});
