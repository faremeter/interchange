import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import {
  MailAccept,
  isMailAcceptNamespaceResource,
  isValidCoordId,
} from "./mail-accept";

describe("MailAccept validator", () => {
  test("accepts well-formed explicit ids and relational toggles", () => {
    const result = MailAccept({
      invoker: true,
      principals: ["prn_alice", "prn_bob"],
      assets: ["ast_report"],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an empty policy", () => {
    expect(MailAccept({}) instanceof type.errors).toBe(false);
  });

  test("rejects an empty principals id at the boundary", () => {
    expect(MailAccept({ principals: [""] }) instanceof type.errors).toBe(true);
  });

  test("rejects a principals id containing a colon", () => {
    expect(MailAccept({ principals: ["prn:bad"] }) instanceof type.errors).toBe(
      true,
    );
  });

  test("rejects an assets id containing a colon", () => {
    expect(MailAccept({ assets: ["ast:bad"] }) instanceof type.errors).toBe(
      true,
    );
  });

  test("rejects a wildcard principals or assets id", () => {
    expect(MailAccept({ principals: ["*"] }) instanceof type.errors).toBe(true);
    expect(MailAccept({ principals: ["prn_*"] }) instanceof type.errors).toBe(
      true,
    );
    expect(MailAccept({ assets: ["*"] }) instanceof type.errors).toBe(true);
  });

  test("rejects an undeclared key", () => {
    expect(MailAccept({ parrent: true }) instanceof type.errors).toBe(true);
  });
});

describe("isValidCoordId", () => {
  test("accepts a non-empty id without a colon", () => {
    expect(isValidCoordId("prn_alice")).toBe(true);
  });

  test("rejects an empty id", () => {
    expect(isValidCoordId("")).toBe(false);
  });

  test("rejects an id containing a colon", () => {
    expect(isValidCoordId("prn:bad")).toBe(false);
  });

  test("rejects an id containing a wildcard", () => {
    expect(isValidCoordId("*")).toBe(false);
    expect(isValidCoordId("prn_*")).toBe(false);
  });
});

describe("isMailAcceptNamespaceResource", () => {
  test("holds for the bare namespace and every mail.accept resource", () => {
    expect(isMailAcceptNamespaceResource("mail.accept")).toBe(true);
    expect(isMailAcceptNamespaceResource("mail.accept:*")).toBe(true);
    expect(isMailAcceptNamespaceResource("mail.accept:principal:prn_a")).toBe(
      true,
    );
  });

  test("does not hold for broader or look-alike resources", () => {
    expect(isMailAcceptNamespaceResource("*")).toBe(false);
    expect(isMailAcceptNamespaceResource("mail.*")).toBe(false);
    expect(isMailAcceptNamespaceResource("mail.accept*")).toBe(false);
    expect(isMailAcceptNamespaceResource("mail.acceptor:x")).toBe(false);
  });
});
