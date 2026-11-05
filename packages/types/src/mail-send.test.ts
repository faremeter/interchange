import { describe, test, expect } from "bun:test";

import { isMailSendNamespaceResource } from "./mail-send";

describe("isMailSendNamespaceResource", () => {
  test("holds for the bare namespace and its resources", () => {
    expect(isMailSendNamespaceResource("mail.send")).toBe(true);
    expect(isMailSendNamespaceResource("mail.send:*:*")).toBe(true);
    expect(isMailSendNamespaceResource("mail.send:domain:example.com")).toBe(
      true,
    );
  });

  test("does not hold for broader or neighbouring resources", () => {
    expect(isMailSendNamespaceResource("*")).toBe(false);
    expect(isMailSendNamespaceResource("mail.*")).toBe(false);
    expect(isMailSendNamespaceResource("mail.sender:x")).toBe(false);
    expect(isMailSendNamespaceResource("mail.accept:*:*")).toBe(false);
  });
});
