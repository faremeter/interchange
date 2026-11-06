import { describe, test, expect } from "bun:test";

import { reservedGrantNamespace } from "./reserved-namespaces";

describe("reservedGrantNamespace", () => {
  test("names the reserved namespace a resource lies in", () => {
    expect(reservedGrantNamespace("mail.accept")?.namespace).toBe(
      "mail.accept",
    );
    expect(reservedGrantNamespace("mail.accept:*:*")?.namespace).toBe(
      "mail.accept",
    );
  });

  test("returns null for a resource outside every reserved namespace", () => {
    expect(reservedGrantNamespace("*")).toBeNull();
    expect(reservedGrantNamespace("mail.*")).toBeNull();
    expect(reservedGrantNamespace("mail.acceptor:x")).toBeNull();
    expect(reservedGrantNamespace("mail.accept*")).toBeNull();
    expect(reservedGrantNamespace("tool:mail_send")).toBeNull();
  });
});
