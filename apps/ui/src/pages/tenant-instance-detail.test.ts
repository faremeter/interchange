import { describe, expect, test } from "bun:test";

import { shouldShowMail } from "@interchange/hub-client";

// Agent addresses always start with "ins_".
const AGENT_ADDR = "ins_abc123@tenant.example";
const HUMAN_ADDR = "usr_alice@tenant.example";

describe("shouldShowMail", () => {
  test("inbound mail is shown regardless of recipient address", () => {
    expect(
      shouldShowMail({
        direction: "inbound",
        to: [{ name: null, email: AGENT_ADDR }],
      }),
    ).toBe(true);
    expect(
      shouldShowMail({
        direction: "inbound",
        to: [{ name: null, email: HUMAN_ADDR }],
      }),
    ).toBe(true);
  });

  test("outbound mail to another agent is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        to: [{ name: "Other Agent", email: AGENT_ADDR }],
      }),
    ).toBe(false);
  });

  test("outbound connector reply to human is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        to: [{ name: "Alice", email: HUMAN_ADDR }],
      }),
    ).toBe(false);
  });

  test("outbound mail with no recipients is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
      }),
    ).toBe(false);
  });

  test("outbound mail with empty recipients is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        to: [],
      }),
    ).toBe(false);
  });
});
