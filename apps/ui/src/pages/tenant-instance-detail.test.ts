import { describe, expect, test } from "bun:test";

import { shouldShowMail } from "@interchange/hub-client";

describe("shouldShowMail", () => {
  test("inbound mail is always shown", () => {
    expect(
      shouldShowMail({
        direction: "inbound",
        headers: {},
      }),
    ).toBe(true);
    expect(
      shouldShowMail({
        direction: "inbound",
        headers: { "interchange-type": "conversation.message" },
      }),
    ).toBe(true);
  });

  test("outbound connector reply is suppressed", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        headers: { "interchange-type": "conversation.message" },
      }),
    ).toBe(false);
  });

  test("outbound non-connector mail is shown", () => {
    expect(
      shouldShowMail({
        direction: "outbound",
        headers: {},
      }),
    ).toBe(true);
    expect(
      shouldShowMail({
        direction: "outbound",
        headers: { "interchange-type": "offering.request" },
      }),
    ).toBe(true);
  });
});
