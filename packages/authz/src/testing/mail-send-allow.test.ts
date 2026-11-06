import { describe, expect, test } from "bun:test";

import { permitAllMailSend } from "./mail-send-allow";

describe("permitAllMailSend", () => {
  test("allows every recipient, in order", async () => {
    const result = await permitAllMailSend({
      sender: { kind: "address", address: "run_a@example.com" },
      recipients: ["b@example.com", "c@example.com"],
    });
    expect(result).toEqual({
      ok: true,
      decisions: [
        {
          recipient: "b@example.com",
          outcome: "evaluated",
          allowed: true,
          effect: "allow",
          grant: expect.objectContaining({ effect: "allow" }),
        },
        {
          recipient: "c@example.com",
          outcome: "evaluated",
          allowed: true,
          effect: "allow",
          grant: expect.objectContaining({ effect: "allow" }),
        },
      ],
    });
  });
});
