import { describe, test, expect } from "bun:test";

import { MailComposeError, isMailComposeError } from "./mail-signer";

describe("isMailComposeError", () => {
  test("accepts a compose refusal the class did not construct", () => {
    // The reason the guard is structural: a workflow bundle inlines its
    // workspace imports, so the consumer classifying a failure holds its own
    // copy of MailComposeError and an `instanceof` check against the host's
    // copy answers false. This foreign class stands in for that second copy.
    class ForeignComposeError extends Error {
      readonly attachmentError = undefined;
      override readonly name = "MailComposeError";
    }
    const foreign = new ForeignComposeError("refused");

    expect(isMailComposeError(foreign)).toBe(true);
    expect(foreign instanceof MailComposeError).toBe(false);
  });

  test("accepts the error the class constructs and reads back its attachment error", () => {
    const attachmentError = {
      code: "too_many_attachments" as const,
      message: "11 attachments, over the limit of 10",
      count: 11,
      limit: 10,
    };
    const failure = new MailComposeError("refused", attachmentError);

    expect(isMailComposeError(failure)).toBe(true);
    expect(failure.attachmentError).toEqual(attachmentError);
  });

  test("refuses a plain error, another error class, or a malformed attachment error", () => {
    expect(isMailComposeError(new Error("cancelled"))).toBe(false);
    expect(
      isMailComposeError(
        Object.assign(new Error("x"), {
          name: "MailComposeError",
          attachmentError: { code: "invented" },
        }),
      ),
    ).toBe(false);
    expect(
      isMailComposeError({
        name: "MailComposeError",
        attachmentError: undefined,
      }),
    ).toBe(false);
    expect(isMailComposeError(undefined)).toBe(false);
  });
});
