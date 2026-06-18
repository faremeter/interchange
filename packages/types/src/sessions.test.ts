import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { SendMessage, AttachmentError } from "./sessions";

const okData = Buffer.from("hello world").toString("base64");

describe("SendMessage attachments schema", () => {
  test("accepts a message with no attachments", () => {
    const result = SendMessage({ content: "hi" });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an attachment with mimeType, base64 data, and name", () => {
    const result = SendMessage({
      content: "see attached",
      attachments: [{ mimeType: "image/png", data: okData, name: "shot.png" }],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an attachment without the optional name", () => {
    const result = SendMessage({
      content: "",
      attachments: [{ mimeType: "image/png", data: okData }],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an attachment missing mimeType", () => {
    const result = SendMessage({
      content: "x",
      attachments: [{ data: okData }],
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an attachment missing data", () => {
    const result = SendMessage({
      content: "x",
      attachments: [{ mimeType: "image/png" }],
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts any string data; base64 validity is the route's job", () => {
    // The schema only checks that data is a string. The route boundary
    // decodes it and emits malformed_base64 when it is not valid base64.
    const result = SendMessage({
      content: "x",
      attachments: [{ mimeType: "image/png", data: "not base64!!!" }],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects the removed type field (closed attachment schema)", () => {
    const result = SendMessage({
      content: "x",
      attachments: [{ mimeType: "image/png", data: okData, type: "image" }],
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("AttachmentError schema", () => {
  test("accepts each structured error variant", () => {
    const variants = [
      {
        code: "oversize_attachment",
        message: "too big",
        attachmentIndex: 0,
        byteLength: 99,
        limitBytes: 10,
      },
      {
        code: "disallowed_mime_type",
        message: "nope",
        attachmentIndex: 1,
        mimeType: "image/tiff",
      },
      { code: "malformed_base64", message: "bad", attachmentIndex: 2 },
      {
        code: "oversize_total",
        message: "too much",
        totalBytes: 99,
        limitBytes: 30,
      },
    ];
    for (const variant of variants) {
      expect(AttachmentError(variant) instanceof type.errors).toBe(false);
    }
  });

  test("rejects an unknown code", () => {
    const result = AttachmentError({ code: "nope", message: "x" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a variant missing its structured fields", () => {
    const result = AttachmentError({
      code: "oversize_attachment",
      message: "x",
      attachmentIndex: 0,
    });
    expect(result instanceof type.errors).toBe(true);
  });
});
