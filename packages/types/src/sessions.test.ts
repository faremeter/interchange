import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import { SendMessage } from "./sessions";

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
