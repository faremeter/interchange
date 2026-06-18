import { describe, test, expect } from "bun:test";
import {
  validateAttachments,
  type AttachmentInput,
  type AttachmentPolicy,
} from "./attachment-validation";

function b64(bytes: number[]): string {
  return Buffer.from(new Uint8Array(bytes)).toString("base64");
}

function bytesOfLength(n: number): string {
  return Buffer.alloc(n, 0x61).toString("base64");
}

// Small limits so oversize cases need only tiny buffers.
const policy: AttachmentPolicy = {
  isAllowed: (m) => m === "image/png" || m === "application/pdf",
  perAttachmentLimitBytes: 100,
  perMessageTotalLimitBytes: 150,
};

describe("validateAttachments", () => {
  test("accepts valid attachments and defaults names by index", () => {
    const inputs: AttachmentInput[] = [
      { mimeType: "image/png", data: b64([1, 2, 3]), name: "shot.png" },
      { mimeType: "application/pdf", data: b64([4, 5]) },
    ];
    const result = validateAttachments(inputs, policy);
    expect(result).toEqual({
      ok: true,
      attachments: [
        {
          name: "shot.png",
          contentType: "image/png",
          data: new Uint8Array([1, 2, 3]),
        },
        {
          name: "attachment-1",
          contentType: "application/pdf",
          data: new Uint8Array([4, 5]),
        },
      ],
    });
  });

  test("empty input is valid", () => {
    expect(validateAttachments([], policy)).toEqual({
      ok: true,
      attachments: [],
    });
  });

  test("rejects a disallowed mimeType with the offending index", () => {
    const result = validateAttachments(
      [
        { mimeType: "image/png", data: b64([1]) },
        { mimeType: "image/tiff", data: b64([2]) },
      ],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "disallowed_mime_type",
        attachmentIndex: 1,
        mimeType: "image/tiff",
      },
    });
  });

  test("rejects a name with header-unsafe characters", () => {
    const result = validateAttachments(
      [
        { mimeType: "image/png", data: b64([1]) },
        { mimeType: "image/png", data: b64([2]), name: 'a"b.png' },
      ],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: { code: "invalid_attachment_name", attachmentIndex: 1 },
    });
  });

  test("rejects malformed base64 with the offending index", () => {
    const result = validateAttachments(
      [{ mimeType: "image/png", data: "@@not-base64@@" }],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: { code: "malformed_base64", attachmentIndex: 0 },
    });
  });

  test("rejects an oversize attachment with byte length and limit", () => {
    const result = validateAttachments(
      [{ mimeType: "image/png", data: bytesOfLength(101) }],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "oversize_attachment",
        attachmentIndex: 0,
        byteLength: 101,
        limitBytes: 100,
      },
    });
  });

  test("rejects an oversize total only after each attachment passes", () => {
    const result = validateAttachments(
      [
        { mimeType: "image/png", data: bytesOfLength(80) },
        { mimeType: "application/pdf", data: bytesOfLength(80) },
      ],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: { code: "oversize_total", totalBytes: 160, limitBytes: 150 },
    });
  });

  test("per-attachment oversize wins over total (ordering)", () => {
    // One oversize attachment plus a small one. Per-attachment size is the
    // most specific error and must win over oversize_total.
    const result = validateAttachments(
      [
        { mimeType: "image/png", data: bytesOfLength(200) },
        { mimeType: "application/pdf", data: bytesOfLength(10) },
      ],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "oversize_attachment",
        attachmentIndex: 0,
        byteLength: 200,
        limitBytes: 100,
      },
    });
  });

  test("oversize wins over disallowed mimeType and malformed base64", () => {
    const result = validateAttachments(
      [
        { mimeType: "image/tiff", data: b64([1]) }, // disallowed
        { mimeType: "image/png", data: "@@bad@@" }, // malformed
        { mimeType: "image/png", data: bytesOfLength(200) }, // oversize
      ],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "oversize_attachment",
        attachmentIndex: 2,
        byteLength: 200,
        limitBytes: 100,
      },
    });
  });

  test("structured errors carry a human-readable message", () => {
    const result = validateAttachments(
      [{ mimeType: "image/tiff", data: b64([1]) }],
      policy,
    );
    if (result.ok) throw new Error("expected a validation error");
    expect(typeof result.error.message).toBe("string");
    expect(result.error.message.length).toBeGreaterThan(0);
  });
});
