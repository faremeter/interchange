import { describe, test, expect } from "bun:test";

import {
  DEFAULT_ATTACHMENT_POLICY,
  validateAttachments,
  type AttachmentInput,
  type AttachmentPolicy,
} from "./attachment-validation";
import { PER_ATTACHMENT_LIMIT_BYTES } from "./attachments";
import { base64Encode } from "./base64";

function b64(bytes: number[]): string {
  return base64Encode(new Uint8Array(bytes));
}

function bytesOfLength(n: number): string {
  return base64Encode(new Uint8Array(n).fill(0x61));
}

function compactDecodedSize(compact: string): number {
  const pad = compact.endsWith("==") ? 2 : compact.endsWith("=") ? 1 : 0;
  return Math.floor(compact.length / 4) * 3 - pad;
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

  test("already-decoded bytes pass through the same checks", () => {
    const bytes = new Uint8Array([1, 2, 3]);
    expect(
      validateAttachments([{ mimeType: "image/png", data: bytes }], policy),
    ).toEqual({
      ok: true,
      attachments: [
        { name: "attachment-0", contentType: "image/png", data: bytes },
      ],
    });

    const result = validateAttachments(
      [{ mimeType: "image/png", data: new Uint8Array(101) }],
      policy,
    );
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error.code).toBe("oversize_attachment");
  });

  test("empty input is valid", () => {
    expect(validateAttachments([], policy)).toEqual({
      ok: true,
      attachments: [],
    });
  });

  test("an allowlisted type with parameters is stored as type/subtype", () => {
    const result = validateAttachments([
      {
        mimeType: "text/plain; charset=utf-8",
        data: new TextEncoder().encode("hi"),
        name: "notes.txt",
      },
    ]);
    expect(result).toEqual({
      ok: true,
      attachments: [
        {
          name: "notes.txt",
          contentType: "text/plain",
          data: new TextEncoder().encode("hi"),
        },
      ],
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
    if (result.ok) return;
    expect(result.error.message).toContain("invalid characters");
  });

  test("rejects an empty or whitespace-only name", () => {
    for (const name of ["", "   "]) {
      const result = validateAttachments(
        [{ mimeType: "image/png", data: b64([1]), name }],
        policy,
      );
      expect(result).toMatchObject({
        ok: false,
        error: { code: "invalid_attachment_name", attachmentIndex: 0 },
      });
      if (result.ok) return;
      expect(result.error.message).toContain("empty name");
    }
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
      [{ mimeType: "image/png", data: new Uint8Array(101) }],
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

  test("accepts compact base64 of exactly the per-attachment limit", () => {
    const bytes = new Uint8Array(policy.perAttachmentLimitBytes);
    expect(
      validateAttachments([{ mimeType: "image/png", data: bytes }], policy).ok,
    ).toBe(true);

    const encoded = base64Encode(bytes);
    const result = validateAttachments(
      [{ mimeType: "image/png", data: encoded }],
      policy,
    );
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.attachments[0]?.data.length).toBe(
      policy.perAttachmentLimitBytes,
    );
  });

  test("rejects compact base64 one byte over the per-attachment limit", () => {
    const encoded = base64Encode(
      new Uint8Array(policy.perAttachmentLimitBytes + 1),
    );
    const result = validateAttachments(
      [{ mimeType: "image/png", data: encoded }],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "oversize_attachment",
        attachmentIndex: 0,
        byteLength: policy.perAttachmentLimitBytes + 1,
        limitBytes: policy.perAttachmentLimitBytes,
      },
    });
  });

  test("accepts compact base64 of exactly 10MiB under the default policy", () => {
    const bytes = new Uint8Array(PER_ATTACHMENT_LIMIT_BYTES);
    const encoded = base64Encode(bytes);
    const result = validateAttachments([
      { mimeType: "image/png", data: encoded },
    ]);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.attachments[0]?.data.length).toBe(
      DEFAULT_ATTACHMENT_POLICY.perAttachmentLimitBytes,
    );
  });

  test("accepts whitespace-padded compact base64 of an in-policy payload", () => {
    const encoded = ` ${base64Encode(new Uint8Array(policy.perAttachmentLimitBytes))} \n`;
    const result = validateAttachments(
      [{ mimeType: "image/png", data: encoded }],
      policy,
    );
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.attachments[0]?.data.length).toBe(
      policy.perAttachmentLimitBytes,
    );
  });

  test("rejects encoded base64 whose length already exceeds the decoded limit", () => {
    // Compact length minus padding is the decoded size; 200 chars of junk
    // cannot decode to ≤100 bytes, so this fails before base64Decode.
    const encoded = "x".repeat(200);
    const result = validateAttachments(
      [{ mimeType: "image/png", data: encoded }],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "oversize_attachment",
        attachmentIndex: 0,
        byteLength: compactDecodedSize(encoded),
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
        { mimeType: "image/png", data: new Uint8Array(200) },
        { mimeType: "application/pdf", data: new Uint8Array(10) },
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

  test("returns the first error without decoding later entries", () => {
    const result = validateAttachments(
      [
        { mimeType: "image/tiff", data: b64([1]) },
        { mimeType: "image/png", data: "x".repeat(200) },
      ],
      policy,
    );
    expect(result).toMatchObject({
      ok: false,
      error: {
        code: "disallowed_mime_type",
        attachmentIndex: 0,
        mimeType: "image/tiff",
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
