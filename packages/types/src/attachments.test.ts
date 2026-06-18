import { describe, test, expect } from "bun:test";
import {
  ATTACHMENT_ALLOWLIST,
  attachmentCategory,
  isAllowedMimeType,
  PER_ATTACHMENT_LIMIT_BYTES,
  PER_MESSAGE_TOTAL_LIMIT_BYTES,
} from "./attachments";

describe("attachment allowlist", () => {
  test("every allowlisted mimeType is accepted by the helper", () => {
    for (const mimeType of Object.keys(ATTACHMENT_ALLOWLIST)) {
      expect(isAllowedMimeType(mimeType)).toBe(true);
    }
  });

  test("unknown mimeTypes are rejected", () => {
    expect(isAllowedMimeType("application/x-evil")).toBe(false);
    expect(isAllowedMimeType("image/tiff")).toBe(false);
    expect(isAllowedMimeType("")).toBe(false);
  });

  test("category dispatch maps each major type and the document category", () => {
    expect(attachmentCategory("image/png")).toBe("image");
    expect(attachmentCategory("video/mp4")).toBe("video");
    expect(attachmentCategory("audio/mpeg")).toBe("audio");
    expect(attachmentCategory("application/pdf")).toBe("document");
    expect(attachmentCategory("text/plain")).toBe("document");
    expect(attachmentCategory("text/csv")).toBe("document");
    expect(attachmentCategory("text/markdown")).toBe("document");
    expect(attachmentCategory("application/json")).toBe("document");
  });

  test("category is undefined for unknown mimeTypes", () => {
    expect(attachmentCategory("image/tiff")).toBeUndefined();
  });

  test("size limits are the documented defaults", () => {
    expect(PER_ATTACHMENT_LIMIT_BYTES).toBe(10 * 1024 * 1024);
    expect(PER_MESSAGE_TOTAL_LIMIT_BYTES).toBe(30 * 1024 * 1024);
  });
});
