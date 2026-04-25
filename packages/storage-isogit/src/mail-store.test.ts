import { describe, test, expect, beforeEach } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { initAgentRepo } from "./init";
import { createMailAuditStore } from "./mail-store";

function buildRawMessage(opts: {
  messageId: string;
  from?: string;
  to?: string;
  inReplyTo?: string;
  references?: string[];
  body?: string;
}): Uint8Array {
  const lines: string[] = [];
  lines.push(`Message-ID: ${opts.messageId}`);
  lines.push(`From: ${opts.from ?? "sender@example.com"}`);
  lines.push(`To: ${opts.to ?? "recipient@example.com"}`);
  lines.push(`Date: ${new Date().toUTCString()}`);
  if (opts.inReplyTo !== undefined) {
    lines.push(`In-Reply-To: ${opts.inReplyTo}`);
  }
  if (opts.references !== undefined && opts.references.length > 0) {
    lines.push(`References: ${opts.references.join(" ")}`);
  }
  lines.push("");
  lines.push(opts.body ?? "test body");
  return new TextEncoder().encode(lines.join("\r\n"));
}

let testDir: string;

beforeEach(async () => {
  testDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "mail-store-"));
  await initAgentRepo(testDir);
});

describe("createMailAuditStore", () => {
  test("first message creates a new thread", async () => {
    const store = await createMailAuditStore(testDir);
    const raw = buildRawMessage({ messageId: "<msg-1@test>" });
    const result = await store.commitMail(raw, "in");

    expect(result.messageId).toBe("<msg-1@test>");
    expect(result.threadId).toMatch(/^[0-9a-f]{8}$/);
    expect(result.filepath).toBe(`state/mail/${result.threadId}/0001-in.eml`);

    const filePath = path.join(testDir, result.filepath);
    const stored = await fs.promises.readFile(filePath, "utf-8");
    expect(stored).toBe(new TextDecoder().decode(raw));
  });

  test("reply via In-Reply-To joins existing thread", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");

    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      inReplyTo: "<msg-1@test>",
    });
    const r2 = await store.commitMail(msg2, "out");

    expect(r2.threadId).toBe(r1.threadId);
    expect(r2.filepath).toBe(`state/mail/${r1.threadId}/0002-out.eml`);
  });

  test("reply via References joins correct thread", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");

    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      references: ["<msg-1@test>"],
    });
    const r2 = await store.commitMail(msg2, "out");

    expect(r2.threadId).toBe(r1.threadId);
  });

  test("unrelated message creates a separate thread", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");

    const msg2 = buildRawMessage({ messageId: "<msg-2@test>" });
    const r2 = await store.commitMail(msg2, "in");

    expect(r2.threadId).not.toBe(r1.threadId);
  });

  test("duplicate Message-ID throws", async () => {
    const store = await createMailAuditStore(testDir);

    const msg = buildRawMessage({ messageId: "<msg-1@test>" });
    await store.commitMail(msg, "in");

    expect(store.commitMail(msg, "in")).rejects.toThrow(
      "Duplicate mail: Message-ID <msg-1@test> already stored",
    );
  });

  test("missing Message-ID throws", async () => {
    const store = await createMailAuditStore(testDir);

    const raw = new TextEncoder().encode(
      "From: sender@example.com\r\nDate: Mon, 01 Jan 2024 00:00:00 GMT\r\n\r\nbody",
    );

    expect(store.commitMail(raw, "in")).rejects.toThrow(
      "Message-ID header is missing or empty",
    );
  });

  test("index rebuilds correctly from disk on init", async () => {
    const store1 = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store1.commitMail(msg1, "in");

    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      inReplyTo: "<msg-1@test>",
    });
    await store1.commitMail(msg2, "out");

    // Create a fresh store from the same directory
    const store2 = await createMailAuditStore(testDir);

    const msg3 = buildRawMessage({
      messageId: "<msg-3@test>",
      inReplyTo: "<msg-2@test>",
    });
    const r3 = await store2.commitMail(msg3, "in");

    expect(r3.threadId).toBe(r1.threadId);
    expect(r3.filepath).toBe(`state/mail/${r1.threadId}/0003-in.eml`);
  });

  test("References list walks to find first matching thread", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");

    // Reference an unknown ID first, then the known one
    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      references: ["<unknown@test>", "<msg-1@test>"],
    });
    const r2 = await store.commitMail(msg2, "out");

    expect(r2.threadId).toBe(r1.threadId);
  });
});
