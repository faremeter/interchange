import { describe, test, expect, beforeEach } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { initAgentRepo } from "./init";
import { createMailAuditStore, listMail } from "./mail-store";

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
    if (result === null) throw new Error("expected non-null result");

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
    if (r1 === null) throw new Error("expected non-null r1");

    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      inReplyTo: "<msg-1@test>",
    });
    const r2 = await store.commitMail(msg2, "out");
    if (r2 === null) throw new Error("expected non-null r2");

    expect(r2.threadId).toBe(r1.threadId);
    expect(r2.filepath).toBe(`state/mail/${r1.threadId}/0002-out.eml`);
  });

  test("reply via References joins correct thread", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");
    if (r1 === null) throw new Error("expected non-null r1");

    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      references: ["<msg-1@test>"],
    });
    const r2 = await store.commitMail(msg2, "out");
    if (r2 === null) throw new Error("expected non-null r2");

    expect(r2.threadId).toBe(r1.threadId);
  });

  test("unrelated message creates a separate thread", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");
    if (r1 === null) throw new Error("expected non-null r1");

    const msg2 = buildRawMessage({ messageId: "<msg-2@test>" });
    const r2 = await store.commitMail(msg2, "in");
    if (r2 === null) throw new Error("expected non-null r2");

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

  test("duplicate Message-ID returns null with ignoreDuplicate", async () => {
    const store = await createMailAuditStore(testDir);

    const msg = buildRawMessage({ messageId: "<msg-1@test>" });
    await store.commitMail(msg, "in");

    const result = await store.commitMail(msg, "in", {
      ignoreDuplicate: true,
    });
    expect(result).toBeNull();
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
    if (r1 === null) throw new Error("expected non-null r1");

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
    if (r3 === null) throw new Error("expected non-null r3");

    expect(r3.threadId).toBe(r1.threadId);
    expect(r3.filepath).toBe(`state/mail/${r1.threadId}/0003-in.eml`);
  });

  test("References list walks to find first matching thread", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");
    if (r1 === null) throw new Error("expected non-null r1");

    // Reference an unknown ID first, then the known one
    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      references: ["<unknown@test>", "<msg-1@test>"],
    });
    const r2 = await store.commitMail(msg2, "out");
    if (r2 === null) throw new Error("expected non-null r2");

    expect(r2.threadId).toBe(r1.threadId);
  });
});

describe("listMail", () => {
  test("returns empty array when no mail exists", async () => {
    const entries = await listMail(testDir);
    expect(entries).toEqual([]);
  });

  test("returns single entry after one commit", async () => {
    const store = await createMailAuditStore(testDir);
    const raw = buildRawMessage({ messageId: "<msg-1@test>" });
    await store.commitMail(raw, "in");

    const entries = await listMail(testDir);
    expect(entries).toHaveLength(1);

    const entry = entries[0];
    if (entry === undefined) throw new Error("expected entry");
    expect(entry.messageId).toBe("<msg-1@test>");
    expect(entry.direction).toBe("in");
    expect(entry.ordinal).toBe(1);
    expect(entry.raw).toEqual(raw);
  });

  test("returns entries sorted by threadId then ordinal", async () => {
    const store = await createMailAuditStore(testDir);

    const msg1 = buildRawMessage({ messageId: "<msg-1@test>" });
    const r1 = await store.commitMail(msg1, "in");
    if (r1 === null) throw new Error("expected non-null r1");

    const msg2 = buildRawMessage({
      messageId: "<msg-2@test>",
      inReplyTo: "<msg-1@test>",
    });
    await store.commitMail(msg2, "out");

    const msg3 = buildRawMessage({ messageId: "<msg-3@test>" });
    const r3 = await store.commitMail(msg3, "in");
    if (r3 === null) throw new Error("expected non-null r3");

    const entries = await listMail(testDir);
    expect(entries).toHaveLength(3);

    // Entries are sorted by threadId then ordinal — verify cross-thread
    // ordering is lexicographic and entries within a thread are contiguous
    const threadIds = entries.map((e) => e.threadId);
    const uniqueThreadIds = [...new Set(threadIds)];
    const sortedThreadIds = [...uniqueThreadIds].sort();
    expect(uniqueThreadIds).toEqual(sortedThreadIds);

    // Thread 1 entries should be grouped and ordered
    const thread1 = entries.filter((e) => e.threadId === r1.threadId);
    expect(thread1).toHaveLength(2);
    expect(thread1[0]?.ordinal).toBe(1);
    expect(thread1[0]?.direction).toBe("in");
    expect(thread1[1]?.ordinal).toBe(2);
    expect(thread1[1]?.direction).toBe("out");

    // Thread 2
    const thread2 = entries.filter((e) => e.threadId === r3.threadId);
    expect(thread2).toHaveLength(1);
    expect(thread2[0]?.ordinal).toBe(1);
  });

  test("reads correctly from a fresh directory without store", async () => {
    // Write some mail via store, then read via standalone listMail
    const store = await createMailAuditStore(testDir);
    const raw = buildRawMessage({
      messageId: "<msg-1@test>",
      from: "alice@example.com",
      to: "bob@example.com",
      body: "hello from alice",
    });
    await store.commitMail(raw, "in");

    // listMail should work without a store instance
    const entries = await listMail(testDir);
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("expected entry");
    expect(new TextDecoder().decode(entry.raw)).toContain("hello from alice");
  });
});
