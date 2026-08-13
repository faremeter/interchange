import { describe, test, expect, beforeEach } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import git, { type PromiseFsClient } from "isomorphic-git";
import { initAgentRepo } from "./node";
import { createMailAuditStore, listMail } from "./node";
import { createIsogitStorage } from "./index";
import { createNodeIsogitRuntime } from "./node-runtime";

function createRefWriteFailureRuntime(
  refPath: string,
  effect: "before" | "after",
) {
  const runtime = createNodeIsogitRuntime();
  const nodePromises = fs.promises;
  let armed = false;
  const promiseFs: PromiseFsClient = {
    promises: {
      readFile: nodePromises.readFile.bind(nodePromises),
      writeFile: async (...args: unknown[]) => {
        const filepath = args[0];
        if (!armed || filepath !== refPath) {
          return Reflect.apply(nodePromises.writeFile, nodePromises, args);
        }
        if (effect === "after") {
          await Reflect.apply(nodePromises.writeFile, nodePromises, args);
        }
        throw new Error(`injected ${effect}-effect ref write failure`);
      },
      unlink: nodePromises.unlink.bind(nodePromises),
      readdir: nodePromises.readdir.bind(nodePromises),
      mkdir: nodePromises.mkdir.bind(nodePromises),
      rmdir: nodePromises.rmdir.bind(nodePromises),
      stat: nodePromises.stat.bind(nodePromises),
      lstat: nodePromises.lstat.bind(nodePromises),
      readlink: nodePromises.readlink.bind(nodePromises),
      symlink: nodePromises.symlink.bind(nodePromises),
      chmod: nodePromises.chmod.bind(nodePromises),
    },
  };
  return {
    runtime: { ...runtime, fs: promiseFs },
    arm: () => {
      armed = true;
    },
    disarm: () => {
      armed = false;
    },
  };
}

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

  test("does not reuse an ordinal after uncertain ref publication", async () => {
    let flushes = 0;
    const storage = createIsogitStorage({
      ...createNodeIsogitRuntime(),
      flush: () => {
        flushes += 1;
        if (flushes === 4) {
          return Promise.reject(new Error("injected final flush failure"));
        }
        return Promise.resolve();
      },
    });
    const store = await storage.createMailAuditStore(testDir);

    const first = await store.commitMail(
      buildRawMessage({ messageId: "<uncertain-1@test>" }),
      "in",
    );
    if (first === null) throw new Error("expected first mail commit");

    await expect(
      store.commitMail(
        buildRawMessage({
          messageId: "<uncertain-2@test>",
          inReplyTo: "<uncertain-1@test>",
        }),
        "in",
      ),
    ).rejects.toThrow("injected final flush failure");

    const third = await store.commitMail(
      buildRawMessage({
        messageId: "<uncertain-3@test>",
        inReplyTo: "<uncertain-2@test>",
      }),
      "in",
    );
    if (third === null) throw new Error("expected third mail commit");

    expect(third.filepath).toBe(`state/mail/${first.threadId}/0003-in.eml`);
    expect(
      (await storage.listMail(testDir)).map((entry) => entry.messageId),
    ).toEqual([
      "<uncertain-1@test>",
      "<uncertain-2@test>",
      "<uncertain-3@test>",
    ]);
  });

  test("reconciles a ref write that takes effect before rejecting", async () => {
    const refPath = path.join(testDir, ".git", "refs", "heads", "main");
    const failure = createRefWriteFailureRuntime(refPath, "after");
    const storage = createIsogitStorage(failure.runtime);
    const store = await storage.createMailAuditStore(testDir);

    const first = await store.commitMail(
      buildRawMessage({ messageId: "<write-ref-1@test>" }),
      "in",
    );
    if (first === null) throw new Error("expected first mail commit");

    failure.arm();
    await expect(
      store.commitMail(
        buildRawMessage({
          messageId: "<write-ref-2@test>",
          inReplyTo: "<write-ref-1@test>",
        }),
        "out",
      ),
    ).rejects.toThrow("Ref publication");
    failure.disarm();

    const third = await store.commitMail(
      buildRawMessage({
        messageId: "<write-ref-3@test>",
        inReplyTo: "<write-ref-2@test>",
      }),
      "in",
    );
    if (third === null) throw new Error("expected third mail commit");

    expect(third.threadId).toBe(first.threadId);
    expect(third.filepath).toBe(`state/mail/${first.threadId}/0003-in.eml`);
    expect(
      (await storage.listMail(testDir)).map((entry) => entry.messageId),
    ).toEqual([
      "<write-ref-1@test>",
      "<write-ref-2@test>",
      "<write-ref-3@test>",
    ]);
  });

  test("does not advance mail state when a ref write has no effect", async () => {
    const refPath = path.join(testDir, ".git", "refs", "heads", "main");
    const failure = createRefWriteFailureRuntime(refPath, "before");
    const storage = createIsogitStorage(failure.runtime);
    const store = await storage.createMailAuditStore(testDir);

    const first = await store.commitMail(
      buildRawMessage({ messageId: "<no-write-1@test>" }),
      "in",
    );
    if (first === null) throw new Error("expected first mail commit");
    const previousOid = await git.resolveRef({ fs, dir: testDir, ref: "HEAD" });

    failure.arm();
    await expect(
      store.commitMail(
        buildRawMessage({
          messageId: "<no-write-2@test>",
          inReplyTo: "<no-write-1@test>",
        }),
        "out",
      ),
    ).rejects.toThrow("Ref publication");
    failure.disarm();
    expect(await git.resolveRef({ fs, dir: testDir, ref: "HEAD" })).toBe(
      previousOid,
    );

    const third = await store.commitMail(
      buildRawMessage({
        messageId: "<no-write-3@test>",
        inReplyTo: "<no-write-1@test>",
      }),
      "out",
    );
    if (third === null) throw new Error("expected third mail commit");

    expect(third.threadId).toBe(first.threadId);
    expect(third.filepath).toBe(`state/mail/${first.threadId}/0002-out.eml`);
    expect(
      (await storage.listMail(testDir)).map((entry) => entry.messageId),
    ).toEqual(["<no-write-1@test>", "<no-write-3@test>"]);
  });

  test("a failed mail commit cannot leak into a cycle commit", async () => {
    let failed = false;
    const storage = createIsogitStorage({
      ...createNodeIsogitRuntime(),
      flush: () => {
        if (!failed) {
          failed = true;
          return Promise.reject(new Error("injected object flush failure"));
        }
        return Promise.resolve();
      },
    });
    const mailStore = await storage.createMailAuditStore(testDir);

    await expect(
      mailStore.commitMail(
        buildRawMessage({ messageId: "<failed-mail@test>" }),
        "in",
      ),
    ).rejects.toThrow("injected object flush failure");

    const [threadId] = await fs.promises.readdir(
      path.join(testDir, "state", "mail"),
    );
    if (threadId === undefined) throw new Error("expected mail thread");
    const [filename] = await fs.promises.readdir(
      path.join(testDir, "state", "mail", threadId),
    );
    if (filename === undefined) throw new Error("expected mail file");

    const contextStore = await storage.createIsogitStore(testDir);
    await contextStore.writeTurns([]);
    const cycle = await contextStore.commit({ message: "Cycle: inference" });

    await expect(
      git.readBlob({
        fs,
        dir: testDir,
        oid: cycle.hash,
        filepath: `state/mail/${threadId}/${filename}`,
      }),
    ).rejects.toMatchObject({ code: "NotFoundError" });
  });
});
