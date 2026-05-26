import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import {
  createIsogitStore,
  currentBranch,
  createAndSwitchBranch,
  switchBranch,
  listBranches,
  logHistory,
} from "./index";
import { IsogitStore } from "./store";
import { initAgentRepo } from "./init";
import type {
  AssistantTurn,
  ConversationTurn,
  TokenUsage,
  TransformRecord,
} from "@intx/types/runtime";
import type { AuditRecord, ErrorRecord } from "@intx/types/audit";

const ZERO_USAGE: TokenUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

async function makeTempDir(): Promise<string> {
  return fs.promises.mkdtemp(path.join(os.tmpdir(), "interchange-test-"));
}

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await makeTempDir();
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

describe("createIsogitStore", () => {
  test("initializes a new agent repo and returns a usable store", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);
    const { turns, pendingOperations, tokenUsage } = await store.load();

    expect(turns).toEqual([]);
    expect(pendingOperations).toEqual([]);
    expect(tokenUsage).toEqual(ZERO_USAGE);
  });

  test("is idempotent — calling twice returns a working store", async () => {
    const dir = await tempDir();
    await createIsogitStore(dir);
    const store = await createIsogitStore(dir);
    const { turns } = await store.load();
    expect(turns).toEqual([]);
  });
});

describe("save and load round-trip", () => {
  test("messages survive a commit/load cycle", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "Hello" }],
        timestamp: 1000,
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "Hi there" }],
        model: "test-model",
        timestamp: 2000,
      },
    ];

    await store.writeTurns(messages);

    await store.commit({ message: "first checkpoint" });
    const { turns: loaded } = await store.load();

    expect(loaded).toEqual(messages);
  });

  test("multiple commits accumulate correctly", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const first: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "step 1" }],
        timestamp: 1000,
      },
    ];
    const second: ConversationTurn[] = [
      ...first,
      {
        role: "assistant",
        content: [{ type: "text", text: "step 2" }],
        model: "m",
        timestamp: 2000,
      },
    ];

    await store.writeTurns(first);

    await store.commit({ message: "step 1" });
    await store.writeTurns(second);

    await store.commit({ message: "step 2" });

    const { turns } = await store.load();
    expect(turns).toEqual(second);
  });
});

describe("checkpoint creates commit", () => {
  test("commit returns a ContextCommit with correct metadata", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([]);

    const commitResult = await store.commit({ message: "named checkpoint" });

    expect(typeof commitResult.hash).toBe("string");
    expect(commitResult.hash.length).toBeGreaterThan(0);
    expect(commitResult.message).toBe("named checkpoint");
    expect(typeof commitResult.timestamp).toBe("number");
    expect(commitResult.timestamp).toBeGreaterThan(0);
  });

  test("log contains the checkpoint after commit", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([]);

    await store.commit({ message: "my checkpoint" });
    const entries = await store.log(5);

    const found = entries.find((e) => e.message === "my checkpoint");
    expect(found).toBeDefined();
    if (!found) throw new Error("unreachable");
    expect(found.hash.length).toBeGreaterThan(0);
  });
});

describe("branch operations", () => {
  test("createAndSwitchBranch creates and activates a new branch", async () => {
    const dir = await tempDir();
    await createIsogitStore(dir);

    await createAndSwitchBranch(dir, "feature-branch");
    const branch = await currentBranch(dir);

    expect(branch).toBe("feature-branch");
  });

  test("changes on a branch are isolated from main", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const mainTurns: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "on main" }],
        timestamp: 1000,
      },
    ];
    await store.writeTurns(mainTurns);

    await store.commit({ message: "main work" });

    await createAndSwitchBranch(dir, "experiment");

    const branchTurns: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "on branch" }],
        timestamp: 2000,
      },
    ];
    await store.writeTurns(branchTurns);

    await store.commit({ message: "branch work" });

    await switchBranch(dir, "main");

    const { turns } = await store.load();
    expect(turns).toEqual(mainTurns);
  });

  test("listBranches includes main and created branches", async () => {
    const dir = await tempDir();
    await createIsogitStore(dir);

    await createAndSwitchBranch(dir, "branch-a");
    await switchBranch(dir, "main");

    const branches = await listBranches(dir);
    expect(branches).toContain("main");
    expect(branches).toContain("branch-a");
  });
});

describe("history log", () => {
  test("log returns entries in reverse chronological order", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([]);

    await store.commit({ message: "commit one" });
    await store.writeTurns([]);

    await store.commit({ message: "commit two" });

    const entries = await logHistory(dir, 5);

    const first = entries[0];
    const second = entries[1];
    if (!first || !second) throw new Error("unreachable");
    expect(first.message).toBe("commit two");
    expect(second.message).toBe("commit one");
  });

  test("parentHash chains commits together", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([]);

    const first = await store.commit({ message: "first" });
    await store.writeTurns([]);

    const second = await store.commit({ message: "second" });

    expect(second.parentHash).toBe(first.hash);
  });
});

describe("readAt", () => {
  test("reads message history at an earlier commit", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const v1: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "version 1" }],
        timestamp: 1000,
      },
    ];
    await store.writeTurns(v1);

    const first = await store.commit({ message: "v1" });

    const v2: ConversationTurn[] = [
      ...v1,
      {
        role: "assistant",
        content: [{ type: "text", text: "version 2" }],
        model: "m",
        timestamp: 2000,
      },
    ];
    await store.writeTurns(v2);

    await store.commit({ message: "v2" });

    const atFirst = await store.readAt(first.hash);
    expect(atFirst).toEqual(v1);
  });
});

function makeAuditRecord(overrides: Partial<AuditRecord> = {}): AuditRecord {
  return {
    callId: "call-1",
    tool: "bash",
    arguments: { cmd: "ls" },
    authz: null,
    result: { content: "output", isError: false },
    timestamp: "2026-04-17T00:00:00.000Z",
    sessionId: "session-1",
    seq: 0,
    ...overrides,
  };
}

async function createAuditStore(dir: string): Promise<IsogitStore> {
  await initAgentRepo(dir);
  return new IsogitStore(dir);
}

describe("audit store", () => {
  test("round-trips a single audit record", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeAuditRecord();
    await store.commitAudit([record]);

    const loaded = await store.loadAudit("session-1");
    expect(loaded).toEqual([record]);
  });

  test("commits multiple records in a single batch", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const records = [
      makeAuditRecord({ callId: "c1", seq: 0 }),
      makeAuditRecord({ callId: "c2", seq: 1 }),
      makeAuditRecord({ callId: "c3", seq: 2 }),
    ];
    await store.commitAudit(records);

    const loaded = await store.loadAudit("session-1");
    expect(loaded.length).toBe(3);
    expect(loaded.map((r) => r.callId)).toEqual(["c1", "c2", "c3"]);
  });

  test("isolates records by session", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([
      makeAuditRecord({ sessionId: "s1", callId: "c1", seq: 0 }),
      makeAuditRecord({ sessionId: "s2", callId: "c2", seq: 0 }),
    ]);

    const s1 = await store.loadAudit("s1");
    const s2 = await store.loadAudit("s2");
    expect(s1.length).toBe(1);
    const r1 = s1[0];
    if (r1 === undefined) throw new Error("expected s1 record");
    expect(r1.callId).toBe("c1");
    expect(s2.length).toBe(1);
    const r2 = s2[0];
    if (r2 === undefined) throw new Error("expected s2 record");
    expect(r2.callId).toBe("c2");
  });

  test("returns empty array for nonexistent session", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const loaded = await store.loadAudit("no-such-session");
    expect(loaded).toEqual([]);
  });

  test("sorts loaded records by seq", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([
      makeAuditRecord({ callId: "c3", seq: 5 }),
      makeAuditRecord({ callId: "c1", seq: 1 }),
      makeAuditRecord({ callId: "c2", seq: 3 }),
    ]);

    const loaded = await store.loadAudit("session-1");
    expect(loaded.map((r) => r.seq)).toEqual([1, 3, 5]);
  });

  test("empty records array does not disturb existing records", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([makeAuditRecord({ callId: "c1", seq: 0 })]);
    await store.commitAudit([]);

    const loaded = await store.loadAudit("session-1");
    expect(loaded.length).toBe(1);
    expect(loaded[0]?.callId).toBe("c1");
  });

  test("accumulates records across multiple commits", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([makeAuditRecord({ callId: "c1", seq: 0 })]);
    await store.commitAudit([makeAuditRecord({ callId: "c2", seq: 1 })]);

    const loaded = await store.loadAudit("session-1");
    expect(loaded.length).toBe(2);
    expect(loaded.map((r) => r.callId)).toEqual(["c1", "c2"]);
  });

  test("preserves authz fields through round-trip", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeAuditRecord({
      authz: {
        effect: "allow",
        resolvedBy: {
          id: "g1",
          resource: "tool:bash",
          action: "invoke",
          effect: "allow",
          origin: "creator",
          specificity: 1009,
        },
        matchingGrants: [
          {
            id: "g1",
            resource: "tool:bash",
            action: "invoke",
            effect: "allow",
            origin: "creator",
            specificity: 1009,
          },
        ],
        blocked: false,
      },
    });
    await store.commitAudit([record]);

    const loaded = await store.loadAudit("session-1");
    const r = loaded[0];
    if (r === undefined) throw new Error("expected record");
    expect(r.authz).toEqual(record.authz);
  });

  test("rejects corrupted audit files on load", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([makeAuditRecord()]);

    const corruptPath = path.join(
      dir,
      "state",
      "audit",
      "session-1",
      "call-1.json",
    );
    await fs.promises.writeFile(corruptPath, JSON.stringify({ garbage: true }));

    let thrown: Error | undefined;
    try {
      await store.loadAudit("session-1");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Invalid audit record");
  });

  test("rejects sessionId with path traversal", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeAuditRecord({ sessionId: "../escape" });
    let thrown: Error | undefined;
    try {
      await store.commitAudit([record]);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("unsafe characters");
  });

  test("rejects callId with path traversal", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeAuditRecord({ callId: "../escape" });
    let thrown: Error | undefined;
    try {
      await store.commitAudit([record]);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("unsafe characters");
  });

  test("rejects sessionId with path traversal on load", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    let thrown: Error | undefined;
    try {
      await store.loadAudit("../escape");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("unsafe characters");
  });

  test("rejects duplicate callId within a session", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([makeAuditRecord({ callId: "c1", seq: 0 })]);
    let thrown: Error | undefined;
    try {
      await store.commitAudit([makeAuditRecord({ callId: "c1", seq: 1 })]);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Duplicate audit record");
  });

  test("commitAudit duplicate in batch leaves no orphaned files", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([makeAuditRecord({ callId: "c1", seq: 0 })]);

    // Batch contains a new record and a duplicate. Pre-flight should
    // reject before writing the new record to disk.
    const fresh = makeAuditRecord({ callId: "c2", seq: 1 });
    const dup = makeAuditRecord({ callId: "c1", seq: 2 });
    let thrown: Error | undefined;
    try {
      await store.commitAudit([fresh, dup]);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Duplicate audit record");

    const freshPath = path.join(dir, "state", "audit", "session-1", "c2.json");
    expect(fs.existsSync(freshPath)).toBe(false);
  });
});

function makeErrorRecord(overrides: Partial<ErrorRecord> = {}): ErrorRecord {
  return {
    source: "inference",
    category: "credential_failure",
    message: "Authentication failed",
    fatal: false,
    timestamp: "2026-04-17T00:00:00.000Z",
    sessionId: "session-1",
    seq: 1,
    ...overrides,
  };
}

describe("error store", () => {
  test("commitErrors writes error records to state/errors directory", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeErrorRecord();
    await store.commitErrors([record]);

    const expectedPath = path.join(
      dir,
      "state",
      "errors",
      "session-1",
      "00000001-credential_failure.json",
    );
    const raw = await fs.promises.readFile(expectedPath, "utf-8");
    expect(JSON.parse(raw)).toEqual(record);
  });

  test("commitErrors creates a git commit", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitErrors([makeErrorRecord()]);

    const entries = await git.log({ fs, dir, depth: 1 });
    const entry = entries[0];
    if (!entry) throw new Error("no commit found");
    expect(entry.commit.message.trimEnd()).toBe("Record 1 error record");
  });

  test("commitErrors rejects duplicate error records", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeErrorRecord({ seq: 1, category: "credential_failure" });
    await store.commitErrors([record]);
    let thrown: Error | undefined;
    try {
      await store.commitErrors([record]);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Duplicate error record");
  });

  test("commitErrors duplicate in batch leaves no orphaned files", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const good = makeErrorRecord({ seq: 1, category: "first" });
    await store.commitErrors([good]);

    // Second batch contains a duplicate of the first record and a new one.
    // The duplicate should be caught in pre-flight before any writes.
    const dup = makeErrorRecord({ seq: 1, category: "first" });
    const extra = makeErrorRecord({ seq: 2, category: "second" });
    let thrown: Error | undefined;
    try {
      await store.commitErrors([extra, dup]);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Duplicate error record");

    // The non-duplicate record from the failed batch must not exist on disk.
    const extraPath = path.join(
      dir,
      "state",
      "errors",
      "session-1",
      "00000002-second.json",
    );
    expect(fs.existsSync(extraPath)).toBe(false);
  });

  test("commitErrors validates sessionId path segments", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeErrorRecord({ sessionId: "../evil" });
    let thrown: Error | undefined;
    try {
      await store.commitErrors([record]);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("unsafe characters");
  });
});

describe("load reads from the working tree", () => {
  test("load reflects the most recent writeTurns + commit cycle", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "committed data" }],
        timestamp: 1000,
      },
    ];
    await store.writeTurns(messages);
    await store.commit({ message: "checkpoint" });

    const { turns: loaded } = await store.load();
    expect(loaded).toEqual(messages);
  });

  test("load throws when turns.jsonl is malformed", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    // Write garbage directly to the working tree to simulate corruption.
    await fs.promises.writeFile(
      path.join(dir, "turns.jsonl"),
      "NOT VALID JSON",
    );

    let thrown: Error | undefined;
    try {
      await store.load();
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown).toBeDefined();
  });

  test("load returns empty defaults on a fresh agent repo", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);
    const { turns, pendingOperations, tokenUsage, connectorState } =
      await store.load();
    expect(turns).toEqual([]);
    expect(pendingOperations).toEqual([]);
    expect(tokenUsage).toEqual(ZERO_USAGE);
    expect(connectorState).toBeNull();
  });
});

describe("connector thread state", () => {
  test("connector state round-trips through writeMetadata + commit/load", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const connectorState = {
      threadRoot: "<root@example.com>",
      lastMessageId: "<last@example.com>",
      replyTo: "user@example.com",
      cc: ["second@example.com"],
      subject: "Re: Test thread",
    };

    store.setConnectorState(connectorState);
    await store.writeTurns([]);
    await store.writeMetadata({
      pendingOperations: [],
      tokenUsage: ZERO_USAGE,
    });
    await store.commit({ message: "checkpoint" });
    const loaded = await store.load();

    expect(loaded.connectorState).toEqual(connectorState);
  });

  test("connector state with undefined subject round-trips", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const connectorState = {
      threadRoot: "<root@example.com>",
      lastMessageId: "<last@example.com>",
      replyTo: "user@example.com",
      cc: [],
    };

    store.setConnectorState(connectorState);
    await store.writeTurns([]);
    await store.writeMetadata({
      pendingOperations: [],
      tokenUsage: ZERO_USAGE,
    });
    await store.commit({ message: "checkpoint" });
    const loaded = await store.load();

    expect(loaded.connectorState).toEqual(connectorState);
  });

  test("null connector state round-trips through commit/load", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([]);
    await store.writeMetadata({
      pendingOperations: [],
      tokenUsage: ZERO_USAGE,
    });
    await store.commit({ message: "checkpoint" });
    const loaded = await store.load();

    expect(loaded.connectorState).toBeNull();
  });
});

describe("commit signing", () => {
  test("commits are signed when a signer is provided", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    const signer = async (payload: string) =>
      `-----BEGIN SSH SIGNATURE-----\n${Buffer.from(payload).toString("base64").slice(0, 70)}\n-----END SSH SIGNATURE-----`;

    const store = new IsogitStore(dir, signer);
    await store.writeTurns([]);

    await store.commit({ message: "signed commit" });

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (!entry) throw new Error("no commit found");
    const { commit } = await git.readCommit({ fs, dir, oid: entry.oid });
    expect(commit.gpgsig).toBeDefined();
    expect(commit.gpgsig).toContain("BEGIN SSH SIGNATURE");
  });

  test("commits are unsigned when no signer is provided", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    const store = new IsogitStore(dir);
    await store.writeTurns([]);

    await store.commit({ message: "unsigned commit" });

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (!entry) throw new Error("no commit found");
    const { commit } = await git.readCommit({ fs, dir, oid: entry.oid });
    expect(commit.gpgsig).toBeUndefined();
  });

  test("audit commits are signed when a signer is provided", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    const signer = async (payload: string) =>
      `-----BEGIN SSH SIGNATURE-----\n${Buffer.from(payload).toString("base64").slice(0, 70)}\n-----END SSH SIGNATURE-----`;

    const store = new IsogitStore(dir, signer);
    await store.commitAudit([makeAuditRecord()]);

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (!entry) throw new Error("no commit found");
    const { commit } = await git.readCommit({ fs, dir, oid: entry.oid });
    expect(commit.gpgsig).toBeDefined();
    expect(commit.gpgsig).toContain("BEGIN SSH SIGNATURE");
  });

  test("error commits are signed when a signer is provided", async () => {
    const dir = await tempDir();
    await initAgentRepo(dir);

    const signer = async (payload: string) =>
      `-----BEGIN SSH SIGNATURE-----\n${Buffer.from(payload).toString("base64").slice(0, 70)}\n-----END SSH SIGNATURE-----`;

    const store = new IsogitStore(dir, signer);
    await store.commitErrors([makeErrorRecord()]);

    const [entry] = await git.log({ fs, dir, depth: 1 });
    if (!entry) throw new Error("no commit found");
    const { commit } = await git.readCommit({ fs, dir, oid: entry.oid });
    expect(commit.gpgsig).toBeDefined();
    expect(commit.gpgsig).toContain("BEGIN SSH SIGNATURE");
  });
});

function makeTransformRecord(
  overrides: Partial<TransformRecord> = {},
): TransformRecord {
  return {
    strategy: "size-cap",
    version: "1",
    parameters: { maxChars: 10000 },
    reason: "exceeded-cap",
    decisions: { callId: "c1", originalBytes: 50000, kept: 10000 },
    ...overrides,
  };
}

describe("writeBlob / readBlob", () => {
  test("round-trips arbitrary bytes for a callId", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const bytes = new Uint8Array([1, 2, 3, 4, 5, 250]);
    await store.writeBlob("call-abc", bytes);
    const read = await store.readBlob("call-abc");

    expect(Array.from(read)).toEqual(Array.from(bytes));
  });

  test("text/plain content type yields .txt extension", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const bytes = new TextEncoder().encode("hello world");
    await store.writeBlob("text-call", bytes, "text/plain");

    const expectedPath = path.join(dir, "tool-output", "text-call.txt");
    expect(fs.existsSync(expectedPath)).toBe(true);
  });

  test("application/json content type yields .json extension", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const bytes = new TextEncoder().encode('{"ok":true}');
    await store.writeBlob("json-call", bytes, "application/json");

    const expectedPath = path.join(dir, "tool-output", "json-call.json");
    expect(fs.existsSync(expectedPath)).toBe(true);
  });

  test("unknown content type yields no extension", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const bytes = new Uint8Array([0xff, 0xee]);
    await store.writeBlob("raw-call", bytes, "application/octet-stream");

    const expectedPath = path.join(dir, "tool-output", "raw-call");
    expect(fs.existsSync(expectedPath)).toBe(true);
  });

  test("readBlob throws a clear error when the key has no blob", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    let thrown: Error | undefined;
    try {
      await store.readBlob("missing");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Blob not found for key");
  });

  test("readBlob throws a clear error when tool-output/ does not exist", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    let thrown: Error | undefined;
    try {
      await store.readBlob("call-x");
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toContain("Blob not found for key");
  });

  test("writeBlob rejects callIds containing path traversal", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    let thrownTraversal: Error | undefined;
    try {
      await store.writeBlob("../escape", new Uint8Array());
    } catch (cause) {
      thrownTraversal =
        cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrownTraversal?.message).toContain("unsafe characters");

    let thrownSlash: Error | undefined;
    try {
      await store.writeBlob("a/b", new Uint8Array());
    } catch (cause) {
      thrownSlash = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrownSlash?.message).toContain("unsafe characters");
  });

  test("writeBlob sanitizes other unsafe characters in the filename", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeBlob("call!@#xyz", new Uint8Array([7]));

    const expectedPath = path.join(dir, "tool-output", "call___xyz");
    expect(fs.existsSync(expectedPath)).toBe(true);

    // The same sanitization applies on read.
    const read = await store.readBlob("call!@#xyz");
    expect(Array.from(read)).toEqual([7]);
  });

  test("writeBlob overwrites the file when the same key is written twice", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeBlob("dup", new Uint8Array([1, 2, 3]));
    await store.writeBlob("dup", new Uint8Array([9, 8]));

    const read = await store.readBlob("dup");
    expect(Array.from(read)).toEqual([9, 8]);
  });
});

describe("writeTurns / writePrompt / writeResponse / writeManifest", () => {
  test("writeTurns writes parseable JSONL to turns.jsonl", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const turns: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "hi" }],
        timestamp: 1000,
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "hello" }],
        model: "m",
        timestamp: 2000,
      },
    ];
    await store.writeTurns(turns);

    const raw = await fs.promises.readFile(
      path.join(dir, "turns.jsonl"),
      "utf-8",
    );
    const lines = raw.split("\n").filter((l) => l.length > 0);
    expect(lines.length).toBe(2);
    expect(JSON.parse(lines[0] ?? "")).toEqual(turns[0]);
    expect(JSON.parse(lines[1] ?? "")).toEqual(turns[1]);
  });

  test("writeTurns with an empty array produces an empty file", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([]);

    const raw = await fs.promises.readFile(
      path.join(dir, "turns.jsonl"),
      "utf-8",
    );
    expect(raw).toBe("");
  });

  test("writePrompt writes JSONL to prompt.jsonl", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const turns: ConversationTurn[] = [
      {
        role: "system",
        content: [{ type: "text", text: "be nice" }],
        timestamp: 100,
      },
      {
        role: "user",
        content: [{ type: "text", text: "do the thing" }],
        timestamp: 200,
      },
    ];
    await store.writePrompt(turns);

    const raw = await fs.promises.readFile(
      path.join(dir, "prompt.jsonl"),
      "utf-8",
    );
    const lines = raw.split("\n").filter((l) => l.length > 0);
    expect(lines.length).toBe(2);
    expect(JSON.parse(lines[0] ?? "")).toEqual(turns[0]);
    expect(JSON.parse(lines[1] ?? "")).toEqual(turns[1]);
  });

  test("writeResponse writes single-line JSONL with an AssistantTurn", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const turn: AssistantTurn = {
      role: "assistant",
      content: [{ type: "text", text: "done" }],
      model: "test-model",
      timestamp: 5000,
    };
    await store.writeResponse(turn);

    const raw = await fs.promises.readFile(
      path.join(dir, "response.jsonl"),
      "utf-8",
    );
    const lines = raw.split("\n").filter((l) => l.length > 0);
    expect(lines.length).toBe(1);
    expect(JSON.parse(lines[0] ?? "")).toEqual(turn);
  });

  test("writeManifest writes TransformRecord entries in order", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const records = [
      makeTransformRecord({ strategy: "first", reason: "r1" }),
      makeTransformRecord({ strategy: "second", reason: "r2" }),
    ];
    await store.writeManifest(records);

    const raw = await fs.promises.readFile(
      path.join(dir, "manifest.jsonl"),
      "utf-8",
    );
    const lines = raw.split("\n").filter((l) => l.length > 0);
    expect(lines.length).toBe(2);
    expect(JSON.parse(lines[0] ?? "")).toEqual(records[0]);
    expect(JSON.parse(lines[1] ?? "")).toEqual(records[1]);
  });
});

describe("commit({ message }) overload", () => {
  test("commits the working-tree files written via the new writers", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const turns: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "first" }],
        timestamp: 1000,
      },
    ];
    await store.writeTurns(turns);
    await store.writeManifest([makeTransformRecord()]);

    const result = await store.commit({ message: "cycle 1" });

    expect(typeof result.hash).toBe("string");
    expect(result.message).toBe("cycle 1");

    // turns.jsonl is in the commit tree.
    const { blob } = await git.readBlob({
      fs,
      dir,
      oid: result.hash,
      filepath: "turns.jsonl",
    });
    const text = new TextDecoder().decode(blob);
    expect(text.includes("first")).toBe(true);
  });

  test("commit writes only the per-cycle files and tool-output blobs", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([
      {
        role: "user",
        content: [{ type: "text", text: "x" }],
        timestamp: 100,
      },
    ]);
    const result = await store.commit({ message: "wt only" });

    const { tree } = await git.readTree({ fs, dir, oid: result.hash });
    const paths = tree.map((entry) => entry.path).sort();
    // Initial commit included .gitignore; the new commit stages turns.jsonl
    // at the repo root. The legacy single-file serializer is gone.
    expect(paths).toContain("turns.jsonl");
    expect(paths).not.toContain("state");
  });

  test("two consecutive working-tree commits yield two distinct commits", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeTurns([
      {
        role: "user",
        content: [{ type: "text", text: "v1" }],
        timestamp: 100,
      },
    ]);
    const c1 = await store.commit({ message: "cycle one" });

    await store.writeTurns([
      {
        role: "user",
        content: [{ type: "text", text: "v1" }],
        timestamp: 100,
      },
      {
        role: "assistant",
        content: [{ type: "text", text: "v2" }],
        model: "m",
        timestamp: 200,
      },
    ]);
    const c2 = await store.commit({ message: "cycle two" });

    expect(c1.hash).not.toBe(c2.hash);
    expect(c2.parentHash).toBe(c1.hash);

    const entries = await store.log(5);
    const messages = entries.map((e) => e.message);
    expect(messages).toContain("cycle one");
    expect(messages).toContain("cycle two");

    // The latest turns.jsonl reflects the second write.
    const { blob } = await git.readBlob({
      fs,
      dir,
      oid: c2.hash,
      filepath: "turns.jsonl",
    });
    const text = new TextDecoder().decode(blob);
    expect(text.includes("v2")).toBe(true);
  });

  test("blobs written via writeBlob land in the commit tree", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeBlob(
      "spill-1",
      new TextEncoder().encode("big payload"),
      "text/plain",
    );
    const result = await store.commit({ message: "with spill" });

    const { blob } = await git.readBlob({
      fs,
      dir,
      oid: result.hash,
      filepath: "tool-output/spill-1.txt",
    });
    expect(new TextDecoder().decode(blob)).toBe("big payload");
  });

  test("initAgentRepo does not create the legacy state context file", async () => {
    const dir = await tempDir();
    await createIsogitStore(dir);

    // The legacy serializer wrote a single state file under `state/`. The
    // working-tree layout replaces it with per-cycle files at the repo root.
    const legacyName = ["context", "json"].join(".");
    const exists = await fs.promises
      .access(path.join(dir, "state", legacyName))
      .then(() => true)
      .catch(() => false);
    expect(exists).toBe(false);
  });
});

describe("readManifestHistory", () => {
  test("returns records from the most recent commits, newest first", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const r1 = makeTransformRecord({ strategy: "first" });
    await store.writeManifest([r1]);
    await store.commit({ message: "c1" });

    const r2 = makeTransformRecord({ strategy: "second" });
    await store.writeManifest([r2]);
    await store.commit({ message: "c2" });

    const r3a = makeTransformRecord({ strategy: "third-a" });
    const r3b = makeTransformRecord({ strategy: "third-b" });
    await store.writeManifest([r3a, r3b]);
    await store.commit({ message: "c3" });

    const history = await store.readManifestHistory(5);

    // Newest commit first; within a commit, natural file order.
    expect(history.map((r) => r.strategy)).toEqual([
      "third-a",
      "third-b",
      "second",
      "first",
    ]);
  });

  test("respects the limit parameter", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    for (let i = 0; i < 3; i++) {
      await store.writeManifest([
        makeTransformRecord({ strategy: `cycle-${String(i)}` }),
      ]);
      await store.commit({ message: `c${String(i)}` });
    }

    const limited = await store.readManifestHistory(2);
    expect(limited.map((r) => r.strategy)).toEqual(["cycle-2", "cycle-1"]);
  });

  test("returns empty array when limit is zero", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.writeManifest([makeTransformRecord()]);
    await store.commit({ message: "c" });

    const history = await store.readManifestHistory(0);
    expect(history).toEqual([]);
  });

  test("skips commits that lack manifest.jsonl", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    // The initial commit from initAgentRepo and the legacy commit below both
    // have no manifest.jsonl, so they should be skipped.
    await store.writeTurns([]);

    await store.commit({ message: "legacy" });

    const r = makeTransformRecord();
    await store.writeManifest([r]);
    await store.commit({ message: "with manifest" });

    const history = await store.readManifestHistory(10);
    expect(history).toEqual([r]);
  });

  test("rejects a manifest with an invalid record", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    // Hand-write a corrupt manifest.jsonl and commit it via the working-tree
    // overload so it lands in git without going through writeManifest.
    await fs.promises.writeFile(
      path.join(dir, "manifest.jsonl"),
      JSON.stringify({ strategy: "bogus" }) + "\n",
    );
    await store.commit({ message: "corrupt" });

    let thrown: Error | undefined;
    try {
      await store.readManifestHistory(5);
    } catch (cause) {
      thrown = cause instanceof Error ? cause : new Error(String(cause));
    }
    expect(thrown?.message).toMatch(/Invalid manifest record/);
  });
});
