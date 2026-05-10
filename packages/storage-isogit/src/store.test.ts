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
import type { ConversationTurn, TokenUsage } from "@interchange/types/runtime";
import type { AuditRecord, ErrorRecord } from "@interchange/types/audit";

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

    await store.commit(messages, [], ZERO_USAGE, "first checkpoint");
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

    await store.commit(first, [], ZERO_USAGE, "step 1");
    await store.commit(second, [], ZERO_USAGE, "step 2");

    const { turns } = await store.load();
    expect(turns).toEqual(second);
  });
});

describe("checkpoint creates commit", () => {
  test("commit returns a ContextCommit with correct metadata", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const commitResult = await store.commit(
      [],
      [],
      ZERO_USAGE,
      "named checkpoint",
    );

    expect(typeof commitResult.hash).toBe("string");
    expect(commitResult.hash.length).toBeGreaterThan(0);
    expect(commitResult.message).toBe("named checkpoint");
    expect(typeof commitResult.timestamp).toBe("number");
    expect(commitResult.timestamp).toBeGreaterThan(0);
  });

  test("log contains the checkpoint after commit", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.commit([], [], ZERO_USAGE, "my checkpoint");
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
    await store.commit(mainTurns, [], ZERO_USAGE, "main work");

    await createAndSwitchBranch(dir, "experiment");

    const branchTurns: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "on branch" }],
        timestamp: 2000,
      },
    ];
    await store.commit(branchTurns, [], ZERO_USAGE, "branch work");

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

    await store.commit([], [], ZERO_USAGE, "commit one");
    await store.commit([], [], ZERO_USAGE, "commit two");

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

    const first = await store.commit([], [], ZERO_USAGE, "first");
    const second = await store.commit([], [], ZERO_USAGE, "second");

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
    const first = await store.commit(v1, [], ZERO_USAGE, "v1");

    const v2: ConversationTurn[] = [
      ...v1,
      {
        role: "assistant",
        content: [{ type: "text", text: "version 2" }],
        model: "m",
        timestamp: 2000,
      },
    ];
    await store.commit(v2, [], ZERO_USAGE, "v2");

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

    await expect(store.loadAudit("session-1")).rejects.toThrow(
      "Invalid audit record",
    );
  });

  test("rejects sessionId with path traversal", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeAuditRecord({ sessionId: "../escape" });
    await expect(store.commitAudit([record])).rejects.toThrow(
      "unsafe characters",
    );
  });

  test("rejects callId with path traversal", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    const record = makeAuditRecord({ callId: "../escape" });
    await expect(store.commitAudit([record])).rejects.toThrow(
      "unsafe characters",
    );
  });

  test("rejects sessionId with path traversal on load", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await expect(store.loadAudit("../escape")).rejects.toThrow(
      "unsafe characters",
    );
  });

  test("rejects duplicate callId within a session", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([makeAuditRecord({ callId: "c1", seq: 0 })]);
    await expect(
      store.commitAudit([makeAuditRecord({ callId: "c1", seq: 1 })]),
    ).rejects.toThrow("Duplicate audit record");
  });

  test("commitAudit duplicate in batch leaves no orphaned files", async () => {
    const dir = await tempDir();
    const store = await createAuditStore(dir);

    await store.commitAudit([makeAuditRecord({ callId: "c1", seq: 0 })]);

    // Batch contains a new record and a duplicate. Pre-flight should
    // reject before writing the new record to disk.
    const fresh = makeAuditRecord({ callId: "c2", seq: 1 });
    const dup = makeAuditRecord({ callId: "c1", seq: 2 });
    await expect(store.commitAudit([fresh, dup])).rejects.toThrow(
      "Duplicate audit record",
    );

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
    await expect(store.commitErrors([record])).rejects.toThrow(
      "Duplicate error record",
    );
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
    await expect(store.commitErrors([extra, dup])).rejects.toThrow(
      "Duplicate error record",
    );

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
    await expect(store.commitErrors([record])).rejects.toThrow(
      "unsafe characters",
    );
  });
});

describe("load reads from git HEAD, not working tree", () => {
  test("load ignores a corrupt working-tree context.json and reads from HEAD", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const messages: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "committed data" }],
        timestamp: 1000,
      },
    ];
    await store.commit(messages, [], ZERO_USAGE, "checkpoint");

    // Corrupt the working-tree file without committing.
    const contextPath = path.join(dir, "state", "context.json");
    await fs.promises.writeFile(contextPath, "NOT VALID JSON }{");

    // load() should read from git HEAD, not the corrupt working-tree file.
    const { turns: loaded } = await store.load();
    expect(loaded).toEqual(messages);
  });

  test("load falls back to the last good commit when HEAD context.json is unparseable", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const goodTurns: ConversationTurn[] = [
      {
        role: "user",
        content: [{ type: "text", text: "good data" }],
        timestamp: 1000,
      },
    ];
    await store.commit(goodTurns, [], ZERO_USAGE, "good checkpoint");

    // Commit garbage directly into context.json via the git plumbing to
    // simulate a corrupt HEAD blob. We write the garbage, stage it, and
    // commit it so HEAD has the invalid state.
    const contextPath = path.join(dir, "state", "context.json");
    await fs.promises.writeFile(contextPath, "CORRUPT");
    await git.add({ fs, dir, filepath: "state/context.json" });
    await git.commit({
      fs,
      dir,
      message: "bad checkpoint",
      author: { name: "test", email: "test@test.com" },
    });

    // load() should walk back and find the last good checkpoint.
    const { turns: loaded } = await store.load();
    expect(loaded).toEqual(goodTurns);
  });
});

describe("connector thread state", () => {
  test("connector state round-trips through commit/load", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    const connectorState = {
      threadRoot: "<root@example.com>",
      lastMessageId: "<last@example.com>",
      replyTo: "user@example.com",
      subject: "Re: Test thread",
    };

    store.setConnectorState(connectorState);
    await store.commit([], [], ZERO_USAGE, "checkpoint");
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
    };

    store.setConnectorState(connectorState);
    await store.commit([], [], ZERO_USAGE, "checkpoint");
    const loaded = await store.load();

    expect(loaded.connectorState).toEqual(connectorState);
  });

  test("null connector state round-trips through commit/load", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    await store.commit([], [], ZERO_USAGE, "checkpoint");
    const loaded = await store.load();

    expect(loaded.connectorState).toBeNull();
  });

  test("load returns null connector state for old commits lacking the field", async () => {
    const dir = await tempDir();
    const store = await createIsogitStore(dir);

    // Write a context.json that mimics an old commit without connectorState.
    const contextPath = path.join(dir, "state", "context.json");
    const oldFormat = {
      turns: [],
      pendingOperations: [],
      tokenUsage: ZERO_USAGE,
    };
    await fs.promises.writeFile(
      contextPath,
      JSON.stringify(oldFormat, null, 2),
    );
    await git.add({ fs, dir, filepath: "state/context.json" });
    await git.commit({
      fs,
      dir,
      message: "old format checkpoint",
      author: { name: "test", email: "test@test.com" },
    });

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
    await store.commit([], [], ZERO_USAGE, "signed commit");

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
    await store.commit([], [], ZERO_USAGE, "unsigned commit");

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
