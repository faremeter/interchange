import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import type {
  ContextStore,
  AuditStore,
  ContextCommit,
  ConversationMessage,
  PendingOperation,
  TokenUsage,
} from "@interchange/types/runtime";
import { type } from "arktype";
import {
  AuditRecord,
  type AuditRecord as AuditRecordType,
  type ErrorRecord,
} from "@interchange/types/audit";
import { AUTHOR } from "./init";
import type { CommitSigner } from "./signer";
import { buildSigningArgs } from "./commit-helpers";

const CONTEXT_FILE = "state/context.json";

type ContextData = {
  messages: ConversationMessage[];
  pendingOperations: PendingOperation[];
  tokenUsage: TokenUsage;
};

function parseContextData(raw: unknown): ContextData {
  if (
    typeof raw !== "object" ||
    raw === null ||
    !Array.isArray((raw as Record<string, unknown>)["messages"]) ||
    !Array.isArray((raw as Record<string, unknown>)["pendingOperations"]) ||
    typeof (raw as Record<string, unknown>)["tokenUsage"] !== "object"
  ) {
    throw new Error("context data has unexpected structure");
  }
  return raw as ContextData;
}

async function readCommitLog(
  dir: string,
  limit: number,
): Promise<ContextCommit[]> {
  const entries = await git.log({ fs, dir, depth: limit });
  return entries.map((e) => {
    const base = {
      hash: e.oid,
      message: e.commit.message.trimEnd(),
      timestamp: e.commit.author.timestamp * 1000,
    };
    const parent = e.commit.parent[0];
    return parent !== undefined ? { ...base, parentHash: parent } : base;
  });
}

const AUDIT_DIR = "state/audit";
const ERRORS_DIR = "state/errors";

const SAFE_PATH_SEGMENT = /^[a-zA-Z0-9_-]+$/;

function assertSafeSegment(value: string, label: string): void {
  if (!SAFE_PATH_SEGMENT.test(value)) {
    throw new Error(
      `${label} contains unsafe characters: ${JSON.stringify(value)}`,
    );
  }
}

/**
 * isomorphic-git-backed implementation of ContextStore and AuditStore.
 *
 * Context state is serialized into `state/context.json`. Audit records are
 * written as individual JSON files under `state/audit/{sessionId}/`. Both
 * are tracked by the git repository at `dir`. The caller is responsible
 * for calling `initAgentRepo(dir)` before constructing.
 */
export class IsogitStore implements ContextStore, AuditStore {
  private readonly dir: string;
  private readonly signer: CommitSigner | undefined;

  constructor(dir: string, signer?: CommitSigner) {
    this.dir = dir;
    this.signer = signer;
  }

  private signingArgs() {
    return buildSigningArgs(this.signer);
  }

  async load(_signal?: AbortSignal): Promise<{
    messages: ConversationMessage[];
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
  }> {
    const contextPath = path.join(this.dir, CONTEXT_FILE);
    const raw = await fs.promises.readFile(contextPath, "utf-8");
    const parsed = JSON.parse(raw) as unknown;
    return parseContextData(parsed);
  }

  async commit(
    messages: ConversationMessage[],
    pendingOperations: PendingOperation[],
    tokenUsage: TokenUsage,
    message: string,
    _signal?: AbortSignal,
  ): Promise<ContextCommit> {
    const data: ContextData = { messages, pendingOperations, tokenUsage };
    const contextPath = path.join(this.dir, CONTEXT_FILE);
    await fs.promises.writeFile(contextPath, JSON.stringify(data, null, 2));

    await git.add({ fs, dir: this.dir, filepath: CONTEXT_FILE });
    const oid = await git.commit({
      fs,
      dir: this.dir,
      message,
      author: AUTHOR,
      ...this.signingArgs(),
    });

    const entries = await git.log({ fs, dir: this.dir, depth: 2 });
    const entry = entries[0];
    if (entry === undefined || entry.oid !== oid) {
      throw new Error(
        `Unexpected log state after commit: expected ${oid} as HEAD`,
      );
    }
    const parentOid = entries[1]?.oid;
    const base = {
      hash: oid,
      message: message.trimEnd(),
      timestamp: entry.commit.author.timestamp * 1000,
    };
    return parentOid !== undefined ? { ...base, parentHash: parentOid } : base;
  }

  async branch(name: string, _signal?: AbortSignal): Promise<void> {
    await git.branch({ fs, dir: this.dir, ref: name });
  }

  async log(limit?: number, _signal?: AbortSignal): Promise<ContextCommit[]> {
    return readCommitLog(this.dir, limit ?? 10);
  }

  async readAt(
    hash: string,
    _signal?: AbortSignal,
  ): Promise<ConversationMessage[]> {
    const { blob } = await git.readBlob({
      fs,
      dir: this.dir,
      oid: hash,
      filepath: CONTEXT_FILE,
    });
    const text = new TextDecoder().decode(blob);
    const parsed = JSON.parse(text) as unknown;
    const data = parseContextData(parsed);
    return data.messages;
  }

  async commitAudit(
    records: AuditRecordType[],
    _signal?: AbortSignal,
  ): Promise<void> {
    if (records.length === 0) return;

    for (const record of records) {
      assertSafeSegment(record.sessionId, "sessionId");
      assertSafeSegment(record.callId, "callId");

      const sessionDir = path.join(this.dir, AUDIT_DIR, record.sessionId);
      await fs.promises.mkdir(sessionDir, { recursive: true });

      const filepath = path.join(
        AUDIT_DIR,
        record.sessionId,
        `${record.callId}.json`,
      );
      const fullPath = path.join(this.dir, filepath);

      try {
        await fs.promises.access(fullPath);
        throw new Error(
          `Duplicate audit record: ${record.sessionId}/${record.callId}`,
        );
      } catch (e) {
        if (e instanceof Error && "code" in e && e.code === "ENOENT") {
          // Expected: file does not exist yet.
        } else {
          throw e;
        }
      }

      await fs.promises.writeFile(fullPath, JSON.stringify(record, null, 2));
      await git.add({ fs, dir: this.dir, filepath });
    }

    const count = records.length;
    const noun = count === 1 ? "record" : "records";
    await git.commit({
      fs,
      dir: this.dir,
      message: `Record ${count} tool audit ${noun}`,
      author: AUTHOR,
      ...this.signingArgs(),
    });
  }

  async commitErrors(
    records: ErrorRecord[],
    _signal?: AbortSignal,
  ): Promise<void> {
    if (records.length === 0) return;

    for (const record of records) {
      assertSafeSegment(record.sessionId, "sessionId");

      const sessionDir = path.join(this.dir, ERRORS_DIR, record.sessionId);
      await fs.promises.mkdir(sessionDir, { recursive: true });

      const sanitizedCategory = record.category.replace(/[^a-zA-Z0-9_-]/g, "_");
      const seq = String(record.seq).padStart(4, "0");
      const filepath = path.join(
        ERRORS_DIR,
        record.sessionId,
        `${seq}-${sanitizedCategory}.json`,
      );
      const fullPath = path.join(this.dir, filepath);

      if (fs.existsSync(fullPath)) {
        throw new Error(
          `Duplicate error record: ${record.sessionId}/${seq}-${sanitizedCategory}`,
        );
      }

      await fs.promises.writeFile(fullPath, JSON.stringify(record, null, 2));
      await git.add({ fs, dir: this.dir, filepath });
    }

    const count = records.length;
    const noun = count === 1 ? "record" : "records";
    await git.commit({
      fs,
      dir: this.dir,
      message: `Record ${count} error ${noun}`,
      author: AUTHOR,
      ...this.signingArgs(),
    });
  }

  async loadAudit(
    sessionId: string,
    _signal?: AbortSignal,
  ): Promise<AuditRecordType[]> {
    assertSafeSegment(sessionId, "sessionId");
    const sessionDir = path.join(this.dir, AUDIT_DIR, sessionId);

    let entries: string[];
    try {
      entries = await fs.promises.readdir(sessionDir);
    } catch (cause) {
      if (
        cause instanceof Error &&
        "code" in cause &&
        cause.code === "ENOENT"
      ) {
        return [];
      }
      throw cause;
    }

    const records: AuditRecordType[] = [];
    for (const entry of entries) {
      if (!entry.endsWith(".json")) continue;
      const fullPath = path.join(sessionDir, entry);
      const raw = await fs.promises.readFile(fullPath, "utf-8");
      const parsed = JSON.parse(raw) as unknown;
      const result = AuditRecord(parsed);
      if (result instanceof type.errors) {
        throw new Error(`Invalid audit record in ${entry}: ${result.summary}`);
      }
      records.push(result);
    }

    records.sort((a, b) => a.seq - b.seq);
    return records;
  }
}
