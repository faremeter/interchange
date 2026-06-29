import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import {
  ContentBlock,
  TokenUsage,
  TransformRecord,
  type AssistantTurn,
  type TransformRecord as TransformRecordType,
  type ContextStore,
  type AuditStore,
  type ContextCommit,
  type ConversationTurn,
  type ConnectorThreadState,
  type PendingOperation,
} from "@intx/types/runtime";
import { type } from "arktype";
import {
  AuditRecord,
  type AuditRecord as AuditRecordType,
  type ErrorRecord,
} from "@intx/types/audit";
import { AUTHOR } from "./init";
import type { CommitSigner } from "./signer";
import { buildSigningArgs } from "./commit-helpers";
import { withRepoDirLock } from "./repo-lock";

const TURNS_FILE = "turns.jsonl";
const PROMPT_FILE = "prompt.jsonl";
const RESPONSE_FILE = "response.jsonl";
const MANIFEST_FILE = "manifest.jsonl";
const METADATA_FILE = "metadata.json";
const TOOL_OUTPUT_DIR = "tool-output";

const BLOB_EXTENSIONS: Readonly<Record<string, string>> = {
  "text/plain": ".txt",
  "application/json": ".json",
};

function blobExtensionFor(contentType: string | undefined): string {
  if (contentType === undefined) return "";
  const ext = BLOB_EXTENSIONS[contentType];
  return ext ?? "";
}

const ConnectorThreadStateSchema = type({
  threadRoot: "string",
  lastMessageId: "string",
  replyTo: "string",
  cc: "string[]",
  "subject?": "string",
});

const ConversationTurnSchema = type({
  role: "'user' | 'assistant' | 'system'",
  content: ContentBlock.array(),
  "model?": "string",
  timestamp: "number",
});

const PendingOperationSchema = type({
  correlationId: "string",
  "expectedFrom?": "string",
  registeredAt: "number",
  gateId: "string",
});

const MetadataSchema = type({
  pendingOperations: PendingOperationSchema.array(),
  tokenUsage: TokenUsage,
  connectorState: type("null").or(ConnectorThreadStateSchema),
});

type MetadataData = {
  pendingOperations: PendingOperation[];
  tokenUsage: TokenUsage;
  connectorState: ConnectorThreadState | null;
};

const EMPTY_USAGE: TokenUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function parseMetadata(raw: unknown): MetadataData {
  const result = MetadataSchema(raw);
  if (result instanceof type.errors) {
    throw new Error(
      `metadata.json has unexpected structure: ${result.summary}`,
    );
  }
  return {
    pendingOperations: result.pendingOperations,
    tokenUsage: result.tokenUsage,
    connectorState: result.connectorState,
  };
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
const UNSAFE_FILENAME_CHARS = /[^a-zA-Z0-9_-]/g;

function assertSafeSegment(value: string, label: string): void {
  if (!SAFE_PATH_SEGMENT.test(value)) {
    throw new Error(
      `${label} contains unsafe characters: ${JSON.stringify(value)}`,
    );
  }
}

/**
 * Validate a callId for use in a filesystem path and return the sanitized
 * form used as the filename. Rejects path traversal (`..`, `/`) outright;
 * other unsafe characters are replaced with `_`.
 */
function sanitizeCallId(callId: string): string {
  if (callId.includes("..") || callId.includes("/")) {
    throw new Error(
      `callId contains unsafe characters: ${JSON.stringify(callId)}`,
    );
  }
  return callId.replace(UNSAFE_FILENAME_CHARS, "_");
}

async function pathExists(fullPath: string): Promise<boolean> {
  try {
    await fs.promises.access(fullPath);
    return true;
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") {
      return false;
    }
    throw cause;
  }
}

function encodeJsonlLines(records: readonly unknown[]): string {
  if (records.length === 0) return "";
  return records.map((r) => JSON.stringify(r)).join("\n") + "\n";
}

function decodeJsonlLines(text: string): unknown[] {
  if (text.length === 0) return [];
  const lines = text.split("\n");
  if (lines[lines.length - 1] === "") lines.pop();
  return lines.map((line) => JSON.parse(line) as unknown);
}

async function readBlobAtCommit(
  dir: string,
  oid: string,
  filepath: string,
): Promise<Uint8Array | null> {
  try {
    const { blob } = await git.readBlob({ fs, dir, oid, filepath });
    return blob;
  } catch (cause) {
    if (
      cause instanceof Error &&
      "code" in cause &&
      (cause.code === "NotFoundError" || cause.code === "ENOENT")
    ) {
      return null;
    }
    return null;
  }
}

function parseTurns(text: string): ConversationTurn[] {
  const lines = decodeJsonlLines(text);
  const turns: ConversationTurn[] = [];
  for (const raw of lines) {
    const result = ConversationTurnSchema(raw);
    if (result instanceof type.errors) {
      throw new Error(
        `turns.jsonl has unexpected structure: ${result.summary}`,
      );
    }
    turns.push(result);
  }
  return turns;
}

/**
 * isomorphic-git-backed implementation of ContextStore and AuditStore.
 *
 * Conversation state lives in `turns.jsonl`; per-cycle prompt/response/manifest
 * data lives in `prompt.jsonl`, `response.jsonl`, and `manifest.jsonl`. Pending
 * operations, token usage, and connector state are serialized into
 * `metadata.json`. Audit records are written as individual JSON files under
 * `state/audit/{sessionId}/`. All files are tracked by the git repository at
 * `dir`. The caller is responsible for calling `initAgentRepo(dir)` before
 * constructing.
 */
export class IsogitStore implements ContextStore, AuditStore {
  private readonly dir: string;
  private readonly signer: CommitSigner | undefined;
  private pendingConnectorState: ConnectorThreadState | null = null;

  constructor(dir: string, signer?: CommitSigner) {
    this.dir = dir;
    this.signer = signer;
  }

  private signingArgs() {
    return buildSigningArgs(this.signer);
  }

  setConnectorState(state: ConnectorThreadState | null): void {
    this.pendingConnectorState = state;
  }

  async load(_signal?: AbortSignal): Promise<{
    turns: ConversationTurn[];
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
    connectorState: ConnectorThreadState | null;
  }> {
    const turnsPath = path.join(this.dir, TURNS_FILE);
    const metadataPath = path.join(this.dir, METADATA_FILE);

    let turns: ConversationTurn[] = [];
    if (await pathExists(turnsPath)) {
      const text = await fs.promises.readFile(turnsPath, "utf-8");
      turns = parseTurns(text);
    }

    let pendingOperations: PendingOperation[] = [];
    let tokenUsage: TokenUsage = { ...EMPTY_USAGE };
    let connectorState: ConnectorThreadState | null = null;

    if (await pathExists(metadataPath)) {
      const text = await fs.promises.readFile(metadataPath, "utf-8");
      const parsed: unknown = JSON.parse(text);
      const data = parseMetadata(parsed);
      pendingOperations = data.pendingOperations;
      tokenUsage = data.tokenUsage;
      connectorState = data.connectorState;
    }

    return { turns, pendingOperations, tokenUsage, connectorState };
  }

  async commit(
    options: { message: string },
    _signal?: AbortSignal,
  ): Promise<ContextCommit> {
    return withRepoDirLock(this.dir, async () => {
      const tracked = [
        TURNS_FILE,
        PROMPT_FILE,
        RESPONSE_FILE,
        MANIFEST_FILE,
        METADATA_FILE,
      ];
      for (const filepath of tracked) {
        const fullPath = path.join(this.dir, filepath);
        if (await pathExists(fullPath)) {
          await git.add({ fs, dir: this.dir, filepath });
        }
      }

      const blobsDir = path.join(this.dir, TOOL_OUTPUT_DIR);
      if (await pathExists(blobsDir)) {
        const entries = await fs.promises.readdir(blobsDir);
        for (const entry of entries) {
          await git.add({
            fs,
            dir: this.dir,
            filepath: `${TOOL_OUTPUT_DIR}/${entry}`,
          });
        }
      }

      const oid = await git.commit({
        fs,
        dir: this.dir,
        message: options.message,
        author: AUTHOR,
        ...this.signingArgs(),
      });

      return this.describeHead(oid, options.message);
    });
  }

  private async describeHead(
    expectedOid: string,
    message: string,
  ): Promise<ContextCommit> {
    const entries = await git.log({ fs, dir: this.dir, depth: 2 });
    const entry = entries[0];
    if (entry === undefined || entry.oid !== expectedOid) {
      throw new Error(
        `Unexpected log state after commit: expected ${expectedOid} as HEAD`,
      );
    }
    const parentOid = entries[1]?.oid;
    const base = {
      hash: expectedOid,
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
  ): Promise<ConversationTurn[]> {
    const blob = await readBlobAtCommit(this.dir, hash, TURNS_FILE);
    if (blob === null) return [];
    const text = new TextDecoder().decode(blob);
    return parseTurns(text);
  }

  async writeBlob(
    key: string,
    bytes: Uint8Array,
    contentType?: string,
    _signal?: AbortSignal,
  ): Promise<void> {
    const safeKey = sanitizeCallId(key);
    const filename = `${safeKey}${blobExtensionFor(contentType)}`;
    const dirPath = path.join(this.dir, TOOL_OUTPUT_DIR);
    await fs.promises.mkdir(dirPath, { recursive: true });
    await fs.promises.writeFile(path.join(dirPath, filename), bytes);
  }

  async readBlob(key: string, _signal?: AbortSignal): Promise<Uint8Array> {
    const safeKey = sanitizeCallId(key);
    const dirPath = path.join(this.dir, TOOL_OUTPUT_DIR);
    let entries: string[];
    try {
      entries = await fs.promises.readdir(dirPath);
    } catch (cause) {
      if (
        cause instanceof Error &&
        "code" in cause &&
        cause.code === "ENOENT"
      ) {
        throw new Error(`Blob not found for key: ${JSON.stringify(key)}`);
      }
      throw cause;
    }

    const match = entries.find(
      (entry) => entry === safeKey || entry.startsWith(`${safeKey}.`),
    );
    if (match === undefined) {
      throw new Error(`Blob not found for key: ${JSON.stringify(key)}`);
    }
    const buf = await fs.promises.readFile(path.join(dirPath, match));
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  }

  async writePrompt(
    turns: ConversationTurn[],
    _signal?: AbortSignal,
  ): Promise<void> {
    await fs.promises.writeFile(
      path.join(this.dir, PROMPT_FILE),
      encodeJsonlLines(turns),
    );
  }

  async writeResponse(
    turn: AssistantTurn,
    _signal?: AbortSignal,
  ): Promise<void> {
    await fs.promises.writeFile(
      path.join(this.dir, RESPONSE_FILE),
      encodeJsonlLines([turn]),
    );
  }

  async writeManifest(
    records: TransformRecordType[],
    _signal?: AbortSignal,
  ): Promise<void> {
    await fs.promises.writeFile(
      path.join(this.dir, MANIFEST_FILE),
      encodeJsonlLines(records),
    );
  }

  async writeTurns(
    turns: ConversationTurn[],
    _signal?: AbortSignal,
  ): Promise<void> {
    await fs.promises.writeFile(
      path.join(this.dir, TURNS_FILE),
      encodeJsonlLines(turns),
    );
  }

  /**
   * Write `metadata.json` containing pending operations, token usage, and the
   * currently-buffered connector state. The reactor calls this once per cycle
   * before issuing the working-tree commit so the file is staged atomically
   * with the per-cycle conversation data.
   */
  async writeMetadata(
    metadata: {
      pendingOperations: PendingOperation[];
      tokenUsage: TokenUsage;
    },
    _signal?: AbortSignal,
  ): Promise<void> {
    const payload: MetadataData = {
      pendingOperations: metadata.pendingOperations,
      tokenUsage: metadata.tokenUsage,
      connectorState: this.pendingConnectorState,
    };
    await fs.promises.writeFile(
      path.join(this.dir, METADATA_FILE),
      JSON.stringify(payload, null, 2),
    );
  }

  async readManifestHistory(
    limit: number,
    _signal?: AbortSignal,
  ): Promise<TransformRecordType[]> {
    if (limit <= 0) return [];
    const entries = await git.log({ fs, dir: this.dir, depth: limit });
    const collected: TransformRecordType[] = [];
    for (const entry of entries) {
      let blob: Uint8Array;
      try {
        ({ blob } = await git.readBlob({
          fs,
          dir: this.dir,
          oid: entry.oid,
          filepath: MANIFEST_FILE,
        }));
      } catch {
        continue;
      }
      const text = new TextDecoder().decode(blob);
      const parsedLines = decodeJsonlLines(text);
      for (const raw of parsedLines) {
        const result = TransformRecord(raw);
        if (result instanceof type.errors) {
          throw new Error(
            `Invalid manifest record at commit ${entry.oid}: ${result.summary}`,
          );
        }
        collected.push(result);
      }
    }
    return collected;
  }

  async commitAudit(
    records: AuditRecordType[],
    _signal?: AbortSignal,
  ): Promise<void> {
    if (records.length === 0) return;
    await withRepoDirLock(this.dir, async () => {
      // Pre-flight: validate all records and check for duplicates before
      // writing anything to disk. This avoids orphaned files if a
      // duplicate is detected partway through the batch.
      const planned: { record: AuditRecordType; filepath: string }[] = [];
      for (const record of records) {
        assertSafeSegment(record.sessionId, "sessionId");
        const safeCallId = sanitizeCallId(record.callId);

        const filepath = path.join(
          AUDIT_DIR,
          record.sessionId,
          `${safeCallId}.json`,
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

        planned.push({ record, filepath });
      }

      // Write phase: all validation passed, safe to write files.
      for (const { record, filepath } of planned) {
        const sessionDir = path.join(this.dir, AUDIT_DIR, record.sessionId);
        await fs.promises.mkdir(sessionDir, { recursive: true });
        const fullPath = path.join(this.dir, filepath);
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
    });
  }

  async commitErrors(
    records: ErrorRecord[],
    _signal?: AbortSignal,
  ): Promise<void> {
    if (records.length === 0) return;
    await withRepoDirLock(this.dir, async () => {
      // Pre-flight: validate all records and check for duplicates before
      // writing anything to disk. This avoids orphaned files if a
      // duplicate is detected partway through the batch.
      const planned: { record: ErrorRecord; filepath: string }[] = [];
      for (const record of records) {
        assertSafeSegment(record.sessionId, "sessionId");

        const sanitizedCategory = record.category.replace(
          /[^a-zA-Z0-9_-]/g,
          "_",
        );
        const seq = String(record.seq).padStart(8, "0");
        const filepath = path.join(
          ERRORS_DIR,
          record.sessionId,
          `${seq}-${sanitizedCategory}.json`,
        );
        const fullPath = path.join(this.dir, filepath);

        try {
          await fs.promises.access(fullPath);
          throw new Error(
            `Duplicate error record: ${record.sessionId}/${seq}-${sanitizedCategory}`,
          );
        } catch (e) {
          if (e instanceof Error && "code" in e && e.code === "ENOENT") {
            // Expected: file does not exist yet.
          } else {
            throw e;
          }
        }

        planned.push({ record, filepath });
      }

      // Write phase: all validation passed, safe to write files.
      for (const { record, filepath } of planned) {
        const sessionDir = path.join(this.dir, ERRORS_DIR, record.sessionId);
        await fs.promises.mkdir(sessionDir, { recursive: true });
        const fullPath = path.join(this.dir, filepath);
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
