import git from "isomorphic-git";
import {
  ApprovalSnapshot,
  ContentBlock,
  TokenUsage,
  ToolCall,
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
import {
  buildSigningArgs,
  commitDurably,
  restoreIndexAfterFailedCommit,
} from "./commit-helpers";
import { withRepoDirLock } from "./repo-lock";
import { maybeGCUnderLock, type GCPolicy } from "./gc";
import { decodeUTF8, flushRuntime, type StorageRuntime } from "./runtime";
import { hasCode } from "@intx/types";

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
  kind: "'approval'",
  "expectedFrom?": "string",
  registeredAt: "number",
  gateId: "string",
  "timeoutAt?": "number",
  "suspendedCall?": ToolCall,
  "approvalSnapshot?": ApprovalSnapshot,
});

// The persisted schema and the in-memory PendingOperation type are two
// separate declarations kept in lockstep. arktype passes undeclared keys
// through at runtime, so dropping `suspendedCall?` from the schema would not
// surface as a runtime failure. Projecting the field off the schema's
// inferred type makes the declaration load-bearing: the indexed access
// errors under `tsc` if the schema stops carrying the field, and the return
// annotation pins its persisted type to `ToolCall`.
const _persistedSuspendedCall = (
  op: typeof PendingOperationSchema.infer,
): ToolCall | undefined => op.suspendedCall;
void _persistedSuspendedCall;

// Same lockstep guard for the approval snapshot: the persisted schema must
// keep carrying `approvalSnapshot` so a rehydrated pending operation still
// exposes it. The projection errors under `tsc` if the schema drops the field.
const _persistedApprovalSnapshot = (
  op: typeof PendingOperationSchema.infer,
): ApprovalSnapshot | undefined => op.approvalSnapshot;
void _persistedApprovalSnapshot;

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

/**
 * Walk the first-parent commit chain from HEAD, newest-first, stopping at
 * `limit` entries or at the first parent that is not present on disk.
 *
 * `git.log` throws `NotFoundError` the moment it reaches a missing commit,
 * which is the steady state under `tip-only` GC: the collector prunes
 * ancestry, leaving the tip's older parents absent. The durable conversation
 * lives in the working-tree files at the tip, not in this history, so the
 * commit log is a best-effort time-travel surface — degrade to the surviving
 * slice rather than throwing into a caller (e.g. the agent's `checkpoints`
 * tool). Any non-absence read error still surfaces.
 */
async function tolerantLog(
  runtime: StorageRuntime,
  dir: string,
  limit: number,
): Promise<Awaited<ReturnType<typeof git.readCommit>>[]> {
  const out: Awaited<ReturnType<typeof git.readCommit>>[] = [];
  let oid: string | undefined;
  try {
    oid = await git.resolveRef({ fs: runtime.fs.git, dir, ref: "HEAD" });
  } catch {
    return out;
  }
  while (oid !== undefined && out.length < limit) {
    let entry: Awaited<ReturnType<typeof git.readCommit>>;
    try {
      entry = await git.readCommit({ fs: runtime.fs.git, dir, oid });
    } catch (err) {
      if (err instanceof Error && "code" in err && err.code === "NotFoundError")
        break;
      throw err;
    }
    out.push(entry);
    oid = entry.commit.parent[0];
  }
  return out;
}

async function readCommitLog(
  runtime: StorageRuntime,
  dir: string,
  limit: number,
): Promise<ContextCommit[]> {
  const entries = await tolerantLog(runtime, dir, limit);
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

type DurableRecordFile = {
  filepath: string;
  directory: string;
  contents: string;
  duplicateMessage: string;
};

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

async function pathExists(
  runtime: StorageRuntime,
  fullPath: string,
): Promise<boolean> {
  try {
    await runtime.fs.access(fullPath);
    return true;
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") {
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
  runtime: StorageRuntime,
  dir: string,
  oid: string,
  filepath: string,
): Promise<Uint8Array | null> {
  try {
    const { blob } = await git.readBlob({
      fs: runtime.fs.git,
      dir,
      oid,
      filepath,
    });
    return blob;
  } catch (cause) {
    if (
      cause instanceof Error &&
      "code" in cause &&
      (cause.code === "NotFoundError" || cause.code === "ENOENT")
    ) {
      return null;
    }
    throw cause;
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
/**
 * Extra reads the durable WAL mirror needs beyond `ContextStore`, kept
 * off the shared interface because they are isogit-specific.
 */
export interface DurableMirrorReads {
  /**
   * The turns most recently handed to `writeTurns`, by reference -- the
   * reactor's live array, not a copy. Lets the WAL mirror slice the new
   * turns from memory instead of re-reading and re-parsing `turns.jsonl`
   * every boundary. Safe because the local store is single-writer and
   * in-process: nothing else writes `turns.jsonl`, so the last-written
   * array equals the on-disk state at the mirror boundary.
   */
  peekTurns(): ConversationTurn[];
  /**
   * Read only `metadata.json` (pending operations, token usage, connector
   * state), skipping the O(N) turns parse `load` pays. The mirror gets
   * its turns from `peekTurns`.
   */
  loadMetadata(): Promise<{
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
    connectorState: ConnectorThreadState | null;
  }>;
}

export class IsogitStore
  implements ContextStore, AuditStore, DurableMirrorReads
{
  private readonly runtime: StorageRuntime;
  private readonly dir: string;
  private readonly signer: CommitSigner | undefined;
  private readonly gcPolicy: GCPolicy | undefined;
  private pendingConnectorState: ConnectorThreadState | null = null;
  private lastTurns: ConversationTurn[] = [];

  constructor(
    runtime: StorageRuntime,
    dir: string,
    signer?: CommitSigner,
    gcPolicy?: GCPolicy,
  ) {
    this.runtime = runtime;
    this.dir = dir;
    this.signer = signer;
    this.gcPolicy = gcPolicy;
  }

  private signingArgs() {
    return buildSigningArgs(this.signer);
  }

  // Reclaim after a write while the per-directory lock is still held, per
  // the configured policy. A no-op when no policy was supplied.
  private async maybeGC(): Promise<void> {
    if (this.gcPolicy === undefined) return;
    await maybeGCUnderLock(this.runtime, this.dir, this.gcPolicy);
  }

  /** Persist append-only record files with retry-safe publication semantics. */
  private async commitRecordFiles(
    files: readonly DurableRecordFile[],
    commitMessage: (count: number) => string,
  ): Promise<void> {
    const prepared: {
      file: DurableRecordFile;
      exists: boolean;
      committed: boolean;
    }[] = [];
    const headOid = await git.resolveRef({
      fs: this.runtime.fs.git,
      dir: this.dir,
      ref: "HEAD",
    });

    for (const file of files) {
      const committedBlob = await readBlobAtCommit(
        this.runtime,
        this.dir,
        headOid,
        file.filepath,
      );
      const committed = committedBlob !== null;
      if (
        committedBlob !== null &&
        decodeUTF8(committedBlob) !== file.contents
      ) {
        throw new Error(file.duplicateMessage);
      }

      const fullPath = this.runtime.path.join(this.dir, file.filepath);
      try {
        const existing = await this.runtime.fs.readTextFile(fullPath);
        if (existing !== file.contents) {
          throw new Error(file.duplicateMessage);
        }
        prepared.push({ file, exists: true, committed });
      } catch (cause) {
        if (hasCode(cause) && cause.code === "ENOENT") {
          prepared.push({ file, exists: false, committed });
        } else {
          throw cause;
        }
      }
    }

    try {
      for (const { file, exists } of prepared) {
        if (!exists) {
          await this.runtime.fs.mkdir(file.directory, { recursive: true });
          await this.runtime.fs.writeFile(
            this.runtime.path.join(this.dir, file.filepath),
            file.contents,
          );
        }
        await git.add({
          fs: this.runtime.fs.git,
          dir: this.dir,
          filepath: file.filepath,
        });
      }

      const changedCount = prepared.filter(
        ({ committed }) => !committed,
      ).length;
      if (changedCount === 0) {
        await flushRuntime(this.runtime);
      } else {
        await commitDurably(this.runtime, this.dir, {
          message: commitMessage(changedCount),
          author: AUTHOR,
          ...this.signingArgs(),
        });
      }
    } catch (cause) {
      try {
        await restoreIndexAfterFailedCommit(
          this.runtime,
          this.dir,
          prepared.map(({ file }) => file.filepath),
        );
      } catch (resetCause) {
        throw new Error("Could not restore the record index after failure", {
          cause: new AggregateError([cause, resetCause]),
        });
      }
      throw cause;
    }
    await this.maybeGC();
    await flushRuntime(this.runtime);
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
    const turnsPath = this.runtime.path.join(this.dir, TURNS_FILE);

    let turns: ConversationTurn[] = [];
    if (await pathExists(this.runtime, turnsPath)) {
      const text = await this.runtime.fs.readTextFile(turnsPath);
      turns = parseTurns(text);
    }

    const metadata = await this.loadMetadata();
    return { turns, ...metadata };
  }

  async loadMetadata(): Promise<{
    pendingOperations: PendingOperation[];
    tokenUsage: TokenUsage;
    connectorState: ConnectorThreadState | null;
  }> {
    const metadataPath = this.runtime.path.join(this.dir, METADATA_FILE);
    if (!(await pathExists(this.runtime, metadataPath))) {
      return {
        pendingOperations: [],
        tokenUsage: { ...EMPTY_USAGE },
        connectorState: null,
      };
    }
    const text = await this.runtime.fs.readTextFile(metadataPath);
    const parsed: unknown = JSON.parse(text);
    const data = parseMetadata(parsed);
    return {
      pendingOperations: data.pendingOperations,
      tokenUsage: data.tokenUsage,
      connectorState: data.connectorState,
    };
  }

  async commit(
    options: { message: string },
    _signal?: AbortSignal,
  ): Promise<ContextCommit> {
    return withRepoDirLock(this.runtime, this.dir, async () => {
      const tracked = [
        TURNS_FILE,
        PROMPT_FILE,
        RESPONSE_FILE,
        MANIFEST_FILE,
        METADATA_FILE,
      ];
      const stagedFilepaths: string[] = [];
      for (const filepath of tracked) {
        const fullPath = this.runtime.path.join(this.dir, filepath);
        if (await pathExists(this.runtime, fullPath)) {
          stagedFilepaths.push(filepath);
        }
      }

      const blobsDir = this.runtime.path.join(this.dir, TOOL_OUTPUT_DIR);
      if (await pathExists(this.runtime, blobsDir)) {
        const entries = await this.runtime.fs.readdir(blobsDir);
        for (const entry of entries) {
          stagedFilepaths.push(`${TOOL_OUTPUT_DIR}/${entry}`);
        }
      }

      let oid: string;
      try {
        for (const filepath of stagedFilepaths) {
          await git.add({
            fs: this.runtime.fs.git,
            dir: this.dir,
            filepath,
          });
        }

        oid = await commitDurably(this.runtime, this.dir, {
          message: options.message,
          author: AUTHOR,
          ...this.signingArgs(),
        });
      } catch (cause) {
        try {
          await restoreIndexAfterFailedCommit(
            this.runtime,
            this.dir,
            stagedFilepaths,
          );
        } catch (resetCause) {
          throw new Error("Could not restore the context index after failure", {
            cause: new AggregateError([cause, resetCause]),
          });
        }
        throw cause;
      }

      const described = await this.describeHead(oid, options.message);
      await this.maybeGC();
      return described;
    });
  }

  private async describeHead(
    expectedOid: string,
    message: string,
  ): Promise<ContextCommit> {
    const entries = await git.log({
      fs: this.runtime.fs.git,
      dir: this.dir,
      depth: 2,
    });
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
    await git.branch({ fs: this.runtime.fs.git, dir: this.dir, ref: name });
    await flushRuntime(this.runtime);
  }

  async log(limit?: number, _signal?: AbortSignal): Promise<ContextCommit[]> {
    return readCommitLog(this.runtime, this.dir, limit ?? 10);
  }

  async readAt(
    hash: string,
    _signal?: AbortSignal,
  ): Promise<ConversationTurn[]> {
    const blob = await readBlobAtCommit(
      this.runtime,
      this.dir,
      hash,
      TURNS_FILE,
    );
    if (blob === null) return [];
    const text = decodeUTF8(blob);
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
    const dirPath = this.runtime.path.join(this.dir, TOOL_OUTPUT_DIR);
    await this.runtime.fs.mkdir(dirPath, { recursive: true });
    await this.runtime.fs.writeFile(
      this.runtime.path.join(dirPath, filename),
      bytes,
    );
  }

  async readBlob(key: string, _signal?: AbortSignal): Promise<Uint8Array> {
    const safeKey = sanitizeCallId(key);
    const dirPath = this.runtime.path.join(this.dir, TOOL_OUTPUT_DIR);
    let entries: string[];
    try {
      entries = await this.runtime.fs.readdir(dirPath);
    } catch (cause) {
      if (hasCode(cause) && cause.code === "ENOENT") {
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
    return this.runtime.fs.readFile(this.runtime.path.join(dirPath, match));
  }

  async writePrompt(
    turns: ConversationTurn[],
    _signal?: AbortSignal,
  ): Promise<void> {
    await this.runtime.fs.writeFile(
      this.runtime.path.join(this.dir, PROMPT_FILE),
      encodeJsonlLines(turns),
    );
  }

  async writeResponse(
    turn: AssistantTurn,
    _signal?: AbortSignal,
  ): Promise<void> {
    await this.runtime.fs.writeFile(
      this.runtime.path.join(this.dir, RESPONSE_FILE),
      encodeJsonlLines([turn]),
    );
  }

  async writeManifest(
    records: TransformRecordType[],
    _signal?: AbortSignal,
  ): Promise<void> {
    await this.runtime.fs.writeFile(
      this.runtime.path.join(this.dir, MANIFEST_FILE),
      encodeJsonlLines(records),
    );
  }

  async writeTurns(
    turns: ConversationTurn[],
    _signal?: AbortSignal,
  ): Promise<void> {
    await this.runtime.fs.writeFile(
      this.runtime.path.join(this.dir, TURNS_FILE),
      encodeJsonlLines(turns),
    );
    // Advance the in-memory marker only after the durable write succeeds.
    // peekTurns must never surface an array that failed to persist -- a
    // write failure leaves it pointing at the last array that did.
    this.lastTurns = turns;
  }

  peekTurns(): ConversationTurn[] {
    return this.lastTurns;
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
    await this.runtime.fs.writeFile(
      this.runtime.path.join(this.dir, METADATA_FILE),
      JSON.stringify(payload, null, 2),
    );
  }

  async readManifestHistory(
    limit: number,
    _signal?: AbortSignal,
  ): Promise<TransformRecordType[]> {
    if (limit <= 0) return [];
    const entries = await tolerantLog(this.runtime, this.dir, limit);
    const collected: TransformRecordType[] = [];
    for (const entry of entries) {
      let blob: Uint8Array;
      try {
        ({ blob } = await git.readBlob({
          fs: this.runtime.fs.git,
          dir: this.dir,
          oid: entry.oid,
          filepath: MANIFEST_FILE,
        }));
      } catch {
        continue;
      }
      const text = decodeUTF8(blob);
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
    await withRepoDirLock(this.runtime, this.dir, async () => {
      const planned: DurableRecordFile[] = [];
      for (const record of records) {
        assertSafeSegment(record.sessionId, "sessionId");
        const safeCallId = sanitizeCallId(record.callId);

        const filepath = this.runtime.path.join(
          AUDIT_DIR,
          record.sessionId,
          `${safeCallId}.json`,
        );
        planned.push({
          filepath,
          directory: this.runtime.path.join(
            this.dir,
            AUDIT_DIR,
            record.sessionId,
          ),
          contents: JSON.stringify(record, null, 2),
          duplicateMessage: `Duplicate audit record: ${record.sessionId}/${record.callId}`,
        });
      }
      await this.commitRecordFiles(planned, (count) => {
        const noun = count === 1 ? "record" : "records";
        return `Record ${count} tool audit ${noun}`;
      });
    });
  }

  async commitErrors(
    records: ErrorRecord[],
    _signal?: AbortSignal,
  ): Promise<void> {
    if (records.length === 0) return;
    await withRepoDirLock(this.runtime, this.dir, async () => {
      const planned: DurableRecordFile[] = [];
      for (const record of records) {
        assertSafeSegment(record.sessionId, "sessionId");

        const sanitizedCategory = record.category.replace(
          /[^a-zA-Z0-9_-]/g,
          "_",
        );
        const seq = String(record.seq).padStart(8, "0");
        const filepath = this.runtime.path.join(
          ERRORS_DIR,
          record.sessionId,
          `${seq}-${sanitizedCategory}.json`,
        );
        planned.push({
          filepath,
          directory: this.runtime.path.join(
            this.dir,
            ERRORS_DIR,
            record.sessionId,
          ),
          contents: JSON.stringify(record, null, 2),
          duplicateMessage: `Duplicate error record: ${record.sessionId}/${seq}-${sanitizedCategory}`,
        });
      }
      await this.commitRecordFiles(planned, (count) => {
        const noun = count === 1 ? "record" : "records";
        return `Record ${count} error ${noun}`;
      });
    });
  }

  async loadAudit(
    sessionId: string,
    _signal?: AbortSignal,
  ): Promise<AuditRecordType[]> {
    assertSafeSegment(sessionId, "sessionId");
    const sessionDir = this.runtime.path.join(this.dir, AUDIT_DIR, sessionId);

    let entries: string[];
    try {
      entries = await this.runtime.fs.readdir(sessionDir);
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
      const fullPath = this.runtime.path.join(sessionDir, entry);
      const raw = await this.runtime.fs.readTextFile(fullPath);
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
