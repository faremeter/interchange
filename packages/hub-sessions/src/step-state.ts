// On-repo format of an agent step's durable state: the conversation turns,
// pending operations, token usage and reply-thread (connector) state the
// step's agent needs to continue where it left off.
//
// The state lives under one directory of the workflow-run repo as a
// compacted checkpoint plus an append-only, bucket-sharded write-ahead log:
//
//   <state dir>/
//     checkpoint.json          folded turns 0..turnCount-1 plus metadata
//     checkpoint.meta.json     { checkpointSeq, turnCount, metadata }
//     wal/<bucket>/<seq>.json  one entry per mirror boundary since the fold
//
// The WAL is keyed by mirror BOUNDARY, not by turn. Each mirror writes
// exactly one entry, even when the boundary added no turns, and every entry
// carries the freshest metadata, so a turnless boundary that only advanced
// metadata (pending operations, token usage, connector state) is still
// durable. `checkpointSeq` therefore counts boundaries while `turnCount`
// counts folded turns; the two diverge in general.
//
// `bucket = floor(seq / STEP_STATE_WAL_BUCKET_SIZE)` bounds any single WAL
// directory's size, so no commit re-hashes a tree that grows with the
// conversation.
//
// A step whose retries start over stamps the attempt that owns the state in
// its metadata, so a later attempt can tell the stored state is not its own.
// An agent whose conversation spans attempts leaves it unset.
//
// Every reader reconstructs through `reconstructStepState` behind the
// `StepStateReader` seam, whatever it reads the files from.

import { type } from "arktype";

import {
  ConnectorThreadState,
  TokenUsage,
  type ConversationTurn,
  type PendingOperation,
} from "@intx/types/runtime";

import type { CommittedReads, CommittedTreeEntry } from "./repo-store/types";

export const STEP_STATE_CHECKPOINT_FILE = "checkpoint.json";
export const STEP_STATE_CHECKPOINT_META_FILE = "checkpoint.meta.json";
export const STEP_STATE_WAL_DIR = "wal";
export const STEP_STATE_WAL_BUCKET_SIZE = 128;

const StepStateMetadata = type({
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
  "attempt?": "number.integer >= 0",
});

/** The non-turn state stamped on every WAL entry and on the checkpoint. */
export type StepStateMetadata = typeof StepStateMetadata.infer;

export const StepStateCheckpoint = type({
  turns: "unknown[]",
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
  "attempt?": "number.integer >= 0",
});

export const StepStateCheckpointMeta = type({
  checkpointSeq: "number.integer >= 0",
  turnCount: "number.integer >= 0",
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
  "attempt?": "number.integer >= 0",
});

export const StepStateWalEntry = type({
  seq: "number.integer >= 0",
  turns: "unknown[]",
  metadata: StepStateMetadata,
});

export type StepStateContent = {
  turns: ConversationTurn[];
  pendingOperations: PendingOperation[];
  tokenUsage: TokenUsage;
  connectorState: ConnectorThreadState | null;
};

/**
 * The reconstructed state plus the bookkeeping a writer needs to resume
 * appending. `totalTurns` is the full turn count (checkpoint + WAL).
 * `boundaryCount` is the number of mirror boundaries durably committed, so
 * the next WAL entry uses it as its seq. `checkpointBoundarySeq` is the
 * boundary seq the checkpoint folded to, the first WAL seq to expect.
 * `attempt` is the attempt the latest metadata was stamped with, if any.
 */
export type ReconstructedStepState = StepStateContent & {
  totalTurns: number;
  boundaryCount: number;
  checkpointBoundarySeq: number;
  attempt?: number;
};

/**
 * Read access to one step's state directory. Paths are relative to that
 * directory. `readFile` returns `null` for an absent file and `listDir`
 * returns `null` for an absent directory.
 */
export type StepStateReader = {
  readFile(relPath: string): Promise<string | null>;
  listDir(relPath: string): Promise<string[] | null>;
};

/**
 * Read one step's state directory from a pinned committed tree. `stateDir`
 * is repo-relative with a trailing slash.
 */
export function createCommittedStepStateReader(
  reads: CommittedReads,
  stateDir: string,
): StepStateReader {
  const root = stateDir.slice(0, -1);
  const decoder = new TextDecoder();
  // The reads are pinned to one commit, so each directory is listed once
  // rather than once per WAL entry read from it.
  const listings = new Map<string, Promise<CommittedTreeEntry[]>>();
  const list = (relPath: string): Promise<CommittedTreeEntry[]> => {
    let listing = listings.get(relPath);
    if (listing === undefined) {
      listing = reads.listDir(relPath === "" ? root : `${root}/${relPath}`);
      listings.set(relPath, listing);
    }
    return listing;
  };
  return {
    async readFile(relPath) {
      const slash = relPath.lastIndexOf("/");
      const dir = slash === -1 ? "" : relPath.slice(0, slash);
      const name = relPath.slice(slash + 1);
      const entry = (await list(dir)).find(
        (candidate) => candidate.name === name && candidate.type === "blob",
      );
      if (entry === undefined) return null;
      return decoder.decode(await reads.readBlobByOid(entry.oid));
    },
    async listDir(relPath) {
      const entries = await list(relPath);
      return entries.length === 0 ? null : entries.map((entry) => entry.name);
    },
  };
}

export function stepStateWalBucket(seq: number): number {
  return Math.floor(seq / STEP_STATE_WAL_BUCKET_SIZE);
}

/**
 * The repo-relative directory of one WAL bucket, with a trailing slash.
 * `stateDir` is the step's state directory, also with a trailing slash.
 */
export function stepStateWalBucketPrefix(
  stateDir: string,
  bucket: number,
): string {
  return `${stateDir}${STEP_STATE_WAL_DIR}/${String(bucket)}/`;
}

export function stepStateWalEntryPath(stateDir: string, seq: number): string {
  return `${stepStateWalBucketPrefix(stateDir, stepStateWalBucket(seq))}${String(seq)}.json`;
}

/**
 * The bytes of the WAL entry for boundary `seq`: the boundary's new turns
 * (never the prior ones) plus the freshest metadata.
 */
export function serializeStepStateWalEntry(
  seq: number,
  turns: readonly unknown[],
  metadata: StepStateMetadata,
): string {
  return JSON.stringify({ seq, turns, metadata });
}

/**
 * The two checkpoint files, keyed by repo-relative path, that fold `content`
 * at boundary `checkpointSeq`. Written with the state directory as the
 * preserved prefix they also truncate the WAL, since the prefix is cleared
 * of every path the returned set omits.
 */
export function buildStepStateCheckpoint(
  stateDir: string,
  checkpointSeq: number,
  content: {
    turns: readonly unknown[];
    pendingOperations: readonly unknown[];
    tokenUsage: TokenUsage;
    connectorState: ConnectorThreadState | null;
    attempt?: number;
  },
): Record<string, string> {
  const attempt =
    content.attempt !== undefined ? { attempt: content.attempt } : {};
  const snapshot = {
    turns: content.turns,
    pendingOperations: content.pendingOperations,
    tokenUsage: content.tokenUsage,
    connectorState: content.connectorState,
    ...attempt,
  };
  const meta = {
    checkpointSeq,
    turnCount: content.turns.length,
    pendingOperations: content.pendingOperations,
    tokenUsage: content.tokenUsage,
    connectorState: content.connectorState,
    ...attempt,
  };
  return {
    [`${stateDir}${STEP_STATE_CHECKPOINT_FILE}`]: JSON.stringify(snapshot),
    [`${stateDir}${STEP_STATE_CHECKPOINT_META_FILE}`]: JSON.stringify(meta),
  };
}

/**
 * Reconstruct a step's state: the checkpoint's folded turns followed by the
 * replayed WAL tail, with the last WAL entry's metadata winning (the
 * checkpoint's metadata is the base when the WAL is empty). This is pure
 * reconstruction from recorded outputs, never re-inference. Returns `null`
 * when neither a checkpoint nor any WAL entry exists. Throws on a corrupt or
 * unparseable blob, an inconsistent checkpoint pair, or a WAL seq gap: a
 * damaged durable copy must surface, never silently start the agent fresh or
 * drop a turn. `label` names the state in error messages.
 */
export async function reconstructStepState(
  reader: StepStateReader,
  label: string,
): Promise<ReconstructedStepState | null> {
  const checkpoint = await readCheckpoint(reader, label);
  const baseBoundarySeq = checkpoint?.checkpointSeq ?? 0;
  const wal = await readWalTail(reader, label, baseBoundarySeq);
  if (checkpoint === null && wal.length === 0) return null;

  const turns: unknown[] = [...(checkpoint?.turns ?? [])];
  let metadata: StepStateMetadata = checkpoint?.metadata ?? {
    pendingOperations: [],
    tokenUsage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 0,
    },
    connectorState: null,
  };
  for (const entry of wal) {
    for (const turn of entry.turns) {
      turns.push(turn);
    }
    metadata = entry.metadata;
  }
  return {
    // The reactor re-narrows turn and operation elements on load; the
    // validators here enforce only the structural envelope.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope validated in the read helpers; turn element narrows live in the reactor on load
    turns: turns as ConversationTurn[],
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope validated in the read helpers; pending-operation element narrows live in the reactor on load
    pendingOperations: metadata.pendingOperations as PendingOperation[],
    tokenUsage: metadata.tokenUsage,
    connectorState: metadata.connectorState,
    totalTurns: turns.length,
    boundaryCount: baseBoundarySeq + wal.length,
    checkpointBoundarySeq: baseBoundarySeq,
    ...(metadata.attempt !== undefined ? { attempt: metadata.attempt } : {}),
  };
}

/**
 * Read the checkpoint pair. Returns `null` only when no checkpoint exists
 * yet, which reconstruction treats as "no folded turns". A present but
 * corrupt or inconsistent pair throws.
 */
async function readCheckpoint(
  reader: StepStateReader,
  label: string,
): Promise<{
  turns: unknown[];
  checkpointSeq: number;
  metadata: StepStateMetadata;
} | null> {
  const metaRaw = await reader.readFile(STEP_STATE_CHECKPOINT_META_FILE);
  if (metaRaw === null) return null;
  const validatedMeta = StepStateCheckpointMeta(
    parseJSONOrThrow(metaRaw, label, STEP_STATE_CHECKPOINT_META_FILE),
  );
  if (validatedMeta instanceof type.errors) {
    throw new Error(
      `step state ${label}: ${STEP_STATE_CHECKPOINT_META_FILE} failed validation: ${validatedMeta.summary}; refusing to start the agent fresh on a corrupt checkpoint`,
    );
  }
  const snapshotRaw = await reader.readFile(STEP_STATE_CHECKPOINT_FILE);
  if (snapshotRaw === null) {
    throw new Error(
      `step state ${label}: ${STEP_STATE_CHECKPOINT_META_FILE} exists without ${STEP_STATE_CHECKPOINT_FILE}; the checkpoint pair is inconsistent`,
    );
  }
  const validatedSnapshot = StepStateCheckpoint(
    parseJSONOrThrow(snapshotRaw, label, STEP_STATE_CHECKPOINT_FILE),
  );
  if (validatedSnapshot instanceof type.errors) {
    throw new Error(
      `step state ${label}: ${STEP_STATE_CHECKPOINT_FILE} failed validation: ${validatedSnapshot.summary}; refusing to start the agent fresh on a corrupt checkpoint`,
    );
  }
  if (validatedSnapshot.turns.length !== validatedMeta.turnCount) {
    throw new Error(
      `step state ${label}: ${STEP_STATE_CHECKPOINT_FILE} carries ${String(validatedSnapshot.turns.length)} turns but ${STEP_STATE_CHECKPOINT_META_FILE} reports turnCount ${String(validatedMeta.turnCount)}; the checkpoint pair is inconsistent`,
    );
  }
  return {
    turns: validatedSnapshot.turns,
    checkpointSeq: validatedMeta.checkpointSeq,
    metadata: {
      pendingOperations: validatedSnapshot.pendingOperations,
      tokenUsage: validatedSnapshot.tokenUsage,
      connectorState: validatedSnapshot.connectorState,
      ...(validatedSnapshot.attempt !== undefined
        ? { attempt: validatedSnapshot.attempt }
        : {}),
    },
  };
}

/**
 * Read and seq-order the WAL entries for boundary seqs >= `fromSeq`. Throws
 * on an unparseable or out-of-shape entry, and on a gap in the seq sequence:
 * a missing seq is a lost append, which would silently drop a boundary's
 * turns and metadata from the reconstruction.
 */
async function readWalTail(
  reader: StepStateReader,
  label: string,
  fromSeq: number,
): Promise<{ seq: number; turns: unknown[]; metadata: StepStateMetadata }[]> {
  const buckets = await reader.listDir(STEP_STATE_WAL_DIR);
  if (buckets === null) return [];
  const entries: {
    seq: number;
    turns: unknown[];
    metadata: StepStateMetadata;
  }[] = [];
  for (const bucket of buckets) {
    const bucketPath = `${STEP_STATE_WAL_DIR}/${bucket}`;
    for (const file of (await reader.listDir(bucketPath)) ?? []) {
      const entryPath = `${bucketPath}/${file}`;
      if (!file.endsWith(".json")) {
        throw new Error(
          `step state ${label}: unexpected non-JSON WAL entry ${entryPath}`,
        );
      }
      const raw = await reader.readFile(entryPath);
      if (raw === null) {
        throw new Error(
          `step state ${label}: WAL entry ${entryPath} vanished while reading`,
        );
      }
      const validated = StepStateWalEntry(
        parseJSONOrThrow(raw, label, entryPath),
      );
      if (validated instanceof type.errors) {
        throw new Error(
          `step state ${label}: WAL entry ${entryPath} failed validation: ${validated.summary}; refusing to start the agent fresh on a corrupt WAL`,
        );
      }
      if (validated.seq < fromSeq) continue;
      entries.push({
        seq: validated.seq,
        turns: validated.turns,
        metadata: {
          pendingOperations: validated.metadata.pendingOperations,
          tokenUsage: validated.metadata.tokenUsage,
          connectorState: validated.metadata.connectorState,
          ...(validated.metadata.attempt !== undefined
            ? { attempt: validated.metadata.attempt }
            : {}),
        },
      });
    }
  }
  entries.sort((a, b) => a.seq - b.seq);
  for (let i = 0; i < entries.length; i += 1) {
    const expected = fromSeq + i;
    const entry = entries[i];
    if (entry === undefined || entry.seq !== expected) {
      throw new Error(
        `step state ${label}: WAL has a seq gap (expected ${String(expected)}, found ${String(entry?.seq)}); a lost append would silently drop a boundary's turns and metadata`,
      );
    }
  }
  return entries;
}

function parseJSONOrThrow(raw: string, label: string, file: string): unknown {
  try {
    return JSON.parse(raw);
  } catch (cause) {
    throw new Error(
      `step state ${label}: ${file} is not valid JSON; refusing to start the agent fresh on a corrupt durable copy`,
      { cause },
    );
  }
}
