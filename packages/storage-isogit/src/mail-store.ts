import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { parseHeaderSection } from "@interchange/mime";
import { AUTHOR } from "./init";
import type { CommitSigner } from "./signer";
import { buildSigningArgs } from "./commit-helpers";

const MAIL_DIR = "state/mail";

export type MailDirection = "in" | "out";

export type MailCommitResult = {
  threadId: string;
  messageId: string;
  filepath: string;
};

export type MailCommitOptions = {
  ignoreDuplicate?: boolean;
  checkpointHash?: string;
};

export type MailEntry = {
  threadId: string;
  ordinal: number;
  direction: MailDirection;
  messageId: string;
  raw: Uint8Array;
};

export type MailAuditStore = {
  commitMail(
    rawMessage: Uint8Array,
    direction: MailDirection,
    options?: MailCommitOptions,
  ): Promise<MailCommitResult | null>;
};

type ThreadState = {
  nextOrdinal: number;
};

function generateThreadId(): string {
  return crypto.randomBytes(4).toString("hex");
}

function formatOrdinal(n: number): string {
  return String(n).padStart(4, "0");
}

function parseThreadingHeaders(raw: Uint8Array): {
  messageId: string;
  inReplyTo: string | undefined;
  references: string[];
} {
  const { headers } = parseHeaderSection(raw);
  const messageId = headers.get("message-id");
  if (messageId === undefined || messageId.trim() === "") {
    throw new Error("Message-ID header is missing or empty");
  }
  const inReplyTo = headers.get("in-reply-to");
  const refsRaw = headers.get("references");
  const references = refsRaw ? refsRaw.split(/\s+/).filter(Boolean) : [];
  return { messageId, inReplyTo, references };
}

export async function createMailAuditStore(
  dir: string,
  signer?: CommitSigner,
): Promise<MailAuditStore> {
  const signingArgs = buildSigningArgs(signer);

  // Message-ID -> thread-id
  const messageIndex = new Map<string, string>();
  // thread-id -> thread state
  const threads = new Map<string, ThreadState>();

  await rebuildIndex(dir, messageIndex, threads);

  function resolveThread(
    inReplyTo: string | undefined,
    references: string[],
  ): string {
    if (inReplyTo !== undefined) {
      const threadId = messageIndex.get(inReplyTo);
      if (threadId !== undefined) return threadId;
    }
    for (const ref of references) {
      const threadId = messageIndex.get(ref);
      if (threadId !== undefined) return threadId;
    }
    return generateThreadId();
  }

  function peekNextOrdinal(threadId: string): number {
    const state = threads.get(threadId);
    if (state === undefined) return 1;
    return state.nextOrdinal;
  }

  function advanceOrdinal(threadId: string): void {
    let state = threads.get(threadId);
    if (state === undefined) {
      state = { nextOrdinal: 2 };
      threads.set(threadId, state);
    } else {
      state.nextOrdinal++;
    }
  }

  async function commitMail(
    rawMessage: Uint8Array,
    direction: MailDirection,
    options?: MailCommitOptions,
  ): Promise<MailCommitResult | null> {
    const { messageId, inReplyTo, references } =
      parseThreadingHeaders(rawMessage);

    if (messageIndex.has(messageId)) {
      if (options?.ignoreDuplicate === true) return null;
      throw new Error(`Duplicate mail: Message-ID ${messageId} already stored`);
    }

    const threadId = resolveThread(inReplyTo, references);
    const ordinal = peekNextOrdinal(threadId);
    const filename = `${formatOrdinal(ordinal)}-${direction}.eml`;
    const filepath = path.join(MAIL_DIR, threadId, filename);

    const fullDir = path.join(dir, MAIL_DIR, threadId);
    await fs.promises.mkdir(fullDir, { recursive: true });

    const fullPath = path.join(dir, filepath);
    await fs.promises.writeFile(fullPath, rawMessage);
    await git.add({ fs, dir, filepath });

    const label = direction === "in" ? "inbound" : "outbound";
    const subject = `Record ${label} mail ${messageId}`;
    const message =
      options?.checkpointHash !== undefined
        ? `${subject}\n\nCheckpoint: ${options.checkpointHash}`
        : subject;
    await git.commit({
      fs,
      dir,
      message,
      author: AUTHOR,
      ...signingArgs,
    });

    advanceOrdinal(threadId);
    messageIndex.set(messageId, threadId);
    return { threadId, messageId, filepath };
  }

  return { commitMail };
}

function parseFilename(filename: string): {
  ordinal: number;
  direction: MailDirection;
} {
  const stem = filename.replace(/\.eml$/, "");
  const dashIndex = stem.indexOf("-");
  if (dashIndex === -1) {
    throw new Error(`Malformed mail filename: ${filename}`);
  }

  const ordinalStr = stem.slice(0, dashIndex);
  const ordinal = parseInt(ordinalStr, 10);
  if (isNaN(ordinal)) {
    throw new Error(`Malformed mail filename: ${filename}`);
  }

  const directionStr = stem.slice(dashIndex + 1);
  if (directionStr !== "in" && directionStr !== "out") {
    throw new Error(
      `Invalid mail direction '${directionStr}' in filename: ${filename}`,
    );
  }

  return { ordinal, direction: directionStr };
}

async function scanMail(dir: string): Promise<MailEntry[]> {
  const mailDir = path.join(dir, MAIL_DIR);

  let threadDirs: string[];
  try {
    threadDirs = await fs.promises.readdir(mailDir);
  } catch (e: unknown) {
    if (e instanceof Error && "code" in e && e.code === "ENOENT") {
      return [];
    }
    throw e;
  }

  threadDirs.sort();

  const entries: MailEntry[] = [];

  for (const threadId of threadDirs) {
    const threadPath = path.join(mailDir, threadId);
    const stat = await fs.promises.stat(threadPath);
    if (!stat.isDirectory()) continue;

    const files = await fs.promises.readdir(threadPath);
    const emlFiles = files.filter((f) => f.endsWith(".eml")).sort();

    for (const file of emlFiles) {
      const { ordinal, direction } = parseFilename(file);
      const fullPath = path.join(threadPath, file);
      const raw = await fs.promises.readFile(fullPath);
      const { messageId } = parseThreadingHeaders(raw);

      entries.push({ threadId, ordinal, direction, messageId, raw });
    }
  }

  return entries;
}

export async function listMail(dir: string): Promise<MailEntry[]> {
  return scanMail(dir);
}

async function rebuildIndex(
  dir: string,
  messageIndex: Map<string, string>,
  threads: Map<string, ThreadState>,
): Promise<void> {
  const entries = await scanMail(dir);

  for (const entry of entries) {
    messageIndex.set(entry.messageId, entry.threadId);

    const state = threads.get(entry.threadId);
    if (state === undefined) {
      threads.set(entry.threadId, { nextOrdinal: entry.ordinal + 1 });
    } else if (entry.ordinal >= state.nextOrdinal) {
      state.nextOrdinal = entry.ordinal + 1;
    }
  }
}
