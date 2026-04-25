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
    await git.commit({
      fs,
      dir,
      message: `Record ${label} mail ${messageId}`,
      author: AUTHOR,
      ...signingArgs,
    });

    advanceOrdinal(threadId);
    messageIndex.set(messageId, threadId);
    return { threadId, messageId, filepath };
  }

  return { commitMail };
}

async function rebuildIndex(
  dir: string,
  messageIndex: Map<string, string>,
  threads: Map<string, ThreadState>,
): Promise<void> {
  const mailDir = path.join(dir, MAIL_DIR);

  let threadDirs: string[];
  try {
    threadDirs = await fs.promises.readdir(mailDir);
  } catch (e) {
    if (e instanceof Error && "code" in e && e.code === "ENOENT") {
      return;
    }
    throw e;
  }

  for (const threadId of threadDirs) {
    const threadPath = path.join(mailDir, threadId);
    const stat = await fs.promises.stat(threadPath);
    if (!stat.isDirectory()) continue;

    const files = await fs.promises.readdir(threadPath);
    const emlFiles = files.filter((f) => f.endsWith(".eml")).sort();

    let maxOrdinal = 0;
    for (const file of emlFiles) {
      const ordinalStr = file.split("-")[0];
      if (ordinalStr !== undefined) {
        const ordinal = parseInt(ordinalStr, 10);
        if (!isNaN(ordinal) && ordinal > maxOrdinal) {
          maxOrdinal = ordinal;
        }
      }

      const fullPath = path.join(threadPath, file);
      const raw = await fs.promises.readFile(fullPath);
      const { messageId } = parseThreadingHeaders(raw);
      messageIndex.set(messageId, threadId);
    }

    threads.set(threadId, { nextOrdinal: maxOrdinal + 1 });
  }
}
