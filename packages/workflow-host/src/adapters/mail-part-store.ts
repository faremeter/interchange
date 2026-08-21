// Durable store for a run's inbound-mail parts.
//
// The supervisor decodes an inbound MIME message into parts (via `decodeMail`)
// and commits each part's decoded bytes here as a real file under
// `runs/<runId>/parts/<urlEncoded(messageId)>/<index>-<name>`, returning the
// JSON-safe `MailPart[]` descriptors that ride in the run's trigger/signal
// payload. A `MailPartReader` resolves a descriptor's opaque `ref` back to the
// committed bytes for any consumer -- an agent's content-block projection, a
// workflow tool, a child run -- through the single, environment-agnostic
// `MailPartReader` interface.
//
// Modeled on the sibling `blob-substrate` adapter: same per-run handles, same
// substrate primitives (`writeTreePreservingPrefix` to write raw bytes;
// `openCommittedReads` to read them back from a coherent object-store snapshot
// rather than the lagging working tree). The write happens in one commit per
// message (write-once, atomic). The kind handler validates the subtree shape;
// this module sanitizes untrusted names to satisfy it and reuses the handler's
// path-component byte cap.

import {
  MAX_MAIL_PART_PATH_COMPONENT_BYTES,
  WORKFLOW_RUN_PARTS_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
} from "@intx/hub-sessions/substrate";
import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import type {
  Mail,
  MailPart,
  MailPartReader,
  MessageHeaders,
  MessagePart,
} from "@intx/types/runtime";

const REF_SCHEME = "mail-part:///";

// Content types whose bytes are UTF-8 text and small enough to also inline as
// `MailPart.text`, so a selector can read them without resolving the ref.
const INLINE_TEXT_MAX_BYTES = 1024 * 1024;

const CONTROL_CHAR_MAX = 0x1f;
const DEL_CHAR = 0x7f;
// Unicode line/paragraph separators. JavaScript's regex `.` does NOT match
// these, so the kind handler's `<index>-<name>` check (whose name group is
// `.+`) rejects a filename containing them. The sanitizer must strip them to
// keep its "satisfies the handler by construction" contract.
const LINE_SEPARATOR = 0x2028;
const PARAGRAPH_SEPARATOR = 0x2029;

const encoder = new TextEncoder();

function byteLength(value: string): number {
  return encoder.encode(value).length;
}

/**
 * Thrown for a DETERMINISTIC, input-shaped rejection of an inbound mail -- a
 * messageId that cannot form a usable path segment. Distinct from a transient
 * substrate write failure so the caller drops the offending mail (replaying it
 * would fail identically) rather than treating it as a retryable fault.
 */
export class InvalidMailError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "InvalidMailError";
  }
}

function encodeMessageSegment(messageId: string): string {
  const encoded = encodeURIComponent(messageId);
  if (byteLength(encoded) > MAX_MAIL_PART_PATH_COMPONENT_BYTES) {
    throw new InvalidMailError(
      `mail part store: messageId ${JSON.stringify(messageId)} url-encodes to ${String(byteLength(encoded))} bytes, over the ${String(MAX_MAIL_PART_PATH_COMPONENT_BYTES)}-byte path-component limit`,
    );
  }
  // `encodeURIComponent` leaves `.` unescaped, so "." or ".." would form a
  // traversal segment; reject it where the messageId -> segment constraint is
  // owned. An empty segment is unreachable for a non-empty messageId but is
  // refused for the same reason.
  if (encoded.length === 0 || encoded === "." || encoded === "..") {
    throw new InvalidMailError(
      `mail part store: messageId ${JSON.stringify(messageId)} url-encodes to ${JSON.stringify(encoded)}, which is not a usable path segment`,
    );
  }
  return encoded;
}

/**
 * Reduce an untrusted part name (a MIME filename, or a fallback) to one safe
 * path segment: path separators, NUL, and control characters become `_`. The
 * `<index>-` prefix guarantees per-message uniqueness, so a sanitization
 * collision between two parts of one message is harmless.
 */
function sanitizePartName(name: string): string {
  let out = "";
  for (const ch of name) {
    const code = ch.codePointAt(0) ?? 0;
    out +=
      ch === "/" ||
      ch === "\\" ||
      code <= CONTROL_CHAR_MAX ||
      code === DEL_CHAR ||
      code === LINE_SEPARATOR ||
      code === PARAGRAPH_SEPARATOR
        ? "_"
        : ch;
  }
  return out.length > 0 ? out : "part";
}

/** Truncate to at most `maxBytes` UTF-8 bytes on a codepoint boundary. */
function truncateToBytes(value: string, maxBytes: number): string {
  if (byteLength(value) <= maxBytes) return value;
  let out = "";
  let used = 0;
  for (const ch of value) {
    const chBytes = byteLength(ch);
    if (used + chBytes > maxBytes) break;
    out += ch;
    used += chBytes;
  }
  return out;
}

/**
 * The on-disk filename for one part: `<index>-<name>`, sanitized and truncated
 * to the handler's byte cap. Satisfies the handler's `<index>-<name>` shape by
 * construction.
 */
function partFilename(index: number, part: MessagePart): string {
  const prefix = `${String(index)}-`;
  const budget = MAX_MAIL_PART_PATH_COMPONENT_BYTES - byteLength(prefix);
  const rawName = part.filename ?? defaultPartName(part.contentType);
  const safeName = truncateToBytes(sanitizePartName(rawName), budget);
  return `${prefix}${safeName.length > 0 ? safeName : "part"}`;
}

/** A stable fallback name for a part with no filename, derived from its type. */
function defaultPartName(contentType: string): string {
  const slash = contentType.indexOf("/");
  const subtype = slash === -1 ? contentType : contentType.slice(slash + 1);
  const safeSubtype = subtype.replace(/[^a-z0-9]+/gi, "") || "bin";
  return `part.${safeSubtype}`;
}

function isTextType(contentType: string): boolean {
  return (
    contentType.startsWith("text/") ||
    contentType === "application/json" ||
    contentType === "application/vnd.interchange+json"
  );
}

function mailPartRef(
  runId: string,
  messageSegment: string,
  filename: string,
): string {
  return `${REF_SCHEME}${encodeURIComponent(runId)}/${messageSegment}/${encodeURIComponent(filename)}`;
}

/**
 * Parse a `mail-part:///` ref into its run id, message segment, and filename.
 * The ref is persisted in the event log and re-read on resume, so it is
 * treated as untrusted: the scheme must match, it must be exactly three
 * non-empty segments, and no segment may traverse.
 */
function parseMailPartRef(ref: string): {
  runId: string;
  messageSegment: string;
  filename: string;
} {
  if (!ref.startsWith(REF_SCHEME)) {
    throw new Error(
      `mail part reader: unrecognized ref ${JSON.stringify(ref)}`,
    );
  }
  const rest = ref.slice(REF_SCHEME.length);
  const segments = rest.split("/");
  const malformed = `mail part reader: malformed ref ${JSON.stringify(ref)}`;
  if (
    segments.length !== 3 ||
    segments.some((s) => s.length === 0) ||
    rest.includes("\\")
  ) {
    throw new Error(malformed);
  }
  // `runId` and `filename` were percent-encoded into the ref; `messageSegment`
  // is stored encoded and matches the on-disk directory name verbatim.
  let runId: string;
  let filename: string;
  try {
    runId = decodeURIComponent(segments[0] ?? "");
    filename = decodeURIComponent(segments[2] ?? "");
  } catch (cause) {
    throw new Error(malformed, { cause });
  }
  const messageSegment = segments[1] ?? "";
  // Reject traversal on the DECODED values too: a ref could encode `..`
  // (`%2e%2e`) or a path separator (`%2f`, `%5c`) that only reveals itself
  // after decoding, forming a compound traversal segment like `../..`.
  const traverses = (s: string): boolean =>
    s === "." || s === ".." || s.includes("/") || s.includes("\\");
  if ([runId, messageSegment, filename].some(traverses)) {
    throw new Error(malformed);
  }
  return { runId, messageSegment, filename };
}

export type MailPartStoreOpts = {
  substrate: SubstrateRepoStore;
  repoId: RepoId;
  principal: Principal;
  runId: string;
  ref: string;
};

/**
 * Commit a decoded message's parts and assemble the JSON-safe `Mail`. Every
 * part's bytes are written in ONE prefix-preserving commit under the message's
 * directory (write-once, atomic), and each part becomes a `MailPart` descriptor
 * carrying its metadata, an opaque `ref`, and -- for a small UTF-8 text part --
 * its decoded `text` inline.
 */
export async function commitMail(
  opts: MailPartStoreOpts,
  messageId: string,
  decoded: {
    headers: MessageHeaders;
    rawHeaders: Record<string, string[]>;
    parts: MessagePart[];
  },
): Promise<Mail> {
  const messageSegment = encodeMessageSegment(messageId);
  const messagePrefix = `${WORKFLOW_RUN_RUNS_PREFIX}/${opts.runId}/${WORKFLOW_RUN_PARTS_DIR}/${messageSegment}/`;
  const fresh: Record<string, Uint8Array> = {};
  const mailParts: MailPart[] = decoded.parts.map((part, index) => {
    const filename = partFilename(index, part);
    fresh[`${messagePrefix}${filename}`] = part.content;
    const descriptor: MailPart = {
      contentType: part.contentType,
      ref: mailPartRef(opts.runId, messageSegment, filename),
    };
    if (part.filename !== undefined) descriptor.filename = part.filename;
    if (part.disposition !== undefined)
      descriptor.disposition = part.disposition;
    if (
      isTextType(part.contentType) &&
      part.content.byteLength <= INLINE_TEXT_MAX_BYTES
    ) {
      descriptor.text = new TextDecoder("utf-8", { fatal: false }).decode(
        part.content,
      );
    }
    return descriptor;
  });

  if (Object.keys(fresh).length > 0) {
    try {
      await opts.substrate.writeTreePreservingPrefix(
        opts.principal,
        opts.repoId,
        opts.ref,
        {
          preservePrefix: messagePrefix,
          merge: async (existing) => {
            const files: Record<string, string | Uint8Array> = {};
            for (const [k, v] of existing) files[k] = v;
            for (const [k, v] of Object.entries(fresh)) files[k] = v;
            return files;
          },
          message: `commit ${String(decoded.parts.length)} mail part(s) for message ${messageId} of run ${opts.runId}`,
        },
      );
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      // A path_violation is a shape rejection of this message's own (already
      // sanitized) content: it is deterministic, so replaying the same bytes
      // fails identically. Surface it as InvalidMailError so the caller drops
      // the mail rather than retrying it forever as a transient fault.
      if (message.startsWith("path_violation: ")) {
        throw new InvalidMailError(message.slice("path_violation: ".length), {
          cause,
        });
      }
      throw cause;
    }
  }

  return {
    headers: decoded.headers,
    rawHeaders: decoded.rawHeaders,
    parts: mailParts,
  };
}

export type MailPartReaderOpts = {
  substrate: SubstrateRepoStore;
  repoId: RepoId;
  principal: Principal;
  ref: string;
};

/**
 * Construct the single mail-part reader for a deployment's workflow-run repo.
 * `read` resolves any run's `MailPart.ref` to the committed bytes through a
 * committed read pinned to the object store, so a cross-run read (a childflow
 * or body step resolving a parent's part) never observes the lagging working
 * tree.
 */
export function createMailPartReader(opts: MailPartReaderOpts): MailPartReader {
  return {
    async read(ref) {
      const { runId, messageSegment, filename } = parseMailPartRef(ref);
      const dir = `${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/${WORKFLOW_RUN_PARTS_DIR}/${messageSegment}`;
      const reads = await opts.substrate.openCommittedReads(
        opts.principal,
        opts.repoId,
        opts.ref,
      );
      if (reads === null) {
        throw new Error(
          `mail part reader: repo ${opts.repoId.id} ref ${opts.ref} has no committed tree; cannot resolve ${ref}`,
        );
      }
      const entry = (await reads.listDir(dir)).find(
        (e) => e.name === filename && e.type === "blob",
      );
      if (entry === undefined) {
        throw new Error(
          `mail part reader: no committed part at ${dir}/${filename}`,
        );
      }
      return reads.readBlobByOid(entry.oid);
    },
  };
}
