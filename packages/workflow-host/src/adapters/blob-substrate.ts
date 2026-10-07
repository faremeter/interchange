// Production `WorkflowRuntimeEnv.BlobSubstrate` adapter, mirroring the
// in-memory `runlocal/blob-substrate.ts` ref shape:
//
//   - `inline:<encoded-json>` for values whose JSON-stringified form
//     fits the inline threshold (1 MiB by default); no substrate write.
//   - `blob:<sha256>` for larger values, stored at
//     `runs/<runId>/blobs/<sha256>` and read back from the repo.
//
// Constructed per-run (`runId` is part of the on-disk path). Error
// translation matches the sibling repo-store adapter: the substrate's
// `path_violation:` prefix is stripped; other errors propagate.
// `ephemeral: false` -- refs resolve back to repo bytes.

import { hexEncode } from "@intx/types";
import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import type { BlobSubstrate } from "@intx/workflow";

const ONE_MIB = 1024 * 1024;
const SHA256_PREFIX_BYTES = 32;
const RUNS_PREFIX = "runs";
const BLOBS_DIR = "blobs";
const INLINE_PREFIX = "inline:";
const BLOB_PREFIX = "blob:";

export type WorkflowRunBlobSubstrateOpts = {
  /** Substrate handle the adapter reads from and writes to. */
  substrate: SubstrateRepoStore;
  /** Workflow-run repo identifying the owning deployment. */
  repoId: RepoId;
  /** Principal the adapter presents to the substrate. */
  principal: Principal;
  /** Run id whose outputs this adapter owns. */
  runId: string;
  /** Ref the adapter reads from and writes to (typically `"refs/heads/main"`). */
  ref: string;
  /** Inline-spill threshold in bytes; larger outputs spill to a blob. */
  inlineMaxBytes?: number;
};

/** Construct the production `WorkflowRuntimeEnv.BlobSubstrate` adapter. */
export function createWorkflowRunBlobSubstrate(
  opts: WorkflowRunBlobSubstrateOpts,
): BlobSubstrate {
  const inlineMax = opts.inlineMaxBytes ?? ONE_MIB;
  return {
    ephemeral: false,
    async recordOutput(stepId, attempt, value) {
      const encoded = JSON.stringify(value);
      if (encoded === undefined) {
        // JSON.stringify returns undefined for undefined, functions,
        // and symbols; surface it rather than coercing to null.
        throw new Error(
          `step ${stepId} attempt ${String(attempt)} produced an output the blob substrate cannot serialize (typeof ${typeof value})`,
        );
      }
      if (encoded.length <= inlineMax) {
        return { ref: `${INLINE_PREFIX}${encoded}` };
      }
      const bytes = new TextEncoder().encode(encoded);
      const key = await sha256Hex(bytes);
      await writeBlob(opts, key, bytes);
      return { ref: `${BLOB_PREFIX}${key}` };
    },
    async resolveRef(ref) {
      if (ref.startsWith(INLINE_PREFIX)) {
        return JSON.parse(ref.slice(INLINE_PREFIX.length));
      }
      if (ref.startsWith(BLOB_PREFIX)) {
        const key = ref.slice(BLOB_PREFIX.length);
        const bytes = await readBlob(opts, key);
        return JSON.parse(new TextDecoder().decode(bytes));
      }
      throw new Error(`unrecognized ref ${ref}`);
    },
  };
}

function blobsPrefixFor(runId: string): string {
  return `${RUNS_PREFIX}/${runId}/${BLOBS_DIR}/`;
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- ArrayBuffer-backed at the call site; Web Crypto's BufferSource type rejects Uint8Array<ArrayBufferLike> under TS 5.9 (microsoft/TypeScript#62240)
    bytes as Uint8Array<ArrayBuffer>,
  );
  const hex = hexEncode(new Uint8Array(digest));
  // Unreachable today; pins the on-disk blob-key shape should the
  // digest width ever change.
  if (hex.length !== SHA256_PREFIX_BYTES * 2) {
    throw new Error(
      `unexpected sha256 hex length ${String(hex.length)}; expected ${String(SHA256_PREFIX_BYTES * 2)}`,
    );
  }
  return hex;
}

async function writeBlob(
  opts: WorkflowRunBlobSubstrateOpts,
  key: string,
  bytes: Uint8Array,
): Promise<void> {
  const prefix = blobsPrefixFor(opts.runId);
  try {
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.repoId,
      opts.ref,
      {
        preservePrefix: prefix,
        merge: async (existing) => {
          const files: Record<string, string | Uint8Array> = {};
          for (const [k, v] of existing) files[k] = v;
          // Content-addressed: same bytes land at the same path, and
          // the kind handler's append-only check accepts identical
          // prior-vs-prospective bytes.
          files[`${prefix}${key}`] = bytes;
          return files;
        },
        message: `record blob ${key} for run ${opts.runId}`,
      },
    );
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    if (message.startsWith("path_violation: ")) {
      const reason = message.slice("path_violation: ".length);
      throw new Error(reason, { cause });
    }
    throw cause;
  }
}

async function readBlob(
  opts: WorkflowRunBlobSubstrateOpts,
  key: string,
): Promise<Uint8Array> {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const dir = opts.substrate.getRepoDir(opts.repoId);
  const blobPath = path.join(dir, RUNS_PREFIX, opts.runId, BLOBS_DIR, key);
  try {
    return await fs.readFile(blobPath);
  } catch (cause) {
    if (isErrnoNotFound(cause)) {
      throw new Error(
        `workflow-runtime: blob ${key} for run ${opts.runId} not found on disk`,
        { cause },
      );
    }
    throw cause;
  }
}

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
