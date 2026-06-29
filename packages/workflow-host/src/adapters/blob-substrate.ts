// Production `WorkflowRuntimeEnv.BlobSubstrate` adapter.
//
// Mirrors the in-memory `runlocal/blob-substrate.ts` ref shape so the
// runtime body's `ref` contract is symmetric across substrates:
//
//   - `inline:<encoded-json>` for values whose JSON-stringified form
//     fits inside the inline threshold (1 MiB by default). The ref's
//     whole body after the `inline:` prefix is the verbatim JSON
//     payload; `resolveRef` parses it back. Storing the encoded JSON
//     instead of a substrate write avoids a commit per small output.
//   - `blob:<sha256-prefix>` for values above the threshold. The
//     encoded bytes land at `runs/<runId>/blobs/<sha256-prefix>` on
//     the workflow-run repo via `writeTreePreservingPrefix`;
//     `resolveRef` reads them back from the repo directory.
//
// Constructed per-run -- the `runId` is part of the on-disk path and
// each run gets its own adapter. The sibling repo-store adapter is
// per-deployment because every run shares the same events ref; the
// blob-substrate's per-run path means the closure carries `runId`
// rather than re-deriving it on every call.
//
// Error translation matches the sibling repo-store adapter: the
// substrate's `path_violation:` prefix is stripped so the runtime sees
// a clean reason; every other error propagates unchanged.
//
// `ephemeral: false` -- blob refs survive instance turnover because
// they resolve back to bytes on the workflow-run repo.

import { hexEncode } from "@intx/types";
import type {
  Principal,
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions";
import type { BlobSubstrate } from "@intx/workflow";

const ONE_MIB = 1024 * 1024;
const SHA256_PREFIX_BYTES = 32;
const RUNS_PREFIX = "runs";
const BLOBS_DIR = "blobs";
const INLINE_PREFIX = "inline:";
const BLOB_PREFIX = "blob:";

export type WorkflowRunBlobSubstrateOpts = {
  /**
   * Substrate handle the adapter reads from and writes to. Wired
   * against the substrate's registered workflow-run kind handler --
   * the adapter's writes land under `runs/<runId>/blobs/` and any
   * structural rejection surfaces through `writeTreePreservingPrefix`.
   */
  substrate: SubstrateRepoStore;
  /**
   * Workflow-run repo identifying the owning deployment. The runtime
   * routes per-run writes inside this repo via the `runs/<runId>/`
   * subtree.
   */
  repoId: RepoId;
  /**
   * Principal the adapter presents to the substrate. The production
   * wiring supplies a workflow-process principal scoped to the
   * deployment, matching the sibling repo-store adapter.
   */
  principal: Principal;
  /**
   * Run id whose outputs this adapter owns. A fresh adapter is
   * constructed per run so the on-disk path stays in the closure
   * rather than threading through every call.
   */
  runId: string;
  /**
   * Repo ref the adapter reads from and writes to. The workflow-run
   * repo layout pins all `runs/<runId>/` blobs under a single moving
   * ref. Callers typically supply `"refs/heads/main"`.
   */
  ref: string;
  /**
   * Optional inline-spill threshold in bytes. JSON-stringified outputs
   * at or below this size embed inline in the ref; larger outputs
   * spill to a substrate blob. Defaults to 1 MiB to mirror the
   * in-memory adapter.
   */
  inlineMaxBytes?: number;
};

/**
 * Construct the production `WorkflowRuntimeEnv.BlobSubstrate` adapter
 * for the supplied run. The returned object satisfies the runtime-env
 * interface; substrate handle, principal, repo routing, and run id
 * live in closure.
 */
export function createWorkflowRunBlobSubstrate(
  opts: WorkflowRunBlobSubstrateOpts,
): BlobSubstrate {
  const inlineMax = opts.inlineMaxBytes ?? ONE_MIB;
  return {
    ephemeral: false,
    async recordOutput(stepId, attempt, value) {
      const encoded = JSON.stringify(value);
      if (encoded === undefined) {
        // Mirrors the in-memory adapter: JSON.stringify returns
        // undefined for undefined, functions, and symbols. Surfacing
        // it as an error keeps the contract honest -- silent coercion
        // to null destroys information about the actual step output.
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
  // A SHA-256 hex string is always 64 chars, so this is unreachable
  // today; kept as a cheap invariant pinning the on-disk blob-key
  // shape should the digest width ever change.
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
          // Content-addressed by sha256: a re-recorded value with the
          // same bytes lands at the same path. Overwriting the entry
          // with identical bytes is harmless because the workflow-run
          // kind handler's append-only checks compare prior-vs-
          // prospective bytes and accept matches.
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
