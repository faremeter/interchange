/**
 * Smart-HTTP `git-upload-pack` request handler. Parses the
 * `want`/`have`/`done` pkt-line body, builds an `includeSha`
 * predicate from the principal's `tokenClaims.refPattern` (a SHA is
 * included only if reachable from at least one matching ref), and
 * streams the negotiated packfile back wrapped in side-band-64k
 * channel-1 frames.
 *
 * Denial during negotiation is reported as a pkt-line `ERR <msg>\n`
 * frame, never as a non-200 HTTP status: stock git surfaces the ERR
 * payload as `remote: <msg>; fatal: protocol error`, which is the
 * protocol-correct shape for refPattern denial and unknown-ref
 * rejections. (Receive-pack uses `ng` during report-status instead;
 * that vocabulary is not used here.)
 */

import fs from "node:fs";
import git from "isomorphic-git";
import type { RepoId } from "@intx/types/sidecar";
import { glob } from "@intx/hub-common";
import {
  createNegotiatedPack,
  collectReachableObjects,
} from "@intx/storage-isogit";
import { readPktLine, writePktLine, writeFlush, writeErr } from "./pkt-line";
import { chunkPackToSideBand } from "./side-band-64k";
import type { RefEntry } from "./advertise-refs";

export const UPLOAD_PACK_RESULT_CONTENT_TYPE =
  "application/x-git-upload-pack-result";

/**
 * Principal contract consumed by the upload-pack handler. Narrowed to
 * the single field the handler reads (`tokenClaims.refPattern`) so any
 * principal carrying that claim satisfies the type without coupling to
 * the full user-principal shape.
 */
export type UploadPackPrincipal = {
  readonly kind: string;
  readonly tokenClaims: {
    readonly refPattern: string;
  };
};

/**
 * Repository-store capability the upload-pack handler depends on. The
 * substrate or a repo-direct adapter implements this; the handler does
 * not care which. `listRefs` provides the ref names + tip SHAs the
 * refPattern is matched against; `getRepoDir` returns the on-disk path
 * passed to `createNegotiatedPack`.
 */
export interface UploadPackRepoStore {
  listRefs(principal: UploadPackPrincipal, repoId: RepoId): Promise<RefEntry[]>;
  getRepoDir(principal: UploadPackPrincipal, repoId: RepoId): Promise<string>;
}

type ParsedRequest = {
  wants: string[];
  haves: string[];
};

async function parseUploadRequest(body: Uint8Array): Promise<ParsedRequest> {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      if (body.length > 0) controller.enqueue(body);
      controller.close();
    },
  });
  const reader = stream.getReader();
  const wants: string[] = [];
  const haves: string[] = [];
  const dec = new TextDecoder();
  let seenFlush = false;
  for (;;) {
    const frame = await readPktLine(reader);
    if (frame === null) break;
    if (frame.kind === "flush") {
      seenFlush = true;
      continue;
    }
    if (frame.kind === "delim") continue;
    const line = dec.decode(frame.payload).replace(/\n$/, "");
    if (!seenFlush) {
      if (line.startsWith("want ")) {
        const rest = line.slice("want ".length);
        const sha = rest.split(" ", 1)[0];
        if (sha === undefined || sha.length === 0) {
          throw new Error(`upload-pack: malformed want line: ${line}`);
        }
        wants.push(sha);
        continue;
      }
      throw new Error(`upload-pack: unexpected pre-flush line: ${line}`);
    }
    if (line.startsWith("have ")) {
      const sha = line.slice("have ".length).split(" ", 1)[0];
      if (sha === undefined || sha.length === 0) {
        throw new Error(`upload-pack: malformed have line: ${line}`);
      }
      haves.push(sha);
      continue;
    }
    if (line === "done") {
      break;
    }
  }
  return { wants, haves };
}

async function walkAllowedObjects(
  dir: string,
  allowedTipShas: readonly string[],
): Promise<Set<string>> {
  const allowed = new Set<string>();
  const seenCommits = new Set<string>();
  const queue: string[] = [...allowedTipShas];
  while (queue.length > 0) {
    const oid = queue.shift();
    if (oid === undefined) break;
    if (seenCommits.has(oid)) continue;
    seenCommits.add(oid);
    let commit;
    try {
      commit = await git.readCommit({ fs, dir, oid });
    } catch {
      // Tip SHA points at something that no longer reads as a commit;
      // skip rather than fail the whole walk.
      continue;
    }
    const objects = await collectReachableObjects(dir, oid);
    for (const o of objects) allowed.add(o);
    for (const p of commit.commit.parent) {
      if (!seenCommits.has(p)) queue.push(p);
    }
  }
  return allowed;
}

async function shaExistsAsCommit(dir: string, sha: string): Promise<boolean> {
  try {
    await git.readCommit({ fs, dir, oid: sha });
    return true;
  } catch {
    return false;
  }
}

type WantClassification =
  | { kind: "ok" }
  | { kind: "forbidden" }
  | { kind: "unknown" };

async function classifyWants(
  dir: string,
  wants: readonly string[],
  allowedObjects: ReadonlySet<string>,
): Promise<WantClassification> {
  // Two-pass classification with a stable preference: `forbidden`
  // outranks `unknown` so the client always sees the same error
  // vocabulary regardless of how it ordered its want lines. A SHA
  // that exists but is reachable only from refs the token cannot see
  // is more useful diagnostic information than a SHA we cannot find
  // at all, and incident triage benefits from determinism.
  let sawForbidden = false;
  let sawUnknown = false;
  for (const want of wants) {
    if (allowedObjects.has(want)) continue;
    const exists = await shaExistsAsCommit(dir, want);
    if (exists) {
      sawForbidden = true;
    } else {
      sawUnknown = true;
    }
  }
  if (sawForbidden) return { kind: "forbidden" };
  if (sawUnknown) return { kind: "unknown" };
  return { kind: "ok" };
}

function errorResponse(msg: string): Response {
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const sink = new WritableStream<Uint8Array>({
        write(chunk) {
          controller.enqueue(chunk);
        },
      });
      const writer = sink.getWriter();
      try {
        await writeErr(writer, msg);
        await writer.close();
        controller.close();
      } catch (cause) {
        await writer.abort(cause).catch(() => undefined);
        controller.error(cause);
      }
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { "content-type": UPLOAD_PACK_RESULT_CONTENT_TYPE },
  });
}

function packToReadable(pack: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      if (pack.length > 0) controller.enqueue(pack);
      controller.close();
    },
  });
}

function successResponse(pack: Uint8Array | null): Response {
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const sink = new WritableStream<Uint8Array>({
        write(chunk) {
          controller.enqueue(chunk);
        },
      });
      const writer = sink.getWriter();
      try {
        await writePktLine(writer, "NAK\n");
      } catch (cause) {
        await writer.abort(cause).catch(() => undefined);
        controller.error(cause);
        return;
      }
      if (pack !== null && pack.length > 0) {
        const sideBand = chunkPackToSideBand(packToReadable(pack));
        const reader = sideBand.getReader();
        try {
          for (;;) {
            const r = await reader.read();
            if (r.done) break;
            if (r.value) controller.enqueue(r.value);
          }
        } catch (cause) {
          await writer.abort(cause).catch(() => undefined);
          controller.error(cause);
          return;
        }
      }
      // Per the smart-HTTP transfer spec, the upload-pack response
      // terminates with a flush pkt-line after the side-band stream.
      // Without this stock git aborts with `unexpected disconnect
      // while reading sideband packet`.
      try {
        await writeFlush(writer);
        await writer.close();
        controller.close();
      } catch (cause) {
        await writer.abort(cause).catch(() => undefined);
        controller.error(cause);
      }
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { "content-type": UPLOAD_PACK_RESULT_CONTENT_TYPE },
  });
}

/**
 * Translate substrate-thrown errors that escape the listRefs /
 * getRepoDir / pack-build call chain into the upload-pack pkt-line
 * ERR vocabulary. The receive-pack side does the equivalent for its
 * own `ng <ref> <reason>` lines; upload-pack has no per-ref status
 * channel, so the substrate's authorize denial collapses into the
 * same `ERR forbidden ref` shape used for refPattern denial.
 *
 * Returns `null` when the error message does not carry a known
 * substrate prefix; the caller rethrows in that case so a genuine
 * crash still bubbles to the HTTP layer as a 500.
 */
function translateSubstrateError(err: unknown): Response | null {
  if (!(err instanceof Error)) return null;
  const message = err.message;
  if (message.startsWith("authorize_denied:")) {
    return errorResponse("forbidden ref");
  }
  for (const prefix of [
    "non_fast_forward:",
    "path_violation:",
    "sha_mismatch:",
  ] as const) {
    if (message.startsWith(prefix)) {
      const detail = message.substring(prefix.length).trimStart();
      return errorResponse(`upload-pack: ${prefix.slice(0, -1)}: ${detail}`);
    }
  }
  return null;
}

export async function handleUploadPack(
  repoStore: UploadPackRepoStore,
  principal: UploadPackPrincipal,
  repoId: RepoId,
  request: Request,
): Promise<Response> {
  const bodyBuf = new Uint8Array(await request.arrayBuffer());
  const { wants, haves } = await parseUploadRequest(bodyBuf);
  if (wants.length === 0) {
    return errorResponse("upload-pack: no want lines");
  }

  try {
    const dir = await repoStore.getRepoDir(principal, repoId);
    const allRefs = await repoStore.listRefs(principal, repoId);
    const refPattern = principal.tokenClaims.refPattern;
    const allowedTips = allRefs
      .filter((r) => glob.match(refPattern, r.name))
      .map((r) => r.sha);

    const allowedObjects = await walkAllowedObjects(dir, allowedTips);

    const classification = await classifyWants(dir, wants, allowedObjects);
    if (classification.kind === "forbidden") {
      return errorResponse("forbidden ref");
    }
    if (classification.kind === "unknown") {
      return errorResponse("upload-pack: not our ref");
    }

    const result = await createNegotiatedPack(dir, wants, haves, (oid) =>
      allowedObjects.has(oid),
    );
    return successResponse(result === null ? null : result.pack);
  } catch (err) {
    const translated = translateSubstrateError(err);
    if (translated !== null) return translated;
    throw err;
  }
}
