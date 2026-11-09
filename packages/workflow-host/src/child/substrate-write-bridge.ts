// Child-side substrate-write bridge.
//
// The workflow-process child holds no write authority over the
// workflow-run repo's ref. The hub linearizes writes at the ref tip;
// a child that opened its own substrate would race the supervisor's
// inbox / processing / consumed writes at the same ref. The supervisor
// owns the write contract; the child proxies its
// `writeTreePreservingPrefix` calls over the control IPC into the
// supervisor's substrate.
//
// Lifecycle of one proxied write:
//
//   1. The child's proxy `RepoStore.writeTreePreservingPrefix` mints a
//      `requestId`, registers a pending entry holding the caller's
//      original merge closure plus resolve/reject hooks, and emits
//      `substrate.write.request` upstream.
//   2. The supervisor receives the request and invokes its own wrapped
//      `writeTreePreservingPrefix` against the supervisor's repoStore.
//      Inside the supervisor's merge callback, the supervisor sends
//      `substrate.merge.request` back to the child carrying the
//      existing prefix entries.
//   3. The bridge resolves the existing entries through the pending
//      entry's merge closure, encodes the resulting tree as
//      base64-coded files, and replies with `substrate.merge.response`.
//   4. The supervisor's merge callback returns the decoded files; the
//      substrate commits the prospective tree under the per-repo lock.
//   5. The supervisor sends `substrate.write.response` with the
//      resulting `commitSha` (or the structured failure). The bridge
//      resolves / rejects the pending awaiter; the child's substrate
//      proxy returns the result to its caller.
//
// The bridge does NOT serialize the merge closure: the closure lives
// in the child's address space, so the merge invocation always runs
// here. The IPC carries the bytes the closure consumes and the bytes
// the closure produces, both base64-encoded.
//
// A proxied `writeTreeDelta` runs the same lifecycle with
// `substrate.write-delta.request` and a `substrate.delta.request` /
// `substrate.delta.response` pair in place of the merge pair. The
// supervisor sends only the parent commit it pinned; the child reads
// that commit from its own store and returns only the files it writes
// and the paths it deletes, so an append moves only what it adds.

import { getLogger } from "@intx/log";
import { base64Decode, base64Encode } from "@intx/types/base64";

import type {
  ControlChannelSender,
  ControlPayload,
} from "../ipc/control-channel";

import { createPendingRequestCore } from "./pending-request";

const logger = getLogger(["workflow-host", "child", "substrate-write-bridge"]);

/**
 * Arguments the bridge takes per `writeTreePreservingPrefix` call. The
 * shape mirrors the substrate's `WriteTreePreservingPrefixArgs` plus
 * the repoId/ref the supervisor needs to route the write to the right
 * underlying substrate.
 */
export interface SubstrateWriteRequest {
  repoId: { kind: string; id: string };
  ref: string;
  preservePrefix: string;
  message: string;
  /**
   * Caller's merge closure. The bridge invokes it locally inside the
   * supervisor-driven merge round-trip; the closure receives the
   * existing prefix entries the supervisor decoded from
   * `substrate.merge.request`'s payload and returns the prospective
   * tree the bridge encodes back into `substrate.merge.response`.
   */
  merge: (
    existing: ReadonlyMap<string, Uint8Array>,
  ) => Promise<Record<string, string | Uint8Array>>;
}

/**
 * Arguments the bridge takes per `writeTreeDelta` call. `computeDelta`
 * receives the parent commit the supervisor pinned under its repo lock
 * and returns the files to write and the paths to delete; the caller
 * reads the parent tree itself, so no tree content crosses the IPC.
 */
export interface SubstrateDeltaWriteRequest {
  repoId: { kind: string; id: string };
  ref: string;
  changedPathPrefixes: ReadonlySet<string> | undefined;
  message: string;
  computeDelta: (parentCommitSha: string | null) => Promise<{
    puts: Record<string, string | Uint8Array>;
    deletes: readonly string[];
  }>;
}

type PendingWrite =
  | { kind: "prefix"; request: SubstrateWriteRequest }
  | { kind: "delta"; request: SubstrateDeltaWriteRequest };

/**
 * Bridge surface the child's substrate proxy reaches into. `submit`
 * sends a `substrate.write.request` and `submitDelta` a
 * `substrate.write-delta.request` upstream; each resolves once the
 * supervisor's matching `substrate.write.response` lands. The
 * `handleMergeRequest`, `handleDeltaRequest` and `handleWriteResponse`
 * hooks are the receiver-side entry points the child's control loop
 * invokes when the corresponding downstream frames arrive.
 *
 * `cancelAll` is the cleanup hook the control loop invokes on any
 * exit path so a pending write does not leak an awaiter when the
 * supervisor has torn the IPC down.
 */
export interface ChildSubstrateWriteBridge {
  submit(req: SubstrateWriteRequest): Promise<{ commitSha: string }>;
  submitDelta(req: SubstrateDeltaWriteRequest): Promise<{ commitSha: string }>;
  handleMergeRequest(
    data: Extract<ControlPayload, { type: "substrate.merge.request" }>["data"],
  ): void;
  handleDeltaRequest(
    data: Extract<ControlPayload, { type: "substrate.delta.request" }>["data"],
  ): void;
  handleWriteResponse(
    data: Extract<ControlPayload, { type: "substrate.write.response" }>["data"],
  ): void;
  cancelAll(reason: string): void;
  readonly pendingCount: number;
}

export interface CreateChildSubstrateWriteBridgeOpts {
  upstreamSender: ControlChannelSender;
  /**
   * Optional `requestId` allocator. Production wires a per-instance
   * monotonic counter plus a random suffix; tests inject a
   * deterministic factory so the upstream frame's `requestId` is
   * predictable.
   */
  allocateRequestId?: () => string;
}

/**
 * Construct the child-side substrate-write bridge. Pending writes
 * live in the shared pending-request core keyed by `requestId`; the
 * bridge resolves the awaiter when the supervisor's matching
 * `substrate.write.response` lands.
 *
 * The supervisor may emit zero or more `substrate.merge.request` (or,
 * for a delta write, `substrate.delta.request`) frames per pending
 * write. The bridge answers each through the pending entry's closure.
 * The pending entry stays alive until the terminal write response lands.
 */
export function createChildSubstrateWriteBridge(
  opts: CreateChildSubstrateWriteBridgeOpts,
): ChildSubstrateWriteBridge {
  const pending = createPendingRequestCore<{ commitSha: string }, PendingWrite>(
    {
      label: "workflow-child substrate write",
      allocatorPrefix: "sw",
      allocateRequestId: opts.allocateRequestId,
    },
  );

  async function sendRequest(
    write: PendingWrite,
    frame: (requestId: string) => ControlPayload,
  ): Promise<{ commitSha: string }> {
    const { requestId, promise } = pending.register(write);
    try {
      await opts.upstreamSender.send(frame(requestId));
    } catch (cause) {
      pending.discard(requestId);
      throw pending.sendFailedError(requestId, cause);
    }
    return promise;
  }

  // Answer a supervisor round-trip with the frame `frame` builds, or
  // with a structured failure so the supervisor's callback does not
  // wedge waiting on a response that will never come.
  function reply<Ok>(
    requestId: string,
    label: string,
    compute: (write: PendingWrite) => Promise<Ok>,
    frame: (
      result: ({ ok: true } & Ok) | { ok: false; reason: string },
    ) => ControlPayload,
  ): void {
    void (async () => {
      let result: ({ ok: true } & Ok) | { ok: false; reason: string };
      const entry = pending.get(requestId);
      if (entry === undefined) {
        logger.warn`${label} landed with no pending entry; requestId=${requestId} dropped`;
        result = {
          ok: false,
          reason: pending.noPendingError(requestId).message,
        };
      } else {
        try {
          result = { ok: true, ...(await compute(entry.meta)) };
        } catch (cause) {
          result = {
            ok: false,
            reason: cause instanceof Error ? cause.message : String(cause),
          };
        }
      }
      try {
        await opts.upstreamSender.send(frame(result));
      } catch (cause) {
        const msg = cause instanceof Error ? cause.message : String(cause);
        logger.error`reply to ${label} upstream send failed: ${msg}`;
      }
    })();
  }

  return {
    get pendingCount() {
      return pending.pendingCount;
    },
    submit(req) {
      return sendRequest({ kind: "prefix", request: req }, (requestId) => ({
        type: "substrate.write.request",
        data: {
          requestId,
          repoId: { kind: req.repoId.kind, id: req.repoId.id },
          ref: req.ref,
          preservePrefix: req.preservePrefix,
          message: req.message,
        },
      }));
    },
    submitDelta(req) {
      return sendRequest({ kind: "delta", request: req }, (requestId) => ({
        type: "substrate.write-delta.request",
        data: {
          requestId,
          repoId: { kind: req.repoId.kind, id: req.repoId.id },
          ref: req.ref,
          changedPathPrefixes:
            req.changedPathPrefixes === undefined
              ? null
              : [...req.changedPathPrefixes],
          message: req.message,
        },
      }));
    },
    handleMergeRequest(data) {
      reply(
        data.requestId,
        "substrate.merge.request",
        async (write) => {
          if (write.kind !== "prefix") {
            throw new Error(
              `substrate.merge.request for requestId ${data.requestId}, which is a delta write`,
            );
          }
          const merged = await write.request.merge(
            decodeMergeRequest(data.existing),
          );
          return { files: encodeFiles(merged) };
        },
        (result) => ({
          type: "substrate.merge.response",
          data: { requestId: data.requestId, result },
        }),
      );
    },
    handleDeltaRequest(data) {
      reply(
        data.requestId,
        "substrate.delta.request",
        async (write) => {
          if (write.kind !== "delta") {
            throw new Error(
              `substrate.delta.request for requestId ${data.requestId}, which is a prefix-preserving write`,
            );
          }
          const delta = await write.request.computeDelta(data.parentCommitSha);
          return { puts: encodeFiles(delta.puts), deletes: [...delta.deletes] };
        },
        (result) => ({
          type: "substrate.delta.response",
          data: { requestId: data.requestId, result },
        }),
      );
    },
    handleWriteResponse(data) {
      const entry = pending.settle(data.requestId);
      if (entry === undefined) {
        logger.warn`substrate.write.response landed with no pending entry; requestId=${data.requestId} dropped`;
        return;
      }
      if (data.result.ok) {
        entry.resolve({ commitSha: data.result.commitSha });
        return;
      }
      entry.reject(pending.rejectedError(data.requestId, data.result.reason));
    },
    cancelAll(reason: string) {
      pending.cancelAll(reason);
    },
  };
}

function decodeMergeRequest(
  existing: readonly { path: string; contentBase64: string }[],
): ReadonlyMap<string, Uint8Array> {
  const out = new Map<string, Uint8Array>();
  for (const entry of existing) {
    out.set(entry.path, base64Decode(entry.contentBase64));
  }
  return out;
}

function encodeFiles(
  files: Record<string, string | Uint8Array>,
): { path: string; contentBase64: string }[] {
  const out: { path: string; contentBase64: string }[] = [];
  for (const [path, content] of Object.entries(files)) {
    const bytes =
      typeof content === "string" ? new TextEncoder().encode(content) : content;
    out.push({ path, contentBase64: base64Encode(bytes) });
  }
  return out;
}
