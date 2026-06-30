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

import { getLogger } from "@intx/log";
import { base64Decode, base64Encode } from "@intx/types";

import type {
  ControlChannelSender,
  ControlPayload,
} from "../ipc/control-channel";

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
 * Bridge surface the child's substrate proxy reaches into. `submit`
 * sends a `substrate.write.request` upstream and resolves once the
 * supervisor's matching `substrate.write.response` lands. The
 * `handleMergeRequest` and `handleWriteResponse` hooks are the
 * receiver-side entry points the child's control loop invokes when
 * the corresponding downstream frames arrive.
 *
 * `cancelAll` is the cleanup hook the control loop invokes on any
 * exit path so a pending write does not leak an awaiter when the
 * supervisor has torn the IPC down.
 */
export interface ChildSubstrateWriteBridge {
  submit(req: SubstrateWriteRequest): Promise<{ commitSha: string }>;
  handleMergeRequest(
    data: Extract<ControlPayload, { type: "substrate.merge.request" }>["data"],
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

type PendingEntry = {
  req: SubstrateWriteRequest;
  resolve: (value: { commitSha: string }) => void;
  reject: (err: Error) => void;
};

/**
 * Construct the child-side substrate-write bridge. Pending writes
 * live in a map keyed by `requestId`; the bridge resolves the awaiter
 * when the supervisor's matching `substrate.write.response` lands.
 *
 * The supervisor may emit zero or more `substrate.merge.request`
 * frames per pending write (the supervisor's merge callback may run
 * once per attempt; the substrate retries on conflict). The bridge
 * resolves each merge request synchronously through the pending
 * entry's `merge` closure and emits `substrate.merge.response`. The
 * pending entry stays alive until the terminal write response lands.
 */
export function createChildSubstrateWriteBridge(
  opts: CreateChildSubstrateWriteBridgeOpts,
): ChildSubstrateWriteBridge {
  const pending = new Map<string, PendingEntry>();
  const allocate = opts.allocateRequestId ?? defaultRequestIdAllocator();

  return {
    get pendingCount() {
      return pending.size;
    },
    async submit(req: SubstrateWriteRequest): Promise<{ commitSha: string }> {
      const requestId = allocate();
      const resultPromise = new Promise<{ commitSha: string }>(
        (resolve, reject) => {
          pending.set(requestId, { req, resolve, reject });
        },
      );
      try {
        await opts.upstreamSender.send({
          type: "substrate.write.request",
          data: {
            requestId,
            repoId: { kind: req.repoId.kind, id: req.repoId.id },
            ref: req.ref,
            preservePrefix: req.preservePrefix,
            message: req.message,
          },
        });
      } catch (cause) {
        pending.delete(requestId);
        const message = cause instanceof Error ? cause.message : String(cause);
        throw new Error(
          `workflow-child substrate write: upstream send failed for requestId ${requestId}: ${message}`,
          { cause },
        );
      }
      return resultPromise;
    },
    handleMergeRequest(data) {
      const entry = pending.get(data.requestId);
      if (entry === undefined) {
        logger.warn`substrate.merge.request landed with no pending entry; requestId=${data.requestId} dropped`;
        // Reply with a structured failure so the supervisor's merge
        // callback can short-circuit rather than wedge waiting on a
        // response that will never come.
        void opts.upstreamSender
          .send({
            type: "substrate.merge.response",
            data: {
              requestId: data.requestId,
              result: {
                ok: false,
                reason: `workflow-child substrate write: no pending entry for requestId ${data.requestId}`,
              },
            },
          })
          .catch((cause) => {
            const msg = cause instanceof Error ? cause.message : String(cause);
            logger.error`substrate.merge.response upstream send (no-pending) failed: ${msg}`;
          });
        return;
      }
      void (async () => {
        try {
          const existing = decodeMergeRequest(data.existing);
          const merged = await entry.req.merge(existing);
          const files = encodeFiles(merged);
          await opts.upstreamSender.send({
            type: "substrate.merge.response",
            data: {
              requestId: data.requestId,
              result: { ok: true, files },
            },
          });
        } catch (cause) {
          const reason = cause instanceof Error ? cause.message : String(cause);
          try {
            await opts.upstreamSender.send({
              type: "substrate.merge.response",
              data: {
                requestId: data.requestId,
                result: { ok: false, reason },
              },
            });
          } catch (sendCause) {
            const msg =
              sendCause instanceof Error
                ? sendCause.message
                : String(sendCause);
            logger.error`substrate.merge.response upstream send (failure path) failed: ${msg}`;
          }
        }
      })();
    },
    handleWriteResponse(data) {
      const entry = pending.get(data.requestId);
      if (entry === undefined) {
        logger.warn`substrate.write.response landed with no pending entry; requestId=${data.requestId} dropped`;
        return;
      }
      pending.delete(data.requestId);
      if (data.result.ok) {
        entry.resolve({ commitSha: data.result.commitSha });
        return;
      }
      entry.reject(
        new Error(
          `workflow-child substrate write (requestId=${data.requestId}) rejected by supervisor: ${data.result.reason}`,
        ),
      );
    },
    cancelAll(reason: string) {
      for (const [requestId, entry] of pending) {
        entry.reject(
          new Error(
            `workflow-child substrate write (requestId=${requestId}) cancelled: ${reason}`,
          ),
        );
      }
      pending.clear();
    },
  };
}

function decodeMergeRequest(
  existing: readonly { path: string; contentBase64: string }[],
): ReadonlyMap<string, Uint8Array> {
  const out = new Map<string, Uint8Array>();
  for (const entry of existing) {
    out.set(entry.path, base64ToBytes(entry.contentBase64));
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
    out.push({ path, contentBase64: bytesToBase64(bytes) });
  }
  return out;
}

function bytesToBase64(bytes: Uint8Array): string {
  return base64Encode(bytes);
}

function base64ToBytes(value: string): Uint8Array {
  return base64Decode(value);
}

function defaultRequestIdAllocator(): () => string {
  let counter = 0;
  return () => {
    counter += 1;
    const rand = Math.random().toString(36).slice(2, 10);
    return `sw-${String(counter)}-${rand}`;
  };
}
