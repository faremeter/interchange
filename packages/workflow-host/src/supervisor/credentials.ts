// The per-step `credentialsSnapshot` the supervisor forwards to the
// workflow-process over the control IPC. The snapshot never lands on disk in
// the child's view and is never placed in spawn-time env -- the env carries
// only the IPC trust anchors. The child receives credentials over the
// authenticated control channel once both halves are connected.
//
// Each step's snapshot is pinned to a content hash so the child can
// detect a stale snapshot (e.g. one that arrived after a `grants-
// updated` push the child already processed). The hash is the
// sha256 of the canonicalized JSON serialization of the snapshot's
// per-step `grants` array.

import { hexEncode } from "@intx/types/hex";

import type { CredentialsSnapshotPayload } from "../ipc/control-payloads";

export type CredentialsSnapshotStep = {
  /**
   * Workflow step id: a `WorkflowDefinition.stepOrder` entry, or a step id
   * from one of the definition's `loop` bodies (a loop body shares the
   * enclosing definition's flat step-id namespace).
   */
  stepId: string;
  /** Opaque `grants` array the run holds. */
  grants: readonly unknown[];
  /** sha256 hex over the canonical JSON of `grants`. */
  contentHash: string;
};

export type CredentialsSnapshot = {
  /** Step-id keyed entries in the caller's traversal order. */
  steps: readonly CredentialsSnapshotStep[];
};

/**
 * Compute the per-step content hash used to pin a credentialsSnapshot
 * push to a specific grants payload. Stable across processes because
 * the JSON.stringify pass produces the same byte string for a given
 * grants array.
 */
export async function hashGrants(grants: readonly unknown[]): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(grants)),
  );
  return hexEncode(new Uint8Array(digest));
}

/** The wire form of a snapshot, as a control frame carries it. */
export function toCredentialsSnapshotPayload(
  snapshot: CredentialsSnapshot,
): typeof CredentialsSnapshotPayload.infer {
  return {
    steps: snapshot.steps.map((s) => ({
      stepId: s.stepId,
      grants: [...s.grants],
      contentHash: s.contentHash,
    })),
  };
}

/** The snapshot a control frame's wire form carries. */
export function fromCredentialsSnapshotPayload(
  payload: typeof CredentialsSnapshotPayload.infer,
): CredentialsSnapshot {
  return {
    steps: payload.steps.map((s) => ({
      stepId: s.stepId,
      grants: s.grants,
      contentHash: s.contentHash,
    })),
  };
}

export function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
