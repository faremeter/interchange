// In-memory event log keyed by run id.

import type { RepoStore } from "../runtime/env";
import type { WorkflowEvent } from "../state-machine/index";

export function createInMemoryRepoStore(): RepoStore {
  const logs = new Map<string, WorkflowEvent[]>();
  return {
    async read(runId) {
      return logs.get(runId) ?? [];
    },
    async append(runId, event) {
      let log = logs.get(runId);
      if (!log) {
        log = [];
        logs.set(runId, log);
      }
      const last = log[log.length - 1];
      if (last && last.seq >= event.seq) {
        // A same-seq append is an idempotent re-seed only when the
        // payload is structurally identical (the resume seam relies
        // on this behavior). A same-seq append whose kind or content
        // differs would silently drop a real event and corrupt the
        // log; reject it with a diagnostic that names both sides.
        // Non-monotonic appends with a lower seq are also rejected.
        if (last.seq === event.seq) {
          if (!eventsEqual(last, event)) {
            throw new Error(
              `same-seq conflict at ${runId} seq ${String(event.seq)}: store holds ${last.kind}, append carries ${event.kind}; payloads do not match`,
            );
          }
          return;
        }
        throw new Error(
          `non-monotonic append to ${runId}: last seq ${String(last.seq)}, event seq ${String(event.seq)}`,
        );
      }
      log.push(event);
    },
  };
}

/**
 * Structural equality check for two events at the same seq. The
 * canonical-JSON comparison is good enough for the in-memory store --
 * events are plain JSON-serializable objects by the state-machine
 * contract -- and treats key ordering and undefined fields as
 * insignificant.
 */
function eventsEqual(a: WorkflowEvent, b: WorkflowEvent): boolean {
  return canonicalize(a) === canonicalize(b);
}

function canonicalize(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalize).join(",")}]`;
  }
  const entries = Object.entries(value)
    .filter(([, v]) => v !== undefined)
    .sort(([l], [r]) => (l < r ? -1 : l > r ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalize(v)}`).join(",")}}`;
}
