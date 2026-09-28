// Helpers for forcing two branch prunes to race on a step they share.
//
// Both prunes have to read the shared step as unstarted before either writes
// it, and the window between that read and the write opens on an `await`.
// Holding the first prune's blob recording for the step until the second
// arrives places both inside it deterministically, without a timer.

import type { WorkflowRuntimeEnv } from "./env";

/**
 * Wrap `inner` so the first recording of `key` waits for a second one;
 * then both proceed.
 */
export function holdFirstUntilSecond(
  inner: WorkflowRuntimeEnv["blobs"],
  key: string,
): WorkflowRuntimeEnv["blobs"] {
  let release: (() => void) | undefined;
  let calls = 0;
  return {
    ...inner,
    async recordOutput(stepId, attempt, value) {
      if (stepId === key) {
        calls += 1;
        if (calls === 1) {
          await new Promise<void>((resolve) => {
            release = resolve;
          });
        } else {
          release?.();
        }
      }
      return inner.recordOutput(stepId, attempt, value);
    },
  };
}
