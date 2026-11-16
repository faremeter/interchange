import type { ApprovalSnapshot } from "@intx/types/runtime";

import type { SpawnSuspendableChild, SuspendableChildHandle } from "./env";

/**
 * The park reads of a fake body whose parks no caller reads or watches. A
 * call is a bug in the test that built the fake, so each rejects.
 */
export const noParkReleases: Pick<
  SuspendableChildHandle,
  "readParkRelease" | "waitForParkRelease"
> = {
  readParkRelease: (name) =>
    Promise.reject(new Error(`this fake body has no park on ${name} to read`)),
  waitForParkRelease: (name) =>
    Promise.reject(new Error(`this fake body has no park on ${name} to watch`)),
};

/**
 * A fake body that parks on one approval and then completes. Its park reads
 * open until the container waits on it and released from then on, and the
 * watch never reports the release. The container therefore takes a decision
 * first and learns only then that the body left, as it does when a timed
 * step times out while a decision is on its way.
 */
export function bodyLeavingApprovalUnderRelay(
  correlationId: string,
  approvalSnapshot: ApprovalSnapshot,
): { spawn: SpawnSuspendableChild; resumed: () => boolean } {
  let resumed = false;
  return {
    resumed: () => resumed,
    spawn: async () => {
      let parked = false;
      let reads = 0;
      return {
        next: async () => {
          if (parked) return { kind: "terminal", terminalStatus: "completed" };
          parked = true;
          return { kind: "park", park: { correlationId, approvalSnapshot } };
        },
        resume: async () => {
          resumed = true;
        },
        deliverSignal: async () => undefined,
        readParkRelease: async () => {
          reads += 1;
          return reads === 1 ? { kind: "open" } : { kind: "released" };
        },
        waitForParkRelease: (_name, abort) =>
          new Promise((_resolve, reject) => {
            abort.addEventListener(
              "abort",
              () => {
                reject(abort.reason);
              },
              { once: true },
            );
          }),
      };
    },
  };
}
