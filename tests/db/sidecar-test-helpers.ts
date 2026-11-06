import type { WsHandle } from "@intx/hub-sessions";

export function createMockWs(): WsHandle & {
  sent: string[];
  closed: boolean;
  awaitSent(predicate: (sent: readonly string[]) => boolean): Promise<void>;
} {
  const waiters = new Set<() => void>();
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
      for (const wake of [...waiters]) wake();
    },
    close() {
      this.closed = true;
    },
    async awaitSent(predicate) {
      while (!predicate(this.sent)) {
        await new Promise<void>((resolve) => {
          const wake = () => {
            waiters.delete(wake);
            resolve();
          };
          waiters.add(wake);
        });
      }
    },
  };
}

// For a lifecycle service whose test asks for no standby: it holds no binding,
// so the service never sends a command.
export const NO_STANDBY_ROUTER = {
  holdsAllocatedBinding: () => false,
  putAllocationInStandby: () => Promise.reject(new Error("no standby here")),
  wakeAllocation: () => Promise.reject(new Error("no standby here")),
};
