// Records the environment each spawn was invoked with, and lets a caller wait
// for the first one.
//
// A test that needs the spawn-time env -- almost always to read the IPC
// channel id and mint a child-side sender -- has to wait for the spawner to
// be called. Twenty-two sites did that by assigning a mutable binding inside
// the spawner and re-reading it on a one-millisecond timer until it stopped
// being undefined. The spawner being called IS the event, so it is reported
// rather than inferred.

export type SpawnObserver = {
  /** Call from inside the spawner double with the env it received. */
  record(env: Record<string, string>): void;
  /**
   * Resolve with the first recorded env, whether the spawn has happened yet
   * or not. A respawn records again; this always resolves with the first.
   */
  first(): Promise<Record<string, string>>;
  /** Every env recorded so far, in spawn order. */
  all(): readonly Record<string, string>[];
};

export function createSpawnObserver(): SpawnObserver {
  const envs: Record<string, string>[] = [];
  let waiters: (() => void)[] = [];
  return {
    record(env) {
      envs.push(env);
      const waiting = waiters;
      waiters = [];
      for (const waiter of waiting) waiter();
    },
    async first() {
      for (;;) {
        // Re-read on every pass, so a spawn that happened before this call
        // resolves it rather than leaving it waiting for another one.
        const spawned = new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
        const head = envs[0];
        if (head !== undefined) return head;
        await spawned;
      }
    },
    all: () => envs.slice(),
  };
}
