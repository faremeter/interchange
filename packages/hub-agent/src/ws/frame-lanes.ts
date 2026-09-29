export type FrameLane = string | symbol;

/**
 * Orders the handling of inbound hub frames. Frames that share a lane are
 * handled one at a time, in the order they arrived; frames on different lanes
 * do not wait on each other. A barrier is handled after every frame that
 * arrived before it and before every frame that arrives after it.
 */
export interface FrameLanes {
  run(lanes: readonly FrameLane[], handle: () => Promise<void>): void;
  barrier(handle: () => Promise<void>): void;
}

export function createFrameLanes(onError: (err: unknown) => void): FrameLanes {
  const tails = new Map<FrameLane, Promise<void>>();
  let lastBarrier: Promise<void> = Promise.resolve();

  function after(
    earlier: readonly Promise<void>[],
    handle: () => Promise<void>,
  ): Promise<void> {
    return Promise.all(earlier).then(handle).catch(onError);
  }

  function run(lanes: readonly FrameLane[], handle: () => Promise<void>): void {
    const handled = after(
      [lastBarrier, ...lanes.flatMap((lane) => tails.get(lane) ?? [])],
      handle,
    );
    for (const lane of lanes) {
      tails.set(lane, handled);
      void handled.then(() => {
        if (tails.get(lane) === handled) tails.delete(lane);
      });
    }
  }

  function barrier(handle: () => Promise<void>): void {
    lastBarrier = after([lastBarrier, ...tails.values()], handle);
  }

  return { run, barrier };
}
