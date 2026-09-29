export type FrameLane = string | symbol;

/**
 * Orders the handling of inbound hub frames. Frames that share a lane are
 * handled one at a time, in the order they arrived; frames on different lanes
 * do not wait on each other.
 */
export interface FrameLanes {
  run(lanes: readonly FrameLane[], handle: () => Promise<void>): void;
}

export function createFrameLanes(onError: (err: unknown) => void): FrameLanes {
  const tails = new Map<FrameLane, Promise<void>>();

  function run(lanes: readonly FrameLane[], handle: () => Promise<void>): void {
    const handled = Promise.all(lanes.flatMap((lane) => tails.get(lane) ?? []))
      .then(handle)
      .catch(onError);
    for (const lane of lanes) {
      tails.set(lane, handled);
      void handled.then(() => {
        if (tails.get(lane) === handled) tails.delete(lane);
      });
    }
  }

  return { run };
}
