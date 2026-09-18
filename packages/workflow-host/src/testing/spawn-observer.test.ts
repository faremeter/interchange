import { describe, test, expect } from "bun:test";

import { createSpawnObserver } from "./spawn-observer";

describe("createSpawnObserver", () => {
  test("first resolves for a spawn that already happened", async () => {
    const spawns = createSpawnObserver();
    spawns.record({ IPC_CHANNEL_ID: "chan-1" });
    // The spawner is usually called before the test gets a chance to wait,
    // so this has to be satisfied by the recorded spawn rather than by the
    // next one.
    expect(await spawns.first()).toEqual({ IPC_CHANNEL_ID: "chan-1" });
  });

  test("first resolves on a later spawn", async () => {
    const spawns = createSpawnObserver();
    const waited = spawns.first();
    spawns.record({ IPC_CHANNEL_ID: "chan-2" });
    expect(await waited).toEqual({ IPC_CHANNEL_ID: "chan-2" });
  });

  test("first stays the first across a respawn", async () => {
    const spawns = createSpawnObserver();
    spawns.record({ IPC_CHANNEL_ID: "chan-a" });
    spawns.record({ IPC_CHANNEL_ID: "chan-b" });
    expect(await spawns.first()).toEqual({ IPC_CHANNEL_ID: "chan-a" });
    expect(spawns.all()).toHaveLength(2);
  });
});
