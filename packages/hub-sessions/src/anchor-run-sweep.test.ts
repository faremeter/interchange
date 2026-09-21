import { describe, expect, test } from "bun:test";

import {
  createAnchorRunSweep,
  type AnchorRunSweepQueries,
} from "./anchor-run-sweep";

function createRunTable(ids: string[]) {
  const runs = new Set(ids);
  const queries: AnchorRunSweepQueries<{ id: string }> = {
    async findPassEnd(activeRunIds) {
      return [...runs]
        .filter((id) => !activeRunIds.includes(id))
        .sort()
        .at(-1);
    },
    async findNext({ afterRunId, passEnd, activeRunIds }) {
      const id = [...runs]
        .filter(
          (candidate) =>
            candidate <= passEnd &&
            (afterRunId === undefined || candidate > afterRunId) &&
            !activeRunIds.includes(candidate),
        )
        .sort()
        .at(0);
      return id === undefined ? undefined : { id };
    },
  };
  return { runs, queries };
}

async function selectAndRelease(
  sweep: ReturnType<typeof createAnchorRunSweep<{ id: string }>>,
) {
  const run = await sweep.select();
  if (run !== null) sweep.release(run.id);
  return run?.id ?? null;
}

describe("createAnchorRunSweep", () => {
  test("visits runs in id order and starts a new pass after the last", async () => {
    const { queries } = createRunTable(["b", "a", "c"]);
    const sweep = createAnchorRunSweep(queries);
    const visited: (string | null)[] = [];
    for (let i = 0; i < 4; i += 1) visited.push(await selectAndRelease(sweep));
    expect(visited).toEqual(["a", "b", "c", "a"]);
  });

  test("bounds a pass by the end chosen when it started", async () => {
    const { runs, queries } = createRunTable(["a", "b"]);
    const sweep = createAnchorRunSweep(queries);
    expect(await selectAndRelease(sweep)).toBe("a");
    runs.add("c");
    expect(await selectAndRelease(sweep)).toBe("b");
    expect(await selectAndRelease(sweep)).toBe("a");
  });

  test("excludes a selected run until it is released", async () => {
    const { queries } = createRunTable(["a", "b"]);
    const sweep = createAnchorRunSweep(queries);
    const first = await sweep.select();
    expect(first?.id).toBe("a");
    expect((await sweep.select())?.id).toBe("b");
    expect(await sweep.select()).toBeNull();
    sweep.release("a");
    expect((await sweep.select())?.id).toBe("a");
  });

  test("serializes concurrent selections", async () => {
    const { queries } = createRunTable(["a", "b", "c"]);
    const sweep = createAnchorRunSweep(queries);
    const selected = await Promise.all([
      sweep.select(),
      sweep.select(),
      sweep.select(),
    ]);
    expect(selected.map((run) => run?.id)).toEqual(["a", "b", "c"]);
  });

  test("pauses between passes when configured", async () => {
    const { queries } = createRunTable(["a"]);
    let clock = 0;
    const sweep = createAnchorRunSweep(queries, {
      intervalMs: 1_000,
      now: () => new Date(clock),
    });
    expect(await selectAndRelease(sweep)).toBe("a");
    expect(await selectAndRelease(sweep)).toBeNull();
    clock = 999;
    expect(await selectAndRelease(sweep)).toBeNull();
    clock = 1_000;
    expect(await selectAndRelease(sweep)).toBe("a");
  });

  test("pauses after finding no runs", async () => {
    const { runs, queries } = createRunTable([]);
    let clock = 0;
    const sweep = createAnchorRunSweep(queries, {
      intervalMs: 1_000,
      now: () => new Date(clock),
    });
    expect(await selectAndRelease(sweep)).toBeNull();
    runs.add("a");
    expect(await selectAndRelease(sweep)).toBeNull();
    clock = 1_000;
    expect(await selectAndRelease(sweep)).toBe("a");
  });

  test("keeps selecting after a query fails", async () => {
    const { queries } = createRunTable(["a"]);
    let fail = true;
    const sweep = createAnchorRunSweep({
      ...queries,
      async findNext(range) {
        if (fail) throw new Error("database unavailable");
        return queries.findNext(range);
      },
    });
    await expect(sweep.select()).rejects.toThrow("database unavailable");
    fail = false;
    expect((await sweep.select())?.id).toBe("a");
  });
});
