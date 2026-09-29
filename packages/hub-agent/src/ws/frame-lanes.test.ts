import { describe, expect, test } from "bun:test";

import { createFrameLanes } from "./frame-lanes";

// Frame lanes are pure promise chains, so one macrotask turn runs every
// continuation they can make without a release: a frame that has not run by
// then is waiting on a held one, not on a slow machine.
async function drain(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function recorder() {
  const events: string[] = [];
  const errors: unknown[] = [];
  const lanes = createFrameLanes((err) => errors.push(err));
  function held(name: string) {
    const release = Promise.withResolvers<boolean>();
    return {
      release: () => release.resolve(true),
      handle: async () => {
        events.push(`${name} start`);
        await release.promise;
        events.push(`${name} end`);
      },
    };
  }
  function quick(name: string) {
    return async () => {
      events.push(name);
    };
  }
  return { lanes, events, errors, held, quick };
}

describe("frame lanes", () => {
  test("handle one lane's frames one at a time, in arrival order", async () => {
    const { lanes, events, held, quick } = recorder();
    const first = held("first");
    lanes.run(["a"], first.handle);
    lanes.run(["a"], quick("second"));

    await drain();
    expect(events).toEqual(["first start"]);

    first.release();
    await drain();
    expect(events).toEqual(["first start", "first end", "second"]);
  });

  test("do not hold one lane's frames behind another lane's", async () => {
    const { lanes, events, held, quick } = recorder();
    const slow = held("slow");
    lanes.run(["a"], slow.handle);
    lanes.run(["b"], quick("other"));

    await drain();
    expect(events).toEqual(["slow start", "other"]);
    slow.release();
    await drain();
  });

  test("handle a barrier after every earlier frame and before every later one", async () => {
    const { lanes, events, held, quick } = recorder();
    const earlier = held("earlier");
    lanes.run(["a"], earlier.handle);
    lanes.barrier(quick("barrier"));
    lanes.run(["b"], quick("later"));

    await drain();
    expect(events).toEqual(["earlier start"]);

    earlier.release();
    await drain();
    expect(events).toEqual([
      "earlier start",
      "earlier end",
      "barrier",
      "later",
    ]);
  });

  test("hold a frame on several lanes until each of them is clear", async () => {
    const { lanes, events, held, quick } = recorder();
    const onA = held("a");
    const onB = held("b");
    lanes.run(["a"], onA.handle);
    lanes.run(["b"], onB.handle);
    lanes.run(["a", "b"], quick("both"));
    lanes.run(["b"], quick("after both"));

    onA.release();
    await drain();
    expect(events).toEqual(["a start", "b start", "a end"]);

    onB.release();
    await drain();
    expect(events).toEqual([
      "a start",
      "b start",
      "a end",
      "b end",
      "both",
      "after both",
    ]);
  });

  test("report a failed frame and keep handling its lane", async () => {
    const { lanes, events, errors, quick } = recorder();
    const failure = new Error("handler failed");
    lanes.run(["a"], () => Promise.reject(failure));
    lanes.run(["a"], quick("next"));

    await drain();
    expect(errors).toEqual([failure]);
    expect(events).toEqual(["next"]);
  });
});
