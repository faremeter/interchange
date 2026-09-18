import { describe, test, expect } from "bun:test";

import { waitForReactorDone } from "./reactor-waiters";

async function* stream(...events: { type: string }[]) {
  for (const event of events) yield event;
}

describe("waitForReactorDone", () => {
  test("resolves on the terminal event", async () => {
    await waitForReactorDone(
      stream({ type: "reactor.start" }, { type: "reactor.done" }),
    );
  });

  test("throws when the stream ends without the terminal event", async () => {
    // Returning instead would resolve as though the run had completed, and
    // the caller would then fail on a later assertion rather than on the
    // real cause. All four copies of this helper returned.
    await expect(
      waitForReactorDone(stream({ type: "reactor.start" })),
    ).rejects.toThrow(/ended before reactor.done/);
  });

  test("ignores events after the terminal one", async () => {
    await waitForReactorDone(
      stream({ type: "reactor.done" }, { type: "reactor.stray" }),
    );
  });
});
