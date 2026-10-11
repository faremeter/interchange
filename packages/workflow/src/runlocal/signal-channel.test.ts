import { describe, expect, test } from "bun:test";

import { createInMemorySignalChannel } from "./signal-channel";

describe("in-memory signal channel correlation", () => {
  test("a queued name delivery is not taken by a correlated awaiter", async () => {
    const channel = createInMemorySignalChannel();
    await channel.deliver("go", { n: 1 }, "sig-name");
    await channel.deliver("go", { c: 1 }, "sig-c", "corr-1");

    const correlated = await channel.awaitNext("go", undefined, "corr-1");
    expect(correlated).toEqual({
      payload: { c: 1 },
      signalId: "sig-c",
      correlationId: "corr-1",
    });

    const named = await channel.awaitNext("go");
    expect(named).toEqual({ payload: { n: 1 }, signalId: "sig-name" });
    expect(named).not.toHaveProperty("correlationId");
  });

  test("a correlated awaiter registered first does not consume a name delivery", async () => {
    const channel = createInMemorySignalChannel();
    const correlated = channel.awaitNext("go", undefined, "corr-1");
    const named = channel.awaitNext("go");

    await channel.deliver("go", { n: 1 }, "sig-name");
    const namedResult = await named;
    expect(namedResult).toEqual({ payload: { n: 1 }, signalId: "sig-name" });
    expect(namedResult).not.toHaveProperty("correlationId");

    let correlatedSettled = false;
    void correlated.then(() => {
      correlatedSettled = true;
    });
    await Promise.resolve();
    expect(correlatedSettled).toBe(false);

    await channel.deliver("go", { c: 1 }, "sig-c", "corr-1");
    expect(await correlated).toEqual({
      payload: { c: 1 },
      signalId: "sig-c",
      correlationId: "corr-1",
    });
  });

  test("same-correlation deliveries stay ordered around a different correlation", async () => {
    const channel = createInMemorySignalChannel();
    const first = channel.awaitNext("go", undefined, "corr-a");
    const other = channel.awaitNext("go", undefined, "corr-b");
    const second = channel.awaitNext("go", undefined, "corr-a");

    await channel.deliver("go", { n: 1 }, "a1", "corr-a");
    await channel.deliver("go", { n: 2 }, "b1", "corr-b");
    await channel.deliver("go", { n: 3 }, "a2", "corr-a");

    expect((await first).signalId).toBe("a1");
    expect((await other).signalId).toBe("b1");
    expect((await second).signalId).toBe("a2");
  });

  test("a name delivery queued between two correlations stays for a name awaiter", async () => {
    const channel = createInMemorySignalChannel();
    await channel.deliver("go", { c: 1 }, "sig-c1", "corr-1");
    await channel.deliver("go", { n: 1 }, "sig-name");
    await channel.deliver("go", { c: 2 }, "sig-c2", "corr-1");

    expect((await channel.awaitNext("go", undefined, "corr-1")).signalId).toBe(
      "sig-c1",
    );
    expect((await channel.awaitNext("go", undefined, "corr-1")).signalId).toBe(
      "sig-c2",
    );
    expect((await channel.awaitNext("go")).signalId).toBe("sig-name");
  });

  test("same-id deliveries stay ordered when a different id is queued between them", async () => {
    const channel = createInMemorySignalChannel();
    await channel.deliver("go", { n: 1 }, "a1", "corr-a");
    await channel.deliver("go", { n: 2 }, "b1", "corr-b");
    await channel.deliver("go", { n: 3 }, "a2", "corr-a");

    expect((await channel.awaitNext("go", undefined, "corr-a")).signalId).toBe(
      "a1",
    );
    expect((await channel.awaitNext("go", undefined, "corr-a")).signalId).toBe(
      "a2",
    );
    expect((await channel.awaitNext("go", undefined, "corr-b")).signalId).toBe(
      "b1",
    );
  });

  test("a name awaiter does not take a queued correlated delivery", async () => {
    const channel = createInMemorySignalChannel();
    await channel.deliver("go", { c: 1 }, "sig-c", "corr-1");
    await channel.deliver("go", { n: 1 }, "sig-name");

    const named = await channel.awaitNext("go");
    expect(named).toEqual({ payload: { n: 1 }, signalId: "sig-name" });
    expect(named).not.toHaveProperty("correlationId");
    expect((await channel.awaitNext("go", undefined, "corr-1")).signalId).toBe(
      "sig-c",
    );
  });
});
