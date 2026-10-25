import { expect, test } from "bun:test";
import { Worker } from "node:worker_threads";
import { type } from "arktype";

import { ControlPayload } from "./control-channel";
import { parseControlPayload } from "./control-payloads";

test("payload schemas are constructed on first use and reused by kind", async () => {
  const worker = new Worker(
    new URL("./control-payloads-cache-worker.ts", import.meta.url),
  );
  try {
    const observed = await new Promise<unknown>((resolve, reject) => {
      worker.once("message", resolve);
      worker.once("error", reject);
      worker.once("exit", (code) => {
        reject(
          new Error(`Payload cache worker exited without a result: ${code}`),
        );
      });
    });
    expect(observed).toEqual({
      afterImport: [],
      afterUnknown: [],
      afterFirst: ["'shutdown'"],
      afterRepeat: ["'shutdown'"],
      afterSecond: ["'shutdown'", "'drain'"],
      afterSecondRepeat: ["'shutdown'", "'drain'"],
    });
  } finally {
    await worker.terminate();
  }
});

test("dispatching control payloads preserves validation and parsed values", () => {
  const source = {
    id: "source",
    provider: "anthropic",
    model: "test-model",
    baseURL: "https://example.com",
    credentialId: "credential",
  };
  const inputs: unknown[] = [
    null,
    [],
    "shutdown",
    {},
    { type: 1 },
    { type: "constructor", data: {} },
    { type: "__proto__", data: {} },
    { type: "unknown", data: {} },
    { type: "shutdown", data: { reason: "done" } },
    { type: "shutdown", data: { reason: 1 } },
    { type: "drain", data: { deadlineMs: 100 } },
    { type: "drain", data: { deadlineMs: "100" } },
    { type: "ready" },
    {
      type: "definition",
      data: {
        childPid: 1,
        childPublicKey: "key",
        result: { ok: true, projection: {} },
      },
    },
    {
      type: "definition",
      data: {
        childPid: 1,
        childPublicKey: "key",
        result: { ok: false, error: "the closure did not load" },
      },
    },
    {
      type: "definition",
      data: { childPid: 1, childPublicKey: "key", result: { ok: false } },
    },
    {
      type: "sources-updated",
      data: { sources: [source], defaultSource: "source" },
    },
    {
      type: "sources-updated",
      data: { sources: [source, source], defaultSource: "source" },
    },
    {
      type: "sources-updated",
      data: { sources: [source], defaultSource: "missing" },
    },
    {
      type: "credentials-updated",
      data: { delivery: { bindings: [], materials: [] }, revoke: ["old"] },
    },
    {
      type: "credentials-updated",
      data: { delivery: { bindings: [], materials: [] }, revoke: [1] },
    },
    {
      type: "substrate.write.response",
      data: { requestId: "request", result: { ok: true, commitSha: "sha" } },
    },
    {
      type: "substrate.write.response",
      data: { requestId: "request", result: { ok: true } },
    },
  ];

  for (const input of inputs) {
    const combined = ControlPayload(input);
    const dispatched = parseControlPayload(input);
    expect(dispatched instanceof type.errors).toBe(
      combined instanceof type.errors,
    );
    if (!(combined instanceof type.errors))
      expect(dispatched).toEqual(combined);
  }
});

test("a quiet answer names its earliest timer as an ISO time", () => {
  const answer = (wakeAt: string) => ({
    type: "quiet.response",
    data: { requestId: "quiet-1", quiet: true, wakeAt },
  });
  expect(
    parseControlPayload(answer("2026-10-02T00:00:00.000Z")) instanceof
      type.errors,
  ).toBe(false);
  expect(parseControlPayload(answer("tomorrow")) instanceof type.errors).toBe(
    true,
  );
});
