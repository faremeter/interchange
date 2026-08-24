import { describe, test, expect } from "bun:test";

import { api } from "./api";

// The hub's async-accept routes return 202. A signal route returns 202 with
// an empty body; a trigger route returns 202 with a JSON acknowledgement.
// api() must resolve both without throwing "Unexpected end of JSON input",
// which res.json() raises on an empty body. Driving a real Response through
// a stubbed global fetch exercises the actual body-reading branch rather
// than a mock that pre-decides the result.
describe("api 202 handling", () => {
  test("resolves undefined on a real 202 empty-body response without throwing", async () => {
    const originalFetch = globalThis.fetch;
    const seen: { url: string; method: string | undefined }[] = [];
    globalThis.fetch = Object.assign(
      (input: string | URL | Request, init?: RequestInit) => {
        seen.push({ url: String(input), method: init?.method });
        return Promise.resolve(new Response(null, { status: 202 }));
      },
      { preconnect: () => undefined },
    );
    try {
      const result = await api("POST", "/api/workflows/run_1/signals");
      expect(result).toBeUndefined();
      expect(seen).toHaveLength(1);
      expect(seen[0]?.method).toBe("POST");
      expect(seen[0]?.url).toBe("/api/workflows/run_1/signals");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("returns the parsed acknowledgement from a real 202-with-body response", async () => {
    const ack = {
      runId: "run_1",
      address: "wf@deployments.example.com",
      messageId: "<msg_1@example.com>",
    };
    const originalFetch = globalThis.fetch;
    globalThis.fetch = Object.assign(
      () =>
        Promise.resolve(
          new Response(JSON.stringify(ack), {
            status: 202,
            headers: { "Content-Type": "application/json" },
          }),
        ),
      { preconnect: () => undefined },
    );
    try {
      const result = await api<typeof ack>(
        "POST",
        "/api/workflows/run_1/mail",
        { content: "kick off" },
      );
      expect(result).toEqual(ack);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
