// Behaviour of `InferenceError.message` and `InferenceError.raw` when
// the upstream returns a non-OK HTTP response with various body shapes.
//
// Three consumers read `error.message` in production: the default
// director synthesises it into the user-facing reply, the hub event
// collector stores it as the timeline error part, and the reactor
// harness writes it into the audit store. A meaningful message is the
// difference between an actionable diagnostic and a generic
// `statusText` echo, so the extraction path needs explicit coverage —
// especially for plain-text bodies (HTML error pages, raw exception
// strings, load-balancer diagnostics) which were previously dropped on
// the floor.

import { describe, test, expect } from "bun:test";

import { runInference } from "@intx/inference";
import type { Dependencies, Scheduler } from "@intx/inference";
import type {
  InferenceEvent,
  InferenceError,
  ConversationTurn,
  InferenceSource,
} from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "openai:test-model",
  provider: "openai",
  baseURL: "https://test.invalid/v1",
  apiKey: "test",
  model: "test-model",
};

function makeTurns(): ConversationTurn[] {
  return [
    { role: "user", content: [{ type: "text", text: "hi" }], timestamp: 0 },
  ];
}

const inertScheduler: Scheduler = {
  setTimeout: () => () => {
    /* tests do not exercise timer firing */
  },
};

async function drain(
  stream: AsyncIterable<InferenceEvent>,
): Promise<InferenceEvent[]> {
  const out: InferenceEvent[] = [];
  for await (const event of stream) {
    out.push(event);
  }
  return out;
}

async function runAgainstFetch(
  fetchStub: Dependencies["fetch"],
): Promise<InferenceError> {
  const deps: Dependencies = { fetch: fetchStub, scheduler: inertScheduler };
  let seq = 0;
  const events = await drain(
    runInference({
      turns: makeTurns(),
      source: SOURCE,
      nextSeq: () => seq++,
      deps,
    }),
  );
  const errorEvent = events.find((e) => e.type === "inference.error");
  if (errorEvent === undefined || errorEvent.type !== "inference.error") {
    throw new Error("expected an inference.error event");
  }
  return errorEvent.data.error;
}

function plainTextResponse(status: number, body: string): Response {
  return new Response(body, {
    status,
    headers: { "content-type": "text/plain" },
  });
}

describe("inference.error.message extraction from non-OK responses", () => {
  test("structured JSON envelope: message is the nested error.message", async () => {
    // Sanity check that the prior behaviour for { error: { message } }
    // is preserved.
    const err = await runAgainstFetch(() =>
      Promise.resolve(
        new Response(
          JSON.stringify({ error: { message: "rate limit reached" } }),
          { status: 429, headers: { "content-type": "application/json" } },
        ),
      ),
    );
    expect(err.message).toBe("rate limit reached");
    expect(err.statusCode).toBe(429);
  });

  test("plain-text body: message surfaces the body content", async () => {
    // Previously: error.message fell back to statusText because the
    // body wasn't valid JSON. Now: the diagnostic text reaches the
    // operator/user-facing message.
    const body = "Database connection pool exhausted at db-replica-3:5432";
    const err = await runAgainstFetch(() =>
      Promise.resolve(plainTextResponse(503, body)),
    );
    expect(err.message).toBe(body);
    expect(err.statusCode).toBe(503);
    expect(err.raw).toBe(body);
  });

  test("long plain-text body: message is truncated with a marker; raw retains the full body", async () => {
    // HTML error pages and stack traces can easily exceed a chat-reply's
    // practical budget. The truncation keeps `error.message` bounded
    // while preserving the full payload in `error.raw` for audit-time
    // inspection.
    const longBody = "x".repeat(2000);
    const err = await runAgainstFetch(() =>
      Promise.resolve(plainTextResponse(500, longBody)),
    );
    expect(typeof err.message).toBe("string");
    expect(err.message.length).toBeLessThan(longBody.length);
    expect(err.message).toContain("truncated");
    expect(err.message).toContain("error.raw");
    expect(err.raw).toBe(longBody);
  });

  test("empty body: message falls back to statusText", async () => {
    // No body, no content. The pre-existing statusText fallback is
    // still the right behaviour — nothing else is available.
    // `statusText` is set explicitly because runtime defaults
    // (Bun returns "" rather than the IANA reason phrase) are not
    // portable across the engines runInference may execute under.
    const err = await runAgainstFetch(() =>
      Promise.resolve(
        new Response(null, { status: 502, statusText: "Bad Gateway" }),
      ),
    );
    expect(err.message).toBe("Bad Gateway");
    expect(err.statusCode).toBe(502);
  });
});
