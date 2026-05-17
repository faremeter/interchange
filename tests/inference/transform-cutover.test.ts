// Headline integration tests for Phase 4 (pluggable context transforms).
//
// These tests wire the real `IsogitStore`, the real `createSizeCapTransform`,
// the real `read_file` POSIX tool, and a real `BlobReader`, against a
// temporary git repo. The inference HTTP path is driven by the
// `@interchange/inference-testing` harness so every production code path
// (the real adapter, the real SSE parser, the real reactor) runs end to end;
// only the live network and live SMTP transports are stubbed.

import { describe, test, expect, afterAll, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { createIsogitStore, initAgentRepo } from "@interchange/storage-isogit";
import {
  createDefaultDependencies,
  createReactor,
  createSizeCapTransform,
  type Reactor,
  type ReactorEmittedEvent,
  type Dependencies,
} from "@interchange/inference";
import type {
  AuditStore,
  ContextStore,
  ReactorDirector,
  ToolRunner,
  TokenUsage,
  TransformRecord,
} from "@interchange/types/runtime";
import { createBlobReader } from "@interchange/types/runtime";
import { createPosixTools } from "@interchange/tools-posix";
import { setupHarness, wire } from "@interchange/inference-testing";
import type { Harness } from "@interchange/inference-testing";

const tempDirs: string[] = [];

async function makeTempDir(prefix = "phase4-"): Promise<string> {
  const dir = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

afterAll(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

// The wire-driven inference cycles below use the Anthropic provider; the
// harness creates the SSE stream, registers the matcher, and serves
// `wire.completeResponse("anthropic", ...)` bytes whose head + tail usage
// frames decode to a positive `tokenUsage.input`/`tokenUsage.output`. This
// matches the original `emittedDoneRunner` default of `{ input: 10,
// output: 20 }` so Test C's `tokenUsage > 0` assertion still holds.
const HEAD_USAGE: TokenUsage = {
  input: 10,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const TAIL_USAGE: TokenUsage = {
  input: 0,
  output: 20,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

function enqueueAnthropicTextResponse(
  harness: Harness,
  text: string,
): {
  stream: ReturnType<Harness["scenario"]["createStream"]>;
  closeAt: number;
} {
  const stream = harness.scenario.createStream();
  const chunks = wire.completeResponse("anthropic", {
    text,
    headUsage: HEAD_USAGE,
    tailUsage: TAIL_USAGE,
  });
  let when = 10;
  for (const chunk of chunks) {
    stream.enqueueAt(when, chunk);
    when += 1;
  }
  stream.closeAt(when);
  return { stream, closeAt: when };
}

function makeInboundMessage(
  text: string,
): import("@interchange/types/runtime").InboundMessage {
  return {
    ref: { uid: 1, mailbox: "INBOX" },
    headers: {
      from: "user@test",
      to: ["agent@test"],
      date: new Date().toISOString(),
      messageId: `<m-${String(Math.random())}@test>`,
    },
    flags: [],
    content: text,
    signatureStatus: "missing",
  };
}

function waitForEvent(
  events: ReactorEmittedEvent[],
  predicate: (e: ReactorEmittedEvent) => boolean,
  timeoutMs = 5000,
): Promise<ReactorEmittedEvent> {
  return new Promise((resolve, reject) => {
    const deadline = setTimeout(
      () => reject(new Error("Timed out waiting for event")),
      timeoutMs,
    );
    function check() {
      const found = events.find(predicate);
      if (found !== undefined) {
        clearTimeout(deadline);
        resolve(found);
        return;
      }
      setTimeout(check, 10);
    }
    check();
  });
}

type RunHandle = {
  reactor: Reactor;
  events: ReactorEmittedEvent[];
  waitFor: (type: ReactorEmittedEvent["type"]) => Promise<ReactorEmittedEvent>;
  store: ContextStore & AuditStore;
  contextStore: ContextStore;
};

async function startReactor(opts: {
  dir: string;
  director: ReactorDirector;
  toolRunner: ToolRunner;
  deps?: Dependencies;
  store?: ContextStore & AuditStore;
}): Promise<RunHandle> {
  const store = opts.store ?? (await createIsogitStore(opts.dir));
  const events: ReactorEmittedEvent[] = [];
  const contextStore: ContextStore = store;
  const sizeCap = createSizeCapTransform({ maxChars: 100, contextStore });

  const reactor = createReactor({
    sessionId: `headline-${String(Math.random())}`,
    director: opts.director,
    providerConfig: {
      provider: "anthropic",
      baseURL: "https://api.anthropic.com",
      apiKey: "test",
    },
    toolRunner: opts.toolRunner,
    contextStore,
    onEvent: (e) => events.push(e),
    deps: opts.deps ?? createDefaultDependencies(),
    shutdownTimeoutMs: 200,
    toolResultTransforms: [sizeCap],
  });

  function waitFor(
    type: ReactorEmittedEvent["type"],
  ): Promise<ReactorEmittedEvent> {
    return waitForEvent(events, (e) => e.type === type);
  }

  reactor.start();
  await waitFor("reactor.start");

  return { reactor, events, waitFor, store, contextStore };
}

// ---------------------------------------------------------------------------
// Test A — Spill round-trip (the headline test)
// ---------------------------------------------------------------------------

describe("Phase 4 headline tests", () => {
  test("Test A — spill round-trip end-to-end", async () => {
    const dir = await makeTempDir();
    const workDir = path.join(dir, "workspace");
    await fs.promises.mkdir(workDir, { recursive: true });

    // The agent's read tool sees the BlobReader; the size-cap transform
    // produces the spill the read tool resolves.
    const store = await createIsogitStore(dir);
    const blobReader = createBlobReader(store);
    const posix = createPosixTools({ cwd: workDir, blobReader });

    // The "noisy" tool returns an oversize payload; the reactor's size-cap
    // transform spills the full content to the context store and replaces
    // the inline content with a marker pointing at `tool-output:///{callId}`.
    const fullPayload = "A".repeat(500);
    const noisyTool: ToolRunner = {
      async run(call) {
        if (call.name === "noisy") {
          return { callId: call.id, content: fullPayload };
        }
        return posix.run(call, new AbortController().signal);
      },
    };

    let directorPhase = 0;
    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        directorPhase++;
        if (event.type === "message.received") {
          return caps.executeTools([
            { id: "noisy-1", name: "noisy", arguments: {} },
          ]);
        }
        if (event.type === "tool.done") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const handle = await startReactor({
      dir,
      director,
      toolRunner: noisyTool,
      store,
    });
    handle.reactor.deliver(makeInboundMessage("please run noisy"));
    await handle.waitFor("reactor.done");

    expect(directorPhase).toBeGreaterThanOrEqual(2);

    // The conversation history (turns.jsonl) carries the truncated marker
    // referencing the spill URI.
    const { turns } = await store.load();
    const toolResultTurn = turns.find((t) =>
      t.content.some((b) => b.type === "tool_result"),
    );
    expect(toolResultTurn).toBeDefined();
    if (toolResultTurn === undefined) throw new Error("unreachable");
    const block = toolResultTurn.content.find((b) => b.type === "tool_result");
    if (block === undefined || block.type !== "tool_result") {
      throw new Error("unreachable");
    }
    const text = block.content
      .map((c) => (c.type === "text" ? c.text : ""))
      .join("");
    expect(text).toContain("tool-output:///noisy-1");
    expect(text).toContain("omitted ");

    // The working tree has a spill file under tool-output/.
    const spillPath = path.join(dir, "tool-output", "noisy-1.txt");
    expect(fs.existsSync(spillPath)).toBe(true);
    expect(await fs.promises.readFile(spillPath, "utf-8")).toBe(fullPayload);

    // read_file resolves the URI and returns the full original content.
    const readResult = await posix.run(
      {
        id: "rd-1",
        name: "read_file",
        arguments: { path: "tool-output:///noisy-1" },
      },
      new AbortController().signal,
    );
    expect(readResult.isError).toBeFalsy();
    const readContent =
      typeof readResult.content === "string"
        ? readResult.content
        : JSON.stringify(readResult.content);
    // read_file formats output with line numbers; the raw payload appears as
    // a single line, so the result should contain a substring of the payload.
    expect(readContent.includes("AAAA")).toBe(true);

    // A git commit was created. The most recent commit includes turns.jsonl,
    // manifest.jsonl, and the blob file in its tree.
    const entries = await git.log({ fs, dir, depth: 5 });
    const head = entries[0];
    expect(head).toBeDefined();
    if (head === undefined) throw new Error("unreachable");
    const tree = await git.readTree({ fs, dir, oid: head.oid });
    const paths = tree.tree.map((e) => e.path);
    expect(paths).toContain("turns.jsonl");
    expect(paths).toContain("manifest.jsonl");
    expect(paths).toContain("tool-output");

    // manifest.jsonl has the size-cap record with the expected decisions.
    const { blob: manifestBlob } = await git.readBlob({
      fs,
      dir,
      oid: head.oid,
      filepath: "manifest.jsonl",
    });
    const lines = new TextDecoder()
      .decode(manifestBlob)
      .split("\n")
      .filter((l) => l.length > 0);
    const records = lines.map((l) => JSON.parse(l) as TransformRecord);
    const sizeCap = records.find((r) => r.strategy === "size-cap");
    expect(sizeCap).toBeDefined();
    expect(sizeCap?.reason).toBe("exceeded-cap");
    expect(sizeCap?.decisions["callId"]).toBe("noisy-1");
    expect(sizeCap?.decisions["spillURI"]).toBe("tool-output:///noisy-1");

    await posix.dispose();
  });

  // -------------------------------------------------------------------------
  // Test B — Fresh-agent first cycle
  // -------------------------------------------------------------------------

  test("Test B — fresh agent first cycle commits the new layout", async () => {
    const dir = await makeTempDir();
    await initAgentRepo(dir);

    const store = await createIsogitStore(dir);
    // Capture the initAgentRepo baseline commit count.
    const baselineEntries = await git.log({ fs, dir, depth: 10 });
    const baselineCount = baselineEntries.length;

    const loaded = await store.load();
    expect(loaded.turns).toEqual([]);
    expect(loaded.pendingOperations).toEqual([]);
    expect(loaded.tokenUsage).toEqual(emptyUsage());
    expect(loaded.connectorState).toBeNull();

    // Run one inference cycle driven by real wire bytes from the testing
    // harness. The director asks for inference on `message.received` and
    // calls done on `inference.done`; the harness serves a complete
    // Anthropic SSE response carrying the assistant text plus head/tail
    // usage frames.
    const director: ReactorDirector = {
      async decide(event, _state, caps) {
        if (event.type === "message.received") {
          return caps.infer("test-model");
        }
        if (event.type === "inference.done") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const noopRunner: ToolRunner = {
      async run(call) {
        return { callId: call.id, content: "" };
      },
    };

    const harness = setupHarness();
    try {
      const { closeAt, stream } = enqueueAnthropicTextResponse(
        harness,
        "hello back",
      );
      harness.scenario.whenRequestMatches(() => true, stream);

      const handle = await startReactor({
        dir,
        director,
        toolRunner: noopRunner,
        deps: harness.deps,
        store,
      });
      handle.reactor.deliver(makeInboundMessage("hi"));
      await harness.advanceTo(closeAt + 10);
      await handle.waitFor("reactor.done");
    } finally {
      harness.dispose();
    }

    const after = await git.log({ fs, dir, depth: 10 });
    // Exactly one new commit on top of the baseline.
    expect(after.length).toBe(baselineCount + 1);

    const head = after[0];
    if (head === undefined) throw new Error("unreachable");
    const tree = await git.readTree({ fs, dir, oid: head.oid });
    const paths = tree.tree.map((e) => e.path);
    expect(paths).toContain("turns.jsonl");
    expect(paths).toContain("prompt.jsonl");
    expect(paths).toContain("response.jsonl");
    expect(paths).toContain("manifest.jsonl");
    expect(paths).toContain("metadata.json");
    // The legacy state/context.json path is not in the new commit's tree.
    const stateEntry = tree.tree.find((e) => e.path === "state");
    if (stateEntry !== undefined && stateEntry.type === "tree") {
      const stateTree = await git.readTree({
        fs,
        dir,
        oid: head.oid,
        filepath: "state",
      });
      expect(
        stateTree.tree.find((e) => e.path === "context.json"),
      ).toBeUndefined();
    }

    // Commit message is non-empty.
    expect(head.commit.message.trim().length).toBeGreaterThan(0);
  });

  // -------------------------------------------------------------------------
  // Test C — Multi-cycle restart
  // -------------------------------------------------------------------------

  test("Test C — multi-cycle restart resumes state from working tree", async () => {
    const dir = await makeTempDir();

    async function driveCycle(
      store: ContextStore & AuditStore,
      responseText: string,
    ): Promise<void> {
      const director: ReactorDirector = {
        async decide(event, _state, caps) {
          if (event.type === "message.received") {
            return caps.infer("test-model");
          }
          if (event.type === "inference.done") {
            return caps.done();
          }
          return caps.done();
        },
      };
      const noop: ToolRunner = {
        async run(c) {
          return { callId: c.id, content: "" };
        },
      };
      const harness = setupHarness();
      try {
        const { closeAt, stream } = enqueueAnthropicTextResponse(
          harness,
          responseText,
        );
        harness.scenario.whenRequestMatches(() => true, stream);

        const handle = await startReactor({
          dir,
          director,
          toolRunner: noop,
          deps: harness.deps,
          store,
        });
        handle.reactor.deliver(makeInboundMessage(`cycle:${responseText}`));
        await harness.advanceTo(closeAt + 10);
        await handle.waitFor("reactor.done");
      } finally {
        harness.dispose();
      }
    }

    {
      const store = await createIsogitStore(dir);
      await driveCycle(store, "r1");
      await driveCycle(store, "r2");
      await driveCycle(store, "r3");
    }

    // Restart: build a fresh IsogitStore against the same dir.
    const reloaded = await createIsogitStore(dir);
    const loaded = await reloaded.load();
    // Three user turns + three assistant turns = 6 turns.
    expect(loaded.turns.length).toBe(6);
    // The reactor wrote pendingOperations and tokenUsage to metadata.json.
    expect(loaded.pendingOperations).toEqual([]);
    expect(loaded.tokenUsage.input).toBeGreaterThan(0);
    expect(loaded.tokenUsage.output).toBeGreaterThan(0);

    const entriesBefore = await git.log({ fs, dir, depth: 50 });

    await driveCycle(reloaded, "r4");
    const entriesAfter = await git.log({ fs, dir, depth: 50 });
    expect(entriesAfter.length).toBe(entriesBefore.length + 1);

    const reloaded2 = await createIsogitStore(dir);
    const final = await reloaded2.load();
    // Four user turns + four assistant turns = 8.
    expect(final.turns.length).toBe(8);
  });

  // -------------------------------------------------------------------------
  // Test D — Restart after a spill
  // -------------------------------------------------------------------------

  test("Test D — spill blobs survive restart and remain addressable", async () => {
    const dir = await makeTempDir();
    const workDir = path.join(dir, "workspace");
    await fs.promises.mkdir(workDir, { recursive: true });

    const fullPayload = "Z".repeat(400);

    // First process: drive one cycle whose tool result triggers a spill.
    {
      const store = await createIsogitStore(dir);
      const noisyTool: ToolRunner = {
        async run(call) {
          if (call.name === "noisy-d") {
            return { callId: call.id, content: fullPayload };
          }
          return { callId: call.id, content: "" };
        },
      };
      const director: ReactorDirector = {
        async decide(event, _state, caps) {
          if (event.type === "message.received") {
            return caps.executeTools([
              { id: "noisyD-1", name: "noisy-d", arguments: {} },
            ]);
          }
          if (event.type === "tool.done") {
            return caps.done();
          }
          return caps.done();
        },
      };
      const handle = await startReactor({
        dir,
        director,
        toolRunner: noisyTool,
        store,
      });
      handle.reactor.deliver(makeInboundMessage("spill please"));
      await handle.waitFor("reactor.done");
    }

    // Second process: a brand-new IsogitStore + a brand-new posix tool runner
    // against the same dir. The spill blob should be addressable.
    {
      const store = await createIsogitStore(dir);
      const blobReader = createBlobReader(store);
      const posix = createPosixTools({ cwd: workDir, blobReader });

      const result = await posix.run(
        {
          id: "rd-2",
          name: "read_file",
          arguments: { path: "tool-output:///noisyD-1" },
        },
        new AbortController().signal,
      );
      expect(result.isError).toBeFalsy();
      const text =
        typeof result.content === "string"
          ? result.content
          : JSON.stringify(result.content);
      expect(text.includes("ZZZZ")).toBe(true);

      await posix.dispose();
    }
  });
});
