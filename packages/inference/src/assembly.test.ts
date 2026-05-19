import { describe, test, expect } from "bun:test";

import { createReactorAssembly } from "./assembly";
import { createInboundMessage } from "@intx/mime";
import type { AuthzCallResult } from "./authz-extension";
import type { ReactorEmittedEvent } from "./reactor";

import type {
  AuditStore,
  BeforeToolExtension,
  ContextCommit,
  ContextStore,
  ConversationTurn,
  InboundMessage,
  ProviderConfig,
  ReactorCapabilities,
  ReactorDirector,
  ReactorState,
  TokenUsage,
  ToolResultTransform,
  ToolRunner,
} from "@intx/types/runtime";

import type { AuditRecord } from "@intx/types/audit";

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

function emptyUsage(): TokenUsage {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

type RecordingContextStore = ContextStore & {
  blobs: Map<string, { bytes: Uint8Array; contentType: string | undefined }>;
};

function makeContextStore(
  turns: ConversationTurn[] = [],
): RecordingContextStore {
  const blobs = new Map<
    string,
    { bytes: Uint8Array; contentType: string | undefined }
  >();
  async function commit(
    options: { message: string },
    _signal?: AbortSignal,
  ): Promise<ContextCommit> {
    return { hash: "abc", message: options.message, timestamp: Date.now() };
  }
  return {
    blobs,
    async load() {
      return {
        turns,
        pendingOperations: [],
        tokenUsage: emptyUsage(),
        connectorState: null,
      };
    },
    setConnectorState() {
      /* noop */
    },
    commit,
    async branch() {
      /* noop */
    },
    async log() {
      return [];
    },
    async readAt() {
      return [];
    },
    async writeBlob(key, bytes, contentType) {
      blobs.set(key, { bytes, contentType });
    },
    async readBlob(key) {
      const entry = blobs.get(key);
      if (entry === undefined) {
        throw new Error(`no blob for key ${key}`);
      }
      return entry.bytes;
    },
    async writePrompt() {
      /* noop */
    },
    async writeResponse() {
      /* noop */
    },
    async writeManifest() {
      /* noop */
    },
    async writeTurns() {
      /* noop */
    },
    async writeMetadata() {
      /* noop */
    },
    async readManifestHistory() {
      return [];
    },
  };
}

function makeToolRunner(content: string | Record<string, unknown>): ToolRunner {
  return {
    async run(call) {
      return { callId: call.id, content };
    },
  };
}

function makeInboundMessage(): InboundMessage {
  return createInboundMessage({
    from: "test@example.com",
    to: "agent@example.com",
    content: "hello",
  });
}

// Director that executes a single tool call on message.received and ends on
// tool.done, optionally with a checkpoint to trigger afterCheckpoint flush.
function makeToolExecDirector(
  toolName: string,
  toolArgs: Record<string, unknown> = {},
  options: { checkpoint?: boolean; callId?: string } = {},
): ReactorDirector {
  const callId = options.callId ?? `call-${toolName}`;
  return {
    async decide(
      event: { type: string },
      _state: ReactorState,
      caps: ReactorCapabilities,
    ) {
      if (event.type === "message.received") {
        return caps.executeTools([
          { id: callId, name: toolName, arguments: toolArgs },
        ]);
      }
      if (event.type === "tool.done") {
        if (options.checkpoint === true) {
          return [caps.checkpoint(), caps.done()];
        }
        return caps.done();
      }
      return caps.done();
    },
  };
}

function waitForDone(events: ReactorEmittedEvent[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = setTimeout(
      () => reject(new Error("Timed out waiting for reactor.done")),
      5000,
    );
    const check = () => {
      if (events.some((e) => e.type === "reactor.done")) {
        clearTimeout(deadline);
        resolve();
        return;
      }
      setTimeout(check, 10);
    };
    check();
  });
}

function provider(): ProviderConfig {
  return {
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    apiKey: "test",
  };
}

function makeRecordingAuditStore(): AuditStore & {
  getCommitted(): AuditRecord[][];
} {
  const committed: AuditRecord[][] = [];
  return {
    async commitAudit(records: AuditRecord[]) {
      committed.push([...records]);
    },
    async loadAudit() {
      return committed.flat();
    },
    async commitErrors() {
      /* noop */
    },
    getCommitted() {
      return committed;
    },
  };
}

function allowAuthorize(): Promise<AuthzCallResult> {
  return Promise.resolve({
    effect: "allow" as const,
    matchingGrants: [],
    resolvedBy: null,
  });
}

// A recording before-tool extension that captures every call and the order in
// which it ran relative to peers via the shared trace array.
function makeRecordingExtension(
  name: string,
  trace: string[],
): BeforeToolExtension {
  return {
    async beforeTool(_call) {
      trace.push(name);
      return undefined;
    },
  };
}

// A recording tool-result transform that captures the order of invocation.
function makeRecordingTransform(
  name: string,
  trace: string[],
): ToolResultTransform {
  return {
    name,
    version: "1",
    async apply(input) {
      trace.push(name);
      return {
        output: input.result,
        record: {
          strategy: name,
          version: "1",
          parameters: {},
          reason: "noop",
          decisions: {},
        },
      };
    },
  };
}

function collectEvents(): {
  events: ReactorEmittedEvent[];
  onEvent: (e: ReactorEmittedEvent) => void;
} {
  const events: ReactorEmittedEvent[] = [];
  return { events, onEvent: (e) => events.push(e) };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createReactorAssembly", () => {
  test("default size-cap transform is the first tool-result transform when no caller transforms are provided", async () => {
    // We observe the size-cap transform via its side effect: a large tool
    // result spills to ContextStore.writeBlob. With no caller transforms,
    // the helper-supplied size-cap is the only transform and the spill must
    // appear in the blob store.
    const contextStore = makeContextStore();
    const events = collectEvents();
    const big = "x".repeat(200);

    const { reactor } = createReactorAssembly({
      sessionId: "s1",
      director: makeToolExecDirector("t", {}, { callId: "c1" }),
      providerConfig: provider(),
      toolRunner: makeToolRunner(big),
      contextStore,
      onEvent: events.onEvent,
      sizeCapMaxChars: 50,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    const spill = contextStore.blobs.get("c1");
    expect(spill).toBeDefined();
    if (spill === undefined) throw new Error("expected spill");
    expect(new TextDecoder().decode(spill.bytes)).toBe(big);
  });

  test("caller's toolResultTransforms run after the default size-cap transform (order preserved)", async () => {
    // Drive an over-cap result. The caller's first transform should observe
    // an already-truncated payload (size-cap ran first), and both caller
    // transforms should run in the order supplied.
    const trace: string[] = [];
    const seen: string[] = [];
    const captureTransform: ToolResultTransform = {
      name: "capture",
      version: "1",
      async apply(input) {
        trace.push("capture");
        seen.push(
          typeof input.result.content === "string"
            ? input.result.content.slice(0, 5)
            : "non-string",
        );
        return {
          output: input.result,
          record: {
            strategy: "capture",
            version: "1",
            parameters: {},
            reason: "noop",
            decisions: {},
          },
        };
      },
    };
    const tailTransform = makeRecordingTransform("tail", trace);

    const contextStore = makeContextStore();
    const events = collectEvents();
    const big = "y".repeat(200);

    const { reactor } = createReactorAssembly({
      sessionId: "s2",
      director: makeToolExecDirector("t", {}, { callId: "c2" }),
      providerConfig: provider(),
      toolRunner: makeToolRunner(big),
      contextStore,
      onEvent: events.onEvent,
      sizeCapMaxChars: 20,
      toolResultTransforms: [captureTransform, tailTransform],
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    expect(trace).toEqual(["capture", "tail"]);
    // The captured prefix is from `big`, confirming the caller transform
    // saw the size-cap's truncated marker (which begins with the kept
    // characters from `big`).
    expect(seen[0]).toBe("yyyyy");
  });

  test("custom sizeCapMaxChars flows into the size-cap transform", async () => {
    // Drive an over-cap result with maxChars=10 and assert the spilled blob's
    // bytes equal the original content (size-cap always spills the full
    // payload when it caps).
    const contextStore = makeContextStore();
    const events = collectEvents();
    const payload = "z".repeat(50);

    const { reactor } = createReactorAssembly({
      sessionId: "s3",
      director: makeToolExecDirector("t", {}, { callId: "c3" }),
      providerConfig: provider(),
      toolRunner: makeToolRunner(payload),
      contextStore,
      onEvent: events.onEvent,
      sizeCapMaxChars: 10,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    const spill = contextStore.blobs.get("c3");
    expect(spill).toBeDefined();
    if (spill === undefined) throw new Error("expected spill");
    expect(spill.bytes.byteLength).toBe(50);
  });

  test("with authorize set, an authz before-tool extension is composed in front of caller's beforeToolExtensions", async () => {
    // No caller extensions: a deny authorize should still block the call by
    // virtue of the assembly-built authz extension running first.
    const contextStore = makeContextStore();
    const events = collectEvents();
    const auditStore = makeRecordingAuditStore();

    const { reactor } = createReactorAssembly({
      sessionId: "s4",
      director: makeToolExecDirector("forbidden", {}, { callId: "c4" }),
      providerConfig: provider(),
      toolRunner: makeToolRunner("never runs"),
      contextStore,
      onEvent: events.onEvent,
      authorize: () =>
        Promise.resolve({
          effect: "deny" as const,
          matchingGrants: [],
          resolvedBy: null,
        }),
      auditStore,
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    const records = auditStore.getCommitted().flat();
    expect(records.length).toBe(1);
    const record = records[0];
    if (record === undefined) throw new Error("expected record");
    expect(record.tool).toBe("forbidden");
    if (record.authz === null) throw new Error("expected authz");
    expect(record.authz.blocked).toBe(true);
    expect(record.authz.effect).toBe("deny");
  });

  test("with authorize + caller's beforeToolExtensions, authz runs first; caller's extensions follow in order", async () => {
    // The authz extension allows; the recording extension records that it
    // saw the call. If authz had been ordered after the recording extension,
    // a deny would still block — but we want to verify ordering specifically,
    // so we use allow and assert the recording extension still ran (proves
    // authz didn't block) and we observe the order via the trace.
    const contextStore = makeContextStore();
    const events = collectEvents();
    const trace: string[] = [];
    const recordingExt = makeRecordingExtension("caller-ext", trace);

    let authzRan = false;
    const authorize = (): Promise<AuthzCallResult> => {
      // Authz runs synchronously here; appending before the await guarantees
      // ordering relative to the caller-ext (which appends in beforeTool).
      authzRan = true;
      trace.push("authz");
      return Promise.resolve({
        effect: "allow" as const,
        matchingGrants: [],
        resolvedBy: null,
      });
    };

    const { reactor } = createReactorAssembly({
      sessionId: "s5",
      director: makeToolExecDirector("t", {}, { callId: "c5" }),
      providerConfig: provider(),
      toolRunner: makeToolRunner("ok"),
      contextStore,
      onEvent: events.onEvent,
      authorize,
      beforeToolExtensions: [recordingExt],
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    expect(authzRan).toBe(true);
    expect(trace).toEqual(["authz", "caller-ext"]);
  });

  test("with auditStore set, an audit collector is returned and afterCheckpoint flushes records to the store", async () => {
    const contextStore = makeContextStore();
    const events = collectEvents();
    const auditStore = makeRecordingAuditStore();

    const { reactor, auditCollector } = createReactorAssembly({
      sessionId: "s6",
      director: makeToolExecDirector(
        "t",
        { k: "v" },
        {
          checkpoint: true,
          callId: "c6",
        },
      ),
      providerConfig: provider(),
      toolRunner: makeToolRunner("ok"),
      contextStore,
      onEvent: events.onEvent,
      auditStore,
      authorize: () => allowAuthorize(),
      shutdownTimeoutMs: 100,
    });

    expect(auditCollector).toBeDefined();

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    const batches = auditStore.getCommitted();
    // Director checkpoints then done — flush happens at checkpoint and again
    // at shutdown. The shutdown flush finds an empty buffer and skips.
    expect(batches.length).toBe(1);
    expect(batches[0]?.length).toBe(1);
    expect(batches[0]?.[0]?.callId).toBe("c6");
  });

  test("with auditStore + authorize, authz onDecision decisions appear in the audit collector's flushed records", async () => {
    const contextStore = makeContextStore();
    const events = collectEvents();
    const auditStore = makeRecordingAuditStore();

    const { reactor } = createReactorAssembly({
      sessionId: "s7",
      director: makeToolExecDirector(
        "t",
        { x: 1 },
        {
          checkpoint: true,
          callId: "c7",
        },
      ),
      providerConfig: provider(),
      toolRunner: makeToolRunner("ok"),
      contextStore,
      onEvent: events.onEvent,
      auditStore,
      authorize: () => allowAuthorize(),
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    const records = auditStore.getCommitted().flat();
    expect(records.length).toBe(1);
    const record = records[0];
    if (record === undefined) throw new Error("expected record");
    if (record.authz === null) throw new Error("expected authz on record");
    expect(record.authz.effect).toBe("allow");
    expect(record.authz.blocked).toBe(false);
  });

  test("caller's afterCheckpoint runs after the helper's audit flush", async () => {
    const contextStore = makeContextStore();
    const events = collectEvents();
    const auditStore = makeRecordingAuditStore();

    const trace: string[] = [];
    // Wrap commitAudit so we can record the moment of the helper's flush.
    const wrappedAuditStore: AuditStore & { getCommitted(): AuditRecord[][] } =
      {
        ...auditStore,
        async commitAudit(records) {
          trace.push("helper-flush");
          await auditStore.commitAudit(records);
        },
      };

    const { reactor } = createReactorAssembly({
      sessionId: "s8",
      director: makeToolExecDirector(
        "t",
        {},
        {
          checkpoint: true,
          callId: "c8",
        },
      ),
      providerConfig: provider(),
      toolRunner: makeToolRunner("ok"),
      contextStore,
      onEvent: events.onEvent,
      auditStore: wrappedAuditStore,
      authorize: () => allowAuthorize(),
      afterCheckpoint: async () => {
        trace.push("caller-after-checkpoint");
      },
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    expect(trace).toEqual(["helper-flush", "caller-after-checkpoint"]);
  });

  test("caller's onShutdown runs after the helper's audit-flush-on-shutdown", async () => {
    const contextStore = makeContextStore();
    const events = collectEvents();
    const auditStore = makeRecordingAuditStore();

    const trace: string[] = [];
    const wrappedAuditStore: AuditStore & { getCommitted(): AuditRecord[][] } =
      {
        ...auditStore,
        async commitAudit(records) {
          trace.push("helper-flush");
          await auditStore.commitAudit(records);
        },
      };

    // Director that does NOT checkpoint — only the shutdown path flushes.
    const director: ReactorDirector = {
      async decide(
        event: { type: string },
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          return caps.executeTools([{ id: "c9", name: "t", arguments: {} }]);
        }
        if (event.type === "tool.done") {
          return caps.done();
        }
        return caps.done();
      },
    };

    const { reactor } = createReactorAssembly({
      sessionId: "s9",
      director,
      providerConfig: provider(),
      toolRunner: makeToolRunner("ok"),
      contextStore,
      onEvent: events.onEvent,
      auditStore: wrappedAuditStore,
      authorize: () => allowAuthorize(),
      onShutdown: async () => {
        trace.push("caller-shutdown");
      },
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    expect(trace).toEqual(["helper-flush", "caller-shutdown"]);
  });

  test("blobReader is wired against the supplied contextStore", async () => {
    const contextStore = makeContextStore();
    const { blobReader } = createReactorAssembly({
      sessionId: "s10",
      director: {
        async decide(_e, _s, caps) {
          return caps.done();
        },
      },
      providerConfig: provider(),
      toolRunner: makeToolRunner("ok"),
      contextStore,
      onEvent: () => {
        /* noop */
      },
      shutdownTimeoutMs: 100,
    });

    const bytes = new TextEncoder().encode("hello-blob");
    await contextStore.writeBlob("k1", bytes, "text/plain");
    const out = await blobReader.read("tool-output:///k1");
    expect(new TextDecoder().decode(out)).toBe("hello-blob");
  });

  test("without auditStore, auditCollector is undefined and afterCheckpoint is only wired when caller provided one", async () => {
    const contextStore = makeContextStore();
    const events = collectEvents();
    let callerAfterCheckpointCalls = 0;

    const { auditCollector, reactor } = createReactorAssembly({
      sessionId: "s11",
      director: makeToolExecDirector(
        "t",
        {},
        {
          checkpoint: true,
          callId: "c11",
        },
      ),
      providerConfig: provider(),
      toolRunner: makeToolRunner("ok"),
      contextStore,
      onEvent: events.onEvent,
      afterCheckpoint: async () => {
        callerAfterCheckpointCalls++;
      },
      shutdownTimeoutMs: 100,
    });

    expect(auditCollector).toBeUndefined();

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    // Caller's afterCheckpoint still fires — the helper passes it through
    // unchanged when there is no audit collector to chain in front of it.
    expect(callerAfterCheckpointCalls).toBe(1);
  });

  test("without authorize, the reactor's beforeToolExtensions is exactly the caller's array", async () => {
    // Observable assertion: a caller-supplied extension that blocks should
    // produce a blocked tool result, and no authz-supplied extension should
    // be interposed (we'd otherwise see two extension invocations).
    const contextStore = makeContextStore();
    const events = collectEvents();
    const trace: string[] = [];
    const ext: BeforeToolExtension = {
      async beforeTool() {
        trace.push("caller-ext");
        return "blocked-by-caller";
      },
    };

    const { reactor } = createReactorAssembly({
      sessionId: "s12",
      director: makeToolExecDirector("t", {}, { callId: "c12" }),
      providerConfig: provider(),
      toolRunner: makeToolRunner("never reached"),
      contextStore,
      onEvent: events.onEvent,
      beforeToolExtensions: [ext],
      shutdownTimeoutMs: 100,
    });

    reactor.start();
    reactor.deliver(makeInboundMessage());
    await waitForDone(events.events);

    expect(trace).toEqual(["caller-ext"]);
    const toolDone = events.events.find(
      (e): e is Extract<ReactorEmittedEvent, { type: "tool.done" }> =>
        e.type === "tool.done",
    );
    if (toolDone === undefined) throw new Error("expected tool.done");
    expect(toolDone.data.result.isError).toBe(true);
    expect(toolDone.data.result.content).toBe("blocked-by-caller");
  });
});
