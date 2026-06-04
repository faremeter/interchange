// Composition-layer tests for `createHarness`.
//
// These tests verify the layer's own responsibilities -- INBOX watch
// subscription, the connector router's pass-through default, lifecycle
// teardown, and the pass-through surface exposed to consumers.
// Behaviours that moved into `@intx/agent` as part of the harness
// split (audit accumulation and flush, reactor lifecycle, source
// rotation, env-validation field-by-field blame) are exercised by the
// agent package's own tests.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  AgentContextLockError,
  createDefaultDirectorRegistry,
  createDirectorRegistry,
  defineAgent,
  defineDirector,
} from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { type } from "arktype";
import {
  setupHarness as setupInferenceHarness,
  type Harness as InferenceTestHarness,
} from "@intx/inference-testing";
import { createInboundMessage } from "@intx/mime";
import { createIsogitStore } from "@intx/storage-isogit";
import type {
  ContextStore,
  InboundMessage,
  InferenceSource,
  MessageRef,
  MessageTransport,
  ReactorCapabilities,
  ReactorInboundEvent,
  ReactorState,
} from "@intx/types/runtime";

import {
  createHarness,
  defineMailTools,
  invokeReplyDrainTerminated,
  invokeReplySendFailed,
  type MailEnv,
} from "./harness";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-harness",
  model: "claude-3-5-sonnet",
};

const AGENT_ADDRESS = "agent@test.local";

interface MockTransportShape {
  fireExists(uid: number): void;
  enqueue(uid: number, message: InboundMessage): void;
  watchCount(): number;
  unsubscribeCount(): number;
  getDeletedRefs(): MessageRef[];
  getFetchedUids(): number[];
  getSent(): unknown[];
}

function makeInboundMessage(uid: number): InboundMessage {
  // The harness's INBOX pipeline reads `ref.uid`, `ref.mailbox`, and
  // (via the connector router) headers like `from`, `to`,
  // `inReplyTo`, `references`. Anything else stays mock-shaped.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, never inspected beyond the fields the harness pipeline reads
  return {
    ref: { uid, mailbox: "INBOX" },
    headers: {
      from: "alice@example.com",
      to: AGENT_ADDRESS,
      subject: "subject",
      date: new Date().toISOString(),
      messageId: `<${String(uid)}@test>`,
      inReplyTo: undefined,
      references: [],
    },
  } as unknown as InboundMessage;
}

function makeMockTransport(): {
  transport: MessageTransport;
  control: MockTransportShape;
} {
  type WatchCallback = (event: {
    type: string;
    uid: number;
    headers?: unknown;
  }) => void;
  const callbacks: WatchCallback[] = [];
  const deletedRefs: MessageRef[] = [];
  const fetchedUids: number[] = [];
  const sent: unknown[] = [];
  const messages = new Map<number, InboundMessage>();
  let unsubscribes = 0;

  // The harness reads `transport.watch`, `transport.fetchFull`,
  // `transport.setFlags`, `transport.expunge`, and `transport.send`.
  // The mock provides those; the rest of the `MessageTransport`
  // surface is satisfied via the double-cast pattern, which the
  // project conventions sanction for library-type test stubs.
  const stub = {
    watch(_mailbox: unknown, callback: WatchCallback): () => void {
      callbacks.push(callback);
      return () => {
        unsubscribes += 1;
      };
    },
    async fetchFull(ref: MessageRef): Promise<InboundMessage> {
      fetchedUids.push(ref.uid);
      const message = messages.get(ref.uid);
      if (message === undefined) {
        throw new Error(`no message for uid ${String(ref.uid)}`);
      }
      return message;
    },
    async setFlags(ref: MessageRef): Promise<void> {
      deletedRefs.push(ref);
    },
    async expunge(): Promise<void> {
      // No-op for the mock; the test asserts via deletedRefs.
    },
    async send(message: unknown): Promise<{ messageId: string }> {
      sent.push(message);
      return { messageId: `<sent-${String(sent.length)}@test>` };
    },
  };

  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- partial mock of a large library interface (MESSAGE.md); methods the harness does not call are not implemented
  const transport = stub as unknown as MessageTransport;

  return {
    transport,
    control: {
      fireExists(uid: number) {
        for (const cb of callbacks) {
          cb({ type: "exists", uid });
        }
      },
      enqueue(uid: number, message: InboundMessage) {
        messages.set(uid, message);
      },
      watchCount(): number {
        return callbacks.length;
      },
      unsubscribeCount(): number {
        return unsubscribes;
      },
      getDeletedRefs(): MessageRef[] {
        return deletedRefs;
      },
      getFetchedUids(): number[] {
        return fetchedUids;
      },
      getSent(): unknown[] {
        return sent;
      },
    },
  };
}

function mailEnv(opts: {
  workdir: string;
  storage: ContextStore;
  transport: MessageTransport;
}): MailEnv {
  return {
    source: SOURCE,
    storage: opts.storage,
    workdir: opts.workdir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    transport: opts.transport,
    address: AGENT_ADDRESS,
  };
}

// Empty mail-tool factory: declares the env requirements without
// providing actual mail tools. These tests do not exercise mail-tool
// invocation; they only verify the composition layer's transport-side
// pipeline.
const emptyMailFactory = defineMailTools(() => ({
  definitions: [],
  async run(call) {
    return { callId: call.id, content: "" };
  },
}));

function emptyDef() {
  return defineAgent({
    id: "harness-test",
    systemPrompt: "test",
    tools: [emptyMailFactory],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

describe("createHarness", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "harness-test-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("subscribes to INBOX on construction", async () => {
    const { transport, control } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    try {
      expect(control.watchCount()).toBeGreaterThanOrEqual(1);
    } finally {
      await harness.close();
    }
  });

  test("close unsubscribes the INBOX watch", async () => {
    const { transport, control } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    expect(control.unsubscribeCount()).toBe(0);
    await harness.close();
    expect(control.unsubscribeCount()).toBeGreaterThanOrEqual(1);
  });

  test("close is idempotent", async () => {
    const { transport } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    await harness.close();
    await harness.close();
  });

  test("watch 'exists' event causes the harness to fetch the message", async () => {
    const { transport, control } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );

    try {
      const message = makeInboundMessage(42);
      control.enqueue(42, message);
      control.fireExists(42);

      // Yield so the async watch callback resolves its fetch.
      await new Promise((resolve) => setTimeout(resolve, 50));

      // The harness must have fetched the message via the transport.
      // Whether it then consumes from INBOX (start/continue routing)
      // or leaves it intact (passthrough) depends on the inbound
      // headers; both outcomes mean the pipeline ran, but the fetch
      // is the precondition.
      expect(control.getFetchedUids()).toContain(42);
    } finally {
      await harness.close();
    }
  });

  test("exposes a stream() pass-through to the underlying agent", async () => {
    const { transport } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    try {
      const iter = harness.stream();
      expect(iter).toBeDefined();
      expect(typeof iter[Symbol.asyncIterator]).toBe("function");
    } finally {
      await harness.close();
    }
  });

  test("exposes blobReader from the underlying agent", async () => {
    const { transport } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    try {
      expect(harness.blobReader).toBeDefined();
      expect(typeof harness.blobReader.read).toBe("function");
    } finally {
      await harness.close();
    }
  });
});

describe("createHarness outbound pipeline", () => {
  let workDir: string;
  let inference: InferenceTestHarness;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "harness-outbound-"));
    inference = setupInferenceHarness();
  });

  afterEach(() => {
    inference.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  // The end-to-end outbound test (inbound mail -> reactor cycle ->
  // connector.reply -> transport.send) is blocked at this commit by a
  // wrappedStorage state-restore race: the harness's wrappedStorage
  // calls `connectorRouter.restore(loaded.connectorState)` on every
  // load(), and the reactor's startup-and-recovery loads fire between
  // the watch callback's commit() and the drain's composeReply(),
  // blanking router state. The drain catches the resulting
  // NoActiveConnectorThreadError and silently drops the outbound
  // reply. The infrastructure below (mock transport, inference-testing
  // harness, createInboundMessage) stages the eventual assertion; a
  // later commit makes the restore boot-only and flips the skip to a
  // real assertion.
  test.skip("delivers a connector.reply through to transport.send", async () => {
    inference.scenario.replyOnce("anthropic", { text: "outbound reply" });

    const { transport, control } = makeMockTransport();
    const storage = await createIsogitStore(workDir);

    const env: MailEnv = {
      ...mailEnv({ workdir: workDir, storage, transport }),
      source: {
        id: "anthropic:claude-3-5-sonnet",
        provider: "anthropic",
        baseURL: "https://api.anthropic.com",
        apiKey: "sk-test-harness-outbound",
        model: "claude-3-5-sonnet",
      },
      deps: inference.deps,
    };

    const harness = await createHarness(emptyDef(), env);

    try {
      const message = createInboundMessage({
        from: "alice@example.com",
        to: AGENT_ADDRESS,
        content: "Hello agent",
        interchangeType: "conversation.message",
      });
      const stored: InboundMessage = {
        ...message,
        ref: { uid: 101, mailbox: "INBOX" },
      };
      control.enqueue(101, stored);
      control.fireExists(101);

      // Poll for the outbound send rather than relying on fixed-time
      // sleeps; the reply drain runs on microtasks, so the assertion
      // meets within a few iterations.
      const deadline = Date.now() + 2000;
      while (Date.now() < deadline) {
        await inference.run();
        if (control.getSent().length > 0) break;
        await new Promise((resolve) => setTimeout(resolve, 25));
      }

      expect(control.getSent().length).toBeGreaterThanOrEqual(1);
    } finally {
      await harness.close();
    }
  });
});

describe("invokeReplySendFailed", () => {
  test("awaits an async callback so its rejection is caught", async () => {
    // The risk this guards: a synchronous `callback(cause)` call site
    // would compile against a `void`-returning signature even when the
    // callback is `async () => void` whose body rejects. The rejection
    // would then escape the surrounding try/catch and surface as an
    // unhandled promise rejection in the reply drain. The helper
    // awaits, so resolution is observed and absorbed.
    let observedRejection: unknown = null;
    const onUnhandled = (err: unknown): void => {
      observedRejection = err;
    };
    process.on("unhandledRejection", onUnhandled);
    try {
      const sentinel = new Error("async callback boom");
      const callback: NonNullable<MailEnv["onReplySendFailed"]> = async () => {
        await Promise.resolve();
        throw sentinel;
      };
      await invokeReplySendFailed(callback, new Error("send failed"));
      // Give the event loop a tick so any escaped rejection would land.
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(observedRejection).toBeNull();
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  test("absorbs a synchronous throw from the callback", async () => {
    const callback: NonNullable<MailEnv["onReplySendFailed"]> = () => {
      throw new Error("sync callback boom");
    };
    // Must not throw out of the helper -- the reply drain relies on the
    // surrounding microtask continuing past the failure.
    await invokeReplySendFailed(callback, new Error("send failed"));
  });

  test("returns normally for a callback that resolves", async () => {
    let observedCause: unknown = null;
    const callback: NonNullable<MailEnv["onReplySendFailed"]> = async (
      cause,
    ) => {
      observedCause = cause;
    };
    const sentinel = new Error("send failed");
    await invokeReplySendFailed(callback, sentinel);
    expect(observedCause).toBe(sentinel);
  });
});

describe("invokeReplyDrainTerminated", () => {
  test("awaits an async callback so its rejection is caught", async () => {
    // The risk this guards: same shape as invokeReplySendFailed. A
    // bare invocation would compile but let an async callback's
    // rejection escape as an unhandled promise rejection. The helper
    // awaits, so resolution is observed and absorbed.
    let observedRejection: unknown = null;
    const onUnhandled = (err: unknown): void => {
      observedRejection = err;
    };
    process.on("unhandledRejection", onUnhandled);
    try {
      const sentinel = new Error("async callback boom");
      const callback: NonNullable<
        MailEnv["onReplyDrainTerminated"]
      > = async () => {
        await Promise.resolve();
        throw sentinel;
      };
      await invokeReplyDrainTerminated(callback, new Error("drain dead"));
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(observedRejection).toBeNull();
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  test("absorbs a synchronous throw from the callback", async () => {
    const callback: NonNullable<MailEnv["onReplyDrainTerminated"]> = () => {
      throw new Error("sync callback boom");
    };
    await invokeReplyDrainTerminated(callback, new Error("drain dead"));
  });

  test("returns normally for a callback that resolves and forwards cause", async () => {
    let observedCause: unknown = null;
    const callback: NonNullable<MailEnv["onReplyDrainTerminated"]> = async (
      cause,
    ) => {
      observedCause = cause;
    };
    const sentinel = new Error("drain dead");
    await invokeReplyDrainTerminated(callback, sentinel);
    expect(observedCause).toBe(sentinel);
  });
});

describe("defineMailTools", () => {
  test("produces a factory declaring transport and address requirements", () => {
    const factory = defineMailTools(() => ({
      definitions: [],
      async run(call) {
        return { callId: call.id, content: "" };
      },
    }));
    expect(factory.id).toBe("@intx/harness/mail");
    expect(factory.requires).toContain("transport");
    expect(factory.requires).toContain("address");
  });
});

// ---------------------------------------------------------------------------
// Delivery pipeline -- the message reaches the reactor
// ---------------------------------------------------------------------------

// Director registry whose decide() records every `message.received`
// event and signals via the supplied counter. Used to assert that
// the harness's transport + deliver() paths actually surface the
// message into reactor decisions, not just into the fetch buffer.
function recordingDirectorRegistry(received: { count: number }) {
  const defined = defineDirector({
    id: "@intx-test/harness/delivery-probe",
    configSchema: type({}),
    factory: () => ({
      async decide(
        event: ReactorInboundEvent,
        _state: ReactorState,
        caps: ReactorCapabilities,
      ) {
        if (event.type === "message.received") {
          received.count += 1;
          return caps.done();
        }
        return caps.wait();
      },
    }),
  });
  return createDirectorRegistry({
    factories: [defined.factory],
    defaultId: defined.factory.id,
  });
}

function recordingDef() {
  return defineAgent({
    id: "harness-delivery-probe",
    systemPrompt: "test",
    tools: [emptyMailFactory],
    capabilities: [],
    inference: {
      sources: [{ provider: SOURCE.provider, model: SOURCE.model }],
    },
  });
}

async function waitForReactorDone(
  stream: AsyncIterable<{ type: string }>,
): Promise<void> {
  for await (const event of stream) {
    if (event.type === "reactor.done") return;
  }
}

describe("createHarness message delivery", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "harness-delivery-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("a watch 'exists' event surfaces the message to the reactor", async () => {
    const { transport, control } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const received = { count: 0 };
    const env: MailEnv = {
      ...mailEnv({ workdir: workDir, storage, transport }),
      directors: recordingDirectorRegistry(received),
    };
    const harness = await createHarness(recordingDef(), env);

    try {
      const message = createInboundMessage({
        from: "alice@example.com",
        to: AGENT_ADDRESS,
        content: "Hello",
        interchangeType: "conversation.message",
      });
      const stored: InboundMessage = {
        ...message,
        ref: { uid: 7, mailbox: "INBOX" },
      };
      control.enqueue(7, stored);
      control.fireExists(7);

      const stream = harness.stream();
      await waitForReactorDone(stream);
      expect(received.count).toBe(1);
    } finally {
      await harness.close();
    }
  });

  test("non-'exists' watch events do not produce a fetch or delivery", async () => {
    const { transport, control } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const received = { count: 0 };
    const env: MailEnv = {
      ...mailEnv({ workdir: workDir, storage, transport }),
      directors: recordingDirectorRegistry(received),
    };
    const harness = await createHarness(recordingDef(), env);

    try {
      // The mock's `fireExists` is the only event shape that
      // should reach a `fetchFull`. The harness's watch callback
      // checks `event.type === "exists"` and short-circuits
      // otherwise -- so a callback yield with no fireExists must
      // produce no fetches and no reactor deliveries. Holding
      // off briefly gives any erroneous async fetch a chance to
      // land before we assert.
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
      expect(control.getFetchedUids().length).toBe(0);
      expect(received.count).toBe(0);
    } finally {
      await harness.close();
    }
  });

  test("deliver() injects a message directly into the reactor", async () => {
    const { transport } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const received = { count: 0 };
    const env: MailEnv = {
      ...mailEnv({ workdir: workDir, storage, transport }),
      directors: recordingDirectorRegistry(received),
    };
    const harness = await createHarness(recordingDef(), env);

    try {
      const message = createInboundMessage({
        from: "alice@example.com",
        to: AGENT_ADDRESS,
        content: "Direct",
        interchangeType: "conversation.message",
      });
      harness.deliver(message);
      await waitForReactorDone(harness.stream());
      expect(received.count).toBe(1);
    } finally {
      await harness.close();
    }
  });
});

// ---------------------------------------------------------------------------
// blobReader -- pass-through to the wrapped store
// ---------------------------------------------------------------------------

describe("createHarness blobReader", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "harness-blob-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("resolves a tool-output URI through the wrapped context store", async () => {
    const { transport } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    await storage.writeBlob(
      "abc123",
      new TextEncoder().encode("spilled bytes"),
    );
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    try {
      const bytes = await harness.blobReader.read("tool-output:///abc123");
      expect(new TextDecoder().decode(bytes)).toBe("spilled bytes");
    } finally {
      await harness.close();
    }
  });

  test("throws when the underlying store has no matching blob", async () => {
    const { transport } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    try {
      let thrown: Error | undefined;
      try {
        await harness.blobReader.read("tool-output:///missing");
      } catch (cause) {
        thrown = cause instanceof Error ? cause : new Error(String(cause));
      }
      expect(thrown?.message).toContain("Blob not found");
    } finally {
      await harness.close();
    }
  });

  test("rejects malformed URIs without touching the store", async () => {
    const { transport } = makeMockTransport();
    const storage = await createIsogitStore(workDir);
    let readCount = 0;
    const originalReadBlob = storage.readBlob.bind(storage);
    storage.readBlob = async (key, signal) => {
      readCount += 1;
      return originalReadBlob(key, signal);
    };
    const harness = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport }),
    );
    try {
      let thrown: Error | undefined;
      try {
        await harness.blobReader.read("file:///abc");
      } catch (cause) {
        thrown = cause instanceof Error ? cause : new Error(String(cause));
      }
      expect(thrown?.message).toContain("invalid tool-output URI scheme");
      expect(readCount).toBe(0);
    } finally {
      await harness.close();
    }
  });
});

// ---------------------------------------------------------------------------
// Workdir lock -- second createHarness on the same workdir is rejected
// ---------------------------------------------------------------------------

describe("createHarness workdir lock", () => {
  let workDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "harness-lock-"));
  });

  afterEach(() => {
    rmSync(workDir, { recursive: true, force: true });
  });

  test("rejects a second instance on the same workdir", async () => {
    const { transport: transportA } = makeMockTransport();
    const { transport: transportB } = makeMockTransport();
    const storage = await createIsogitStore(workDir);

    const first = await createHarness(
      emptyDef(),
      mailEnv({ workdir: workDir, storage, transport: transportA }),
    );

    try {
      let thrown: unknown;
      try {
        await createHarness(
          emptyDef(),
          mailEnv({ workdir: workDir, storage, transport: transportB }),
        );
      } catch (cause) {
        thrown = cause;
      }
      expect(thrown).toBeInstanceOf(AgentContextLockError);
    } finally {
      await first.close();
    }
  });
});
