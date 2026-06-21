import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";
import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";
import {
  assembleMessage,
  assembleSignedContent,
  type MessageHeaders,
} from "@intx/mime";

import {
  parseSpawnTimeEnv,
  runWorkflowChild,
  type RunWorkflowChildBindings,
} from "./index";
import {
  createControlChannelSender,
  generateChannelId,
  generateHmacKey,
  receiveControlChannel,
  receiveEventChannel,
  type FrameReader,
  type FrameWriter,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";

async function makeTempDir(prefix: string): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

function createMemoryNdjsonStream() {
  const buffer: string[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: NdjsonReader = {
    read(): AsyncIterableIterator<string> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  const writer: NdjsonWriter = {
    write(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
    },
  };
  return {
    writer,
    reader,
    inject(line: string) {
      buffer.push(line.replace(/\n$/, ""));
      wake();
    },
    flushed(): readonly string[] {
      return buffer.slice();
    },
    close() {
      done = true;
      wake();
    },
  };
}

function createMemoryFrameStream() {
  const buffer: Uint8Array[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: FrameReader = {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("frame buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  const writer: FrameWriter = {
    write(bytes: Uint8Array) {
      buffer.push(bytes);
      wake();
    },
  };
  return {
    reader,
    writer,
    flushed(): readonly Uint8Array[] {
      return buffer.slice();
    },
    close() {
      done = true;
      wake();
    },
  };
}

function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_principal, _repoId, _ref, _args) {
      return { commitSha: "deadbeefcafef00d" };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; missing methods surface as a precise failure via the proxy
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub RepoStore: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
}

async function seedWorkflowDefinition(
  baseDir: string,
  repoId: RepoId,
): Promise<void> {
  const dir = path.join(baseDir, repoId.kind, repoId.id);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(
    path.join(dir, "workflow.json"),
    JSON.stringify({
      id: "test-workflow",
      triggers: [],
      steps: {},
      stepOrder: [],
    }),
  );
}

async function seedRun(
  baseDir: string,
  workflowRunRepoId: RepoId,
  runId: string,
  events: { seq: number; type: string; [k: string]: unknown }[],
): Promise<void> {
  const dir = path.join(
    baseDir,
    workflowRunRepoId.kind,
    workflowRunRepoId.id,
    "runs",
    runId,
    "events",
  );
  await fs.mkdir(dir, { recursive: true });
  for (const event of events) {
    await fs.writeFile(
      path.join(dir, `${String(event.seq)}.json`),
      JSON.stringify(event),
    );
  }
}

/**
 * Seed a claim-check processing entry into the workflow-run repo's
 * working tree so the child's `trigger.fire` handler can recover the
 * inbound message bytes by messageId. Mirrors the production substrate's
 * working-tree materialization: the supervisor's `dequeueToProcessing`
 * commit lands the entry at
 * `addresses/<urlEncoded(address)>/processing/<receivedAt>-<messageId>.json`,
 * which `readProcessingEntry` reads with a flat fs read.
 */
async function seedProcessingEntry(
  baseDir: string,
  workflowRunRepoId: RepoId,
  opts: {
    address: string;
    messageId: string;
    receivedAt: number;
    text: string;
  },
): Promise<void> {
  const dir = path.join(
    baseDir,
    workflowRunRepoId.kind,
    workflowRunRepoId.id,
    "addresses",
    encodeURIComponent(opts.address),
    "processing",
  );
  await fs.mkdir(dir, { recursive: true });
  const rawMessage = assembleConversationMessage(opts.address, opts.text);
  const envelope = {
    messageId: opts.messageId,
    receivedAt: opts.receivedAt,
    address: opts.address,
    mailAuditRef: { store: "test", path: opts.messageId },
    rawMessage: Buffer.from(rawMessage).toString("base64"),
  };
  await fs.writeFile(
    path.join(dir, `${String(opts.receivedAt)}-${opts.messageId}.json`),
    JSON.stringify(envelope),
  );
}

/**
 * Assemble a signed conversation MIME message carrying `text`, matching
 * the on-wire shape the hub's `routeMail` path delivers. The signature
 * bytes are a placeholder: the child's input extraction reads the
 * conversation text at part `1.1` and never verifies the signature.
 */
function assembleConversationMessage(to: string, text: string): Uint8Array {
  const headers: MessageHeaders = {
    from: "user@example.com",
    to: [to],
    cc: undefined,
    date: new Date(0),
    messageId: "<seed@example.com>",
    subject: undefined,
    inReplyTo: undefined,
    references: undefined,
    mimeVersion: "1.0",
    interchangeType: "conversation.message",
    interchangeCorrelationId: undefined,
    interchangeTenantId: undefined,
    interchangeAgentId: undefined,
    interchangeSessionId: undefined,
    interchangeOfferingId: undefined,
    interchangeSchemaVersion: undefined,
    traceparent: undefined,
    tracestate: undefined,
  };
  const signedContent = assembleSignedContent({ kind: "conversation", text });
  return assembleMessage(headers, signedContent, new Uint8Array([0]));
}

function buildBindings(opts: {
  baseDir: string;
  childKeyPair?: { privateKey: Uint8Array; publicKey: Uint8Array };
}): RunWorkflowChildBindings {
  const substrate = createStubRepoStore(opts.baseDir);
  const principal: Principal = { kind: "supervisor" };
  const childKeyPair = opts.childKeyPair;
  return {
    substrate,
    workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
    workflowRunRef: "refs/heads/main",
    principal,
    workflowDefinitionRepoId: { kind: "workflow", id: "workflow-asset" },
    workflowDefinitionRef: "refs/heads/main",
    invokeStep: async () => ({ output: null }),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    scheduler: {
      scheduleIn: () => () => undefined,
    },
    evaluateGrants: async () => ({
      effect: "allow" as const,
      matchingGrants: [],
      resolvedBy: null,
    }),
    ...(childKeyPair !== undefined
      ? { ipcChildKeyPairFactory: () => Promise.resolve(childKeyPair) }
      : {}),
    initialCredentialsSnapshot: {
      steps: [
        {
          stepId: "step-1",
          address: "deployment-x-step-1@example.com",
          grants: [],
          contentHash: "deadbeef",
        },
      ],
    },
  };
}

function makeSpawnEnv(opts: {
  channelId: string;
  hmacKeyHex: string;
  hostPubKeyHex: string;
}): Record<string, string> {
  return {
    IPC_CHANNEL_ID: opts.channelId,
    IPC_HMAC_KEY: opts.hmacKeyHex,
    HOST_PUBKEY: opts.hostPubKeyHex,
    DEPLOYMENT_ID: "deployment-x",
    DEFINITION_HASH: "definition-hash-abc",
    MAILBOX_ADDRESS: "deployment-x@example.com",
  };
}

function bytesToHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}

describe("parseSpawnTimeEnv", () => {
  test("validates the required spawn-time env keys", () => {
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    const keypair = {
      privateKey: new Uint8Array(32),
      publicKey: new Uint8Array(32),
    };
    const env = parseSpawnTimeEnv(
      makeSpawnEnv({
        channelId,
        hmacKeyHex: bytesToHex(hmacKey),
        hostPubKeyHex: bytesToHex(keypair.publicKey),
      }),
    );
    expect(env.channelId).toBe(channelId);
    expect(env.hmacKey).toEqual(hmacKey);
    expect(env.hostPublicKey).toEqual(keypair.publicKey);
    expect(env.deploymentId).toBe("deployment-x");
    expect(env.definitionHash).toBe("definition-hash-abc");
    expect(env.mailboxAddress).toBe("deployment-x@example.com");
  });

  test("rejects env missing a required key", () => {
    expect(() =>
      parseSpawnTimeEnv({
        IPC_CHANNEL_ID: generateChannelId(),
        IPC_HMAC_KEY: bytesToHex(generateHmacKey()),
        HOST_PUBKEY: bytesToHex(new Uint8Array(32)),
        DEPLOYMENT_ID: "d",
        DEFINITION_HASH: "h",
      }),
    ).toThrow(/MAILBOX_ADDRESS/);
  });

  test("rejects an off-size HMAC key", () => {
    expect(() =>
      parseSpawnTimeEnv(
        makeSpawnEnv({
          channelId: generateChannelId(),
          hmacKeyHex: "deadbeef",
          hostPubKeyHex: bytesToHex(new Uint8Array(32)),
        }),
      ),
    ).toThrow(/HMAC_KEY|decode to/);
  });

  test("rejects an off-size HOST_PUBKEY", () => {
    expect(() =>
      parseSpawnTimeEnv(
        makeSpawnEnv({
          channelId: generateChannelId(),
          hmacKeyHex: bytesToHex(generateHmacKey()),
          hostPubKeyHex: "deadbeef",
        }),
      ),
    ).toThrow(/HOST_PUBKEY|decode to/);
  });

  test("rejects an off-size channelId", () => {
    expect(() =>
      parseSpawnTimeEnv(
        makeSpawnEnv({
          channelId: "short",
          hmacKeyHex: bytesToHex(generateHmacKey()),
          hostPubKeyHex: bytesToHex(new Uint8Array(32)),
        }),
      ),
    ).toThrow(/IPC_CHANNEL_ID/);
  });
});

describe("runWorkflowChild", () => {
  test("emits ready, processes a trigger.fire frame, and shuts down", async () => {
    const baseDir = await makeTempDir("child-trigger-");
    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    await seedWorkflowDefinition(baseDir, {
      kind: "workflow",
      id: "workflow-asset",
    });
    // Seed the claim-check processing entry the child reads to recover
    // the inbound message bytes for the `trigger.fire` below. The
    // supervisor's dispatch loop creates this entry via
    // `dequeueToProcessing` before forwarding the trigger.
    await seedProcessingEntry(
      baseDir,
      { kind: "workflow-run", id: "deployment-x" },
      {
        address: "deployment-x@example.com",
        messageId: "msg-1",
        receivedAt: 1,
        text: "hello from the inbox",
      },
    );

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSpawnEnv({
        channelId,
        hmacKeyHex: bytesToHex(hmacKey),
        hostPubKeyHex: bytesToHex(supervisorKeyPair.publicKey),
      }),
    );

    const bindings = buildBindings({
      baseDir,
      childKeyPair,
    });

    // Drive the supervisor side: sign trigger.fire then shutdown.
    const supervisorSender = createControlChannelSender({
      privateKeySeed: supervisorKeyPair.privateKey,
      channelId,
      writer: supervisorToChild.writer,
    });

    const runPromise = runWorkflowChild({
      env,
      controlReader: supervisorToChild.reader,
      controlWriter: childToSupervisor.writer,
      eventWriter: eventStream.writer,
      bindings,
    });

    // Wait briefly for the child to emit `ready`. The upstream
    // sender's seq starts at 1 so the supervisor's receiver iterator
    // can decode it without rejecting a seq gap.
    let readyLine: string | undefined;
    for (let i = 0; i < 200 && readyLine === undefined; i += 1) {
      const flushed = childToSupervisor.flushed();
      if (flushed.length > 0) readyLine = flushed[0];
      else await new Promise((r) => setTimeout(r, 5));
    }
    expect(readyLine).toBeDefined();

    await supervisorSender.send({
      type: "trigger.fire",
      data: {
        runId: "run-1",
        messageId: "msg-1",
        receivedAt: 1,
      },
    });
    await supervisorSender.send({
      type: "shutdown",
      data: { reason: "test done" },
    });
    supervisorToChild.close();

    const result = await runPromise;
    expect(result.triggeredRunIds).toEqual(["run-1"]);
    expect(result.finalCredentialsSnapshot).not.toBeNull();
    expect(result.finalCredentialsSnapshot?.steps).toHaveLength(1);
  });

  test("self-discovery resumes non-terminal runs and skips terminal ones", async () => {
    const baseDir = await makeTempDir("child-discover-");
    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    await seedWorkflowDefinition(baseDir, {
      kind: "workflow",
      id: "workflow-asset",
    });
    // Run "live" has RunStarted but no terminal event.
    await seedRun(
      baseDir,
      { kind: "workflow-run", id: "deployment-x" },
      "run-live",
      [
        {
          seq: 1,
          type: "RunStarted",
          at: "2026-01-01T00:00:00.000Z",
          runId: "run-live",
          definitionHash: "definition-hash-abc",
          trigger: { type: "manual", payload: null },
        },
      ],
    );
    // Run "done" has a RunCompleted terminal event.
    await seedRun(
      baseDir,
      { kind: "workflow-run", id: "deployment-x" },
      "run-done",
      [
        {
          seq: 1,
          type: "RunStarted",
          at: "2026-01-01T00:00:00.000Z",
          runId: "run-done",
          definitionHash: "definition-hash-abc",
          trigger: { type: "manual", payload: null },
        },
        {
          seq: 2,
          type: "RunCompleted",
          at: "2026-01-01T00:00:01.000Z",
        },
      ],
    );

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSpawnEnv({
        channelId,
        hmacKeyHex: bytesToHex(hmacKey),
        hostPubKeyHex: bytesToHex(supervisorKeyPair.publicKey),
      }),
    );

    const bindings = buildBindings({
      baseDir,
      childKeyPair,
    });

    const supervisorSender = createControlChannelSender({
      privateKeySeed: supervisorKeyPair.privateKey,
      channelId,
      writer: supervisorToChild.writer,
    });

    const runPromise = runWorkflowChild({
      env,
      controlReader: supervisorToChild.reader,
      controlWriter: childToSupervisor.writer,
      eventWriter: eventStream.writer,
      bindings,
    });

    await supervisorSender.send({
      type: "shutdown",
      data: { reason: "test done" },
    });
    supervisorToChild.close();

    const result = await runPromise;
    expect(result.resumedRunIds).toEqual(["run-live"]);
    expect(result.resumedRunIds).not.toContain("run-done");
  });

  test("grants-updated frame replaces the active credentialsSnapshot", async () => {
    const baseDir = await makeTempDir("child-grants-");
    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    await seedWorkflowDefinition(baseDir, {
      kind: "workflow",
      id: "workflow-asset",
    });

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSpawnEnv({
        channelId,
        hmacKeyHex: bytesToHex(hmacKey),
        hostPubKeyHex: bytesToHex(supervisorKeyPair.publicKey),
      }),
    );

    const bindings = buildBindings({
      baseDir,
      childKeyPair,
    });

    const supervisorSender = createControlChannelSender({
      privateKeySeed: supervisorKeyPair.privateKey,
      channelId,
      writer: supervisorToChild.writer,
    });

    const runPromise = runWorkflowChild({
      env,
      controlReader: supervisorToChild.reader,
      controlWriter: childToSupervisor.writer,
      eventWriter: eventStream.writer,
      bindings,
    });

    const refreshedSnapshot = {
      steps: [
        {
          stepId: "step-1",
          address: "deployment-x-step-1@example.com",
          grants: [{ resource: "thing", action: "read" }],
          contentHash: "freshhash",
        },
      ],
    };
    await supervisorSender.send({
      type: "grants-updated",
      data: {
        snapshot: refreshedSnapshot,
        stepHashes: { "step-1": "freshhash" },
      },
    });
    await supervisorSender.send({
      type: "shutdown",
      data: { reason: "test done" },
    });
    supervisorToChild.close();

    const result = await runPromise;
    expect(result.finalCredentialsSnapshot).not.toBeNull();
    expect(result.finalCredentialsSnapshot?.steps[0]?.contentHash).toBe(
      "freshhash",
    );
    expect(result.finalCredentialsSnapshot?.steps[0]?.grants).toEqual([
      { resource: "thing", action: "read" },
    ]);
  });

  test("child ready frame is signed by the child's own keypair and bootstraps the supervisor's verification key", async () => {
    const baseDir = await makeTempDir("child-ready-");
    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    await seedWorkflowDefinition(baseDir, {
      kind: "workflow",
      id: "workflow-asset",
    });

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSpawnEnv({
        channelId,
        hmacKeyHex: bytesToHex(hmacKey),
        hostPubKeyHex: bytesToHex(supervisorKeyPair.publicKey),
      }),
    );

    const bindings = buildBindings({
      baseDir,
      childKeyPair,
    });

    // Run the child, capture its ready frame, verify via the
    // receiver iterator's bootstrap mode. The receiver extracts the
    // child's public key from the first frame's payload -- the
    // supervisor's private key never enters this code path.
    const runPromise = runWorkflowChild({
      env,
      controlReader: supervisorToChild.reader,
      controlWriter: childToSupervisor.writer,
      eventWriter: eventStream.writer,
      bindings,
    });
    const crashes: string[] = [];
    const recvIter = receiveControlChannel({
      publicKey: { bootstrapFromReady: true },
      channelId,
      reader: childToSupervisor.reader,
      onCrash: (reason) => crashes.push(reason),
    });
    let readyPayload: { type: string; childPublicKey?: string } | undefined;
    for await (const payload of recvIter) {
      if (payload.type !== "ready") continue;
      readyPayload = {
        type: payload.type,
        childPublicKey: payload.data.childPublicKey,
      };
      break;
    }
    expect(readyPayload?.type).toBe("ready");
    expect(readyPayload?.childPublicKey).toBe(
      Buffer.from(childKeyPair.publicKey).toString("hex"),
    );
    expect(crashes).toHaveLength(0);

    // Tear down.
    const supervisorSender = createControlChannelSender({
      privateKeySeed: supervisorKeyPair.privateKey,
      channelId,
      writer: supervisorToChild.writer,
    });
    await supervisorSender.send({
      type: "shutdown",
      data: { reason: "test done" },
    });
    supervisorToChild.close();
    childToSupervisor.close();
    await runPromise;
  });

  test("rejects a control frame whose signature does not verify", async () => {
    const baseDir = await makeTempDir("child-bad-sig-");
    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const otherKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    await seedWorkflowDefinition(baseDir, {
      kind: "workflow",
      id: "workflow-asset",
    });

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSpawnEnv({
        channelId,
        hmacKeyHex: bytesToHex(hmacKey),
        hostPubKeyHex: bytesToHex(supervisorKeyPair.publicKey),
      }),
    );

    const bindings = buildBindings({
      baseDir,
      childKeyPair,
    });

    // Sign with the WRONG key -- the receiver iterator's signature
    // check should reject the frame and the loop should end without
    // recording a triggered run.
    const wrongSender = createControlChannelSender({
      privateKeySeed: otherKeyPair.privateKey,
      channelId,
      writer: supervisorToChild.writer,
    });

    const runPromise = runWorkflowChild({
      env,
      controlReader: supervisorToChild.reader,
      controlWriter: childToSupervisor.writer,
      eventWriter: eventStream.writer,
      bindings,
    });

    await wrongSender.send({
      type: "trigger.fire",
      data: {
        runId: "run-bad",
        messageId: "msg-bad",
        receivedAt: 1,
      },
    });
    supervisorToChild.close();

    const result = await runPromise;
    expect(result.triggeredRunIds).toEqual([]);
  });
});

describe("runWorkflowChildFromProcessEnv", () => {
  test("missing spawn-time env throws via parseSpawnTimeEnv", async () => {
    const { runWorkflowChildFromProcessEnv } = await import("./index");
    await expect(
      runWorkflowChildFromProcessEnv(
        async () => {
          throw new Error(
            "factory must not be invoked when env validation fails",
          );
        },
        { rawEnv: {} },
      ),
    ).rejects.toThrow(/spawn-time env failed validation/);
  });

  test("missing substrate-config key throws before the factory runs", async () => {
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();
    const hostKeypair = await generateKeyPair();
    const env = makeSpawnEnv({
      channelId,
      hmacKeyHex: Buffer.from(hmacKey).toString("hex"),
      hostPubKeyHex: Buffer.from(hostKeypair.publicKey).toString("hex"),
    });
    const { runWorkflowChildFromProcessEnv } = await import("./index");
    await expect(
      runWorkflowChildFromProcessEnv(
        async () => {
          throw new Error("factory must not be invoked when key is missing");
        },
        {
          rawEnv: env,
          substrateConfigKeys: ["MISSING_KEY"],
        },
      ),
    ).rejects.toThrow(/MISSING_KEY is unset/);
  });
});

describe("event channel writer wiring", () => {
  test("the event sender encodes and authenticates frames sent through it", async () => {
    // Sanity check: a frame the child's event sender would emit is
    // round-trippable through the receiver iterator. This is a
    // synthetic exercise but pinning it here guarantees the writer
    // shape `runWorkflowChild` accepts matches the IPC primitives.
    const hmacKey = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const { createEventChannelSender } = await import("../ipc/index");
    const sender = createEventChannelSender({
      hmacKey,
      channelId,
      writer: stream.writer,
    });
    await sender.send({
      type: "message.run.started",
      seq: 1,
      data: {
        messageId: "m",
        messageRunId: "r",
        receivedAt: 1,
      },
    });
    stream.close();
    const crashes: string[] = [];
    const recv = receiveEventChannel({
      hmacKey,
      channelId,
      reader: stream.reader,
      onCrash: (reason) => crashes.push(reason),
    });
    let firstType: string | undefined;
    for await (const payload of recv) {
      firstType = payload.type;
      break;
    }
    expect(firstType).toBe("message.run.started");
    expect(crashes).toHaveLength(0);
  });
});
