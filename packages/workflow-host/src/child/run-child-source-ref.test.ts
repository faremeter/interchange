// Source-ref lineage tests for the top-level run child.
//
// A source-ref deployment's `workflow.json` is a non-executable approval
// surface: its agents carry `modelSources`/no `inference` and its tool
// factories are plain data, so the runtime cannot execute it. The run child
// must instead EVALUATE the pinned code closure to a LIVE definition and run
// THAT, re-verifying the evaluated definition by project-then-hash against the
// hub-approved wire hash. These tests materialize a fixture closure, drive the
// run-child load boundary in source-ref mode, and assert: a matching hash
// evaluates the closure and runs to a terminal result (fresh AND resumed),
// and a divergent closure fails closed before the run proceeds.

import { describe, test, expect, afterAll } from "bun:test";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { base64Encode, hexEncode } from "@intx/types";
import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";
import {
  assembleMessage,
  assembleSignedContent,
  type MessageHeaders,
} from "@intx/mime";
import {
  loadWorkflowDefinitionFromClosure,
  type RunWorkflowChildBindings,
} from "@intx/workflow-host";
import { computeLiveDefinitionHash } from "@intx/workflow";

import {
  createControlChannelSender,
  generateChannelId,
  generateHmacKey,
  receiveControlChannel,
  type FrameReader,
  type FrameWriter,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";
import { parseSpawnTimeEnv, runWorkflowChild } from "./index";

const REF = "refs/heads/main";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fsp.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fsp.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

// ---------------------------------------------------------------------------
// Fixture closure: a package directory whose `interchange.workflow` entry
// evaluates to a LIVE definition (an agent step carrying `inference`, the
// shape the runtime executes -- distinct from the inert `modelSources`
// projection the approval surface holds).
// ---------------------------------------------------------------------------

function liveFixtureModule(id: string, suffix = ""): string {
  const def = {
    id,
    triggers: [{ type: "manual" }],
    steps: {
      main: {
        kind: "step",
        id: "main",
        agent: {
          id: `agent-${id}${suffix}`,
          systemPrompt: `fixture agent${suffix}`,
          capabilities: [],
          toolFactories: [],
          inference: { sources: [{ provider: "openai", model: "gpt-4o" }] },
        },
        input: { from: "trigger.payload" },
      },
    },
    stepOrder: ["main"],
  };
  return `export const workflow = ${JSON.stringify(def)};\n`;
}

// A fixture whose only step is an onTrigger section carrying an INLINE body,
// so the source-ref child's post-verify rewrite extracts a non-empty bodies
// map -- the shape that drives the in-memory suspendable-child resolver.
function onTriggerFixtureModule(id: string): string {
  const def = {
    id,
    triggers: [{ type: "manual" }],
    steps: {
      sect: {
        kind: "onTrigger",
        id: "sect",
        on: { type: "mail", to: `${id}@example.com` },
        body: {
          inline: {
            id: "body-inner",
            triggers: [{ type: "manual" }],
            steps: { w: { kind: "sleep", id: "w", durationMs: 5 } },
            stepOrder: ["w"],
          },
        },
      },
    },
    stepOrder: ["sect"],
  };
  return `export const workflow = ${JSON.stringify(def)};\n`;
}

/**
 * Materialize a fixture workflow-definition closure on disk and return its
 * package dir plus the hub-approved wire hash of the live definition it
 * evaluates to (project-then-hash), the value a real hub would have approved.
 */
async function materializeFixtureClosure(
  prefix: string,
  id: string,
  suffix = "",
  moduleSource?: string,
): Promise<{ packageDir: string; approvedHash: string }> {
  const packageDir = await makeTempDir(prefix);
  await fsp.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: `@fixture/${id}`,
      version: "1.0.0",
      interchange: { workflow: "workflow.mjs" },
    }),
    "utf8",
  );
  await fsp.writeFile(
    path.join(packageDir, "workflow.mjs"),
    moduleSource ?? liveFixtureModule(id, suffix),
    "utf8",
  );
  const live = await loadWorkflowDefinitionFromClosure({ packageDir });
  const approvedHash = await computeLiveDefinitionHash(live);
  return { packageDir, approvedHash };
}

// ---------------------------------------------------------------------------
// Minimal in-memory IPC + substrate harness (mirrors run-child.test.ts).
// ---------------------------------------------------------------------------

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
            if (next === undefined) throw new Error("buffer shift undefined");
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
    flushed: (): readonly string[] => buffer.slice(),
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
            if (next === undefined) throw new Error("frame shift undefined");
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
    close() {
      done = true;
      wake();
    },
  };
}

async function readPrefixEntries(
  repoDir: string,
  prefix: string,
): Promise<Map<string, Uint8Array>> {
  const entries = new Map<string, Uint8Array>();
  const prefixDir = path.join(repoDir, prefix);
  let names: string[];
  try {
    names = await fsp.readdir(prefixDir);
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") {
      return entries;
    }
    throw cause;
  }
  for (const name of names) {
    const full = path.join(prefixDir, name);
    if (!(await fsp.stat(full)).isFile()) continue;
    entries.set(`${prefix}${name}`, await fsp.readFile(full));
  }
  return entries;
}

function createStubRepoStore(baseDir: string): RepoStore {
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(baseDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_principal, repoId, _ref, args) {
      const repoDir = path.join(baseDir, repoId.kind, repoId.id);
      const existing = await readPrefixEntries(repoDir, args.preservePrefix);
      const merged = await args.merge(existing);
      await fsp.rm(path.join(repoDir, args.preservePrefix), {
        recursive: true,
        force: true,
      });
      for (const [relPath, content] of Object.entries(merged)) {
        const full = path.join(repoDir, relPath);
        await fsp.mkdir(path.dirname(full), { recursive: true });
        await fsp.writeFile(full, content);
      }
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
    },
    async openCommittedReads(_principal, repoId, _ref) {
      const repoDir = path.join(baseDir, repoId.kind, repoId.id);
      return {
        async listDir(relPath: string) {
          let dirents;
          try {
            dirents = await fsp.readdir(path.join(repoDir, relPath), {
              withFileTypes: true,
            });
          } catch (cause) {
            if (
              cause instanceof Error &&
              "code" in cause &&
              cause.code === "ENOENT"
            ) {
              return [];
            }
            throw cause;
          }
          return dirents.map((d) => ({
            name: d.name,
            oid: path.join(relPath, d.name),
            type: d.isDirectory() ? "tree" : "blob",
          }));
        },
        async readBlobByOid(oid: string) {
          return fsp.readFile(path.join(repoDir, oid));
        },
      };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; unimplemented methods surface a precise failure via the proxy
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
  await fsp.mkdir(dir, { recursive: true });
  const envelope = {
    messageId: opts.messageId,
    receivedAt: opts.receivedAt,
    address: opts.address,
    mailAuditRef: { store: "test", path: opts.messageId },
    rawMessage: base64Encode(
      assembleConversationMessage(opts.address, opts.text),
    ),
  };
  await fsp.writeFile(
    path.join(dir, `${String(opts.receivedAt)}-${opts.messageId}.json`),
    JSON.stringify(envelope),
  );
}

async function seedNonTerminalRun(
  baseDir: string,
  runId: string,
): Promise<void> {
  const dir = path.join(
    baseDir,
    "workflow-run",
    "deployment-x",
    "runs",
    runId,
    "events",
  );
  await fsp.mkdir(dir, { recursive: true });
  await fsp.writeFile(
    path.join(dir, "1.json"),
    JSON.stringify({
      seq: 1,
      type: "RunStarted",
      at: "2026-01-01T00:00:00.000Z",
      runId,
      definitionHash: "unrelated-run-started-hash",
      trigger: { type: "manual", payload: null },
    }),
  );
}

function buildBindings(baseDir: string): RunWorkflowChildBindings {
  return {
    substrate: createStubRepoStore(baseDir),
    workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
    workflowRunRef: REF,
    principal: { kind: "supervisor" } as Principal,
    // The source-ref child never reads the workflow-asset repo; these are
    // present because the bindings shape requires them and the live-authored
    // arm consumes them.
    workflowDefinitionRepoId: { kind: "workflow", id: "workflow-asset" },
    workflowDefinitionRef: REF,
    invokeStep: async () => ({ output: null }),
    spawnChild: async () => ({ terminalStatus: "completed" }),
    scheduler: { scheduleIn: () => () => undefined },
    evaluateGrants: async () => ({
      effect: "allow" as const,
      matchingGrants: [],
      resolvedBy: null,
    }),
    initialCredentialsSnapshot: {
      steps: [
        {
          stepId: "main",
          address: "deployment-x-main@example.com",
          grants: [],
          contentHash: "deadbeef",
        },
      ],
    },
  };
}

function makeSourceRefEnv(opts: {
  channelId: string;
  hmacKeyHex: string;
  hostPubKeyHex: string;
  definitionHash: string;
  closurePackageDir: string;
}): Record<string, string> {
  return {
    IPC_CHANNEL_ID: opts.channelId,
    IPC_HMAC_KEY: opts.hmacKeyHex,
    HOST_PUBKEY: opts.hostPubKeyHex,
    DEPLOYMENT_ID: "deployment-x",
    DEFINITION_HASH: opts.definitionHash,
    MAILBOX_ADDRESS: "deployment-x@example.com",
    STEP_COUNT: "1",
    WORKFLOW_LINEAGE: "source-ref",
    CLOSURE_PACKAGE_DIR: opts.closurePackageDir,
  };
}

async function waitForFirstFrame(
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>,
): Promise<string | undefined> {
  for (let i = 0; i < 400; i += 1) {
    const flushed = childToSupervisor.flushed();
    if (flushed.length > 0) return flushed[0];
    await new Promise((r) => setTimeout(r, 5));
  }
  return undefined;
}

describe("source-ref run child", () => {
  test("parses a source-ref spawn env into the lineage + closurePackageDir", () => {
    const env = parseSpawnTimeEnv(
      makeSourceRefEnv({
        channelId: generateChannelId(),
        hmacKeyHex: hexEncode(generateHmacKey()),
        hostPubKeyHex: hexEncode(new Uint8Array(32)),
        definitionHash: "abc",
        closurePackageDir: "/tmp/closure",
      }),
    );
    expect(env.lineage).toBe("source-ref");
    expect(env.closurePackageDir).toBe("/tmp/closure");
  });

  test("a live-authored env (no marker) parses as live-authored with no closure dir", () => {
    const env = parseSpawnTimeEnv({
      IPC_CHANNEL_ID: generateChannelId(),
      IPC_HMAC_KEY: hexEncode(generateHmacKey()),
      HOST_PUBKEY: hexEncode(new Uint8Array(32)),
      DEPLOYMENT_ID: "deployment-x",
      DEFINITION_HASH: "abc",
      MAILBOX_ADDRESS: "deployment-x@example.com",
      STEP_COUNT: "1",
    });
    expect(env.lineage).toBe("live-authored");
    expect(env.closurePackageDir).toBeUndefined();
  });

  test("a source-ref marker with no closure dir fails closed at parse", () => {
    expect(() =>
      parseSpawnTimeEnv({
        IPC_CHANNEL_ID: generateChannelId(),
        IPC_HMAC_KEY: hexEncode(generateHmacKey()),
        HOST_PUBKEY: hexEncode(new Uint8Array(32)),
        DEPLOYMENT_ID: "deployment-x",
        DEFINITION_HASH: "abc",
        MAILBOX_ADDRESS: "deployment-x@example.com",
        STEP_COUNT: "1",
        WORKFLOW_LINEAGE: "source-ref",
      }),
    ).toThrow(/requires CLOSURE_PACKAGE_DIR/);
  });

  test("evaluates the closure to a live definition, re-verifies a matching hash, and runs a fresh trigger to terminal", async () => {
    const baseDir = await makeTempDir("srcref-fresh-ok-base-");
    // The workflow runtime's commit chain and pending-event buffer are
    // module-scoped maps keyed by runId (see `@intx/workflow`'s
    // commit-chain), shared across every run in the process. Production
    // hands each run a unique hub-assigned runId, so the keys never
    // collide; a test that hardcodes a shared literal like "run-1" does
    // collide with sibling tests that reuse the same literal and leave a
    // buffered RunStarted behind (a run they never drove to terminal, so
    // its chain is never dropped). Inheriting that stale buffer makes
    // this fresh run's state read as already-started and stall with no
    // schedulable primitives. A per-test unique runId keeps this run's
    // chain isolated regardless of sibling ordering.
    const runId = `srcref-fresh-run-${generateChannelId()}`;
    const { packageDir, approvedHash } = await materializeFixtureClosure(
      "srcref-fresh-ok-pkg-",
      "srcref-fresh",
    );
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

    const supervisorKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSourceRefEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedHash,
        closurePackageDir: packageDir,
      }),
    );
    const bindings = buildBindings(baseDir);

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

    // Decode the child's upstream frames so the test observes the run reach
    // TERMINAL. Reaching a terminal proves the LIVE definition ran: the
    // runtime's `RunStarted` hashes over `agent.inference`, which the inert
    // approval projection lacks, so an inert definition could not run here.
    const recvIter = receiveControlChannel({
      publicKey: { bootstrapFromReady: true },
      channelId,
      reader: childToSupervisor.reader,
      onCrash: (reason) => {
        throw new Error(`unexpected control channel crash: ${reason}`);
      },
    });

    await supervisorSender.send({
      type: "trigger.fire",
      data: { runId, messageId: "msg-1", receivedAt: 1 },
    });

    let terminalSeq: number | null = null;
    for await (const payload of recvIter) {
      if (payload.type === "terminal.event" && payload.data.runId === runId) {
        terminalSeq = payload.data.seq;
        break;
      }
    }
    expect(terminalSeq).not.toBeNull();
    expect(terminalSeq).toBeGreaterThan(0);

    await supervisorSender.send({
      type: "shutdown",
      data: { reason: "test done" },
    });
    supervisorToChild.close();
    const result = await runPromise;
    expect(result.triggeredRunIds).toEqual([runId]);
    // Evaluating the closure and driving a run to terminal is real work; the
    // default per-test timeout is too tight for it under full-suite load.
  }, 30000);

  test("a divergent closure hash fails closed and never emits ready", async () => {
    const baseDir = await makeTempDir("srcref-divergent-base-");
    // Approve against fixture A's projection, then point the child at fixture
    // B, whose evaluated projection hashes differently. The re-verify must
    // refuse to run it.
    const approvedFixture = await materializeFixtureClosure(
      "srcref-divergent-approved-",
      "srcref-approved",
    );
    const divergentFixture = await materializeFixtureClosure(
      "srcref-divergent-actual-",
      "srcref-actual",
      "-divergent",
    );
    expect(divergentFixture.approvedHash).not.toBe(
      approvedFixture.approvedHash,
    );

    const supervisorKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSourceRefEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedFixture.approvedHash,
        closurePackageDir: divergentFixture.packageDir,
      }),
    );
    const bindings = buildBindings(baseDir);

    await expect(
      runWorkflowChild({
        env,
        controlReader: supervisorToChild.reader,
        controlWriter: childToSupervisor.writer,
        eventWriter: eventStream.writer,
        bindings,
      }),
    ).rejects.toThrow(/does not match the approved hash/);

    // The barrier fires before the child announces readiness: no upstream
    // frame was emitted, so the run never proceeded.
    expect(childToSupervisor.flushed()).toHaveLength(0);
    supervisorToChild.close();
  }, 30000);

  test("source-ref onTrigger bodies with no executor binding fail closed at startup", async () => {
    const baseDir = await makeTempDir("srcref-ontrigger-noexec-base-");
    // A closure whose onTrigger body the child extracts to a non-empty bodies
    // map. `buildBindings` wires no `runSuspendableChild` executor, so the
    // child cannot build the in-memory body resolver -- proving the source-ref
    // arm SELECTS the in-memory path (it detects bodies and requires the
    // executor) rather than silently falling back to a disk read.
    const { packageDir, approvedHash } = await materializeFixtureClosure(
      "srcref-ontrigger-noexec-pkg-",
      "srcref-ot-noexec",
      "",
      onTriggerFixtureModule("srcref-ot-noexec"),
    );

    const supervisorKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSourceRefEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedHash,
        closurePackageDir: packageDir,
      }),
    );
    const bindings = buildBindings(baseDir);

    await expect(
      runWorkflowChild({
        env,
        controlReader: supervisorToChild.reader,
        controlWriter: childToSupervisor.writer,
        eventWriter: eventStream.writer,
        bindings,
      }),
    ).rejects.toThrow(
      /carries onTrigger bodies but the host wired no runSuspendableChild/,
    );

    // The check fires at startup, before the child announces readiness.
    expect(childToSupervisor.flushed()).toHaveLength(0);
    supervisorToChild.close();
  }, 30000);

  test("a matching hash resumes an in-flight run from the evaluated live definition", async () => {
    const baseDir = await makeTempDir("srcref-resume-ok-base-");
    await seedNonTerminalRun(baseDir, "run-live");
    const { packageDir, approvedHash } = await materializeFixtureClosure(
      "srcref-resume-ok-pkg-",
      "srcref-resume",
    );

    const supervisorKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeSourceRefEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedHash,
        closurePackageDir: packageDir,
      }),
    );
    const bindings = buildBindings(baseDir);

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

    const readyLine = await waitForFirstFrame(childToSupervisor);
    expect(readyLine).toBeDefined();

    await supervisorSender.send({
      type: "shutdown",
      data: { reason: "test done" },
    });
    supervisorToChild.close();
    const result = await runPromise;
    expect(result.resumedRunIds).toEqual(["run-live"]);
  }, 30000);
});
