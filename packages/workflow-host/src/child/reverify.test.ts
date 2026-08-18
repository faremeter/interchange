// Mutation tests for the child-side re-verify barrier.
//
// The barrier is `loadVerifiedWorkflowDefinition`: both child-side load
// sites route through it, it recomputes the wire hash over the validated
// projection, and it fails closed when the recompute does not match the
// hub-approved hash. These tests tamper with the on-disk definition AFTER
// approval and assert the barrier throws and the run does not proceed --
// per load site, on resume, and for a referenced onTrigger body -- and
// confirm the negative surface: a matching hash proceeds, and a
// model-credential rotation does not trip re-verify because credentials
// are excluded from the hashed preimage.

import { describe, test, expect, afterAll } from "bun:test";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import {
  createRepoStore,
  workflowKindHandler,
  WORKFLOW_JSON_PATH,
} from "@intx/hub-sessions";
import type { AuthorizeFn, Principal, RepoId } from "@intx/hub-sessions";
import type { RepoStore } from "@intx/hub-sessions/substrate";
import type { WorkflowDefinition } from "@intx/workflow";

import {
  createControlChannelSender,
  generateChannelId,
  generateHmacKey,
  type FrameReader,
  type FrameWriter,
  type NdjsonReader,
  type NdjsonWriter,
} from "../ipc/index";
import {
  createInMemorySpawnChild,
  createWorkflowSpawnSuspendableChild,
  type RunChildWorkflow,
  type RunSuspendableChild,
} from "../adapters/spawn-child";
import { loadVerifiedWorkflowDefinition } from "./verified-definition-loader";
import { parseSpawnTimeEnv, runWorkflowChild } from "./index";
import type { RunWorkflowChildBindings } from "./index";

const REF = "refs/heads/main";
const allowAll: AuthorizeFn = () => ({ allowed: true });

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

/**
 * A minimum-valid workflow envelope: it satisfies
 * `workflowDefinitionEnvelopeSchema`, and its one step carries an agent
 * whose model sources are already in wire form (`{ provider, model }`).
 * `suffix` lets a test build a structurally-distinct sibling that hashes
 * differently.
 */
function wireDefinition(id: string, suffix = ""): Record<string, unknown> {
  return {
    id,
    triggers: [{ type: "manual" }],
    steps: {
      main: {
        kind: "step",
        id: "main",
        agent: {
          id: `agent-${id}${suffix}`,
          systemPrompt: `main agent${suffix}`,
          capabilities: [],
          toolFactories: [],
          modelSources: [{ provider: "openai", model: "gpt-4o" }],
        },
      },
    },
    stepOrder: ["main"],
  };
}

async function writeDefinitionFile(
  filePath: string,
  obj: Record<string, unknown>,
): Promise<void> {
  await fsp.mkdir(path.dirname(filePath), { recursive: true });
  await fsp.writeFile(filePath, JSON.stringify(obj), "utf8");
}

// ---------------------------------------------------------------------------
// The barrier in isolation
// ---------------------------------------------------------------------------

describe("loadVerifiedWorkflowDefinition re-verify barrier", () => {
  test("a definition whose recomputed hash matches the approved hash loads", async () => {
    const dataDir = await makeTempDir("reverify-match-");
    const substrate = createRepoStore({
      dataDir,
      signingKey: await generateKeyPair(),
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-match" };
    const def = wireDefinition("wf-match");
    const approvedHash = await computeWireDefinitionHash(def);
    await writeDefinitionFile(
      path.join(substrate.getRepoDir(repoId), WORKFLOW_JSON_PATH),
      def,
    );

    const loaded = await loadVerifiedWorkflowDefinition({
      substrate,
      repoId,
      workflowPath: WORKFLOW_JSON_PATH,
      approvedHash,
    });
    expect(loaded.id).toBe("wf-match");
  });

  test("a post-approval tamper makes the recomputed hash diverge and the loader throws", async () => {
    const dataDir = await makeTempDir("reverify-tamper-");
    const substrate = createRepoStore({
      dataDir,
      signingKey: await generateKeyPair(),
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-tamper" };
    const approved = wireDefinition("wf-tamper");
    const approvedHash = await computeWireDefinitionHash(approved);

    // The approved bytes hash to `approvedHash`; overwrite the file with a
    // structurally-distinct (still envelope-valid) definition AFTER
    // approval. The recompute over the tampered projection no longer
    // matches, so the barrier must refuse to load it.
    const tampered = wireDefinition("wf-tamper", "-tampered");
    expect(await computeWireDefinitionHash(tampered)).not.toBe(approvedHash);
    await writeDefinitionFile(
      path.join(substrate.getRepoDir(repoId), WORKFLOW_JSON_PATH),
      tampered,
    );

    await expect(
      loadVerifiedWorkflowDefinition({
        substrate,
        repoId,
        workflowPath: WORKFLOW_JSON_PATH,
        approvedHash,
      }),
    ).rejects.toThrow(/does not match the approved hash/);
  });

  test("a model-credential rotation does not trip re-verify (credentials are excluded from the hashed preimage)", async () => {
    const dataDir = await makeTempDir("reverify-cred-");
    const substrate = createRepoStore({
      dataDir,
      signingKey: await generateKeyPair(),
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const repoId: RepoId = { kind: "workflow", id: "wf-cred" };

    // Mirror the production projector's model-source canonicalization
    // (`projectModelSource` in @intx/workflow-deploy's live-inert
    // projector): a source projects to its (provider, model) identity
    // ONLY. The credential-adjacent `parameters` bag is excluded from the
    // wire projection, and therefore from the hashed preimage the loader
    // recomputes -- so rotating it must not change the on-disk projection.
    const projectSource = (src: {
      provider: string;
      model: string;
      parameters?: unknown;
    }) => ({ provider: src.provider, model: src.model });

    const wireDefWithSource = (source: {
      provider: string;
      model: string;
    }): Record<string, unknown> => ({
      id: "wf-cred",
      triggers: [{ type: "manual" }],
      steps: {
        main: {
          kind: "step",
          id: "main",
          agent: {
            id: "agent-wf-cred",
            systemPrompt: "main agent",
            capabilities: [],
            toolFactories: [],
            modelSources: [source],
          },
        },
      },
      stepOrder: ["main"],
    });

    // Approve against the source carrying credential "key-A".
    const approvedProjection = wireDefWithSource(
      projectSource({
        provider: "openai",
        model: "gpt-4o",
        parameters: { apiKey: "key-A" },
      }),
    );
    const approvedHash = await computeWireDefinitionHash(approvedProjection);

    // Rotate the credential to "key-B". The live source changed, but its
    // wire projection -- what the deploy materializes to workflow.json --
    // is byte-identical, so the hash is unchanged.
    const rotatedProjection = wireDefWithSource(
      projectSource({
        provider: "openai",
        model: "gpt-4o",
        parameters: { apiKey: "key-B" },
      }),
    );
    expect(await computeWireDefinitionHash(rotatedProjection)).toBe(
      approvedHash,
    );

    await writeDefinitionFile(
      path.join(substrate.getRepoDir(repoId), WORKFLOW_JSON_PATH),
      rotatedProjection,
    );

    const loaded = await loadVerifiedWorkflowDefinition({
      substrate,
      repoId,
      workflowPath: WORKFLOW_JSON_PATH,
      approvedHash,
    });
    expect(loaded.id).toBe("wf-cred");
  });
});

// ---------------------------------------------------------------------------
// Site B: the spawn-child adapter's referenced-body resolution
// ---------------------------------------------------------------------------

describe("Site B spawn-child load boundary", () => {
  async function seedBody(
    substrate: ReturnType<typeof createRepoStore>,
    definitionRef: string,
    obj: Record<string, unknown>,
  ): Promise<void> {
    const repoId: RepoId = { kind: "workflow", id: definitionRef };
    await writeDefinitionFile(
      path.join(substrate.getRepoDir(repoId), WORKFLOW_JSON_PATH),
      obj,
    );
  }

  const SIDECAR_PRINCIPAL: Principal = { kind: "sidecar" };

  // The re-verify barrier lives on the onTrigger-BODY spawn path
  // (`createWorkflowSpawnSuspendableChild`), where the body's approved hash
  // is intrinsic to the parent's approval and rides the signed frame. The
  // terminal childWorkflow path (`createInMemorySpawnChild`) resolves an owned
  // inline child from the parent's own re-verified closure map, so it needs no
  // separate gate -- the parent's re-verify already covers the inline child.
  // These tests pin both halves of that contract.

  test("the terminal childWorkflow path does NOT re-verify -- it resolves the child from the in-memory closure map", async () => {
    // The inline child rides the parent's hashed projection, so the parent's
    // re-verify already covers it; the terminal resolver takes no approved-hash
    // parameter and does no separate per-child re-verify.
    const calls: Parameters<RunChildWorkflow>[0][] = [];
    const runChild: RunChildWorkflow = async (input) => {
      calls.push(input);
      return { terminalStatus: "completed" };
    };
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test fixture: the terminal resolver copies the definition by reference and hands it to the runChild stub, which reads only its id
    const childDef = wireDefinition(
      "child-nogate",
    ) as unknown as WorkflowDefinition;
    const spawn = createInMemorySpawnChild({
      bodies: new Map([["child-nogate", childDef]]),
      runChild,
    });

    const result = await spawn({
      definitionRef: "child-nogate",
      childRunId: "child-1",
      input: null,
      parentRunId: "parent-1",
      parentStepId: "step-a",
      signal: new AbortController().signal,
    });
    expect(result.terminalStatus).toBe("completed");
    expect(calls).toHaveLength(1);
    expect(calls[0]?.definition.id).toBe("child-nogate");
  });

  test("a matching per-body hash resolves and reaches the body runner", async () => {
    const dataDir = await makeTempDir("reverify-siteb-match-");
    const substrate = createRepoStore({
      dataDir,
      signingKey: await generateKeyPair(),
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const def = wireDefinition("body-ok");
    const approvedHash = await computeWireDefinitionHash(def);
    await seedBody(substrate, "body-ok", def);

    // The runner is reached only past the gate: it records the resolved
    // definition, then throws a sentinel so the test observes "gate passed,
    // runner reached with the right definition" without constructing a live
    // SuspendableChildHandle.
    const resolved: string[] = [];
    const runSuspendableChild: RunSuspendableChild = async (input) => {
      resolved.push(input.definition.id);
      throw new Error("reached-body-runner-sentinel");
    };
    const spawn = createWorkflowSpawnSuspendableChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runSuspendableChild,
      referencedDefinitionHashes: { "body-ok": approvedHash },
    });

    await expect(
      spawn(
        {
          definitionRef: "body-ok",
          childRunId: "child-1",
          input: null,
          parentRunId: "parent-1",
          parentStepId: "step-a",
          signal: new AbortController().signal,
        },
        () => undefined,
      ),
    ).rejects.toThrow(/reached-body-runner-sentinel/);
    expect(resolved).toEqual(["body-ok"]);
  });

  test("a tampered referenced body fails closed and never runs", async () => {
    const dataDir = await makeTempDir("reverify-siteb-tamper-");
    const substrate = createRepoStore({
      dataDir,
      signingKey: await generateKeyPair(),
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    const approvedHash = await computeWireDefinitionHash(
      wireDefinition("body-tampered"),
    );
    // Post-approval tamper: overwrite the body with a divergent projection.
    await seedBody(
      substrate,
      "body-tampered",
      wireDefinition("body-tampered", "-tampered"),
    );

    let runCalls = 0;
    const runSuspendableChild: RunSuspendableChild = async () => {
      runCalls += 1;
      throw new Error("must not be reached");
    };
    const spawn = createWorkflowSpawnSuspendableChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runSuspendableChild,
      referencedDefinitionHashes: { "body-tampered": approvedHash },
    });

    await expect(
      spawn(
        {
          definitionRef: "body-tampered",
          childRunId: "child-2",
          input: null,
          parentRunId: "parent-2",
          parentStepId: "step-a",
          signal: new AbortController().signal,
        },
        () => undefined,
      ),
    ).rejects.toThrow(/does not match the approved hash/);
    expect(runCalls).toBe(0);
  });

  test("a referenced body with no frame-carried hash fails closed", async () => {
    const dataDir = await makeTempDir("reverify-siteb-missing-");
    const substrate = createRepoStore({
      dataDir,
      signingKey: await generateKeyPair(),
      handlers: { workflow: workflowKindHandler },
      authorize: allowAll,
    });
    await seedBody(substrate, "body-unlisted", wireDefinition("body-unlisted"));

    let runCalls = 0;
    const runSuspendableChild: RunSuspendableChild = async () => {
      runCalls += 1;
      throw new Error("must not be reached");
    };
    const spawn = createWorkflowSpawnSuspendableChild({
      substrate,
      principal: SIDECAR_PRINCIPAL,
      deployRef: REF,
      runSuspendableChild,
      referencedDefinitionHashes: {},
    });

    await expect(
      spawn(
        {
          definitionRef: "body-unlisted",
          childRunId: "child-3",
          input: null,
          parentRunId: "parent-3",
          parentStepId: "step-a",
          signal: new AbortController().signal,
        },
        () => undefined,
      ),
    ).rejects.toThrow(/no hub-approved wire hash/);
    expect(runCalls).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Site A: the top-level run child, including resume
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
  const reader: FrameReader = {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        for (;;) {
          const next = buffer.shift();
          if (next === undefined) {
            await new Promise<void>((r) => setTimeout(r, 5));
            continue;
          }
          yield next;
        }
      })();
    },
  };
  const writer: FrameWriter = {
    write(bytes: Uint8Array) {
      buffer.push(bytes);
    },
  };
  return { reader, writer };
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
    if (
      cause !== null &&
      typeof cause === "object" &&
      "code" in cause &&
      cause.code === "ENOENT"
    ) {
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

/**
 * A working-tree-backed stub `RepoStore`: `getRepoDir` composes a flat
 * path, run-event writes land on disk, and committed reads back onto those
 * same files so self-discovery observes seeded and resumed runs.
 */
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
        async treeOid() {
          return null;
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

function buildBindings(baseDir: string): RunWorkflowChildBindings {
  return {
    substrate: createStubRepoStore(baseDir),
    workflowRunRepoId: { kind: "workflow-run", id: "deployment-x" },
    workflowRunRef: REF,
    principal: { kind: "supervisor" },
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

function makeRawEnv(opts: {
  channelId: string;
  hmacKeyHex: string;
  hostPubKeyHex: string;
  definitionHash: string;
}): Record<string, string> {
  return {
    IPC_CHANNEL_ID: opts.channelId,
    IPC_HMAC_KEY: opts.hmacKeyHex,
    HOST_PUBKEY: opts.hostPubKeyHex,
    DEPLOYMENT_ID: "deployment-x",
    DEFINITION_HASH: opts.definitionHash,
    MAILBOX_ADDRESS: "deployment-x@example.com",
    STEP_COUNT: "1",
  };
}

/** Write the workflow-asset `workflow.json` the child boot-loads. */
async function seedAssetDefinition(
  baseDir: string,
  obj: Record<string, unknown>,
): Promise<void> {
  await writeDefinitionFile(
    path.join(baseDir, "workflow", "workflow-asset", WORKFLOW_JSON_PATH),
    obj,
  );
}

/** Seed a non-terminal run so self-discovery has something to resume. */
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
      // The RunStarted event's own definitionHash is deliberately unrelated
      // to the barrier: re-verify never consults it (the wrong-layer trap).
      definitionHash: "unrelated-run-started-hash",
      trigger: { type: "manual", payload: null },
    }),
  );
}

async function waitForFirstFrame(
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>,
): Promise<string | undefined> {
  for (let i = 0; i < 200; i += 1) {
    const flushed = childToSupervisor.flushed();
    if (flushed.length > 0) return flushed[0];
    await new Promise((r) => setTimeout(r, 5));
  }
  return undefined;
}

describe("Site A run-child load boundary", () => {
  test("a fresh run boots past the barrier when the recomputed hash matches", async () => {
    const baseDir = await makeTempDir("reverify-sitea-fresh-ok-");
    const def = wireDefinition("asset-ok");
    await seedAssetDefinition(baseDir, def);
    const approvedHash = await computeWireDefinitionHash(def);

    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeRawEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedHash,
      }),
    );
    const bindings: RunWorkflowChildBindings = {
      ...buildBindings(baseDir),
      ipcChildKeyPairFactory: () => Promise.resolve(childKeyPair),
    };

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
    expect(readyLine).toContain("ready");

    await supervisorSender.send({
      type: "shutdown",
      data: { reason: "test done" },
    });
    supervisorToChild.close();
    const result = await runPromise;
    expect(result.resumedRunIds).toEqual([]);
  });

  test("a fresh run fails closed and never emits ready when the definition is tampered", async () => {
    const baseDir = await makeTempDir("reverify-sitea-fresh-tamper-");
    // Approve against the untampered projection, then seed a tampered one.
    const approvedHash = await computeWireDefinitionHash(
      wireDefinition("asset-tampered"),
    );
    await seedAssetDefinition(
      baseDir,
      wireDefinition("asset-tampered", "-tampered"),
    );

    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeRawEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedHash,
      }),
    );
    const bindings: RunWorkflowChildBindings = {
      ...buildBindings(baseDir),
      ipcChildKeyPairFactory: () => Promise.resolve(childKeyPair),
    };

    await expect(
      runWorkflowChild({
        env,
        controlReader: supervisorToChild.reader,
        controlWriter: childToSupervisor.writer,
        eventWriter: eventStream.writer,
        bindings,
      }),
    ).rejects.toThrow(/does not match the approved hash/);

    // The barrier fires before the child announces readiness, so the run
    // never proceeds: no upstream frame was emitted.
    expect(childToSupervisor.flushed()).toHaveLength(0);
    supervisorToChild.close();
  });

  test("a resumed run still fails closed on a tampered definition and never resumes", async () => {
    const baseDir = await makeTempDir("reverify-sitea-resume-tamper-");
    // A non-terminal run is on disk waiting to be resumed.
    await seedNonTerminalRun(baseDir, "run-live");
    // Approve against the untampered projection, then tamper the asset.
    const approvedHash = await computeWireDefinitionHash(
      wireDefinition("asset-resume"),
    );
    await seedAssetDefinition(
      baseDir,
      wireDefinition("asset-resume", "-tampered"),
    );

    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeRawEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedHash,
      }),
    );
    const bindings: RunWorkflowChildBindings = {
      ...buildBindings(baseDir),
      ipcChildKeyPairFactory: () => Promise.resolve(childKeyPair),
    };

    // The load boundary sits before self-discovery, so the tampered
    // definition halts the child at boot and the waiting run is never
    // resumed. This is why the barrier lives at load, not at run start
    // (a resumed run's RunStarted never re-fires).
    await expect(
      runWorkflowChild({
        env,
        controlReader: supervisorToChild.reader,
        controlWriter: childToSupervisor.writer,
        eventWriter: eventStream.writer,
        bindings,
      }),
    ).rejects.toThrow(/does not match the approved hash/);
    expect(childToSupervisor.flushed()).toHaveLength(0);
    supervisorToChild.close();
  });

  test("a resumed run proceeds when the definition matches the approved hash", async () => {
    const baseDir = await makeTempDir("reverify-sitea-resume-ok-");
    await seedNonTerminalRun(baseDir, "run-live");
    const def = wireDefinition("asset-resume-ok");
    await seedAssetDefinition(baseDir, def);
    const approvedHash = await computeWireDefinitionHash(def);

    const supervisorKeyPair = await generateKeyPair();
    const childKeyPair = await generateKeyPair();
    const channelId = generateChannelId();
    const hmacKey = generateHmacKey();

    const supervisorToChild = createMemoryNdjsonStream();
    const childToSupervisor = createMemoryNdjsonStream();
    const eventStream = createMemoryFrameStream();

    const env = parseSpawnTimeEnv(
      makeRawEnv({
        channelId,
        hmacKeyHex: hexEncode(hmacKey),
        hostPubKeyHex: hexEncode(supervisorKeyPair.publicKey),
        definitionHash: approvedHash,
      }),
    );
    const bindings: RunWorkflowChildBindings = {
      ...buildBindings(baseDir),
      ipcChildKeyPairFactory: () => Promise.resolve(childKeyPair),
    };

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
  });
});
