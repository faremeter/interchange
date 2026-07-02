// Single-step conversation-durability integration test (Phase 4.5).
//
// THE durability dividend: a warm single-step agent's multi-turn
// conversation survives a child respawn. Today (4.4) the warm agent
// holds conversation state in memory only; kill the child and the
// conversation is lost. 4.5 makes that state durable in the workflow-run
// substrate -- committed at the run boundary to a per-agent path
// (`agent-state/<stepId>/`, a bucket-sharded WAL plus a periodic
// checkpoint, sibling to the per-run event log under `runs/<runId>/...`)
// -- and restored (checkpoint load + WAL replay) when the warm agent is
// rebuilt lazily after respawn.
//
// Harness choice (in-process run-loop, mirroring 4.4's warm round-trip
// test). A respawn IS a second `runWorkflowChild` invocation against the
// SAME on-disk substrate with a FRESH (empty) warm cache and a FRESH
// durable-conversation registry. The in-process harness models exactly
// that, deterministically, and exercises the REAL sidecar wiring
// (`createSidecarStepBuildEnv` + `createDurableConversationRegistry`)
// against a real `workflow-run` substrate -- the same production path a
// spawned child takes, minus the OS process boundary whose kill timing
// would only add nondeterminism to the durability assertion.
//
// The spy agent is storage-aware: it loads its prior turns from
// `env.storage` at build (exactly as a real reactor does via
// `contextStore.load()`), appends each inbound message, and writes the
// turns back through `env.storage`. The reply echoes the running
// transcript, so a reply reflecting a PRIOR message is the load-bearing
// proof of continuity. After respawn the rebuilt spy's prior turns come
// only from the substrate restore -- there is no in-memory carry-over
// across the two `runWorkflowChild` invocations.
//
// Against the pre-4.5 behaviour (per-run/per-attempt isogit storage, no
// substrate mirror, no restore) the post-respawn reply would reflect
// ONLY the post-respawn message -- this test fails there.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import { base64Encode, hexEncode } from "@intx/types";
import {
  createDefaultDirectorRegistry,
  type Agent,
  type AgentDefinition,
  type BaseEnv,
  type SendResult,
} from "@intx/agent";
import { createSSHSignature } from "@intx/crypto";
import type {
  KeyPair,
  ConversationTurn,
  InboundMessage,
  InferenceSource,
} from "@intx/types/runtime";
import type {
  RepoId,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import { assembleMessage, assembleSignedContent } from "@intx/mime";
import {
  createControlChannelSender,
  createWorkflowStepInvoker,
  generateChannelId,
  generateHmacKey,
  parseSpawnTimeEnv,
  runWorkflowChild,
  discoverInFlightRuns,
  createWorkflowRunRepoStore,
  type ChildStepInvoker,
  type FrameReader,
  type FrameWriter,
  type NdjsonReader,
  type NdjsonWriter,
  type RunWorkflowChildBindings,
  type StepEnvBase,
} from "@intx/workflow-host";
import {
  createDurableConversationRegistry,
  reconstructDurableConversation,
} from "@intx/sidecar-app/src/conversation-state";

const DEPLOYMENT_ID = "durability-deployment";
const STEP_ID = "step-1";
const MAILBOX = "durability-deployment@example.com";
const WORKFLOW_RUN_REF = "refs/heads/main";

const STUB_SOURCE: InferenceSource = {
  id: "anthropic:stub",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-stub",
  model: "stub-model",
};

// The step-invoker forwarder reads only `.type` off each stream item.
type StreamEvent = Agent["stream"] extends () => AsyncIterable<infer E>
  ? E
  : never;

function stubStreamEvent(): StreamEvent {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub event; only `type` is read by the forwarder
  return {
    type: "inference.start",
    seq: 1,
    data: {},
  } as unknown as StreamEvent;
}

const TurnShape = type({
  role: "string",
  content: type({ type: "string", "text?": "string" }).array(),
});

/**
 * Reconstruct the durable conversation from the two-tier substrate layout
 * (checkpoint + WAL) at the per-agent `agent-state/<stepId>/` dir and
 * return the user-turn texts. Goes through the production
 * `reconstructDurableConversation` so the test reads the conversation the
 * same way the warm agent's restore does -- not by re-deriving the WAL
 * fold independently. Validating each turn at the read boundary keeps the
 * test honest about the on-disk shape without an unchecked `as`.
 */
async function readSnapshotUserTexts(
  agentStateDir: string,
  stepId: string,
): Promise<string[]> {
  const reconstructed = await reconstructDurableConversation(
    agentStateDir,
    stepId,
  );
  if (reconstructed === null) return [];
  const texts: string[] = [];
  for (const rawTurn of reconstructed.turns) {
    const turn = TurnShape(rawTurn);
    if (turn instanceof type.errors) {
      throw new Error(`reconstructed turn failed validation: ${turn.summary}`);
    }
    if (turn.role !== "user") continue;
    for (const block of turn.content) {
      texts.push(block.text ?? "");
    }
  }
  return texts;
}

function createMemoryNdjsonStream() {
  const buffer: string[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  const wake = (): void => {
    const w = waiter;
    waiter = null;
    if (w) w();
  };
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
    async write(line: string): Promise<void> {
      buffer.push(line);
      wake();
    },
  };
  return {
    reader,
    writer,
    flushed: (): readonly string[] => buffer.slice(),
    close: (): void => {
      done = true;
      wake();
    },
  };
}

function createMemoryFrameStream() {
  const buffer: Uint8Array[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  const wake = (): void => {
    const w = waiter;
    waiter = null;
    if (w) w();
  };
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
    write(bytes: Uint8Array): void {
      buffer.push(bytes);
      wake();
    },
  };
  return {
    reader,
    writer,
    close: (): void => {
      done = true;
      wake();
    },
  };
}

function assembleConversationMessage(to: string, text: string): Uint8Array {
  const signedContent = assembleSignedContent({ kind: "conversation", text });
  return assembleMessage(
    {
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
    },
    signedContent,
    new Uint8Array([0]),
  );
}

async function seedProcessingEntry(
  runRepoDir: string,
  opts: { messageId: string; receivedAt: number; text: string },
): Promise<void> {
  const dir = path.join(
    runRepoDir,
    "addresses",
    encodeURIComponent(MAILBOX),
    "processing",
  );
  await fs.mkdir(dir, { recursive: true });
  const rawMessage = assembleConversationMessage(MAILBOX, opts.text);
  const envelope = {
    messageId: opts.messageId,
    receivedAt: opts.receivedAt,
    address: MAILBOX,
    mailAuditRef: { store: "test", path: opts.messageId },
    rawMessage: base64Encode(rawMessage),
  };
  await fs.writeFile(
    path.join(dir, `${String(opts.receivedAt)}-${opts.messageId}.json`),
    JSON.stringify(envelope),
  );
}

async function seedOneStepWorkflowDir(repoDir: string): Promise<void> {
  await fs.mkdir(repoDir, { recursive: true });
  await fs.writeFile(
    path.join(repoDir, "workflow.json"),
    JSON.stringify({
      id: "durability-workflow",
      triggers: [{ type: "manual" }],
      steps: {
        [STEP_ID]: {
          kind: "step",
          id: STEP_ID,
          agent: {
            id: "warm-agent",
            systemPrompt: "warm agent",
            toolFactories: [],
            capabilities: [],
            inference: { sources: [{ provider: "anthropic", model: "stub" }] },
          },
          input: { from: "trigger.payload" },
          drainBehavior: "cancel",
        },
      },
      stepOrder: [STEP_ID],
    }),
  );
}

/**
 * Storage-aware spy agent. Mirrors a real reactor's storage contract:
 * loads prior turns from `env.storage` at build (the restore path feeds
 * these), records each inbound message as a user turn, persists the
 * turns back through `env.storage.writeTurns` + `commit`, and replies
 * with the running transcript so continuity is observable. After respawn
 * the rebuilt spy's prior turns come ONLY from the substrate restore.
 */
function buildStorageAwareSpyAgentFactory(): {
  agentFactory: <EnvReq extends BaseEnv>(
    def: AgentDefinition<EnvReq>,
    env: EnvReq,
  ) => Promise<Agent>;
  builds: () => number;
} {
  let builds = 0;
  const agentFactory = async <EnvReq extends BaseEnv>(
    _def: AgentDefinition<EnvReq>,
    env: EnvReq,
  ): Promise<Agent> => {
    builds += 1;
    const storage = env.storage;
    let endStream: () => void = () => undefined;
    const streamEnded = new Promise<void>((resolve) => {
      endStream = resolve;
    });
    // Load prior turns at build -- exactly the restore-on-build path.
    const loaded = await storage.load();
    const turns: ConversationTurn[] = [...loaded.turns];
    const agent: Agent = {
      async send(content): Promise<SendResult> {
        const text = typeof content === "string" ? content : "message";
        turns.push({
          role: "user",
          content: [{ type: "text", text }],
          timestamp: 0,
        });
        const transcript = turns
          .filter((t) => t.role === "user")
          .map((t) =>
            t.content.map((c) => (c.type === "text" ? c.text : "")).join(""),
          )
          .join("|");
        const reply = `reply:${transcript}`;
        const assistant: ConversationTurn = {
          role: "assistant",
          content: [{ type: "text", text: reply }],
          model: STUB_SOURCE.model,
          timestamp: 0,
        };
        turns.push(assistant);
        // Persist the running history so the run-boundary mirror reads
        // the latest turns from the local store, exactly as a reactor
        // checkpoint would leave them.
        await storage.writeTurns(turns);
        await storage.writeMetadata({
          pendingOperations: [],
          tokenUsage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            thinking: 0,
          },
        });
        await storage.commit({ message: "spy turn" });
        return {
          reply,
          turn: {
            role: "assistant",
            content: [{ type: "text", text: reply }],
            model: STUB_SOURCE.model,
            timestamp: 0,
          },
        };
      },
      async *stream() {
        // One inert event so the step-invoker's forwarder has something
        // to read; the loop ends only when `close()` resolves the
        // stream, mirroring the 4.4 warm-agent round-trip spy.
        yield stubStreamEvent();
        await streamEnded;
      },
      deliver(_message: InboundMessage) {
        throw new Error("spy deliver() unused");
      },
      async close() {
        endStream();
      },
      setSource() {
        throw new Error("spy setSource() unused");
      },
      setSources() {
        throw new Error("spy setSources() unused");
      },
      async history() {
        return turns;
      },
      async checkpoints() {
        return [];
      },
      async readAt() {
        return [];
      },
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; the warm spy path never reads blobReader
      blobReader: {} as Agent["blobReader"],
    };
    return agent;
  };
  return { agentFactory, builds: () => builds };
}

interface ChildHandles {
  supervisorSender: ReturnType<typeof createControlChannelSender>;
  supervisorToChild: ReturnType<typeof createMemoryNdjsonStream>;
  childToSupervisor: ReturnType<typeof createMemoryNdjsonStream>;
  runPromise: Promise<Awaited<ReturnType<typeof runWorkflowChild>>>;
  builds: () => number;
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 600; i += 1) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error("timed out waiting for condition");
}

describe("single-step conversation durability across respawn (Phase 4.5)", () => {
  test("warm agent restores its conversation from the substrate after respawn", async () => {
    const baseDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "conversation-durability-"),
    );
    const signingKey: KeyPair = await generateKeyPair();
    const supervisorKeyPair = await generateKeyPair();

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: DEPLOYMENT_ID,
    };
    const workflowDefinitionRepoId: RepoId = {
      kind: "workflow-run",
      id: "workflow-asset",
    };
    const principal: WorkflowRunWorkflowProcessPrincipal = {
      kind: "workflow-process",
      deploymentId: DEPLOYMENT_ID,
    };

    const substrate = createRepoStore({
      dataDir: baseDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: () => ({ allowed: true }),
    });

    // Genesis the workflow-run + asset repos and seed the workflow def.
    await substrate.writeTree(
      { kind: "hub" },
      workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
        message: "genesis",
      },
    );
    await substrate.writeTree(
      { kind: "hub" },
      workflowDefinitionRepoId,
      WORKFLOW_RUN_REF,
      { files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" }, message: "genesis" },
    );
    await seedOneStepWorkflowDir(
      substrate.getRepoDir(workflowDefinitionRepoId),
    );

    const runRepoDir = substrate.getRepoDir(workflowRunRepoId);
    await seedProcessingEntry(runRepoDir, {
      messageId: "msg-1",
      receivedAt: 1,
      text: "alpha",
    });
    await seedProcessingEntry(runRepoDir, {
      messageId: "msg-2",
      receivedAt: 2,
      text: "bravo",
    });
    await seedProcessingEntry(runRepoDir, {
      messageId: "msg-3",
      receivedAt: 3,
      text: "charlie",
    });

    const conversationSigner = (payload: string): Promise<string> =>
      Promise.resolve(
        createSSHSignature(
          payload,
          signingKey.privateKey,
          signingKey.publicKey,
        ),
      );

    // Build a child against the shared substrate. Each call models one
    // process lifetime: a fresh warm cache + a fresh durable-conversation
    // registry (the registry lives in the child's address space and is
    // empty on respawn -- the substrate is the durable mirror).
    //
    // `localDataDir` is the root of the child's LOCAL conversation isogit
    // store. It defaults to `baseDir` (an in-host recycle that keeps the
    // local FS), but the respawn case points it at a FRESH dir to model a
    // fresh-process / fresh-container respawn where the local store is
    // GONE and the only surviving conversation state is in the shared
    // workflow-run substrate. That makes the substrate restore the SOLE
    // possible source of continuity -- closing the hole where a surviving
    // local store would mask a broken substrate restore.
    const startChild = (
      childKeyPair: KeyPair,
      localDataDir: string,
    ): ChildHandles & {
      registry: ReturnType<typeof createDurableConversationRegistry>;
    } => {
      const channelId = generateChannelId();
      const hmacKey = generateHmacKey();
      const supervisorToChild = createMemoryNdjsonStream();
      const childToSupervisor = createMemoryNdjsonStream();
      const eventStream = createMemoryFrameStream();

      const registry = createDurableConversationRegistry({
        dataDir: localDataDir,
        workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        substrate,
        principal,
        signer: conversationSigner,
      });

      const { agentFactory, builds } = buildStorageAwareSpyAgentFactory();

      const buildEnv = async (): Promise<StepEnvBase> => {
        const store = await registry.acquire(STEP_ID);
        return {
          sources: [STUB_SOURCE],
          defaultSource: STUB_SOURCE.id,
          storage: store.storage,
          workdir: path.join(localDataDir, "workdir", channelId),
          audit: store.storage,
          directors: createDefaultDirectorRegistry(),
        };
      };

      const invokeStep: ChildStepInvoker = async (
        req,
        onEvent,
        authorize,
        warmCache,
      ) =>
        createWorkflowStepInvoker({
          workflowAuthorize: authorize,
          buildEnv,
          agentFactory,
          onEvent: (event) => onEvent(event),
          ...(warmCache !== undefined ? { warmCache } : {}),
          onRunBoundary: async (key) => {
            await registry.get(key).mirrorToSubstrate();
          },
        })(req);

      const bindings: RunWorkflowChildBindings = {
        substrate,
        workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        principal,
        workflowDefinitionRepoId,
        workflowDefinitionRef: WORKFLOW_RUN_REF,
        invokeStep,
        spawnChild: async () => ({ terminalStatus: "completed" }),
        scheduler: { scheduleIn: () => () => undefined },
        evaluateGrants: async () => ({
          effect: "allow" as const,
          matchingGrants: [],
          resolvedBy: null,
        }),
        ipcChildKeyPairFactory: () => Promise.resolve(childKeyPair),
        initialCredentialsSnapshot: {
          steps: [
            {
              stepId: STEP_ID,
              address: MAILBOX,
              grants: [],
              contentHash: "deadbeef",
            },
          ],
        },
      };

      const env = parseSpawnTimeEnv({
        IPC_CHANNEL_ID: channelId,
        IPC_HMAC_KEY: hexEncode(hmacKey),
        HOST_PUBKEY: hexEncode(supervisorKeyPair.publicKey),
        DEPLOYMENT_ID,
        DEFINITION_HASH: "definition-hash",
        MAILBOX_ADDRESS: MAILBOX,
        STEP_COUNT: "1",
        WARM_KEEP: "true",
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

      return {
        supervisorSender,
        supervisorToChild,
        childToSupervisor,
        runPromise,
        builds,
        registry,
      };
    };

    // --- Child #1: two-turn conversation (local store under baseDir) ---
    const child1 = startChild(await generateKeyPair(), baseDir);
    await waitFor(() => child1.childToSupervisor.flushed().length > 0);

    await child1.supervisorSender.send({
      type: "trigger.fire",
      data: { runId: "run-1", messageId: "msg-1", receivedAt: 1 },
    });
    await waitFor(() =>
      child1.childToSupervisor.flushed().some((l) => l.includes("run-1")),
    );

    await child1.supervisorSender.send({
      type: "trigger.fire",
      data: { runId: "run-2", messageId: "msg-2", receivedAt: 2 },
    });
    await waitFor(() =>
      child1.childToSupervisor.flushed().some((l) => l.includes("run-2")),
    );

    // The warm agent was built once across the two messages.
    expect(child1.builds()).toBe(1);

    // The conversation was COMMITTED to the substrate at the per-agent
    // path (checkpoint + WAL), sibling to the run-event log. Reconstruct it
    // straight back from the workflow-run substrate working tree.
    const agentStateDir = path.join(
      runRepoDir,
      WORKFLOW_RUN_AGENT_STATE_PREFIX,
      encodeURIComponent(STEP_ID),
    );
    const afterChild1 = await readSnapshotUserTexts(agentStateDir, STEP_ID);
    expect(afterChild1).toEqual(["alpha", "bravo"]);

    // Tear down child #1 (the respawn). Both runs reached a terminal
    // event, so the substrate carries no in-flight run.
    await child1.supervisorSender.send({
      type: "shutdown",
      data: { reason: "respawn" },
    });
    child1.supervisorToChild.close();
    await child1.runPromise;

    // Consistency of restore ordering vs discoverInFlightRuns: after a
    // clean teardown every run is terminal, so discovery finds none --
    // the conversation restore is orthogonal to run resume and does not
    // double-apply against a resumed run's event-log replay.
    const runtimeRepoStore = createWorkflowRunRepoStore({
      substrate,
      repoId: workflowRunRepoId,
      principal,
      ref: WORKFLOW_RUN_REF,
    });
    const discovered = await discoverInFlightRuns({
      substrate,
      repoId: workflowRunRepoId,
      runtimeRepoStore,
    });
    expect(discovered.map((d) => d.runId).sort()).toEqual([]);

    // --- Child #2: FRESH-CONTAINER respawn ---
    //
    // The respawn points its LOCAL conversation store at a brand-new dir,
    // modelling a recycle onto a different host (or a containerized child
    // with an ephemeral local FS): the in-host local isogit store is GONE.
    // The shared workflow-run substrate is the only surviving conversation
    // state, so any post-respawn continuity can come ONLY from the
    // substrate restore -- not from a local store masking it.
    const freshLocalDataDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "conversation-durability-respawn-"),
    );

    // The fresh local store genuinely has no prior conversation on disk.
    const freshLocalStoreDir = path.join(
      freshLocalDataDir,
      "agent-conversation-state",
      workflowRunRepoId.id,
      encodeURIComponent(STEP_ID),
    );
    expect(await dirExists(freshLocalStoreDir)).toBe(false);

    const child2 = startChild(await generateKeyPair(), freshLocalDataDir);
    await waitFor(() => child2.childToSupervisor.flushed().length > 0);

    await child2.supervisorSender.send({
      type: "trigger.fire",
      data: { runId: "run-3", messageId: "msg-3", receivedAt: 3 },
    });
    await waitFor(() =>
      child2.childToSupervisor.flushed().some((l) => l.includes("run-3")),
    );

    // The rebuilt warm agent was built fresh (cache was empty on
    // respawn) -- there is no in-memory carry-over between the two child
    // lifetimes.
    expect(child2.builds()).toBe(1);

    // THE durability dividend, now proven LOAD-BEARING: the post-respawn
    // transcript in the substrate reflects the PRE-respawn conversation
    // (alpha, bravo) plus the new message (charlie). The respawn's local
    // store started EMPTY (fresh dir), so the rebuilt agent's only
    // possible source for alpha+bravo is the substrate restore. If the
    // substrate restore were broken, the rebuilt agent would have loaded
    // zero prior turns, appended only charlie, and mirrored ["charlie"]
    // back -- overwriting the prior snapshot -- and this would read just
    // ["charlie"]. Reading the full transcript proves the substrate
    // restore reconstructed the conversation with no local-store fallback.
    const afterRespawn = await readSnapshotUserTexts(agentStateDir, STEP_ID);
    expect(afterRespawn).toEqual(["alpha", "bravo", "charlie"]);

    // The restore wrote the prior conversation into the previously-empty
    // fresh local store, confirming the restore path (not a surviving
    // local store) is what reconstructed continuity.
    expect(await dirExists(path.join(freshLocalStoreDir, ".git"))).toBe(true);

    await child2.supervisorSender.send({
      type: "shutdown",
      data: { reason: "done" },
    });
    child2.supervisorToChild.close();
    await child2.runPromise;

    await fs.rm(baseDir, { recursive: true, force: true });
    await fs.rm(freshLocalDataDir, { recursive: true, force: true });
  });
});

async function dirExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}
