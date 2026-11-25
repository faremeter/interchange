// Per-step scratch keying for `createSidecarStepBuildEnv`.
//
// These tests pin the keying directly off the production `buildEnv` the
// substrate factory wires: two different runIds produce DIFFERENT
// `env.workdir`s, each under that run's subtree -- the per-run keying the
// run-completion cleanup reclaims. A workspace lives for one attempt: a
// retry does not see the files of the attempt before it.
//
// The build also backs each invocation's `ContextStore` with the step's
// committed state in the workflow-run substrate, under the deployment's
// top-level run at the key of the step's place: it restores that state on a
// fresh host, the build's conversation commits the turn, and another
// invocation (a retried attempt, or a later run of the same place) continues
// the conversation the last invocation committed.

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  test,
  expect,
} from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type {
  BlobReader,
  ConversationTurn,
  InferenceSource,
  Mail,
  OutboundMessage,
  PendingOperation,
  ToolCall,
} from "@intx/types/runtime";
import type { MailSigner } from "@intx/types/mail-signer";
import type { RuntimeCapabilities } from "@intx/types/runtime-capabilities";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { deriveWorkflowRunId } from "@intx/types";
import { generateKeyPair } from "@intx/crypto";
import { createConnectorRouter } from "@intx/harness";
import { createInboundMessage } from "@intx/mime";
import {
  buildStepStateSeeds,
  createRepoStore,
  readStepStateSnapshot,
  workflowRunKindHandler,
  workflowRunRepoIdForAddress,
  workflowRunStepSeedPath,
  workflowRunStepStatePrefix,
  writeStepStateSeeds,
  WORKFLOW_RUN_GITIGNORE_PATH,
  type RepoId,
  type RepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import { createBuiltinRegistry } from "@intx/inference/providers";
import {
  createDefaultDirectorRegistry,
  type Agent,
  type BaseEnv,
  type SendResult,
} from "@intx/agent";
import {
  createDurableConversationStore,
  createWorkflowStepInvoker,
  readStepState,
  type ChildOutboundMailBridge,
  type StepSources,
  type StepBuild,
} from "@intx/workflow-host";
import {
  commitUnstampedStepState,
  createLogCapture,
} from "@intx/workflow-host/testing";
import { enumerateInertBodies } from "@intx/workflow-deploy";
import {
  scopedStepId,
  StepOutcomeCommitError,
  type StepInvokeRequest,
  unavailableMailSigner,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

import {
  createSidecarStepBuildEnv,
  stepWorkspaceDir,
  type SidecarStepBuildEnvDeps,
} from "./substrate-factory";
import { createMailboxWatchRegistry } from "./mailbox-watch-registry";
import type { SupervisorBackedTransportInbound } from "./supervisor-backed-transport";

const STEP_ID = "step-1";
// The run the deployment mailbox names, under which every step's state lives.
const TOP_LEVEL_RUN_ID = "run_deployment-keying";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: "deployment-keying",
};
const WORKFLOW_RUN_REF = "refs/heads/main";
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: WORKFLOW_RUN_REPO_ID.id,
};
const ZERO_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};
const signer = (payload: string): Promise<string> =>
  Promise.resolve(`sig:${String(payload.length)}`);

/** A workflow-run substrate with the production kind handler validating every write. */
async function createSubstrate(dataDir: string): Promise<RepoStore> {
  const substrate = createRepoStore({
    dataDir: path.join(dataDir, "substrate"),
    signingKey: await generateKeyPair(),
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: () => ({ allowed: true }),
  });
  await substrate.writeTree(
    { kind: "hub" },
    WORKFLOW_RUN_REPO_ID,
    WORKFLOW_RUN_REF,
    { files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" }, message: "genesis" },
  );
  return substrate;
}

const SOURCE: InferenceSource = {
  id: STEP_ID,
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-keying",
  model: "claude-keying",
};

function stubOutboundMailBridge(): ChildOutboundMailBridge {
  return {
    submit: () => Promise.reject(new Error("unused in buildEnv keying test")),
    handleResult: () => undefined,
    cancelAll: () => undefined,
    pendingCount: 0,
  };
}

// buildEnv only hands the inbound surface to the transport; no test here
// calls a mailbox method.
function stubInbound(): SupervisorBackedTransportInbound {
  const unused = () => Promise.reject(new Error("unused in buildEnv test"));
  return {
    watchRegistry: createMailboxWatchRegistry(),
    mutationBridge: {
      submit: unused,
      handleResult: () => undefined,
      cancelAll: () => undefined,
      pendingCount: 0,
    },
    callBridge: {
      submit: unused,
      handleResult: () => undefined,
      cancelAll: () => undefined,
      pendingCount: 0,
    },
  };
}

function buildDeps(opts: {
  dataDir: string;
  substrate: RepoStore;
  mailSigner?: MailSigner;
  outboundMailBridge?: ChildOutboundMailBridge;
  signer?: (payload: string) => Promise<string>;
}): SidecarStepBuildEnvDeps {
  return {
    dataDir: opts.dataDir,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    substrate: opts.substrate,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    signer: opts.signer ?? signer,
    topLevelRunId: TOP_LEVEL_RUN_ID,
    mailboxAddress: `${TOP_LEVEL_RUN_ID}@example.com`,
    outboundMailBridge: opts.outboundMailBridge ?? stubOutboundMailBridge(),
    adapters: createBuiltinRegistry(),
    closurePackageDir: "/unused-closure",
    directors: createDefaultDirectorRegistry(),
    mailSigner: opts.mailSigner ?? unavailableMailSigner,
    inbound: stubInbound(),
  };
}

function stepSourcesFor(chain: InferenceSource[] = [SOURCE]): StepSources {
  return { [STEP_ID]: chain };
}

function requestForRun(runId: string, attempt = 1): StepInvokeRequest {
  return {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- buildEnv reads only authzContext; the agent definition is never consulted here
    agent: { toolFactories: [] } as unknown as StepInvokeRequest["agent"],
    input: null,
    replyByMail: false,
    authzContext: { stepId: STEP_ID, runId, attempt },
    place: { stepPath: [STEP_ID] },
    signal: new AbortController().signal,
  };
}

const tempDirs: string[] = [];

/** Count the WAL entries of the state directory at `stateDir` on disk. */
async function countWalEntries(stateDir: string): Promise<number> {
  const walRoot = path.join(stateDir, "wal");
  let buckets: string[];
  try {
    buckets = await fs.readdir(walRoot);
  } catch {
    return 0;
  }
  let count = 0;
  for (const bucket of buckets) {
    const entries = await fs.readdir(path.join(walRoot, bucket));
    count += entries.filter((entry) => entry.endsWith(".json")).length;
  }
  return count;
}

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

async function makeTempDir(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "sidecar-step-keying-"));
  tempDirs.push(dir);
  return dir;
}

describe("createSidecarStepBuildEnv per-step scratch keying", () => {
  test("keys a distinct per-run workdir under that run's subtree", async () => {
    const dataDir = await makeTempDir();
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate: await createSubstrate(dataDir) }),
    );
    const stepSources = stepSourcesFor();

    const { env: env1 } = await buildEnv(requestForRun("run-1"), stepSources);
    const { env: env2 } = await buildEnv(requestForRun("run-2"), stepSources);

    // Per-run keying: each run gets its own workdir, rooted under that
    // run's `runs/<runId>/` subtree -- exactly what the run-completion
    // cleanup reclaims at run granularity.
    expect(env2.workdir).not.toBe(env1.workdir);
    expect(env1.workdir).toContain(
      path.join(
        "workflow-step-state",
        WORKFLOW_RUN_REPO_ID.id,
        "runs",
        "run-1",
      ),
    );
    expect(env2.workdir).toContain(path.join("runs", "run-2"));
  });

  test("a retried attempt does not see the files of the attempt before it", async () => {
    const dataDir = await makeTempDir();
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate: await createSubstrate(dataDir) }),
    );
    const stepSources = stepSourcesFor();

    const { env: first } = await buildEnv(
      requestForRun("run-1", 1),
      stepSources,
    );
    expect(first.workdir).toBe(
      stepWorkspaceDir({
        dataDir,
        workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
        runId: "run-1",
        stepId: STEP_ID,
        attempt: 1,
      }),
    );
    await fs.writeFile(path.join(first.workdir, "attempt-1.txt"), "lost");

    const { env: retry } = await buildEnv(
      requestForRun("run-1", 2),
      stepSources,
    );
    expect(retry.workdir).toBe(
      stepWorkspaceDir({
        dataDir,
        workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
        runId: "run-1",
        stepId: STEP_ID,
        attempt: 2,
      }),
    );
    expect(await fs.readdir(retry.workdir)).toEqual([]);
  });

  test("an approval resume of the same attempt keeps the attempt's files", async () => {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    await seedColdStore(substrate, "run-1", [
      approvalOp("corr-1", { suspendedCall: SUSPENDED_CALL }),
    ]);
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate }),
    );
    const stepSources = stepSourcesFor();

    const { env: first } = await buildEnv(requestForRun("run-1"), stepSources);
    await fs.writeFile(path.join(first.workdir, "turn-1.txt"), "kept");

    const { env: resumed } = await buildEnv(
      approvalResumeRequest("run-1", "corr-1"),
      stepSources,
    );
    expect(resumed.workdir).toBe(first.workdir);
    expect(
      await fs.readFile(path.join(resumed.workdir, "turn-1.txt"), "utf8"),
    ).toBe("kept");
  });

  test("feeds the reactor the whole failover chain with the head pinned as default", async () => {
    // The reactor resolves its initial source by `defaultSource` and fails
    // over forward across `sources`; the child must hand it the full ordered
    // chain, not just the active source, or cross-provider failover is lost.
    const failoverSource: InferenceSource = {
      id: "failover",
      provider: "openai",
      baseURL: "https://api.openai.com",
      credentialId: "sk-failover",
      model: "gpt-failover",
    };
    const chain = [SOURCE, failoverSource];
    const dataDir = await makeTempDir();
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate: await createSubstrate(dataDir) }),
    );

    const { env } = await buildEnv(
      requestForRun("run-1"),
      stepSourcesFor(chain),
    );

    // The whole chain reaches the reactor, ordered, with element 0 as the
    // initial source.
    expect(env.sources).toEqual(chain);
    expect(env.defaultSource).toBe(SOURCE.id);
  });

  test("a map iteration resolves the base step's source while keeping scratch scoped", async () => {
    // Deploy pins one source per base step, keyed by the base step id; a map
    // iteration runs under a scoped step id `<base>[<index>]`. The source
    // table holds only the base id, so without base resolution the build
    // would throw "no InferenceSource pinned". The scratch, by contrast,
    // stays keyed by the scoped id so concurrent iterations never collide.
    const dataDir = await makeTempDir();
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate: await createSubstrate(dataDir) }),
    );
    const scopedId = scopedStepId(STEP_ID, 0);

    const scopedRequest: StepInvokeRequest = {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- buildEnv reads only authzContext; the agent definition is never consulted here
      agent: { toolFactories: [] } as unknown as StepInvokeRequest["agent"],
      input: null,
      replyByMail: false,
      authzContext: { stepId: scopedId, runId: "run-1", attempt: 1 },
      place: { stepPath: [STEP_ID], mapIndex: 0 },
      signal: new AbortController().signal,
    };
    const { env } = await buildEnv(scopedRequest, stepSourcesFor());

    // The scoped iteration resolves the base step's pinned source.
    expect(env.sources).toEqual([SOURCE]);
    expect(env.defaultSource).toBe(SOURCE.id);
    // Per-iteration scratch stays keyed by the scoped id, not the base.
    expect(env.workdir).toContain(path.join("steps", scopedId));
  });
});

// A re-dispatchable ask-rail approval gate always carries a `suspendedCall`
// (the call to re-run on approval). An async-tool pending marker uses the
// same `kind: "approval"` but never sets `suspendedCall`, and on resume the
// reactor clears its gate WITHOUT re-dispatching. The resume keying
// assertion must therefore find a `suspendedCall`-bearing op for the
// resumed correlationId, mirroring the reactor's own discriminator.
const SUSPENDED_CALL: ToolCall = {
  id: "call-1",
  name: "charge_card",
  arguments: { amount: 100 },
};

function approvalOp(
  correlationId: string,
  opts: { suspendedCall?: ToolCall } = {},
): PendingOperation {
  return {
    correlationId,
    kind: "approval",
    registeredAt: 0,
    gateId: `gate-${correlationId}`,
    turnId: `turn-${correlationId}`,
    gated: true,
    ...(opts.suspendedCall !== undefined
      ? { suspendedCall: opts.suspendedCall }
      : {}),
  };
}

/**
 * Commit a step's state for attempt 1 of the step in `runId` through the
 * production durable store, from a local store the env under test never
 * sees, so the env can only find it by restoring from the substrate.
 */
async function seedColdStore(
  substrate: RepoStore,
  runId: string,
  pendingOperations: PendingOperation[],
): Promise<void> {
  const store = await createDurableConversationStore({
    localStoreDir: await makeTempDir(),
    signer,
    substrate,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    runId: TOP_LEVEL_RUN_ID,
    stepId: STEP_ID,
    lifetime: {
      invocation: { runId, stepId: STEP_ID, attempt: 1 },
    },
    mailboxAddress: "agent@example.com",
  });
  await store.restoreFromSubstrate();
  await store.storage.writeMetadata({
    pendingOperations,
    tokenUsage: ZERO_USAGE,
  });
  await store.mirrorToSubstrate();
}

function approvalResumeRequest(
  runId: string,
  correlationId: string,
): StepInvokeRequest {
  return {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- buildEnv reads only authzContext and resume; the agent definition is never consulted here
    agent: { toolFactories: [] } as unknown as StepInvokeRequest["agent"],
    input: null,
    replyByMail: false,
    authzContext: { stepId: STEP_ID, runId, attempt: 1 },
    place: { stepPath: [STEP_ID] },
    resume: {
      correlationId,
      decision: { outcome: "approved" },
    },
    signal: new AbortController().signal,
  };
}

function isRuntimeCapabilities(value: unknown): value is RuntimeCapabilities {
  return (
    typeof value === "object" &&
    value !== null &&
    "resolve" in value &&
    typeof value.resolve === "function"
  );
}

describe("createSidecarStepBuildEnv capabilities", () => {
  test("a step tool resolves the child's mail signer as mail.signer", async () => {
    const dataDir = await makeTempDir();
    const mailSigner: MailSigner = {
      compose: () => Promise.reject(new Error("not composed in this test")),
    };
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({
        dataDir,
        substrate: await createSubstrate(dataDir),
        mailSigner,
      }),
    );

    const { env } = await buildEnv(requestForRun("run-1"), stepSourcesFor());

    // A tool reads `capabilities` from its own env type; StepEnvBase does
    // not declare it, so narrow it here.
    const capabilities = "capabilities" in env ? env.capabilities : undefined;
    if (!isRuntimeCapabilities(capabilities)) {
      throw new Error("the step env carries no capabilities");
    }
    expect(capabilities.resolve("mail.signer")).toBe(mailSigner);
  });
});

describe("createSidecarStepBuildEnv approval-resume keying", () => {
  test("rejects a resume whose matching pending op carries no suspendedCall", async () => {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    // A pending op that matches the correlationId but is not a
    // re-dispatchable gate (no suspendedCall) -- an async-tool marker, not
    // the ask-rail approval the resume must find.
    await seedColdStore(substrate, "run-1", [approvalOp("corr-1")]);
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate }),
    );

    await expect(
      buildEnv(approvalResumeRequest("run-1", "corr-1"), stepSourcesFor()),
    ).rejects.toThrow(/keying violation/);
  });

  test("accepts a resume that finds a suspendedCall-bearing approval gate", async () => {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    await seedColdStore(substrate, "run-1", [
      approvalOp("corr-1", { suspendedCall: SUSPENDED_CALL }),
    ]);
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate }),
    );

    const { env } = await buildEnv(
      approvalResumeRequest("run-1", "corr-1"),
      stepSourcesFor(),
    );

    expect(env.workdir).toContain(path.join("runs", "run-1"));
  });

  test("rejects a resume whose correlationId has no pending op at all", async () => {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    // A real gate exists, but for a different correlationId: the resumed
    // correlationId finds nothing -- the wrong-attempt forever-hang guard.
    await seedColdStore(substrate, "run-1", [
      approvalOp("other", { suspendedCall: SUSPENDED_CALL }),
    ]);
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate }),
    );

    await expect(
      buildEnv(approvalResumeRequest("run-1", "corr-1"), stepSourcesFor()),
    ).rejects.toThrow(/keying violation/);
  });
});

describe("createSidecarStepBuildEnv approval resume of a park with no stamp", () => {
  const charge = {
    role: "user" as const,
    content: [{ type: "text" as const, text: "charge it" }],
    timestamp: 1,
  };

  // The shape an earlier build wrote for a park: a bare attempt, no stamp.
  async function commitLegacyPark(substrate: RepoStore): Promise<void> {
    const stateDir = workflowRunStepStatePrefix(TOP_LEVEL_RUN_ID, STEP_ID);
    const metadata = {
      pendingOperations: [
        approvalOp("corr-1", { suspendedCall: SUSPENDED_CALL }),
      ],
      tokenUsage: ZERO_USAGE,
      connectorState: null,
      attempt: 1,
    };
    await substrate.writeTreePreservingPrefix(
      PRINCIPAL,
      WORKFLOW_RUN_REPO_ID,
      WORKFLOW_RUN_REF,
      {
        preservePrefix: stateDir,
        merge: async () => ({
          [`${stateDir}checkpoint.json`]: JSON.stringify({
            turns: [charge],
            ...metadata,
          }),
          [`${stateDir}checkpoint.meta.json`]: JSON.stringify({
            checkpointSeq: 1,
            turnCount: 1,
            ...metadata,
          }),
        }),
        message: "legacy park",
      },
    );
  }

  test("a resume of the park's gate on a replacement host continues it and commits it stamped", async () => {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    await commitLegacyPark(substrate);
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate }),
    );

    const build = await buildEnv(
      approvalResumeRequest("run-1", "corr-1"),
      stepSourcesFor(),
    );
    const loaded = await build.env.storage.load();
    expect(loaded.turns).toEqual([charge]);
    if (build.conversation === undefined) {
      throw new Error("a step build returns its conversation");
    }
    const done = {
      role: "user" as const,
      content: [{ type: "text" as const, text: "charged" }],
      timestamp: 2,
    };
    await build.env.storage.writeTurns([charge, done]);
    await build.env.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: ZERO_USAGE,
    });
    await build.env.storage.commit({ message: "turn" });
    await build.conversation.settle();

    const committed = await readStepState({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: TOP_LEVEL_RUN_ID,
      stepId: STEP_ID,
    });
    expect(committed?.turns).toEqual([charge, done]);
    expect(committed?.pendingOperations).toEqual([]);
    expect(committed?.invocation).toEqual({
      runId: "run-1",
      stepId: STEP_ID,
      attempt: 1,
    });
  });

  // The keying assertion throws whether the build adopts the park of gate
  // `corr-1` or starts over, so this test does not show which one it did.
  // The conversation-state and parked-approval tests show that.
  test("a resume of another gate finds no gate to resume", async () => {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    await commitLegacyPark(substrate);
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir, substrate }),
    );

    await expect(
      buildEnv(approvalResumeRequest("run-1", "corr-2"), stepSourcesFor()),
    ).rejects.toThrow(/keying violation/);
  });
});

describe("createSidecarStepBuildEnv step state", () => {
  const turn = (text: string) => ({
    role: "user" as const,
    content: [{ type: "text" as const, text }],
    timestamp: 1,
  });

  /** One host: its own local data dir over the shared workflow-run substrate. */
  async function host(substrate: RepoStore) {
    return createSidecarStepBuildEnv(
      buildDeps({ dataDir: await makeTempDir(), substrate }),
    );
  }

  async function takeTurn(build: StepBuild, turns: ReturnType<typeof turn>[]) {
    if (build.conversation === undefined) {
      throw new Error("a step build returns its conversation");
    }
    await build.env.storage.writeTurns(turns);
    await build.env.storage.commit({ message: "turn" });
    await build.conversation.settle();
  }

  test("a replacement host restores the conversation the last turn committed", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const first = await (
      await host(substrate)
    )(requestForRun("run-1"), stepSourcesFor());
    await takeTurn(first, [turn("first")]);

    const committed = await readStepState({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: TOP_LEVEL_RUN_ID,
      stepId: STEP_ID,
    });
    expect(committed?.turns).toEqual([turn("first")]);
    expect(committed?.invocation).toEqual({
      runId: "run-1",
      stepId: STEP_ID,
      attempt: 1,
    });

    const replacement = await (
      await host(substrate)
    )(requestForRun("run-1"), stepSourcesFor());
    expect((await replacement.env.storage.load()).turns).toEqual([
      turn("first"),
    ]);
  });

  test("a retried attempt continues from the last committed turn", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const buildEnv = await host(substrate);
    await takeTurn(
      await buildEnv(requestForRun("run-1", 1), stepSourcesFor()),
      [turn("attempt one")],
    );

    const retry = await buildEnv(requestForRun("run-1", 2), stepSourcesFor());
    expect((await retry.env.storage.load()).turns).toEqual([
      turn("attempt one"),
    ]);
    await takeTurn(retry, [turn("attempt one"), turn("attempt two")]);

    const committed = await readStepState({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: TOP_LEVEL_RUN_ID,
      stepId: STEP_ID,
    });
    expect(committed?.turns).toEqual([
      turn("attempt one"),
      turn("attempt two"),
    ]);
    expect(committed?.invocation).toEqual({
      runId: "run-1",
      stepId: STEP_ID,
      attempt: 2,
    });
    await expect(
      buildEnv(requestForRun("run-1", 1), stepSourcesFor()),
    ).rejects.toThrow(/later than attempt 1/);
  });

  /** An invocation of the step at `stepPath` in the run `runId`. */
  function placedRequest(
    runId: string,
    stepPath: string[],
    attempt = 1,
  ): StepInvokeRequest {
    return { ...requestForRun(runId, attempt), place: { stepPath } };
  }

  function readKey(substrate: RepoStore, runId: string, key: string) {
    return readStepState({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId,
      stepId: key,
    });
  }

  async function writeSeed(
    substrate: RepoStore,
    key: string,
    turns: ReturnType<typeof turn>[],
  ): Promise<void> {
    const seedPath = workflowRunStepSeedPath(TOP_LEVEL_RUN_ID, key);
    await substrate.writeTreePreservingPrefix(
      { kind: "hub" },
      WORKFLOW_RUN_REPO_ID,
      WORKFLOW_RUN_REF,
      {
        preservePrefix: seedPath.slice(0, seedPath.lastIndexOf("/") + 1),
        merge: async () => ({
          [seedPath]: JSON.stringify({
            version: 1,
            turns,
            tokenUsage: ZERO_USAGE,
            connectorState: null,
          }),
        }),
        message: "seed step",
      },
    );
  }

  test.each([
    ["a loop body", ["loop", STEP_ID]],
    ["an onTrigger body", ["section", STEP_ID]],
    ["a childWorkflow child", ["child", STEP_ID]],
  ])(
    "%s step files its state under the top-level run at its key",
    async (_, stepPath) => {
      const substrate = await createSubstrate(await makeTempDir());
      const build = await (
        await host(substrate)
      )(placedRequest("run-body-1", stepPath), stepSourcesFor());
      await takeTurn(build, [turn("body turn")]);

      const key = stepPath.join(".");
      expect((await readKey(substrate, TOP_LEVEL_RUN_ID, key))?.turns).toEqual([
        turn("body turn"),
      ]);
      expect(await readKey(substrate, "run-body-1", STEP_ID)).toBeNull();
      const reads = await substrate.openCommittedReads(
        PRINCIPAL,
        WORKFLOW_RUN_REPO_ID,
        WORKFLOW_RUN_REF,
      );
      const stateDir = workflowRunStepStatePrefix(TOP_LEVEL_RUN_ID, key);
      expect(stateDir).toBe(`runs/${TOP_LEVEL_RUN_ID}/steps/${key}/state/`);
      expect(
        (await reads?.listDir(`runs/${TOP_LEVEL_RUN_ID}/steps`))?.map(
          (entry) => entry.name,
        ),
      ).toEqual([key]);
      expect(await reads?.listDir("runs/run-body-1")).toEqual([]);
    },
  );

  test("an onTrigger body step continues the conversation the last event committed", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const stepPath = ["section", STEP_ID];
    const key = stepPath.join(".");
    await writeSeed(substrate, key, [turn("seeded")]);
    const buildEnv = await host(substrate);

    const first = await buildEnv(
      placedRequest("run-body-1", stepPath),
      stepSourcesFor(),
    );
    expect((await first.env.storage.load()).turns).toEqual([turn("seeded")]);
    await takeTurn(first, [turn("seeded"), turn("first event")]);

    // The seed applies only while the key holds no committed state.
    const second = await buildEnv(
      placedRequest("run-body-2", stepPath),
      stepSourcesFor(),
    );
    expect((await second.env.storage.load()).turns).toEqual([
      turn("seeded"),
      turn("first event"),
    ]);
    await takeTurn(second, [
      turn("seeded"),
      turn("first event"),
      turn("second event"),
    ]);

    const committed = await readKey(substrate, TOP_LEVEL_RUN_ID, key);
    expect(committed?.turns).toEqual([
      turn("seeded"),
      turn("first event"),
      turn("second event"),
    ]);
    expect(committed?.invocation).toEqual({
      runId: "run-body-2",
      stepId: STEP_ID,
      attempt: 1,
    });
  });

  test("a loop body step continues the conversation across iterations, and a retry drops the failed attempt's work", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const stepPath = ["loop", STEP_ID];
    const key = stepPath.join(".");
    const buildEnv = await host(substrate);

    await takeTurn(
      await buildEnv(placedRequest("run_loop__0", stepPath), stepSourcesFor()),
      [turn("iteration 0")],
    );

    // Attempt 1 of the next iteration records a partial turn locally, then
    // fails, so its conversation never settles.
    const failed = await buildEnv(
      placedRequest("run_loop__1", stepPath, 1),
      stepSourcesFor(),
    );
    expect((await failed.env.storage.load()).turns).toEqual([
      turn("iteration 0"),
    ]);
    await failed.env.storage.writeTurns([turn("iteration 0"), turn("partial")]);
    await failed.env.storage.commit({ message: "partial" });

    const retry = await buildEnv(
      placedRequest("run_loop__1", stepPath, 2),
      stepSourcesFor(),
    );
    expect((await retry.env.storage.load()).turns).toEqual([
      turn("iteration 0"),
    ]);
    await takeTurn(retry, [turn("iteration 0"), turn("iteration 1")]);

    const next = await buildEnv(
      placedRequest("run_loop__2", stepPath),
      stepSourcesFor(),
    );
    expect((await next.env.storage.load()).turns).toEqual([
      turn("iteration 0"),
      turn("iteration 1"),
    ]);
    expect(
      (await readKey(substrate, TOP_LEVEL_RUN_ID, key))?.invocation,
    ).toEqual({ runId: "run_loop__1", stepId: STEP_ID, attempt: 2 });
  });

  test("a map item in a loop body continues the conversation of the same index", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const buildEnv = await host(substrate);
    const item = (runId: string, index: number): StepInvokeRequest => ({
      ...requestForRun(runId),
      authzContext: { stepId: scopedStepId(STEP_ID, index), runId, attempt: 1 },
      place: { stepPath: ["loop", STEP_ID], mapIndex: index },
    });

    for (const index of [0, 1]) {
      await takeTurn(
        await buildEnv(item("run_loop__0", index), stepSourcesFor()),
        [turn(`item ${String(index)} iteration 0`)],
      );
    }
    for (const index of [0, 1]) {
      const next = await buildEnv(item("run_loop__1", index), stepSourcesFor());
      expect((await next.env.storage.load()).turns).toEqual([
        turn(`item ${String(index)} iteration 0`),
      ]);
    }
  });

  test("a childWorkflow body step continues the conversation when the childWorkflow step runs again", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const buildEnv = await host(substrate);
    // The childWorkflow step `child` sits in the onTrigger body `section`.
    // Each event spawns a child run with a new run id, and the child run's
    // step path prefix is the parent's prefix plus the childWorkflow step id.
    const stepPath = ["section", "child", STEP_ID];
    const key = stepPath.join(".");

    await takeTurn(
      await buildEnv(placedRequest("run_child_a", stepPath), stepSourcesFor()),
      [turn("first child run")],
    );

    const second = await buildEnv(
      placedRequest("run_child_b", stepPath),
      stepSourcesFor(),
    );
    expect((await second.env.storage.load()).turns).toEqual([
      turn("first child run"),
    ]);
    await takeTurn(second, [turn("first child run"), turn("second child run")]);

    const committed = await readKey(substrate, TOP_LEVEL_RUN_ID, key);
    expect(committed?.turns).toEqual([
      turn("first child run"),
      turn("second child run"),
    ]);
    expect(committed?.invocation).toEqual({
      runId: "run_child_b",
      stepId: STEP_ID,
      attempt: 1,
    });
  });

  test("a top-level map item files its state under its scoped key", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const itemId = scopedStepId(STEP_ID, 0);
    const request: StepInvokeRequest = {
      ...requestForRun(TOP_LEVEL_RUN_ID),
      authzContext: { stepId: itemId, runId: TOP_LEVEL_RUN_ID, attempt: 1 },
      place: { stepPath: [STEP_ID], mapIndex: 0 },
    };
    const build = await (await host(substrate))(request, stepSourcesFor());
    await takeTurn(build, [turn("item turn")]);

    const reads = await substrate.openCommittedReads(
      PRINCIPAL,
      WORKFLOW_RUN_REPO_ID,
      WORKFLOW_RUN_REF,
    );
    expect(
      (await reads?.listDir(`runs/${TOP_LEVEL_RUN_ID}/steps`))?.map(
        (entry) => entry.name,
      ),
    ).toEqual(["step-1%5B0%5D"]);
    const committed = await readKey(substrate, TOP_LEVEL_RUN_ID, itemId);
    expect(committed?.turns).toEqual([turn("item turn")]);
    expect(committed?.invocation).toEqual({
      runId: TOP_LEVEL_RUN_ID,
      stepId: "step-1[0]",
      attempt: 1,
    });
  });

  test("an invocation keeps its own crashed turn, and another continues it once committed", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const stepPath = ["section", STEP_ID];
    const key = stepPath.join(".");
    const sameHost = await host(substrate);

    await takeTurn(
      await sameHost(placedRequest("run-body-1", stepPath), stepSourcesFor()),
      [turn("body one")],
    );

    // Body run 2 commits its turn to its local store, then the host dies
    // before the settle commits it to the repo.
    const crashed = await sameHost(
      placedRequest("run-body-2", stepPath),
      stepSourcesFor(),
    );
    expect((await crashed.env.storage.load()).turns).toEqual([
      turn("body one"),
    ]);
    await crashed.env.storage.writeTurns([turn("body one"), turn("body two")]);
    await crashed.env.storage.commit({ message: "turn" });

    const rebuilt = await sameHost(
      placedRequest("run-body-2", stepPath),
      stepSourcesFor(),
    );
    expect((await rebuilt.env.storage.load()).turns).toEqual([
      turn("body one"),
      turn("body two"),
    ]);
    if (rebuilt.conversation === undefined) {
      throw new Error("a step build returns its conversation");
    }
    await rebuilt.conversation.settle();
    const committed = await readKey(substrate, TOP_LEVEL_RUN_ID, key);
    expect(committed?.turns).toEqual([turn("body one"), turn("body two")]);
    expect(committed?.invocation).toEqual({
      runId: "run-body-2",
      stepId: STEP_ID,
      attempt: 1,
    });

    const next = await (
      await host(substrate)
    )(placedRequest("run-body-3", stepPath), stepSourcesFor());
    expect((await next.env.storage.load()).turns).toEqual([
      turn("body one"),
      turn("body two"),
    ]);
  });

  test("state stamped with a bare attempt belongs to no invocation and is continued", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const stateDir = workflowRunStepStatePrefix(TOP_LEVEL_RUN_ID, STEP_ID);
    // The shape a build that stamped only the attempt wrote.
    await substrate.writeTreePreservingPrefix(
      PRINCIPAL,
      WORKFLOW_RUN_REPO_ID,
      WORKFLOW_RUN_REF,
      {
        preservePrefix: stateDir,
        merge: async () => ({
          [`${stateDir}checkpoint.json`]: JSON.stringify({
            turns: [turn("legacy")],
            pendingOperations: [],
            tokenUsage: ZERO_USAGE,
            connectorState: null,
            attempt: 1,
          }),
          [`${stateDir}checkpoint.meta.json`]: JSON.stringify({
            checkpointSeq: 1,
            turnCount: 1,
            pendingOperations: [],
            tokenUsage: ZERO_USAGE,
            connectorState: null,
            attempt: 1,
          }),
        }),
        message: "legacy step state",
      },
    );
    expect(
      (await readKey(substrate, TOP_LEVEL_RUN_ID, STEP_ID))?.invocation,
    ).toBeUndefined();

    const build = await (
      await host(substrate)
    )(requestForRun("run-1", 1), stepSourcesFor());
    expect((await build.env.storage.load()).turns).toEqual([turn("legacy")]);
    await takeTurn(build, [turn("legacy"), turn("next")]);

    const committed = await readKey(substrate, TOP_LEVEL_RUN_ID, STEP_ID);
    expect(committed?.turns).toEqual([turn("legacy"), turn("next")]);
    expect(committed?.invocation).toEqual({
      runId: "run-1",
      stepId: STEP_ID,
      attempt: 1,
    });
  });

  test("state an earlier version wrote with no stamp and no park continues, with its thread", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const mailboxAddress = `${TOP_LEVEL_RUN_ID}@example.com`;
    const mail = (opts: {
      messageId: string;
      inReplyTo?: string;
      references?: string[];
    }) =>
      createInboundMessage({
        from: "alice@example.com",
        to: mailboxAddress,
        content: "hello",
        messageId: opts.messageId,
        interchangeType: "conversation.message",
        ...(opts.inReplyTo !== undefined ? { inReplyTo: opts.inReplyTo } : {}),
        ...(opts.references !== undefined
          ? { references: opts.references }
          : {}),
      });
    // An earlier version kept the step of a one-step deployment in a store
    // that lives as long as the deployment and stamps no invocation. Its
    // first turn answered Alice's mail and replied on the thread.
    const router = createConnectorRouter({ ownAddress: mailboxAddress });
    router.beginTurn({});
    router.commit(router.route(mail({ messageId: "<a1@example.com>" })));
    const firstReply = router.composeReply();
    if (firstReply === null) throw new Error("expected a reply address");
    router.onReplySent(
      { messageId: "<r1@example.com>", status: "delivered" },
      firstReply.references ?? [],
    );
    await commitUnstampedStepState({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: TOP_LEVEL_RUN_ID,
      stepId: STEP_ID,
      turns: [turn("first")],
      metadata: {
        pendingOperations: [],
        tokenUsage: ZERO_USAGE,
        connectorState: router.snapshot(),
      },
    });
    const before = await readKey(substrate, TOP_LEVEL_RUN_ID, STEP_ID);
    expect(before?.invocation).toBeUndefined();
    expect(before?.pendingOperations).toEqual([]);
    expect(before?.connectorState?.lastMessageId).toBe("<r1@example.com>");
    const stateDir = path.join(
      substrate.getRepoDir(WORKFLOW_RUN_REPO_ID),
      workflowRunStepStatePrefix(TOP_LEVEL_RUN_ID, STEP_ID),
    );
    const walBefore = await countWalEntries(stateDir);

    const build = await (
      await host(substrate)
    )(requestForRun("run-1"), stepSourcesFor());
    expect((await build.env.storage.load()).turns).toEqual([turn("first")]);
    if (build.conversation === undefined) {
      throw new Error("a step build returns its conversation");
    }
    // Alice answers the earlier reply, so her mail continues the thread.
    await build.conversation.beginTurn({});
    await build.conversation.seedInbound(
      mail({
        messageId: "<a2@example.com>",
        inReplyTo: "<r1@example.com>",
        references: ["<a1@example.com>", "<r1@example.com>"],
      }),
    );
    expect(build.conversation.composeReply()).toMatchObject({
      to: "alice@example.com",
      inReplyTo: "<a2@example.com>",
    });
    await takeTurn(build, [turn("first"), turn("second")]);

    const after = await readKey(substrate, TOP_LEVEL_RUN_ID, STEP_ID);
    expect(after?.turns).toEqual([turn("first"), turn("second")]);
    expect(after?.connectorState?.threadRoot).toBe("<a1@example.com>");
    expect(after?.connectorState?.lastMessageId).toBe("<a2@example.com>");
    expect(after?.invocation).toEqual({
      runId: "run-1",
      stepId: STEP_ID,
      attempt: 1,
    });
    // The first mirror appends the turn and does not rewrite the
    // conversation.
    expect(await countWalEntries(stateDir)).toBe(walBefore + 1);
  });

  test("a build's conversation sends through the bridge as the deployment mailbox", async () => {
    const dataDir = await makeTempDir();
    const submitted: { sender: string; message: OutboundMessage }[] = [];
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({
        dataDir,
        substrate: await createSubstrate(dataDir),
        outboundMailBridge: {
          ...stubOutboundMailBridge(),
          submit: async (sender, message) => {
            submitted.push({ sender, message });
            return { messageId: "<sent@example.com>", status: "delivered" };
          },
        },
      }),
    );
    const { conversation } = await buildEnv(
      requestForRun("run-1"),
      stepSourcesFor(),
    );
    if (conversation === undefined) {
      throw new Error("a step build returns its conversation");
    }
    const message: OutboundMessage = {
      to: "alice@example.com",
      type: "conversation.message",
      content: "hello",
    };

    expect(await conversation.send(message)).toEqual({
      messageId: "<sent@example.com>",
      status: "delivered",
    });
    expect(submitted).toEqual([
      { sender: "run_deployment-keying@example.com", message },
    ]);
  });
});

describe("a step's reply over its attempt's durable store", () => {
  const MAIL: Mail = {
    headers: {
      from: "alice@example.com",
      to: ["run_deployment-keying@example.com"],
      date: "2026-01-02T03:04:05Z",
      messageId: "<a@example.com>",
    },
    rawHeaders: {},
    parts: [
      {
        contentType: "text/plain",
        ref: "mail-part:///r/<a@example.com>/0-text",
        text: "please charge the card",
      },
    ],
  };
  const allow: WorkflowAuthorizeFn = async () => ({
    effect: "allow",
    matchingGrants: [],
    resolvedBy: null,
  });

  // A step agent whose one send runs `send` against the env it was built
  // with. The invoker closes it after the turn.
  function stepAgent(send: () => Promise<SendResult>): Agent {
    return {
      send,
      stream() {
        throw new Error("stub stream() not used");
      },
      deliver() {
        throw new Error("stub deliver() not used");
      },
      async close() {
        /* no-op */
      },
      setSource() {
        throw new Error("stub setSource() not used");
      },
      setSources() {
        throw new Error("stub setSources() not used");
      },
      async history() {
        return [];
      },
      async checkpoints() {
        return [];
      },
      async readAt() {
        return [];
      },
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; the step build never reads blobs
      blobReader: {} as BlobReader,
    };
  }

  // The first send parks on an approval gate the way the reactor does: it
  // commits the gate's pending operation to the step's store, then settles
  // as suspended. The next send replies.
  function parkThenReply(env: BaseEnv, sends: number): Agent {
    if (sends === 0) {
      return stepAgent(async () => {
        await env.storage.writeMetadata({
          pendingOperations: [
            approvalOp("c1", { suspendedCall: SUSPENDED_CALL }),
          ],
          tokenUsage: ZERO_USAGE,
        });
        await env.storage.commit({ message: "park" });
        return {
          type: "suspended",
          turnId: "turn-test",
          correlationId: "c1",
          approvalSnapshot: {
            name: SUSPENDED_CALL.name,
            description: "needs approval",
            inputSchema: { type: "object" },
            arguments: SUSPENDED_CALL.arguments,
          },
        };
      });
    }
    return stepAgent(async () => ({
      type: "reply",
      turnId: "turn-test",
      reply: "charged",
      turn: {
        role: "assistant",
        content: [{ type: "text", text: "charged" }],
        model: SOURCE.model,
        timestamp: 0,
      },
    }));
  }

  async function invoker(
    substrate: RepoStore,
    submitted: OutboundMessage[],
    sends: { count: number },
  ) {
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({
        dataDir: await makeTempDir(),
        substrate,
        outboundMailBridge: {
          ...stubOutboundMailBridge(),
          submit: async (_sender, message) => {
            submitted.push(message);
            return {
              messageId: `<sent-${String(submitted.length)}@example.com>`,
              status: "delivered",
            };
          },
        },
      }),
    );
    return createWorkflowStepInvoker({
      workflowAuthorize: allow,
      buildEnv: (req) => buildEnv(req, stepSourcesFor()),
      agentFactory: async (_def, env) => parkThenReply(env, sends.count++),
    });
  }

  const mailTurn: StepInvokeRequest = {
    ...requestForRun("run-1"),
    input: MAIL,
    replyByMail: true,
  };
  const resumeTurn: StepInvokeRequest = {
    ...approvalResumeRequest("run-1", "c1"),
    replyByMail: true,
  };

  test("a turn that throws commits nothing", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({ dataDir: await makeTempDir(), substrate }),
    );
    const invoke = createWorkflowStepInvoker({
      workflowAuthorize: allow,
      buildEnv: (req) => buildEnv(req, stepSourcesFor()),
      agentFactory: async (_def, env) =>
        stepAgent(async () => {
          // The reactor commits each cycle to the local store before the
          // send settles; the turn then fails.
          await env.storage.writeTurns([
            {
              role: "user",
              content: [{ type: "text", text: "partial" }],
              timestamp: 1,
            },
          ]);
          await env.storage.commit({ message: "partial" });
          throw new Error("inference failed");
        }),
    });

    await expect(invoke(mailTurn)).rejects.toThrow(/inference failed/);

    expect(
      await readStepState({
        substrate,
        workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
        workflowRunRef: WORKFLOW_RUN_REF,
        principal: PRINCIPAL,
        runId: TOP_LEVEL_RUN_ID,
        stepId: STEP_ID,
      }),
    ).toBeNull();
  });

  /**
   * An invoker whose agent for each invocation, in order, is the next of
   * `agents`. Each agent's send runs against the env it was built with. The
   * bridge records every reply it accepts; `submit` may refuse one first.
   * `localSigner` signs the commits of each invocation's local store.
   */
  async function scriptedInvoker(
    substrate: RepoStore,
    agents: ((env: BaseEnv) => Promise<SendResult>)[],
    submit?: (message: OutboundMessage) => Promise<void>,
    localSigner?: (payload: string) => Promise<string>,
  ) {
    const accepted: OutboundMessage[] = [];
    const buildEnv = createSidecarStepBuildEnv(
      buildDeps({
        dataDir: await makeTempDir(),
        substrate,
        ...(localSigner !== undefined ? { signer: localSigner } : {}),
        outboundMailBridge: {
          ...stubOutboundMailBridge(),
          submit: async (_sender, message) => {
            await submit?.(message);
            accepted.push(message);
            return {
              messageId: `<sent-${String(accepted.length)}@example.com>`,
              status: "delivered",
            };
          },
        },
      }),
    );
    let built = 0;
    const invoke = createWorkflowStepInvoker({
      workflowAuthorize: allow,
      buildEnv: (req) => buildEnv(req, stepSourcesFor()),
      agentFactory: async (_def, env) => {
        const send = agents[built++];
        if (send === undefined) throw new Error("no agent left to build");
        return stepAgent(() => send(env));
      },
    });
    return { invoke, accepted };
  }

  const text = (role: "user" | "assistant", value: string) =>
    role === "user"
      ? {
          role,
          content: [{ type: "text" as const, text: value }],
          timestamp: 1,
        }
      : {
          role,
          content: [{ type: "text" as const, text: value }],
          model: SOURCE.model,
          timestamp: 1,
        };

  function toolCall(callId: string) {
    return {
      role: "assistant" as const,
      content: [
        {
          type: "tool_call" as const,
          id: callId,
          name: SUSPENDED_CALL.name,
          arguments: {},
        },
      ],
      model: SOURCE.model,
      timestamp: 1,
    };
  }

  function toolResult(callId: string) {
    return {
      role: "user" as const,
      content: [
        {
          type: "tool_result" as const,
          callId,
          content: [{ type: "text" as const, text: "charged" }],
        },
      ],
      timestamp: 1,
    };
  }

  /**
   * A send that records `turns` after the store's turns and then parks on
   * the gate `correlationId`, the way the reactor does.
   */
  function parkOn(
    correlationId: string,
    turns: ConversationTurn[],
  ): (env: BaseEnv) => Promise<SendResult> {
    return async (env) => {
      const prior = (await env.storage.load()).turns;
      await env.storage.writeTurns([...prior, ...turns]);
      await env.storage.writeMetadata({
        pendingOperations: [
          approvalOp(correlationId, {
            suspendedCall: { ...SUSPENDED_CALL, id: `call-${correlationId}` },
          }),
        ],
        tokenUsage: ZERO_USAGE,
      });
      await env.storage.commit({ message: "park" });
      return {
        type: "suspended",
        turnId: "turn-test",
        correlationId,
        approvalSnapshot: {
          name: SUSPENDED_CALL.name,
          description: "needs approval",
          inputSchema: { type: "object" },
          arguments: SUSPENDED_CALL.arguments,
        },
      };
    };
  }

  /** A send that records `turns` after the store's turns and replies. */
  function replyWith(
    reply: string,
    turns: ConversationTurn[],
  ): (env: BaseEnv) => Promise<SendResult> {
    return async (env) => {
      const prior = (await env.storage.load()).turns;
      await env.storage.writeTurns([...prior, ...turns]);
      await env.storage.writeMetadata({
        pendingOperations: [],
        tokenUsage: ZERO_USAGE,
      });
      await env.storage.commit({ message: "reply" });
      return {
        type: "reply",
        turnId: "turn-test",
        reply,
        turn: {
          role: "assistant",
          content: [{ type: "text", text: reply }],
          model: SOURCE.model,
          timestamp: 0,
        },
      };
    };
  }

  function readCommitted(substrate: RepoStore) {
    return readStepState({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: TOP_LEVEL_RUN_ID,
      stepId: STEP_ID,
    });
  }

  test("a reply that fails to send commits nothing, and the retry replies once", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    let refuse = true;
    const { invoke, accepted } = await scriptedInvoker(
      substrate,
      [
        replyWith("charged", [
          text("user", "inbound"),
          text("assistant", "charged"),
        ]),
        async (env) => {
          // The retry starts from the last committed turn, which is none.
          expect((await env.storage.load()).turns).toEqual([]);
          return replyWith("charged", [
            text("user", "inbound"),
            text("assistant", "charged"),
          ])(env);
        },
      ],
      async () => {
        if (refuse) {
          refuse = false;
          throw new Error("outbound bridge refused the reply");
        }
      },
    );

    await expect(invoke(mailTurn)).rejects.toThrow(/auto-reply send failed/);
    expect(await readCommitted(substrate)).toBeNull();
    await invoke({
      ...mailTurn,
      authzContext: { ...mailTurn.authzContext, attempt: 2 },
    });

    expect(accepted.map((m) => [m.content, m.to])).toEqual([
      ["charged", "alice@example.com"],
    ]);
    expect((await readCommitted(substrate))?.turns).toEqual([
      text("user", "inbound"),
      text("assistant", "charged"),
    ]);
  });

  describe("after the reply is sent", () => {
    const logs = createLogCapture();
    beforeAll(logs.install);
    afterAll(logs.restore);
    beforeEach(logs.reset);

    const replied = [text("user", "inbound"), text("assistant", "charged")];

    test("a local thread record that fails to save leaves the step a success, and the next invocation continues the thread", async () => {
      const substrate = await createSubstrate(await makeTempDir());
      // Set once the bridge accepts the reply, so only the local store's
      // commits after the send fail. The substrate signs with its own key.
      let localCommitsFail = false;
      const { invoke, accepted } = await scriptedInvoker(
        substrate,
        [
          replyWith("charged", replied),
          async (env) => {
            const loaded = await env.storage.load();
            expect(loaded.turns).toEqual(replied);
            expect(loaded.connectorState?.lastMessageId).toBe(
              "<sent-1@example.com>",
            );
            return replyWith("noted", [text("user", "next")])(env);
          },
        ],
        async () => {
          localCommitsFail = true;
        },
        async (payload) => {
          if (localCommitsFail) throw new Error("local disk is full");
          return signer(payload);
        },
      );

      const result = await invoke(mailTurn);
      localCommitsFail = false;

      expect("output" in result).toBe(true);
      expect(accepted.map((m) => m.content)).toEqual(["charged"]);
      const committed = await readCommitted(substrate);
      expect(committed?.turns).toEqual(replied);
      expect(committed?.connectorState?.lastMessageId).toBe(
        "<sent-1@example.com>",
      );
      expect(
        logs
          .records()
          .filter(
            (record) =>
              record.level === "warning" &&
              record.message.includes("local copy of its thread record"),
          ),
      ).toHaveLength(1);

      await invoke(requestForRun("run-2"));
      expect(accepted.map((m) => m.content)).toEqual(["charged"]);
    });

    test("a commit that fails after the send fails the step without a retry and commits nothing", async () => {
      const real = await createSubstrate(await makeTempDir());
      let commitsFail = false;
      const substrate: RepoStore = {
        ...real,
        writeTreePreservingPrefix: async (...args) => {
          if (commitsFail) throw new Error("substrate write refused");
          return real.writeTreePreservingPrefix(...args);
        },
      };
      const { invoke, accepted } = await scriptedInvoker(
        substrate,
        [replyWith("charged", replied)],
        async () => {
          commitsFail = true;
        },
      );

      const thrown = await invoke(mailTurn).then(
        () => undefined,
        (cause: unknown) => cause,
      );

      expect(thrown).toBeInstanceOf(StepOutcomeCommitError);
      expect(accepted.map((m) => m.content)).toEqual(["charged"]);
      expect(await readCommitted(real)).toBeNull();
    });
  });

  test("a retry after a resumed turn that parked again and then failed rolls back to the start of the original turn", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const earlier = [text("user", "earlier"), text("assistant", "noted")];
    const inbound = text("user", "please charge the card");
    const { invoke, accepted } = await scriptedInvoker(substrate, [
      replyWith("noted", earlier),
      parkOn("c1", [inbound, toolCall("call-c1")]),
      parkOn("c2", [toolResult("call-c1"), toolCall("call-c2")]),
      async (env) => {
        const prior = (await env.storage.load()).turns;
        await env.storage.writeTurns([...prior, toolResult("call-c2")]);
        await env.storage.commit({ message: "partial" });
        throw new Error("inference failed");
      },
      async (env) => {
        const loaded = await env.storage.load();
        expect(loaded.turns).toEqual(earlier);
        expect(loaded.pendingOperations).toEqual([]);
        return replyWith("charged", [inbound, text("assistant", "charged")])(
          env,
        );
      },
    ]);

    await invoke(requestForRun("run-0"));
    await invoke(mailTurn);
    await invoke({
      ...approvalResumeRequest("run-1", "c1"),
      replyByMail: true,
    });
    const parkedAgain = await readCommitted(substrate);
    expect(parkedAgain?.parkedTurnStart).toBe(earlier.length);
    expect(Object.keys(parkedAgain?.parkedReplyTargets ?? {})).toEqual(["c2"]);
    const thread = parkedAgain?.connectorState;
    expect(thread).not.toBeNull();

    await expect(
      invoke({ ...approvalResumeRequest("run-1", "c2"), replyByMail: true }),
    ).rejects.toThrow(/inference failed/);
    expect((await readCommitted(substrate))?.pendingOperations).toEqual(
      parkedAgain?.pendingOperations,
    );

    await invoke({
      ...mailTurn,
      authzContext: { ...mailTurn.authzContext, attempt: 2 },
    });

    const committed = await readCommitted(substrate);
    expect(committed?.turns).toEqual([
      ...earlier,
      inbound,
      text("assistant", "charged"),
    ]);
    expect(committed?.pendingOperations).toEqual([]);
    expect(committed?.parkedReplyTargets).toEqual({});
    expect(committed?.parkedTurnStart).toBeUndefined();
    expect(accepted.map((m) => [m.content, m.to])).toEqual([
      ["charged", "alice@example.com"],
    ]);
  });

  test("an onTrigger body that failed while parked leaves the next event the turns before the park", async () => {
    // Each event runs the body in its own run; the first event's body parks
    // and then fails without the approval, the way a tolerated failure ends.
    const substrate = await createSubstrate(await makeTempDir());
    const stepPath = ["section", STEP_ID];
    const event = (runId: string): StepInvokeRequest => ({
      ...requestForRun(runId),
      input: MAIL,
      replyByMail: true,
      place: { stepPath },
    });
    const before = [text("user", "first event"), text("assistant", "done")];
    const { invoke } = await scriptedInvoker(substrate, [
      replyWith("done", before),
      parkOn("c1", [text("user", "second event"), toolCall("call-c1")]),
      async (env) => {
        expect((await env.storage.load()).turns).toEqual(before);
        return replyWith("done", [text("user", "third event")])(env);
      },
    ]);

    await invoke(event("run-body-1"));
    await invoke(event("run-body-2"));
    await invoke(event("run-body-3"));

    const committed = await readStepState({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: TOP_LEVEL_RUN_ID,
      stepId: stepPath.join("."),
    });
    expect(committed?.turns).toEqual([...before, text("user", "third event")]);
  });

  test("the approval resume of a parked mail turn replies on the thread", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const submitted: OutboundMessage[] = [];
    const invoke = await invoker(substrate, submitted, { count: 0 });

    const parked = await invoke(mailTurn);
    if (!("suspend" in parked)) {
      throw new Error("expected a suspend-shaped step result");
    }
    expect(submitted).toEqual([]);
    await invoke(resumeTurn);

    expect(submitted.map((m) => [m.content, m.to, m.inReplyTo])).toEqual([
      ["charged", "alice@example.com", "<a@example.com>"],
    ]);
  });

  test("a replacement host resumes the committed park and replies on the thread", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const submitted: OutboundMessage[] = [];
    const sends = { count: 0 };

    await (
      await invoker(substrate, submitted, sends)
    )(mailTurn);
    await (
      await invoker(substrate, submitted, sends)
    )(resumeTurn);

    expect(submitted.map((m) => [m.content, m.to, m.inReplyTo])).toEqual([
      ["charged", "alice@example.com", "<a@example.com>"],
    ]);
  });
});

// A deployment whose step answered mail with a trigger budget is rewritten
// with the step in the body of an onTrigger section and redeployed. The
// client exports the step's conversation from the old run and imports it at
// the body step's key. This goes through the Hub's export read and its
// deploy-time import gate, and then through the sidecar's restore of the body
// step.
describe("a conversation moved from a top-level step to an onTrigger body step", () => {
  const REPLY = "reply";
  const userTurn = (text: string): ConversationTurn => ({
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 1,
  });
  const assistantTurn = (text: string): ConversationTurn => ({
    role: "assistant",
    content: [{ type: "text", text }],
    model: SOURCE.model,
    timestamp: 2,
  });

  async function takeTurn(build: StepBuild, turns: ConversationTurn[]) {
    if (build.conversation === undefined) {
      throw new Error("a step build returns its conversation");
    }
    await build.env.storage.writeTurns(turns);
    await build.env.storage.commit({ message: "turn" });
    await build.conversation.settle();
  }

  function placedRequest(runId: string, stepPath: string[]): StepInvokeRequest {
    return {
      ...requestForRun(runId),
      authzContext: { stepId: REPLY, runId, attempt: 1 },
      place: { stepPath },
    };
  }

  test("the body's first event continues the turns the one-step deployment exported", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const sources = { [REPLY]: [SOURCE] };

    // --- Two turns of the one-step deployment's step `reply` -------------
    const oneStep = createSidecarStepBuildEnv(
      buildDeps({ dataDir: await makeTempDir(), substrate }),
    );
    const earlier = [
      userTurn("first mail"),
      assistantTurn("first answer"),
      userTurn("second mail"),
      assistantTurn("second answer"),
    ];
    await takeTurn(
      await oneStep(placedRequest(TOP_LEVEL_RUN_ID, [REPLY]), sources),
      earlier,
    );

    // --- Export it as the Hub's export route reads it --------------------
    const exported = await readStepStateSnapshot({
      repoStore: substrate,
      repoId: WORKFLOW_RUN_REPO_ID,
      runId: TOP_LEVEL_RUN_ID,
      stepId: REPLY,
    });
    if (exported === null) throw new Error("the step has no committed state");
    expect(exported.turns).toEqual(earlier);

    // --- Import it at `section.reply` through the deploy's gate ----------
    const address = "run_moved-conversation@example.com";
    const topLevelRunId = deriveWorkflowRunId(address);
    const repoId = workflowRunRepoIdForAddress(address);
    const projection: WorkflowProjectionDefinition = {
      id: "wf-moved",
      triggers: [],
      stepOrder: ["section"],
      steps: {
        section: {
          kind: "onTrigger",
          body: {
            inline: {
              id: "section-body",
              triggers: [],
              stepOrder: [REPLY],
              steps: { [REPLY]: { kind: "step", agent: { modelSources: [] } } },
            },
          },
        },
      },
    };
    const seeds = buildStepStateSeeds({
      projection,
      sources: { section: [SOURCE] },
      referencedDefinitions: enumerateInertBodies(projection).map((body) => ({
        definition: body.definition,
        sources,
      })),
      runId: topLevelRunId,
      stepState: { "section.reply": { ...exported, connectorState: null } },
      uidValidity: 1,
    });
    if (!seeds.ok) throw new Error(`unexpected refusal: ${seeds.reason}`);
    expect(Object.keys(seeds.files)).toEqual([
      workflowRunStepSeedPath(topLevelRunId, "section.reply"),
    ]);
    await writeStepStateSeeds({
      repoStore: substrate,
      deploymentAddress: address,
      files: seeds.files,
    });

    // --- The body's first event restores and continues it ----------------
    const principal: WorkflowRunWorkflowProcessPrincipal = {
      kind: "workflow-process",
      anchorRunId: repoId.id,
    };
    const sectionHost = createSidecarStepBuildEnv({
      ...buildDeps({ dataDir: await makeTempDir(), substrate }),
      workflowRunRepoId: repoId,
      topLevelRunId,
      mailboxAddress: address,
      principal,
    });
    const firstEvent = await sectionHost(
      placedRequest("run_body-1", ["section", REPLY]),
      sources,
    );
    expect((await firstEvent.env.storage.load()).turns).toEqual(earlier);
    const continued = [
      ...earlier,
      userTurn("third mail"),
      assistantTurn("third answer"),
    ];
    await takeTurn(firstEvent, continued);

    const moved = await readStepStateSnapshot({
      repoStore: substrate,
      repoId,
      runId: topLevelRunId,
      stepId: "section.reply",
    });
    expect(moved?.turns).toEqual(continued);
  });
});
