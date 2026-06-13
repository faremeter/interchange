// Co-located tests for the sidecar's workflow-process child substrate
// factory. The tests focus on the workflow-run pack-push wrap the
// factory installs around the substrate's `RepoStore`. The wrap is the
// child-side analogue of the boot-edge facade in
// `apps/sidecar/src/workflow-run-pack-client.ts`; behavior parity
// across the two sites means a multi-step run's `commitRunEvent`
// writes in the child replicate to the hub via the same wire surface
// the trivial branch's writes use at the boot edge.
//
// The tests stub the bare RepoStore via the factory's `deps` slot so
// the wrap's behavior is observable without standing up an on-disk
// substrate. The factory's `createBareRepoStore` and
// `createHubPackSink` overrides are the production-injection points;
// production wiring (`createSubstrate`) closes over the
// `createAgentRepoStore`-backed store and a not-yet-wired hub sink
// that throws on `pushWorkflowRunPack`.

import { describe, test, expect, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";
import type { InferenceSource } from "@intx/types/runtime";
import { defineAgent, type AgentDefinition, type BaseEnv } from "@intx/agent";
import {
  defineWorkflow,
  step,
  type AuthorizeContext,
  type StepInvokeRequest,
  type StepInvokeResult,
} from "@intx/workflow";
import {
  createWorkflowRunRepoStore,
  type ChildPackPushBridge,
  type SubstrateFactoryEnv,
} from "@intx/workflow-host";

import {
  createSidecarRunChild,
  createSidecarStepBuildEnv,
  createSidecarSubstrateFactory,
  createStepInferenceSourceResolver,
  parseStepInferenceSources,
  SIDECAR_SUBSTRATE_CONFIG_KEYS,
  type ChildHubPackSink,
} from "./workflow-substrate-factory";

const STEP_SOURCE_ONE: InferenceSource = {
  id: "anthropic:claude-3-haiku",
  provider: "anthropic",
  baseURL: "https://api.anthropic.example",
  apiKey: "key-one",
  model: "claude-3-haiku",
};
const STEP_SOURCE_TWO: InferenceSource = {
  id: "openai:gpt-4o-mini",
  provider: "openai",
  baseURL: "https://api.openai.example",
  apiKey: "key-two",
  model: "gpt-4o-mini",
};

const DEFAULT_STEP_SOURCES: Record<string, InferenceSource> = {
  "step-1": STEP_SOURCE_ONE,
  "step-2": STEP_SOURCE_TWO,
};

function makeSubstrateConfig(
  overrides: Record<string, string> = {},
): Record<string, string> {
  const base: Record<string, string> = {
    SIDECAR_DATA_DIR: "/tmp/unused",
    WORKFLOW_DEFINITION_REPO_ID: "wfdef-1",
    WORKFLOW_DEFINITION_REF: "refs/heads/main",
    WORKFLOW_RUN_REPO_ID: "dep-1",
    WORKFLOW_RUN_REF: "refs/heads/main",
    SIDECAR_SIGNING_PUBLIC_KEY: "00".repeat(32),
    SIDECAR_SIGNING_PRIVATE_KEY: "11".repeat(32),
    HUB_WS_URL: "ws://hub.example/sidecar",
    SIDECAR_ID: "sidecar-1",
    SIDECAR_TOKEN: "tok-abc",
    STEP_INFERENCE_SOURCES: JSON.stringify(DEFAULT_STEP_SOURCES),
  };
  const merged: Record<string, string> = { ...base };
  for (const [k, v] of Object.entries(overrides)) {
    merged[k] = v;
  }
  return merged;
}

function makeSpawnEnv(): SubstrateFactoryEnv["spawn"] {
  return {
    channelId: "00".repeat(16),
    hmacKey: new Uint8Array(32),
    hostPublicKey: new Uint8Array(32),
    deploymentId: "dep-1",
    definitionHash: "def-hash",
    mailboxAddress: "agent-1@example.com",
  };
}

/**
 * Inert pack-push bridge stub for tests that do not exercise the IPC
 * path. The factory threads `packPushBridge` into the hub-pack-sink
 * builder; tests that supply their own `createHubPackSink` ignore the
 * bridge value entirely. The pendingCount accessor returns `0` so
 * assertions that walk the public surface see a defined value.
 */
function makeStubPackPushBridge(): ChildPackPushBridge {
  return {
    get pendingCount() {
      return 0;
    },
    sendRequest: () => Promise.resolve(),
    handleResponse: () => {
      /* no-op stub */
    },
    cancelAll: () => {
      /* no-op stub */
    },
  };
}

function makeFactoryEnv(
  configOverrides: Record<string, string> = {},
): SubstrateFactoryEnv {
  return {
    spawn: makeSpawnEnv(),
    substrateConfig: makeSubstrateConfig(configOverrides),
    packPushBridge: makeStubPackPushBridge(),
  };
}

type BareStoreRecording = {
  store: RepoStore;
  preserveCalls: {
    principal: Principal;
    repoId: RepoId;
    ref: string;
  }[];
  packs: { principal: Principal; repoId: RepoId; ref: string }[];
  subscribeCalls: { repoId: RepoId; ref: string }[];
};

function createRecordingBareStore(): BareStoreRecording {
  const preserveCalls: BareStoreRecording["preserveCalls"] = [];
  const packs: BareStoreRecording["packs"] = [];
  const subscribeCalls: BareStoreRecording["subscribeCalls"] = [];
  const stub: Partial<RepoStore> = {
    getRepoDir(_repoId: RepoId): string {
      // Returning a path that does not exist on disk is fine for the
      // scheduler's recovery walk: the readdir call ENOENTs and the
      // walk returns an empty event list.
      return "/tmp/workflow-substrate-factory-test-nonexistent";
    },
    async writeTreePreservingPrefix(principal, repoId, ref, args) {
      preserveCalls.push({ principal, repoId, ref });
      await args.merge(new Map());
      return { commitSha: `sha-${String(preserveCalls.length)}` };
    },
    async createPack(principal, repoId, ref) {
      packs.push({ principal, repoId, ref });
      return {
        pack: new Uint8Array([0x01, 0x02, 0x03]),
        commitSha: "stub-pack-sha",
        ref,
      };
    },
    subscribe(
      _principal: Principal,
      repoId: RepoId,
      ref: string,
      _opts: {
        signal: AbortSignal;
        from: "head" | { seq: number };
        bufferLimit?: number;
      },
    ): AsyncIterableIterator<{ seq: number; event: unknown }> {
      subscribeCalls.push({ repoId, ref });
      // Yield nothing and finish immediately so the scheduler's live
      // subscription background loop completes without blocking.
      return (async function* () {
        // empty
      })();
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- in-test stub; Proxy guards unimplemented methods below
  const store = new Proxy(stub as RepoStore, {
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
  return { store, preserveCalls, packs, subscribeCalls };
}

describe("SIDECAR_SUBSTRATE_CONFIG_KEYS", () => {
  test("includes the hub-connection trust anchors so the supervisor's substrateEnv plumbing can carry them", () => {
    expect(SIDECAR_SUBSTRATE_CONFIG_KEYS).toContain("HUB_WS_URL");
    expect(SIDECAR_SUBSTRATE_CONFIG_KEYS).toContain("SIDECAR_ID");
    expect(SIDECAR_SUBSTRATE_CONFIG_KEYS).toContain("SIDECAR_TOKEN");
  });
});

describe("createSidecarSubstrateFactory", () => {
  test("returns bindings whose substrate fires the hub sink on a workflow-run writeTreePreservingPrefix", async () => {
    const recording = createRecordingBareStore();
    const sinkCalls: {
      agentAddress: string;
      repoId: RepoId;
      ref: string;
      commitSha: string;
    }[] = [];
    const sink: ChildHubPackSink = {
      async pushWorkflowRunPack(opts) {
        sinkCalls.push({
          agentAddress: opts.agentAddress,
          repoId: opts.repoId,
          ref: opts.ref,
          commitSha: opts.commitSha,
        });
      },
    };

    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: () => sink,
    });
    const bindings = await factory(makeFactoryEnv());

    const repoId: RepoId = { kind: "workflow-run", id: "dep-1" };
    await bindings.substrate.writeTreePreservingPrefix(
      { kind: "supervisor" },
      repoId,
      "refs/heads/main",
      {
        preservePrefix: "runs/r-1/events/",
        merge: async () => ({ "runs/r-1/events/0.json": "{}" }),
        message: "append RunStarted",
      },
    );

    expect(recording.preserveCalls).toHaveLength(1);
    expect(recording.preserveCalls[0]?.repoId.kind).toBe("workflow-run");
    expect(sinkCalls).toHaveLength(1);
    expect(sinkCalls[0]?.agentAddress).toBe("agent-1@example.com");
    expect(sinkCalls[0]?.repoId.id).toBe("dep-1");
    expect(sinkCalls[0]?.ref).toBe("refs/heads/main");
    expect(sinkCalls[0]?.commitSha).toBe("stub-pack-sha");
    expect(recording.packs).toHaveLength(1);
    expect(recording.packs[0]?.principal.kind).toBe("supervisor");
  });

  test("non-workflow-run writeTreePreservingPrefix calls bypass the hub sink", async () => {
    const recording = createRecordingBareStore();
    const sinkCalls: { agentAddress: string }[] = [];
    const sink: ChildHubPackSink = {
      async pushWorkflowRunPack(opts) {
        sinkCalls.push({ agentAddress: opts.agentAddress });
      },
    };

    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: () => sink,
    });
    const bindings = await factory(makeFactoryEnv());

    const agentStateRepoId: RepoId = {
      kind: "agent-state",
      id: "agent-1",
    };
    await bindings.substrate.writeTreePreservingPrefix(
      { kind: "hub" },
      agentStateRepoId,
      "refs/heads/deploy",
      {
        preservePrefix: "deploy/",
        merge: async () => ({ "deploy/prompt.md": "hi" }),
        message: "deploy",
      },
    );

    expect(recording.preserveCalls).toHaveLength(1);
    expect(sinkCalls).toEqual([]);
    expect(recording.packs).toHaveLength(0);
  });

  test("rejects when a hub-connection substrate-config key is missing", async () => {
    const recording = createRecordingBareStore();
    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: () => ({
        pushWorkflowRunPack: () => Promise.resolve(),
      }),
    });
    const env: SubstrateFactoryEnv = {
      spawn: makeSpawnEnv(),
      substrateConfig: {
        ...makeSubstrateConfig(),
        HUB_WS_URL: "",
      },
      packPushBridge: makeStubPackPushBridge(),
    };
    await expect(factory(env)).rejects.toThrow(/HUB_WS_URL/);
  });

  test("forwards HUB_WS_URL, SIDECAR_ID, and SIDECAR_TOKEN to createHubPackSink", async () => {
    const recording = createRecordingBareStore();
    const observed: {
      hubWsUrl: string;
      sidecarId: string;
      sidecarToken: string;
    }[] = [];
    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: (config) => {
        observed.push(config);
        return { pushWorkflowRunPack: () => Promise.resolve() };
      },
    });
    await factory(
      makeFactoryEnv({
        HUB_WS_URL: "ws://h.example/s",
        SIDECAR_ID: "sc-2",
        SIDECAR_TOKEN: "tok-xyz",
      }),
    );
    expect(observed).toHaveLength(1);
    expect(observed[0]?.hubWsUrl).toBe("ws://h.example/s");
    expect(observed[0]?.sidecarId).toBe("sc-2");
    expect(observed[0]?.sidecarToken).toBe("tok-xyz");
  });

  test("surfaces the hub sink's rejection when a workflow-run push fails", async () => {
    const recording = createRecordingBareStore();
    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: () => ({
        pushWorkflowRunPack: () =>
          Promise.reject(new Error("hub link rejected pack")),
      }),
    });
    const bindings = await factory(makeFactoryEnv());

    await expect(
      bindings.substrate.writeTreePreservingPrefix(
        { kind: "supervisor" },
        { kind: "workflow-run", id: "dep-1" },
        "refs/heads/main",
        {
          preservePrefix: "runs/r-1/events/",
          merge: async () => ({}),
          message: "append",
        },
      ),
    ).rejects.toThrow(/hub link rejected pack/);
  });

  test("rejects at construction when STEP_INFERENCE_SOURCES is malformed JSON", async () => {
    const recording = createRecordingBareStore();
    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: () => ({
        pushWorkflowRunPack: () => Promise.resolve(),
      }),
    });
    await expect(
      factory(
        makeFactoryEnv({
          STEP_INFERENCE_SOURCES: "{not-json",
        }),
      ),
    ).rejects.toThrow(/STEP_INFERENCE_SOURCES is not valid JSON/);
  });

  test("rejects at construction when STEP_INFERENCE_SOURCES is empty", async () => {
    const recording = createRecordingBareStore();
    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: () => ({
        pushWorkflowRunPack: () => Promise.resolve(),
      }),
    });
    await expect(
      factory(
        makeFactoryEnv({
          STEP_INFERENCE_SOURCES: "",
        }),
      ),
    ).rejects.toThrow(/STEP_INFERENCE_SOURCES/);
  });

  test("rejects at construction when STEP_INFERENCE_SOURCES carries a malformed source entry", async () => {
    const recording = createRecordingBareStore();
    const factory = createSidecarSubstrateFactory({
      createBareRepoStore: () => recording.store,
      createHubPackSink: () => ({
        pushWorkflowRunPack: () => Promise.resolve(),
      }),
    });
    const broken = JSON.stringify({
      "step-1": {
        // missing required InferenceSource fields (provider, model, ...)
        id: "broken",
      },
    });
    await expect(
      factory(
        makeFactoryEnv({
          STEP_INFERENCE_SOURCES: broken,
        }),
      ),
    ).rejects.toThrow(/STEP_INFERENCE_SOURCES failed validation/);
  });
});

describe("parseStepInferenceSources", () => {
  test("parses and validates a well-formed source table", () => {
    const raw = JSON.stringify(DEFAULT_STEP_SOURCES);
    const parsed = parseStepInferenceSources(raw);
    expect(parsed["step-1"]).toEqual(STEP_SOURCE_ONE);
    expect(parsed["step-2"]).toEqual(STEP_SOURCE_TWO);
  });

  test("rejects malformed JSON with a structured error", () => {
    expect(() => parseStepInferenceSources("{not-json")).toThrow(
      /STEP_INFERENCE_SOURCES is not valid JSON/,
    );
  });

  test("rejects a non-object JSON root", () => {
    expect(() =>
      parseStepInferenceSources(JSON.stringify(["not", "object"])),
    ).toThrow(/STEP_INFERENCE_SOURCES failed validation/);
  });

  test("rejects entries missing required InferenceSource fields", () => {
    const broken = JSON.stringify({ "step-1": { id: "x", provider: "p" } });
    expect(() => parseStepInferenceSources(broken)).toThrow(
      /STEP_INFERENCE_SOURCES failed validation/,
    );
  });
});

describe("createStepInferenceSourceResolver", () => {
  test("returns the pinned source for each known stepId", () => {
    const resolve = createStepInferenceSourceResolver(DEFAULT_STEP_SOURCES);
    expect(resolve("step-1")).toEqual(STEP_SOURCE_ONE);
    expect(resolve("step-2")).toEqual(STEP_SOURCE_TWO);
  });

  test("throws naming the missing stepId when absent from the table", () => {
    const resolve = createStepInferenceSourceResolver(DEFAULT_STEP_SOURCES);
    expect(() => resolve("step-missing")).toThrow(
      /no InferenceSource pinned for stepId "step-missing"/,
    );
  });
});

describe("createSidecarStepBuildEnv", () => {
  function makeReq(stepId: string | undefined): StepInvokeRequest {
    const authzContext: AuthorizeContext =
      stepId === undefined ? {} : { stepId, attempt: 1, runId: "run-1" };
    return {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; buildEnv does not touch the agent definition
      agent: {} as AgentDefinition<BaseEnv>,
      input: "ignored",
      authzContext,
      signal: new AbortController().signal,
    };
  }

  test("returns the per-step source for each stepId in the pinned table", async () => {
    const buildEnv = createSidecarStepBuildEnv(DEFAULT_STEP_SOURCES);
    const envOne = await buildEnv(makeReq("step-1"));
    const envTwo = await buildEnv(makeReq("step-2"));
    expect(envOne.source).toEqual(STEP_SOURCE_ONE);
    expect(envTwo.source).toEqual(STEP_SOURCE_TWO);
  });

  test("throws naming the missing stepId when not in the table", async () => {
    const buildEnv = createSidecarStepBuildEnv(DEFAULT_STEP_SOURCES);
    await expect(buildEnv(makeReq("step-missing"))).rejects.toThrow(
      /no InferenceSource pinned for stepId "step-missing"/,
    );
  });

  test("throws when AuthorizeContext.stepId is absent", async () => {
    const buildEnv = createSidecarStepBuildEnv(DEFAULT_STEP_SOURCES);
    await expect(buildEnv(makeReq(undefined))).rejects.toThrow(
      /AuthorizeContext\.stepId is required/,
    );
  });

  test("non-source StepEnvBase slots are throwing stubs (storage, audit, directors)", async () => {
    const buildEnv = createSidecarStepBuildEnv(DEFAULT_STEP_SOURCES);
    const env = await buildEnv(makeReq("step-1"));
    // The slots are typed objects whose every read trips the Proxy's
    // throwing get-trap. We probe each slot via a property the
    // contract actually carries so the access is well-typed.
    expect(() => {
      void env.storage.load;
    }).toThrow(/storage slot is not wired/);
    expect(() => {
      void env.audit.commitAudit;
    }).toThrow(/audit slot is not wired/);
    expect(() => {
      void env.directors.resolve;
    }).toThrow(/directors slot is not wired/);
    expect(env.workdir).toMatch(/__sidecar_workflow_child_workdir_not_wired__/);
  });
});

// =========================================================================
// createSidecarRunChild
//
// In-process child-workflow recursion. The sidecar's substrate factory
// wires the `runChild` callback the spawn-child adapter delegates to.
// The callback constructs a child WorkflowRuntimeEnv against the same
// workflow-run substrate the parent runs in, scopes every write under
// `runs/<childRunId>/...` (sub-namespace scoping via the runtime
// body's per-call `runId` argument), and invokes `runtimeRun` to drive
// the child to a terminal phase.
//
// The tests here use a stub `RepoStore` (a thin in-memory layer that
// records writes by path) so the createSidecarRunChild callback's
// behavior is observable end-to-end without relying on the production
// workflow-run kind handler's append-time seq enforcement (which is
// out of scope for this task's allowed files). The stub's
// writeTreePreservingPrefix passes the merge callback through and
// stores the resulting blob set keyed by path, so the test can read
// back which `runs/<runId>/events/...` keys the runtime wrote to.
// =========================================================================

const WORKFLOW_PROCESS_PRINCIPAL: Principal = { kind: "workflow-process" };

const runChildTempDirs: string[] = [];

afterAll(async () => {
  for (const d of runChildTempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

async function makeRunChildTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  runChildTempDirs.push(d);
  return d;
}

function makeTrivialAgent(): AgentDefinition<BaseEnv> {
  // The agent definition is only carried into the step invoker as a
  // tag; the test's injected invokeStep does not call the agent.
  return defineAgent({
    id: "trivial",
    systemPrompt: "you are trivial",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

function makeNoopScheduler(): {
  scheduleIn: (runId: string, timerId: string, fireAt: Date) => () => void;
} {
  // The trivial fixture's single step does not schedule timers, but
  // the WorkflowRuntimeEnv shape requires a scheduler. A no-op is
  // sufficient: it never fires, never disposes, never observes the
  // runtime body's awaitNext.
  return {
    scheduleIn: () => () => {
      /* no-op */
    },
  };
}

/**
 * In-memory stub `RepoStore` (hub-sessions shape) that records every
 * `writeTreePreservingPrefix` call's resulting blob set keyed by path.
 * The stub honors the merge callback's return value verbatim so the
 * runtime adapter's seq-monotonic check still runs against the stub's
 * recorded set; the difference from the production substrate is that
 * the stub does NOT enforce the workflow-run kind handler's
 * validatePush gate, which lets the runtime body's seq starting at 1
 * (state.lastSeq=0 + 1) coexist with the adapter's
 * first-merge-callback-sees-empty-then-passes-seq-1 path.
 */
function createStubRunSubstrate(rootDir: string): RepoStore {
  // path -> bytes per repoId+ref pair. The stub stores one tree per
  // `${repoId.kind}/${repoId.id}/${ref}` namespace.
  const trees = new Map<string, Map<string, string | Uint8Array>>();
  function treeKey(repoId: RepoId, ref: string): string {
    return `${repoId.kind}/${repoId.id}/${ref}`;
  }
  function getTree(
    repoId: RepoId,
    ref: string,
  ): Map<string, string | Uint8Array> {
    const k = treeKey(repoId, ref);
    let tree = trees.get(k);
    if (tree === undefined) {
      tree = new Map();
      trees.set(k, tree);
    }
    return tree;
  }
  const stub: Partial<RepoStore> = {
    getRepoDir(repoId: RepoId): string {
      return path.join(rootDir, repoId.kind, repoId.id);
    },
    async writeTreePreservingPrefix(_principal, repoId, ref, args) {
      const tree = getTree(repoId, ref);
      // The merge callback receives a copy of the existing prefix
      // entries (matching the production substrate's documented
      // semantics). The runtime adapter only ever reads the keys to
      // compute the next seq, so passing the full tree is fine here.
      const existing = new Map<string, Uint8Array>();
      for (const [k, v] of tree) {
        if (k.startsWith(args.preservePrefix)) {
          existing.set(k, typeof v === "string" ? Buffer.from(v) : v);
        }
      }
      const next = await args.merge(existing);
      // Replace every preservePrefix-rooted entry with the merge
      // result. Files outside the prefix are left untouched.
      for (const k of Array.from(tree.keys())) {
        if (k.startsWith(args.preservePrefix)) {
          tree.delete(k);
        }
      }
      for (const [k, v] of Object.entries(next)) {
        tree.set(k, v);
      }
      // Mirror the writes to disk under getRepoDir so a downstream
      // `read` via the production adapter's `fs.readFile` path can
      // observe the events.
      const repoDir = path.join(rootDir, repoId.kind, repoId.id);
      for (const [k, v] of Object.entries(next)) {
        const full = path.join(repoDir, k);
        await fs.promises.mkdir(path.dirname(full), { recursive: true });
        await fs.promises.writeFile(
          full,
          typeof v === "string" ? v : Buffer.from(v),
        );
      }
      return { commitSha: "stub-commit-sha" };
    },
    subscribe(
      _principal: Principal,
      _repoId: RepoId,
      _ref: string,
      _opts: {
        signal: AbortSignal;
        from: "head" | { seq: number };
        bufferLimit?: number;
      },
    ): AsyncIterableIterator<{ seq: number; event: unknown }> {
      // The trivial fixture's single step does not subscribe to the
      // run's event log, so an empty iterator is sufficient.
      return (async function* () {
        // empty
      })();
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- in-test stub; unimplemented methods surface via the Proxy fallthrough
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub RepoStore: ${String(prop)} not implemented for createSidecarRunChild tests`,
        );
      };
    },
  });
}

describe("createSidecarRunChild", () => {
  test("runs a trivial child workflow to completion under runs/<childRunId>/events/", async () => {
    const rootDir = await makeRunChildTempDir("sidecar-run-child-completed-");
    const substrate = createStubRunSubstrate(rootDir);
    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: "dep-run-child-1",
    };

    const definition = defineWorkflow({
      id: "child-trivial",
      trigger: { type: "manual" },
      steps: {
        only: step({ agent: makeTrivialAgent() }),
      },
    });

    const observedRequests: StepInvokeRequest[] = [];
    const invokeStep = async (
      req: StepInvokeRequest,
    ): Promise<StepInvokeResult> => {
      observedRequests.push(req);
      return { output: { reply: "ok" } };
    };

    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId,
      workflowRunRef: "refs/heads/main",
      principal: WORKFLOW_PROCESS_PRINCIPAL,
      scheduler: makeNoopScheduler(),
      invokeStep,
    });

    const ctrl = new AbortController();
    const result = await runChild({
      definition,
      definitionRef: "child-trivial",
      childRunId: "child-run-1",
      input: { goal: "trivial" },
      parentRunId: "parent-run-1",
      parentStepId: "step-spawn",
      signal: ctrl.signal,
    });

    expect(result.terminalStatus).toBe("completed");
    // The invokeStep should have been called once with the runtime's
    // AuthorizeContext carrying the child's runId.
    expect(observedRequests).toHaveLength(1);
    expect(observedRequests[0]?.authzContext.runId).toBe("child-run-1");
    expect(observedRequests[0]?.authzContext.stepId).toBe("only");

    // The child's events log lives under the child's runId via the
    // production `createWorkflowRunRepoStore` adapter, which mirrors
    // writes to disk under `runs/<childRunId>/events/<seq>.json` of
    // the workflow-run repo dir. Constructing a sibling adapter
    // against the same substrate reads those events back.
    const verifier = createWorkflowRunRepoStore({
      substrate,
      repoId: workflowRunRepoId,
      principal: WORKFLOW_PROCESS_PRINCIPAL,
      ref: "refs/heads/main",
    });
    const childEvents = await verifier.read("child-run-1");
    const childTypes = childEvents.map((e) => e.kind);
    expect(childTypes).toContain("RunStarted");
    expect(childTypes).toContain("StepStarted");
    expect(childTypes).toContain("StepCompleted");
    expect(childTypes).toContain("RunCompleted");
    // No write landed under the parent's runId -- the runtime is
    // driven by the child's runId alone.
    const parentEvents = await verifier.read("parent-run-1");
    expect(parentEvents).toHaveLength(0);
  });

  test("propagates a mid-flight parent abort as terminalStatus 'cancelled'", async () => {
    const rootDir = await makeRunChildTempDir("sidecar-run-child-cancelled-");
    const substrate = createStubRunSubstrate(rootDir);
    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: "dep-run-child-2",
    };

    const definition = defineWorkflow({
      id: "child-hang",
      trigger: { type: "manual" },
      steps: {
        only: step({ agent: makeTrivialAgent() }),
      },
    });

    // The step's invocation blocks until the abort signal fires; the
    // runtime body's cancel cascade then settles the step with
    // CancelPropagated and the run with RunCancelled.
    const stepEnteredCtrl = new AbortController();
    const invokeStep = async (
      req: StepInvokeRequest,
    ): Promise<StepInvokeResult> => {
      // Mark step entry so the test can fire the parent abort once the
      // runtime is actually inside the step.
      stepEnteredCtrl.abort();
      await new Promise<void>((_resolve, reject) => {
        if (req.signal.aborted) {
          reject(new DOMException("aborted", "AbortError"));
          return;
        }
        req.signal.addEventListener(
          "abort",
          () => {
            reject(new DOMException("aborted", "AbortError"));
          },
          { once: true },
        );
      });
      throw new Error("unreachable: invokeStep should have aborted");
    };

    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId,
      workflowRunRef: "refs/heads/main",
      principal: WORKFLOW_PROCESS_PRINCIPAL,
      scheduler: makeNoopScheduler(),
      invokeStep,
    });

    const ctrl = new AbortController();
    const settled = runChild({
      definition,
      definitionRef: "child-hang",
      childRunId: "child-run-2",
      input: null,
      parentRunId: "parent-run-2",
      parentStepId: "step-spawn",
      signal: ctrl.signal,
    });
    // Wait until the step actually entered before aborting; otherwise
    // the abort lands before the runtime's main loop reached the step
    // and the test races a no-step-yet cancellation.
    await new Promise<void>((resolve) => {
      if (stepEnteredCtrl.signal.aborted) {
        resolve();
        return;
      }
      stepEnteredCtrl.signal.addEventListener("abort", () => resolve(), {
        once: true,
      });
    });
    ctrl.abort();
    const result = await settled;
    expect(result.terminalStatus).toBe("cancelled");
  });

  test("the child runtime threads childRunId into the AuthorizeContext (not parentRunId)", async () => {
    // Sub-namespace scoping is fundamentally about the runtime body's
    // per-call runId argument: the same scope that drives the
    // adapter's `runs/<runId>/events/<seq>.json` path computation
    // surfaces in the AuthorizeContext threaded into every step
    // invocation. Asserting on the AuthorizeContext lets us verify
    // the scoping without depending on the kind handler's append-seq
    // enforcement contract.
    const rootDir = await makeRunChildTempDir("sidecar-run-child-authz-");
    const substrate = createStubRunSubstrate(rootDir);
    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: "dep-run-child-3",
    };

    const definition = defineWorkflow({
      id: "child-authz",
      trigger: { type: "manual" },
      steps: {
        only: step({ agent: makeTrivialAgent() }),
      },
    });

    const captured: AuthorizeContext[] = [];
    const invokeStep = async (
      req: StepInvokeRequest,
    ): Promise<StepInvokeResult> => {
      captured.push(req.authzContext);
      return { output: { reply: "ok" } };
    };

    const runChild = createSidecarRunChild({
      substrate,
      workflowRunRepoId,
      workflowRunRef: "refs/heads/main",
      principal: WORKFLOW_PROCESS_PRINCIPAL,
      scheduler: makeNoopScheduler(),
      invokeStep,
    });

    const ctrl = new AbortController();
    await runChild({
      definition,
      definitionRef: "child-authz",
      childRunId: "child-run-3",
      input: null,
      parentRunId: "parent-run-3",
      parentStepId: "step-spawn",
      signal: ctrl.signal,
    });

    expect(captured).toHaveLength(1);
    expect(captured[0]?.runId).toBe("child-run-3");
    expect(captured[0]?.runId).not.toBe("parent-run-3");
  });
});
