// A spawned child persists the grant array the injected filter returns.
//
// `createSidecarRunChild` reads the parent run's
// `runs/<parentRunId>/grants.json`, calls both collectors with the
// pre-rewrite body, and writes the filter's array to the child's own
// `runs/<childRunId>/grants.json`. An existing child file is read back and
// the collectors are not called again. A child whose parent has no grants
// file fails closed at spawn. The cap itself lives with the capability walk.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { defineAgent } from "@intx/agent";
import {
  builtinCredentialProviders,
  createCredentialProviderRegistry,
} from "@intx/harness";
import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  RepoId,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  createInMemoryScheduler,
  createInMemoryRepoStore,
  defineWorkflow,
  step,
  type StepInvokeResult,
  type WorkflowDefinition,
} from "@intx/workflow";
import { readRunGrants, runGrantsPath } from "@intx/workflow-host";

import {
  createSidecarRunChild,
  type SidecarChildStepInvoker,
} from "./substrate-factory";

const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-child-grants";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: DEPLOYMENT_ID,
};
const allowAll: AuthorizeFn = () => ({ allowed: true });
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: DEPLOYMENT_ID,
};

// The child body's single agent declares this inference source, so the
// capability walk emits it as a grant the child body declares -- a parent
// grant for it survives the cap.
const DECLARED_RESOURCE = "inference.source:anthropic:m";
// The child body declares nothing that covers this, so a parent grant for it
// is dropped from the child's inherited set.
const UNDECLARED_RESOURCE = "tool:parent-only";

const tempDirs: string[] = [];
let signingKey: KeyPair;
// The real child runtime resolves each step's inference source from a staged
// `assets/workflow/<ref>/sources.json`. These tests use a recording invoker
// that ignores the sources, but `buildChildRunEnv` reads the file eagerly, so
// stage a minimal one per child definition id these tests spawn.
let childSourcesDataDir: string;

// These tests assert grant capping, not event emission, so the sink is noop.
const noopOnEvent = (): void => {
  /* the event sink is not asserted in these tests */
};

async function stageChildSources(defId: string): Promise<void> {
  const dir = path.join(childSourcesDataDir, "assets", "workflow", defId);
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.writeFile(
    path.join(dir, "sources.json"),
    JSON.stringify({
      s: [
        {
          id: "anthropic:m",
          provider: "anthropic",
          baseURL: "http://localhost:1",
          credentialId: "sk-x",
          model: "m",
        },
      ],
    }),
  );
}

beforeAll(async () => {
  signingKey = await generateKeyPair();
  childSourcesDataDir = await makeTempDir("child-grants-assets-");
  await stageChildSources("child-wf");
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

function grant(resource: string, action: string): GrantRule {
  return {
    id: `grant-${resource}-${action}`,
    resource,
    action,
    effect: "allow",
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

// The one-step child definition every spawn in these tests runs. Its single
// agent is toolless and declares the anthropic:m inference source, so
// `DECLARED_RESOURCE` is the one grant the child body declares.
const CHILD_STEP_AGENT_ID = "wallet-spend";
function childDefinition(id: string): WorkflowDefinition {
  const agent = defineAgent({
    id: CHILD_STEP_AGENT_ID,
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: { s: step({ agent }) },
  });
}

// Grant evaluator that delegates the decision to `@intx/authz` against the
// credentials snapshot's grants alone. Unlike the production adapter it does
// NOT merge any per-step tool-mark floor grants, so a decision here reflects
// only the capped grant set the child inherited.
const evaluateGrantsAdapter: SidecarRunChildDepsEvaluator = async ({
  resource,
  action,
  grants,
}) => {
  const result = await evaluateGrants(
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the snapshot's grants are typed unknown[] at the workflow-host boundary; the sidecar owns the GrantRule grammar, so the seeded rows narrow here
    [...(grants as readonly GrantRule[])],
    resource,
    action,
  );
  return { effect: result.effect, matchingGrants: [], resolvedBy: null };
};
type SidecarRunChildDepsEvaluator = Parameters<
  typeof createSidecarRunChild
>[0]["evaluateGrants"];

// Build a real on-disk workflow-run substrate and seed its genesis tree.
async function makeSubstrate(
  prefix: string,
): Promise<ReturnType<typeof createRepoStore>> {
  const dataDir = await makeTempDir(prefix);
  const substrate = createRepoStore({
    dataDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: allowAll,
  });
  await substrate.writeTree({ kind: "hub" }, WORKFLOW_RUN_REPO_ID, REF, {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "genesis",
  });
  return substrate;
}

// Seed a run's grants file the way the hub's `run.grants` delivery does: a
// single `runs/<runId>/grants.json` under the workflow-run repo.
async function seedRunGrants(
  substrate: ReturnType<typeof createRepoStore>,
  runId: string,
  grants: readonly GrantRule[],
): Promise<void> {
  await substrate.writeTree({ kind: "hub" }, WORKFLOW_RUN_REPO_ID, REF, {
    files: {
      [runGrantsPath(runId)]: JSON.stringify({ grants }, null, 2),
    },
    message: `seed grants for ${runId}`,
  });
}

// An invoker that authorizes a fixed list of resources against the child's env
// authorize and records each decision, so a test can assert exactly which
// inherited (capped) grants the child holds.
function recordingInvoker(
  record: { decisions: { resource: string; effect: string | null }[] },
  probeResources: readonly string[],
): SidecarChildStepInvoker {
  return async (
    req,
    authorize,
    _sourcesRef,
    _onEvent,
  ): Promise<StepInvokeResult> => {
    for (const resource of probeResources) {
      const decision = await authorize(resource, "invoke", req.authzContext);
      record.decisions.push({ resource, effect: decision.effect });
    }
    return { output: null };
  };
}

function makeRunChild(
  substrate: ReturnType<typeof createRepoStore>,
  invokeStep: SidecarChildStepInvoker,
  grantCap: {
    collectDeclaredResources: (
      definition: WorkflowDefinition,
    ) => ReadonlySet<string>;
    collectDeclaredCredentialConsumers: (
      definition: WorkflowDefinition,
    ) => ReadonlySet<string>;
    filterGrantsToDeclaredResources: (
      parentGrants: readonly unknown[],
      declared: ReadonlySet<string>,
      credentialConsumers: ReadonlySet<string>,
    ) => readonly unknown[];
  },
): ReturnType<typeof createSidecarRunChild> {
  return createSidecarRunChild({
    substrate,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    workflowRunRef: REF,
    principal: PRINCIPAL,
    scheduler: createInMemoryScheduler({
      repoStore: createInMemoryRepoStore(),
      clock: () => new Date(),
    }),
    invokeStep,
    evaluateGrants: evaluateGrantsAdapter,
    dataDir: childSourcesDataDir,
    bodySources: {},
    credentialProviders: createCredentialProviderRegistry(
      builtinCredentialProviders(),
    ),
    collectDeclaredResources: grantCap.collectDeclaredResources,
    collectDeclaredCredentialConsumers:
      grantCap.collectDeclaredCredentialConsumers,
    filterGrantsToDeclaredResources: grantCap.filterGrantsToDeclaredResources,
  });
}

describe("createSidecarRunChild grant capping", () => {
  const complete: SidecarChildStepInvoker = async () => ({ output: null });

  test("calls both collectors with the pre-rewrite body and persists the filter's array", async () => {
    const substrate = await makeSubstrate("child-grants-persist-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant(DECLARED_RESOURCE, "invoke"),
      grant(UNDECLARED_RESOURCE, "invoke"),
    ]);

    const definition = childDefinition("child-wf");
    const seenResources: WorkflowDefinition[] = [];
    const seenConsumers: WorkflowDefinition[] = [];
    const persisted = [grant("tool:from-the-stub", "invoke")];
    const sentinel = new Set<string>(["tool:sentinel-consumer"]);
    let filteredConsumers: ReadonlySet<string> | undefined;
    const runChild = makeRunChild(substrate, complete, {
      collectDeclaredResources: (body) => {
        seenResources.push(body);
        return new Set<string>();
      },
      collectDeclaredCredentialConsumers: (body) => {
        seenConsumers.push(body);
        return sentinel;
      },
      filterGrantsToDeclaredResources: (_parent, _declared, consumers) => {
        filteredConsumers = consumers;
        return persisted;
      },
    });

    const childRunId = "run-child";
    const result = await runChild(
      {
        definition,
        definitionRef: REF,
        childRunId,
        input: null,
        parentRunId,
        parentStepId: "s",
        signal: new AbortController().signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      },
      noopOnEvent,
    );

    expect(result.terminalStatus).toBe("completed");
    expect(seenResources).toEqual([definition]);
    expect(seenConsumers).toEqual([definition]);
    expect(filteredConsumers).toBe(sentinel);
    const childGrants = await readRunGrants({
      repoStore: substrate,
      anchorRunId: DEPLOYMENT_ID,
      runId: childRunId,
    });
    expect(childGrants).toEqual(persisted);
  });

  test("an existing child grants file is read back, not recomputed (write-once)", async () => {
    const substrate = await makeSubstrate("child-grants-write-once-");
    const parentRunId = "run-parent";
    await seedRunGrants(substrate, parentRunId, [
      grant(DECLARED_RESOURCE, "invoke"),
    ]);

    const childRunId = "run-child";
    const sentinel = grant("inference.source:sentinel:preseeded", "invoke");
    await seedRunGrants(substrate, childRunId, [sentinel]);

    const record = {
      decisions: [] as { resource: string; effect: string | null }[],
    };
    let collected = 0;
    let consumersCollected = 0;
    const runChild = makeRunChild(
      substrate,
      recordingInvoker(record, [DECLARED_RESOURCE]),
      {
        collectDeclaredResources: () => {
          collected += 1;
          return new Set<string>();
        },
        collectDeclaredCredentialConsumers: () => {
          consumersCollected += 1;
          return new Set<string>();
        },
        filterGrantsToDeclaredResources: () => {
          throw new Error("filter must not run when the child file exists");
        },
      },
    );

    const result = await runChild(
      {
        definition: childDefinition("child-wf"),
        definitionRef: REF,
        childRunId,
        input: null,
        parentRunId,
        parentStepId: "s",
        signal: new AbortController().signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      },
      noopOnEvent,
    );

    expect(result.terminalStatus).toBe("completed");
    expect(collected).toBe(0);
    expect(consumersCollected).toBe(0);
    const childGrants = await readRunGrants({
      repoStore: substrate,
      anchorRunId: DEPLOYMENT_ID,
      runId: childRunId,
    });
    expect(childGrants).toEqual([sentinel]);
    expect(record.decisions).toEqual([
      { resource: DECLARED_RESOURCE, effect: null },
    ]);
  });

  test("a child whose parent has no grants file fails closed at spawn", async () => {
    const substrate = await makeSubstrate("child-grants-absent-");
    const parentRunId = "run-parent-ungranted";

    const record = {
      decisions: [] as { resource: string; effect: string | null }[],
    };
    let collected = 0;
    let consumersCollected = 0;
    const runChild = makeRunChild(
      substrate,
      recordingInvoker(record, [DECLARED_RESOURCE]),
      {
        collectDeclaredResources: () => {
          collected += 1;
          return new Set<string>();
        },
        collectDeclaredCredentialConsumers: () => {
          consumersCollected += 1;
          return new Set<string>();
        },
        filterGrantsToDeclaredResources: () => {
          throw new Error(
            "filter must not run when the parent file is missing",
          );
        },
      },
    );

    await expect(
      runChild(
        {
          definition: childDefinition("child-wf"),
          definitionRef: REF,
          childRunId: "run-child",
          input: null,
          parentRunId,
          parentStepId: "s",
          signal: new AbortController().signal,
          depth: 1,
          maxChildSpawnDepth: 32,
        },
        noopOnEvent,
      ),
    ).rejects.toThrow(/has no grants file/);
    expect(collected).toBe(0);
    expect(consumersCollected).toBe(0);
    expect(record.decisions).toEqual([]);
  });
});
