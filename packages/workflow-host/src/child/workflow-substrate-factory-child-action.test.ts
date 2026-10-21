// An action step inside a spawned body runs through that body's own env.
//
// `buildChildRunEnv` is the env both `createSidecarSpawnSuspendableChild`
// (an onTrigger body) and `createSidecarRunChild` (a childWorkflow) build.
// These tests drive a real on-disk substrate and a closure whose
// `interchange.actions` module performs one effect. The effect is proven by
// a marker file the handler writes inside `ctx.perform`, so a completed
// action that never performed the effect cannot pass.
//
// The denied capability is declared on the action and allowed on the parent
// grants file, and the injected collector leaves it out. The child's capped
// authorize must refuse it. A body that reaches an action and has no closure
// directory must reject at spawn, before the handler runs.

import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { actionConsumer, evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import {
  builtinCredentialProviders,
  createCredentialProviderRegistry,
} from "@intx/harness";
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
  action,
  escalation,
  loop,
  createInMemoryScheduler,
  createInMemoryRepoStore,
  defineWorkflow,
  type WorkflowDefinition,
  type WorkflowEvent,
} from "@intx/workflow";

import { createWorkflowRunRepoStore, runGrantsPath } from "@intx/workflow-host";

import {
  createSidecarRunChild,
  createSidecarSpawnSuspendableChild,
  type SidecarChildStepInvoker,
} from "./substrate-factory";

const REF = "refs/heads/main";
const DEPLOYMENT_ID = "deployment-child-action";
const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: DEPLOYMENT_ID,
};
const allowAll: AuthorizeFn = () => ({ allowed: true });
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: DEPLOYMENT_ID,
};

const SHIP_CAPABILITY = "ship";
const OTHER_CAPABILITY = "other";
const SHIP_RESOURCE = `effect:${SHIP_CAPABILITY}`;
const OTHER_RESOURCE = `effect:${OTHER_CAPABILITY}`;
const ALLOW_BODY_ID = "body-allow";
const DENY_BODY_ID = "body-deny";
const CHILD_BODY_ID = "child-allow";
const CREDENTIAL_BODY_ID = "body-credential";
const CREDENTIAL_LOOP_BODY_ID = "body-credential-loop";
const CREDENTIAL_LOOP_ITERATION_ID = "body-credential-loop-iteration";
const CREDENTIAL_HANDLE = "api-token";
const CREDENTIAL_ID = "cred-child-action";
const RESOLVE_CONSUMER = actionConsumer("resolveToken");

const ACTIONS_SOURCE = `
import { writeFile } from "node:fs/promises";

export async function ship(input, ctx) {
  if (input === null || typeof input !== "object") {
    throw new Error("ship input must be an object");
  }
  const marker = input.marker;
  const capability = input.capability;
  if (typeof marker !== "string" || typeof capability !== "string") {
    throw new Error("ship input needs marker and capability strings");
  }
  return ctx.perform({
    effectId: "ship-once",
    capability,
    run: async () => {
      await writeFile(marker, "shipped");
      return { shipped: true };
    },
  });
}

export async function resolveToken(input, ctx) {
  const marker = input.marker;
  const capability = input.capability;
  if (typeof marker !== "string" || typeof capability !== "string") {
    throw new Error("resolveToken input needs marker and capability strings");
  }
  return ctx.perform({
    effectId: "resolve-once",
    capability,
    run: async ({ credentials }) => {
      const handle = await credentials.resolve("api-token");
      await writeFile(marker, handle.kind);
      return { kind: handle.kind };
    },
  });
}

export function once(_output, currentInput) {
  return currentInput.pass === 0;
}

export function next(_output, currentInput) {
  return { ...currentInput, pass: currentInput.pass + 1 };
}
`;

const tempDirs: string[] = [];
let signingKey: KeyPair;
let closurePackageDir: string;
// Action bodies carry no agent, so the delivered source table is empty and
// this directory is never read. It still has to be a real path: the factory
// requires one for the legacy sources fallback.
let sourcesDataDir: string;

beforeAll(async () => {
  signingKey = await generateKeyPair();
  closurePackageDir = await writeClosure();
  sourcesDataDir = await makeTempDir("child-action-sources-");
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

async function writeClosure(): Promise<string> {
  const packageDir = await makeTempDir("child-action-closure-");
  await fs.promises.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(
      {
        name: "@fixture/child-action",
        version: "1.0.0",
        interchange: { actions: "./actions.js", loops: "./actions.js" },
      },
      null,
      2,
    ),
  );
  await fs.promises.writeFile(
    path.join(packageDir, "actions.js"),
    ACTIONS_SOURCE,
  );
  return packageDir;
}

function grant(resource: string): GrantRule {
  return {
    id: `grant-${resource}-invoke`,
    resource,
    action: "invoke",
    effect: "allow",
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

function actionBody(
  id: string,
  requires: readonly string[],
): WorkflowDefinition {
  return defineWorkflow({
    id,
    trigger: { type: "manual" },
    steps: {
      ship: action({
        handler: "ship",
        input: { from: "trigger.payload" },
        effect: { requires },
      }),
    },
  });
}

const evaluateGrantsAdapter: Parameters<
  typeof createSidecarRunChild
>[0]["evaluateGrants"] = async ({ resource, action: grantAction, grants }) => {
  const result = await evaluateGrants(
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the snapshot's grants are typed unknown[] at the workflow-host boundary; the sidecar owns the GrantRule grammar, so the seeded rows narrow here
    [...(grants as readonly GrantRule[])],
    resource,
    grantAction,
    { conditions: "pessimistic" },
  );
  return { effect: result.effect, matchingGrants: [], resolvedBy: null };
};

function keepInheritedGrant(
  grantRule: unknown,
  declared: ReadonlySet<string>,
): boolean {
  if (typeof grantRule !== "object" || grantRule === null) return true;
  if (!("effect" in grantRule) || !("resource" in grantRule)) return true;
  if (grantRule.effect !== "allow") return true;
  return (
    typeof grantRule.resource === "string" && declared.has(grantRule.resource)
  );
}

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

const refuseStep: SidecarChildStepInvoker = () => {
  throw new Error("action body: no agent step should run");
};

function makeDeps(
  substrate: ReturnType<typeof createRepoStore>,
  packageDir: string | undefined,
): Parameters<typeof createSidecarRunChild>[0] {
  return {
    substrate,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    workflowRunRef: REF,
    principal: PRINCIPAL,
    scheduler: createInMemoryScheduler({
      repoStore: createInMemoryRepoStore(),
      clock: () => new Date(),
    }),
    invokeStep: refuseStep,
    evaluateGrants: evaluateGrantsAdapter,
    dataDir: sourcesDataDir,
    bodySources: {
      [ALLOW_BODY_ID]: {},
      [DENY_BODY_ID]: {},
      [CHILD_BODY_ID]: {},
      [CREDENTIAL_BODY_ID]: {},
      [CREDENTIAL_LOOP_BODY_ID]: {},
      [CREDENTIAL_LOOP_ITERATION_ID]: {},
    },
    credentialProviders: createCredentialProviderRegistry(
      builtinCredentialProviders(),
    ),
    ...(packageDir !== undefined ? { closurePackageDir: packageDir } : {}),
    collectDeclaredResources: () => new Set([SHIP_RESOURCE]),
    collectDeclaredCredentialConsumers: () => new Set<string>(),
    filterGrantsToDeclaredResources: (parentGrants, declared) =>
      parentGrants.filter((grantRule) =>
        keepInheritedGrant(grantRule, declared),
      ),
  };
}

function reader(substrate: ReturnType<typeof createRepoStore>) {
  return createWorkflowRunRepoStore({
    substrate,
    repoId: WORKFLOW_RUN_REPO_ID,
    principal: PRINCIPAL,
    ref: REF,
  });
}

async function markerPath(): Promise<string> {
  return path.join(await makeTempDir("child-action-marker-"), "marker");
}

describe("spawned body action steps", () => {
  test("runs a declared effect inside an onTrigger body and completes", async () => {
    const substrate = await makeSubstrate("child-action-allow-");
    const parentRunId = "run-parent-allow";
    await seedRunGrants(substrate, parentRunId, [grant(SHIP_RESOURCE)]);
    const marker = await markerPath();
    const spawn = createSidecarSpawnSuspendableChild(
      makeDeps(substrate, closurePackageDir),
    );

    const handle = await spawn(
      {
        definition: actionBody(ALLOW_BODY_ID, [SHIP_CAPABILITY]),
        definitionRef: REF,
        childRunId: "run-body-allow",
        input: { marker, capability: SHIP_CAPABILITY },
        parentRunId,
        parentStepId: "section",
        signal: new AbortController().signal,
        depth: 0,
        maxChildSpawnDepth: 32,
      },
      () => undefined,
    );

    const terminal = await handle.next();
    expect(terminal.kind).toBe("terminal");
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("completed");
    expect(await fs.promises.readFile(marker, "utf8")).toBe("shipped");
  });

  test("denies an effect the body did not declare even when the parent granted it", async () => {
    const substrate = await makeSubstrate("child-action-deny-");
    const parentRunId = "run-parent-deny";
    await seedRunGrants(substrate, parentRunId, [
      grant(SHIP_RESOURCE),
      grant(OTHER_RESOURCE),
    ]);
    const marker = await markerPath();
    const childRunId = "run-body-deny";
    const spawn = createSidecarSpawnSuspendableChild(
      makeDeps(substrate, closurePackageDir),
    );

    const handle = await spawn(
      {
        definition: actionBody(DENY_BODY_ID, [
          SHIP_CAPABILITY,
          OTHER_CAPABILITY,
        ]),
        definitionRef: REF,
        childRunId,
        input: { marker, capability: OTHER_CAPABILITY },
        parentRunId,
        parentStepId: "section",
        signal: new AbortController().signal,
        depth: 0,
        maxChildSpawnDepth: 32,
      },
      () => undefined,
    );

    const terminal = await handle.next();
    expect(terminal.kind).toBe("terminal");
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("failed");

    const events: readonly WorkflowEvent[] =
      await reader(substrate).read(childRunId);
    const stepFailed = events.find((event) => event.kind === "StepFailed");
    if (stepFailed === undefined || stepFailed.kind !== "StepFailed") {
      throw new Error(`body run ${childRunId} has no StepFailed event`);
    }
    expect(stepFailed.error.message).toContain("was not authorized");
    await expect(fs.promises.access(marker)).rejects.toThrow();
  });

  test("runs a declared effect inside a childWorkflow body and completes", async () => {
    const substrate = await makeSubstrate("child-action-child-");
    const parentRunId = "run-parent-child";
    await seedRunGrants(substrate, parentRunId, [grant(SHIP_RESOURCE)]);
    const marker = await markerPath();
    const runChild = createSidecarRunChild(
      makeDeps(substrate, closurePackageDir),
    );

    const result = await runChild(
      {
        definition: actionBody(CHILD_BODY_ID, [SHIP_CAPABILITY]),
        definitionRef: REF,
        childRunId: "run-child-allow",
        input: { marker, capability: SHIP_CAPABILITY },
        parentRunId,
        parentStepId: "spawn",
        signal: new AbortController().signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      },
      () => undefined,
    );

    const event = await result.next();
    expect(event.kind).toBe("terminal");
    if (event.kind !== "terminal") throw new Error("expected a terminal");
    expect(event.terminalStatus).toBe("completed");
    expect(await fs.promises.readFile(marker, "utf8")).toBe("shipped");
  });

  test("rejects an action body when the closure directory is missing", async () => {
    const substrate = await makeSubstrate("child-action-nodir-");
    const parentRunId = "run-parent-nodir";
    await seedRunGrants(substrate, parentRunId, [grant(SHIP_RESOURCE)]);
    const marker = await markerPath();
    const runChild = createSidecarRunChild(makeDeps(substrate, undefined));

    await expect(
      runChild(
        {
          definition: actionBody(ALLOW_BODY_ID, [SHIP_CAPABILITY]),
          definitionRef: REF,
          childRunId: "run-body-nodir",
          input: { marker, capability: SHIP_CAPABILITY },
          parentRunId,
          parentStepId: "spawn",
          signal: new AbortController().signal,
          depth: 1,
          maxChildSpawnDepth: 32,
        },
        () => undefined,
      ),
    ).rejects.toThrow(/deps\.closurePackageDir is missing/);
    await expect(fs.promises.access(marker)).rejects.toThrow();
  });
});

// A body action that declares the `api-token` credential and resolves it.
function credentialActionStep() {
  return action({
    handler: "resolveToken",
    input: { from: "trigger.payload" },
    effect: { requires: [SHIP_CAPABILITY] },
    credentials: [CREDENTIAL_HANDLE],
  });
}

function credentialBody(): WorkflowDefinition {
  return defineWorkflow({
    id: CREDENTIAL_BODY_ID,
    trigger: { type: "manual" },
    steps: { fetch: credentialActionStep() },
  });
}

// The same action one rung down, inside a loop the spawned body runs once.
function credentialLoopBody(): WorkflowDefinition {
  return defineWorkflow({
    id: CREDENTIAL_LOOP_BODY_ID,
    trigger: { type: "manual" },
    steps: {
      rework: loop({
        body: defineWorkflow({
          id: CREDENTIAL_LOOP_ITERATION_ID,
          trigger: { type: "manual" },
          steps: {
            fetch: action({
              handler: "resolveToken",
              input: { from: "trigger.payload" },
              effect: { requires: [SHIP_CAPABILITY] },
              credentials: [CREDENTIAL_HANDLE],
            }),
          },
        }),
        while: "once",
        carry: "next",
        input: { from: "trigger.payload" },
        maxIterations: 2,
        onExhausted: "exhausted",
      }),
      exhausted: escalation({ to: "operator", after: ["rework"] }),
    },
  });
}

function credentialUseGrant(consumer: string): GrantRule {
  return {
    id: `grant-use-${consumer}`,
    resource: `credential:${CREDENTIAL_ID}`,
    action: "use",
    effect: "allow",
    origin: "system",
    conditions: { tool: consumer },
    expiresAt: null,
    roleId: null,
    principalId: null,
  };
}

// The run's live material cell: one http credential bound to the handler.
function credentialCell() {
  return {
    current: {
      bindings: [
        {
          handle: CREDENTIAL_HANDLE,
          credentialId: CREDENTIAL_ID,
          consumer: RESOLVE_CONSUMER,
        },
      ],
      materials: [
        {
          credentialId: CREDENTIAL_ID,
          providerKey: "http",
          origin: "https://api.example.com",
          secret: "sk-child-action",
        },
      ],
    },
  };
}

// The cap a real body run applies: an effect allow survives when the body
// declares it, and a credential allow when its `{ tool }` condition names a
// consumer the body runs.
function credentialDeps(
  substrate: ReturnType<typeof createRepoStore>,
  consumers: ReadonlySet<string> = new Set([RESOLVE_CONSUMER]),
): Parameters<typeof createSidecarRunChild>[0] {
  return {
    ...makeDeps(substrate, closurePackageDir),
    collectDeclaredCredentialConsumers: () => consumers,
    filterGrantsToDeclaredResources: (parentGrants, declared) =>
      parentGrants.filter((grantRule) => {
        if (
          typeof grantRule === "object" &&
          grantRule !== null &&
          "resource" in grantRule &&
          typeof grantRule.resource === "string" &&
          grantRule.resource.startsWith("credential:")
        ) {
          return (
            "conditions" in grantRule &&
            typeof grantRule.conditions === "object" &&
            grantRule.conditions !== null &&
            "tool" in grantRule.conditions &&
            typeof grantRule.conditions.tool === "string" &&
            consumers.has(grantRule.conditions.tool)
          );
        }
        return keepInheritedGrant(grantRule, declared);
      }),
  };
}

function spawnInput(
  definition: WorkflowDefinition,
  childRunId: string,
  parentRunId: string,
  marker: string,
) {
  return {
    definition,
    definitionRef: REF,
    childRunId,
    input: { marker, capability: SHIP_CAPABILITY, pass: 0 },
    parentRunId,
    parentStepId: "section",
    signal: new AbortController().signal,
    depth: 0,
    maxChildSpawnDepth: 32,
  };
}

describe("spawned body action credentials", () => {
  test("an onTrigger body action resolves its declared credential through the capped grants", async () => {
    const substrate = await makeSubstrate("child-action-cred-");
    const parentRunId = "run-parent-cred";
    await seedRunGrants(substrate, parentRunId, [
      grant(SHIP_RESOURCE),
      credentialUseGrant(RESOLVE_CONSUMER),
    ]);
    const marker = await markerPath();
    const spawn = createSidecarSpawnSuspendableChild(credentialDeps(substrate));

    const handle = await spawn(
      spawnInput(credentialBody(), "run-body-cred", parentRunId, marker),
      () => undefined,
      credentialCell(),
    );

    const terminal = await handle.next();
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("completed");
    expect(await fs.promises.readFile(marker, "utf8")).toBe("http");
  });

  test("a childWorkflow body action resolves its declared credential", async () => {
    const substrate = await makeSubstrate("child-action-cred-child-");
    const parentRunId = "run-parent-cred-child";
    await seedRunGrants(substrate, parentRunId, [
      grant(SHIP_RESOURCE),
      credentialUseGrant(RESOLVE_CONSUMER),
    ]);
    const marker = await markerPath();
    const runChild = createSidecarRunChild(credentialDeps(substrate));

    const result = await runChild(
      {
        ...spawnInput(credentialBody(), "run-child-cred", parentRunId, marker),
        parentStepId: "spawn",
        depth: 1,
      },
      () => undefined,
      credentialCell(),
    );

    const event = await result.next();
    if (event.kind !== "terminal") throw new Error("expected a terminal");
    expect(event.terminalStatus).toBe("completed");
    expect(await fs.promises.readFile(marker, "utf8")).toBe("http");
  });

  test("an action inside a loop in a spawned body resolves through the inherited invoker", async () => {
    const substrate = await makeSubstrate("child-action-cred-loop-");
    const parentRunId = "run-parent-cred-loop";
    await seedRunGrants(substrate, parentRunId, [
      grant(SHIP_RESOURCE),
      credentialUseGrant(RESOLVE_CONSUMER),
    ]);
    const marker = await markerPath();
    const spawn = createSidecarSpawnSuspendableChild(credentialDeps(substrate));

    const handle = await spawn(
      spawnInput(
        credentialLoopBody(),
        "run-body-cred-loop",
        parentRunId,
        marker,
      ),
      () => undefined,
      credentialCell(),
    );

    const terminal = await handle.next();
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("completed");
    expect(await fs.promises.readFile(marker, "utf8")).toBe("http");
  });

  test("a credential grant the cap drops leaves the resolve closed", async () => {
    // The parent grant names this handler, so only the cap can withhold it:
    // the body's declared consumer set leaves the handler out, so the cap
    // drops the grant from the child's run grants and Gate 2 refuses.
    const substrate = await makeSubstrate("child-action-cred-deny-");
    const parentRunId = "run-parent-cred-deny";
    await seedRunGrants(substrate, parentRunId, [
      grant(SHIP_RESOURCE),
      credentialUseGrant(RESOLVE_CONSUMER),
    ]);
    const marker = await markerPath();
    const childRunId = "run-body-cred-deny";
    const spawn = createSidecarSpawnSuspendableChild(
      credentialDeps(substrate, new Set()),
    );

    const handle = await spawn(
      spawnInput(credentialBody(), childRunId, parentRunId, marker),
      () => undefined,
      credentialCell(),
    );

    const terminal = await handle.next();
    if (terminal.kind !== "terminal") throw new Error("expected a terminal");
    expect(terminal.terminalStatus).toBe("failed");
    const events: readonly WorkflowEvent[] =
      await reader(substrate).read(childRunId);
    const stepFailed = events.find((event) => event.kind === "StepFailed");
    if (stepFailed === undefined || stepFailed.kind !== "StepFailed") {
      throw new Error(`body run ${childRunId} has no StepFailed event`);
    }
    expect(stepFailed.error.message).toContain(
      `consumer ${RESOLVE_CONSUMER} is not authorized to use credential ${CREDENTIAL_ID}`,
    );
    await expect(fs.promises.access(marker)).rejects.toThrow();
  });
});
