// Many-definitions-from-one-asset end-to-end: one bun/npm workspaces monorepo
// source asset backs SEVERAL workflow definitions under ONE definition asset,
// keyed on (assetId, wireHash). Two facets:
//
//  - DISTINCT: two members with DIFFERENT needs-surfaces (`@wf/one` and
//    `@wf/two`, distinct workflow ids and system prompts) install to TWO
//    definition rows under the one definition asset, deploy independently, and
//    each runs to completion. Each run evaluates its OWN member's source, so the
//    two are distinguishable by the workflow id their install froze and by the
//    system prompt that reaches inference.
//
//  - COLLAPSE: two members with an IDENTICAL needs-surface but a DIFFERENT
//    `packageName` (`@wf/collapse-a` and `@wf/collapse-b`) install to ONE shared
//    definition row -- the wire hashes match, so the (assetId, wireHash) key
//    resolves both to the same definition. The collapse itself is proven by the
//    equal wire hashes and the equal definition ids across the two installs.
//    The second install's carried closure (its own packageName) and its own
//    enumerated inline onTrigger bodies come from re-probing the SECOND
//    member's source, independent of the row it collapses onto -- so today they
//    echo the install input and cannot mis-carry the first member's source.
//    Asserting them is a forward regression guard: keeping no `packageName`
//    column means run-time code is always carried per-install from the probed
//    source, so a future short-circuit that returned the pre-existing row's
//    cached first projection instead of re-probing the second member would flip
//    these assertions.
//
// The two facets share one seeded monorepo (four members under `packages/*`) and
// one definition asset, so the test embodies "one asset backs many definitions"
// directly.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { dirname } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { eq } from "drizzle-orm";

import {
  DEFAULT_ASSET_REF,
  committedReadsToSourceTree,
  deployCodeSourcedWorkflow,
  installAndApproveWorkflowDefinition,
  type RepoId,
} from "@intx/hub-sessions";
import {
  tenant as tenantTable,
  workflowDefinition as workflowDefinitionTable,
} from "@intx/db/schema";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import { generateId } from "@intx/hub-common";
import { onTriggerBodyRef } from "@intx/workflow";
import {
  deriveRunAddress,
  enumerateInertOnTriggerBodies,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import { createTestDb, type TestDb } from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  fireMailTrigger,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";

const TENANT_ID = "tnt_source_monorepo_many";
const CALLER_PRINCIPAL_ID = "prn_source_monorepo_many_creator";
// One definition asset backs every install below; identity is (this asset,
// wireHash), so distinct-surface members get their own row and identical-surface
// members share one.
const DEFINITION_ASSET_ID = "ast_source_monorepo_many_wf";
// The `workflow`-kind asset holding the monorepo source (root + four members).
const SOURCE_ASSET_ID = "ast_source_monorepo_many_src";

const HUB_PRINCIPAL = { kind: "hub" } as const;

// Two distinct-surface members: different workflow ids, step ids, and system
// prompts, each mail-triggered at its own deployment address.
const ONE_PACKAGE_NAME = "@wf/one";
const ONE_WORKFLOW_ID = "wf_one";
const ONE_STEP_ID = "step_one";
const ONE_SYSTEM_PROMPT = "You are the first distinct monorepo workflow agent.";
const ONE_DEPLOYMENT_ID = generateId("workflowRun");
const oneAddress = deriveRunAddress({
  runId: ONE_DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const TWO_PACKAGE_NAME = "@wf/two";
const TWO_WORKFLOW_ID = "wf_two";
const TWO_STEP_ID = "step_two";
const TWO_SYSTEM_PROMPT =
  "You are the second distinct monorepo workflow agent.";
const TWO_DEPLOYMENT_ID = generateId("workflowRun");
const twoAddress = deriveRunAddress({
  runId: TWO_DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// Two collapse members: an IDENTICAL needs-surface (same workflow id, section,
// agent, trigger address) but a different packageName. Their `workflow.mjs` is
// byte-identical; only their package.json `name` differs. A fixed trigger
// address keeps the surface -- and thus the wire hash -- identical across both;
// routing never consults it (the collapse facet is install-only and fires no
// trigger). The container carries an inline onTrigger body so the carried-source
// assertion can enumerate the second member's own bodies.
const COLLAPSE_A_PACKAGE_NAME = "@wf/collapse-a";
const COLLAPSE_B_PACKAGE_NAME = "@wf/collapse-b";
const COLLAPSE_WORKFLOW_ID = "wf_collapse";
const COLLAPSE_SECTION_ID = "section";
const COLLAPSE_BODY_STEP_ID = "work";
const COLLAPSE_ADDRESS = `collapse-shared@${DEPLOYMENT_DOMAIN}`;
const COLLAPSE_BODY_REF = onTriggerBodyRef(
  COLLAPSE_WORKFLOW_ID,
  COLLAPSE_SECTION_ID,
);

const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const repoRoot = path.resolve(import.meta.dir, "..", "..");

// A single-step agent workflow whose id/step/system-prompt/address the caller
// supplies. Exported as a named binding so the child loader's single-definition
// selection finds exactly one WorkflowDefinition.
function distinctEntrySource(spec: {
  workflowId: string;
  stepId: string;
  systemPrompt: string;
  address: string;
}): string {
  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: ${JSON.stringify(`${spec.workflowId}-agent`)},
  systemPrompt: ${JSON.stringify(spec.systemPrompt)},
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(spec.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(spec.address)} },
  steps: {
    ${spec.stepId}: step({ agent }),
  },
});
`;
}

// The onTrigger-container source both collapse members ship, byte-identical. A
// single inline body (one tool-less agent step) so a frozen projection has an
// enumerable onTrigger body.
const collapseEntrySource = `
import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const bodyAgent = defineAgent({
  id: "collapse-body-agent",
  systemPrompt: "You are the collapse onTrigger body agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(COLLAPSE_WORKFLOW_ID)},
  trigger: { type: "mail", to: ${JSON.stringify(COLLAPSE_ADDRESS)} },
  steps: {
    ${COLLAPSE_SECTION_ID}: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(COLLAPSE_ADDRESS)} },
      body: defineWorkflow({
        id: "authored-body",
        trigger: { type: "manual" },
        steps: {
          ${COLLAPSE_BODY_STEP_ID}: step({ agent: bodyAgent }),
        },
      }),
    }),
  },
});
`;

// Bundle a member's entry: inline its `@intx/*` imports to source (the members
// carry no workspace-local dependency, so nothing is left external). Mirrors the
// single-member monorepo e2e's bundler.
async function bundleEntry(
  scratchDir: string,
  entrySource: string,
  basename: string,
): Promise<string> {
  const entrySrcPath = path.join(scratchDir, `${basename}-src.ts`);
  await fs.writeFile(entrySrcPath, entrySource);

  const built = await Bun.build({
    entrypoints: [entrySrcPath],
    target: "bun",
    format: "esm",
    throw: true,
    plugins: [
      {
        name: "resolve-intx-to-source",
        setup(build) {
          build.onResolve({ filter: /^@intx\// }, (args) => {
            const fromDir = args.importer.startsWith(`${repoRoot}${path.sep}`)
              ? dirname(args.importer)
              : repoRoot;
            return { path: Bun.resolveSync(args.path, fromDir) };
          });
        },
      },
    ],
  });

  const artifact = built.outputs[0];
  if (artifact === undefined) {
    throw new Error(
      `bundleEntry: Bun.build produced no output for ${basename}`,
    );
  }
  const code = await artifact.text();
  if (code.includes("@intx/")) {
    throw new Error(
      `bundleEntry: ${basename} bundle still carries a bare @intx import`,
    );
  }
  return code;
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let sourceCommitSha: string;

const sourceRepoId: RepoId = {
  kind: "workflow",
  id: SOURCE_ASSET_ID,
};

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`many-defs e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("many-defs e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

// Install one monorepo member (select it by packageName) through the real
// probe -> gate -> freeze path against the shared definition asset.
async function installMember(spec: {
  packageName: string;
  approvals: ApprovalSet;
}) {
  const source: WorkflowDefinitionAssetSource = {
    kind: "asset",
    assetId: SOURCE_ASSET_ID,
    package: {
      format: "source",
      commitSha: sourceCommitSha,
      packageName: spec.packageName,
    },
  };
  const committed =
    await env.hub.agentRepoStore.repoStore.openCommittedReadsAtCommit(
      HUB_PRINCIPAL,
      sourceRepoId,
      sourceCommitSha,
    );
  if (committed === null) {
    throw new Error("many-defs e2e: could not open committed reads at commit");
  }
  const approved = await installAndApproveWorkflowDefinition({
    source,
    entry: WORKFLOW_ENTRY,
    assetId: DEFINITION_ASSET_ID,
    approvals: spec.approvals,
    router: env.hub.router,
    db: h.db,
    reads: committedReadsToSourceTree(committed),
    registryName: "npmjs",
    registryConfig: { url: "https://registry.test" },
    resolveAttachment,
  });
  const approval = approved.approval;
  if (!approval.ok) {
    throw new Error(
      `many-defs e2e: install/approve gate did not approve ${spec.packageName} ` +
        `(reason: ${approval.reason}): ${JSON.stringify(approval)}\n${env.sidecarDiagnostics()}`,
    );
  }
  return { ...approved, approval };
}

// Deploy an already-installed distinct member by source-ref and fire its mail
// trigger, returning the terminal event once the run settles.
async function deployAndRun(spec: {
  approved: Awaited<ReturnType<typeof installAndApproveWorkflowDefinition>>;
  packageName: string;
  workflowId: string;
  stepId: string;
  anchorRunId: string;
  address: string;
}): Promise<void> {
  const source: WorkflowDefinitionAssetSource = {
    kind: "asset",
    assetId: SOURCE_ASSET_ID,
    package: {
      format: "source",
      commitSha: sourceCommitSha,
      packageName: spec.packageName,
    },
  };
  const inferenceSource = {
    id: "anthropic:mock-model",
    provider: "anthropic",
    baseURL: `http://localhost:${String(env.inference.server.port)}`,
    apiKey: "sk-mock",
    model: "mock-model",
  };
  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: spec.anchorRunId,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: spec.address,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [inferenceSource],
    defaultSource: "anthropic:mock-model",
  };

  await deployCodeSourcedWorkflow({
    approved: spec.approved,
    source,
    resolveAttachment,
    sidecarRouter: env.hub.router,
    agentAddress: spec.address,
    config,
    sources: { [spec.stepId]: [inferenceSource] },
    db: h.db,
    tenantId: TENANT_ID,
    anchorRunId: spec.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
  });

  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: deriveDeploymentId(spec.address),
  };
  env.registerDeployment({
    anchorRunId: spec.anchorRunId,
    workflowDefinition: {
      id: spec.approved.projection.id,
      triggers: [{ type: "mail", to: spec.address }],
      steps: {},
      stepOrder: [...spec.approved.projection.stepOrder],
    },
    workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    mailAddress: spec.address,
  });

  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(spec.address),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  await fireMailTrigger(env, spec.address, {
    messageId: `<source-monorepo-many-${spec.workflowId}@integration.interchange>`,
  });
  const runId = await waitForFirstRunId(env, workflowRunRepoId, {
    timeoutMs: 30_000,
    diagnostics: env.sidecarDiagnostics,
  });
  const terminal = await waitForWorkflowRunComplete(
    env,
    spec.anchorRunId,
    runId,
    { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
  );
  if (terminal.type !== "RunCompleted") {
    throw new Error(
      `many-defs e2e: expected RunCompleted for ${spec.workflowId}, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
    );
  }
}

const distinctApprovals = (address: string): ApprovalSet =>
  new Set<string>([
    "inference.source:anthropic:mock-model",
    "director:@intx/agent/default",
    `mail.address:${address}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
  ]);

// Pull the text out of a captured inference request's `system` field. The mock
// stores the raw request body; the anthropic provider writes `system` as an
// array of `{ type: "text", text }` blocks. Narrows with `in`/`typeof` guards
// rather than assertions so a shape drift yields no text (and the assertion
// fails loud) instead of a cast that lies.
function systemBlockTexts(req: object): string[] {
  if (!("system" in req)) return [];
  const sys: unknown = req.system;
  if (!Array.isArray(sys)) return [];
  const texts: string[] = [];
  for (const block of sys) {
    if (
      typeof block === "object" &&
      block !== null &&
      "text" in block &&
      typeof block.text === "string"
    ) {
      texts.push(block.text);
    }
  }
  return texts;
}

const collapseApprovals: ApprovalSet = new Set<string>([
  "inference.source:anthropic:mock-model",
  "director:@intx/agent/default",
  `mail.address:${COLLAPSE_ADDRESS}`,
  `mail.send:${DEPLOYMENT_DOMAIN}`,
]);

describe("many-definitions-from-one-asset monorepo e2e", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "source-monorepo-many-"),
    );
    const oneJs = await bundleEntry(
      scratchDir,
      distinctEntrySource({
        workflowId: ONE_WORKFLOW_ID,
        stepId: ONE_STEP_ID,
        systemPrompt: ONE_SYSTEM_PROMPT,
        address: oneAddress,
      }),
      "one",
    );
    const twoJs = await bundleEntry(
      scratchDir,
      distinctEntrySource({
        workflowId: TWO_WORKFLOW_ID,
        stepId: TWO_STEP_ID,
        systemPrompt: TWO_SYSTEM_PROMPT,
        address: twoAddress,
      }),
      "two",
    );
    // Both collapse members ship the SAME bundled workflow.mjs; only their
    // package.json name differs.
    const collapseJs = await bundleEntry(
      scratchDir,
      collapseEntrySource,
      "collapse",
    );

    h = await createTestDb();
    await h.db.insert(tenantTable).values({
      id: TENANT_ID,
      name: TENANT_ID,
      slug: TENANT_ID,
      domain: DEPLOYMENT_DOMAIN,
      parentId: null,
    });
    await seedPrincipal(h.db, {
      id: CALLER_PRINCIPAL_ID,
      tenantId: TENANT_ID,
      kind: "user",
    });
    await seedAsset(h.db, {
      id: DEFINITION_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "source-monorepo-many-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    env = await startDeployFlowEnv({});

    // Seed the monorepo: a private workspaces root plus four members -- two
    // distinct-surface workflows and two identical-surface collapse workflows.
    await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
    const mkPackageJson = (name: string): string =>
      JSON.stringify({
        name,
        version: PACKAGE_VERSION,
        type: "module",
        interchange: { workflow: WORKFLOW_ENTRY },
      });
    const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
      HUB_PRINCIPAL,
      sourceRepoId,
      DEFAULT_ASSET_REF,
      {
        files: {
          "package.json": JSON.stringify({
            name: "@wf/monorepo-many-root",
            private: true,
            workspaces: ["packages/*"],
          }),
          "packages/one/package.json": mkPackageJson(ONE_PACKAGE_NAME),
          "packages/one/workflow.mjs": oneJs,
          "packages/two/package.json": mkPackageJson(TWO_PACKAGE_NAME),
          "packages/two/workflow.mjs": twoJs,
          "packages/collapse-a/package.json": mkPackageJson(
            COLLAPSE_A_PACKAGE_NAME,
          ),
          "packages/collapse-a/workflow.mjs": collapseJs,
          "packages/collapse-b/package.json": mkPackageJson(
            COLLAPSE_B_PACKAGE_NAME,
          ),
          "packages/collapse-b/workflow.mjs": collapseJs,
        },
        message: "Seed many-definitions monorepo source",
      },
    );
    sourceCommitSha = writeResult.commitSha;
  });

  afterAll(async () => {
    if (env !== undefined) await env.teardown();
    if (h !== undefined) await h.close();
    if (scratchDir !== undefined) {
      await fs.rm(scratchDir, { recursive: true, force: true });
    }
  });

  test("two distinct-surface members install to two rows and each deploys and runs", async () => {
    const one = await installMember({
      packageName: ONE_PACKAGE_NAME,
      approvals: distinctApprovals(oneAddress),
    });
    const two = await installMember({
      packageName: TWO_PACKAGE_NAME,
      approvals: distinctApprovals(twoAddress),
    });

    // Distinct surfaces freeze to distinct definitions, each pinning its own
    // member.
    expect(one.projection.id).toBe(ONE_WORKFLOW_ID);
    expect(two.projection.id).toBe(TWO_WORKFLOW_ID);
    expect(two.approval.definitionId).not.toBe(one.approval.definitionId);
    expect(one.closure.topLevel).toEqual([
      { name: ONE_PACKAGE_NAME, version: PACKAGE_VERSION },
    ]);
    expect(two.closure.topLevel).toEqual([
      { name: TWO_PACKAGE_NAME, version: PACKAGE_VERSION },
    ]);

    // One definition asset now backs both distinct rows. Assert membership of
    // the two ids rather than a total count, so the check does not couple to
    // whether the collapse test (which adds a third row to the same asset) has
    // run yet.
    const rowIds = new Set(
      (
        await h.db
          .select({ id: workflowDefinitionTable.id })
          .from(workflowDefinitionTable)
          .where(eq(workflowDefinitionTable.assetId, DEFINITION_ASSET_ID))
      ).map((r) => r.id),
    );
    expect(rowIds.has(one.approval.definitionId)).toBe(true);
    expect(rowIds.has(two.approval.definitionId)).toBe(true);

    // Deploy and run each independently; both reach RunCompleted from their own
    // source-materialized closure.
    await deployAndRun({
      approved: one,
      packageName: ONE_PACKAGE_NAME,
      workflowId: ONE_WORKFLOW_ID,
      stepId: ONE_STEP_ID,
      anchorRunId: ONE_DEPLOYMENT_ID,
      address: oneAddress,
    });
    await deployAndRun({
      approved: two,
      packageName: TWO_PACKAGE_NAME,
      workflowId: TWO_WORKFLOW_ID,
      stepId: TWO_STEP_ID,
      anchorRunId: TWO_DEPLOYMENT_ID,
      address: twoAddress,
    });

    // Each child evaluated its OWN member's source: both members' system prompts
    // reached inference, proving the two runs did not share one definition's
    // code (and that each step's agent prompt overrode the deploy fallback). The
    // anthropic provider serializes the system prompt as a `system` array of
    // text blocks on the wire.
    const systemTexts = env.inference.requests.flatMap(systemBlockTexts);
    expect(systemTexts.some((t) => t.includes(ONE_SYSTEM_PROMPT))).toBe(true);
    expect(systemTexts.some((t) => t.includes(TWO_SYSTEM_PROMPT))).toBe(true);
  }, 180_000);

  test("two identical-surface members collapse to one row that carries the second member's own source", async () => {
    const a = await installMember({
      packageName: COLLAPSE_A_PACKAGE_NAME,
      approvals: collapseApprovals,
    });
    const b = await installMember({
      packageName: COLLAPSE_B_PACKAGE_NAME,
      approvals: collapseApprovals,
    });

    // (a) The collapse itself: identical needs-surfaces yield an equal wire hash
    // and the (assetId, wireHash) key resolves both installs to the SAME
    // definition row. These two equalities are what discriminate the collapse --
    // they fail if the second install forks a second row.
    expect(a.projection.id).toBe(COLLAPSE_WORKFLOW_ID);
    expect(b.projection.id).toBe(COLLAPSE_WORKFLOW_ID);
    expect(b.approval.approvedWireHash).toBe(a.approval.approvedWireHash);
    expect(b.approval.definitionId).toBe(a.approval.definitionId);

    // (b) Forward regression guard: the second install's carried closure is
    // re-probed from the SECOND member's own source, independent of the row it
    // collapses onto, so today it names @wf/collapse-b (its packageName) and
    // never @wf/collapse-a. This cannot mis-carry the first member's source at
    // present -- it pins that a future install short-circuit which resolved
    // source from the shared row (returning A's cached projection) would break.
    expect(b.closure.topLevel).toEqual([
      { name: COLLAPSE_B_PACKAGE_NAME, version: PACKAGE_VERSION },
    ]);
    const bEntry = b.closure.entries.find(
      (e) => e.name === COLLAPSE_B_PACKAGE_NAME,
    );
    if (bEntry?.source.kind !== "asset") {
      throw new Error("many-defs e2e: collapse-b entry is not asset-sourced");
    }
    expect(bEntry.source.package.format).toBe("source");
    // A must NOT appear in B's carried closure.
    expect(
      b.closure.entries.some((e) => e.name === COLLAPSE_A_PACKAGE_NAME),
    ).toBe(false);

    // The collapsed second install still enumerates a well-formed inline
    // onTrigger body from its frozen projection (the same forward guard: a
    // short-circuit returning a bodiless cached projection would drop it). The
    // ref derives from the shared workflow id + section, so this checks
    // well-formedness, not provenance -- provenance is pinned by topLevel above.
    const bBodies = enumerateInertOnTriggerBodies(b.projection);
    expect(bBodies.map((body) => body.ref)).toEqual([COLLAPSE_BODY_REF]);
  }, 180_000);
});
