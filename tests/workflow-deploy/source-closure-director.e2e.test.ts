// Closure-director end-to-end: a workflow whose agent references a director
// shipped NOT by the workflow's own package but by a package elsewhere in the
// materialized closure. Two origins share one monorepo source asset:
//
//  - MEMBER: `@wf/app-member` depends on the workspace-local sibling
//    `@wf/director-lib` (workspace:*), whose `interchange.directors` module
//    ships `@wf/director-lib/coding`. The agent's director ref resolves
//    against the sibling member's module.
//
//  - EXTERNAL: `@wf/app-ext` depends on `wf-ext-director` served from an
//    in-process npm registry (reached via `SIDECAR_TOOL_REGISTRIES`), whose
//    tarball's `interchange.directors` module ships `wf-ext-director/coding`.
//
// Both prove the same thing end to end: the probe advertises
// `director:<dep id>` for approval, the frozen closure carries the dep
// package, the run-child's registry composes the dep's factory, and the
// custom director (not the built-in default) produces the first inference
// turn -- observable by a marker string each director prepends to the
// system prompt it hands `createDefaultDirector`.

import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { dirname } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import {
  DEFAULT_ASSET_REF,
  committedReadsToSourceTree,
  deployCodeSourcedWorkflow,
  installAndApproveWorkflowDefinition,
  type RepoId,
} from "@intx/hub-sessions";
import { createNoopCredentialCipher } from "@intx/crypto";
import { tenant as tenantTable } from "@intx/db/schema";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import type { Packument, PackumentFetcher } from "@intx/tool-packaging";
import { generateId } from "@intx/hub-common";
import {
  createApprovalSet,
  deriveRunAddress,
  type ApprovalSet,
} from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  buildSyntheticNpmPackageTarball,
  fireMailTrigger,
  readWorkflowRunEvents,
  seedInferenceCredentials,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";

const MEMBER_PACKAGE_NAME = "@wf/app-member";
const MEMBER_LIB_PACKAGE_NAME = "@wf/director-lib";
const MEMBER_DIRECTOR_ID = "@wf/director-lib/coding";
const MEMBER_DIRECTOR_MARKER = "[director @wf/director-lib/coding]";
const MEMBER_WORKFLOW_ID = "wf_closure_director_member";
const MEMBER_STEP_ID = "run";
const MEMBER_DEPLOYMENT_ID = generateId("workflowRun");
const memberAddress = deriveRunAddress({
  runId: MEMBER_DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const EXT_PACKAGE_NAME = "wf-ext-director";
const EXT_PACKAGE_VERSION = "1.0.0";
const EXT_TARBALL_PATH = `/${EXT_PACKAGE_NAME}/-/${EXT_PACKAGE_NAME}-${EXT_PACKAGE_VERSION}.tgz`;
const EXT_APP_PACKAGE_NAME = "@wf/app-ext";
const EXT_DIRECTOR_ID = "wf-ext-director/coding";
const EXT_DIRECTOR_MARKER = "[director wf-ext-director/coding]";
const EXT_WORKFLOW_ID = "wf_closure_director_ext";
const EXT_STEP_ID = "run";
const EXT_DEPLOYMENT_ID = generateId("workflowRun");
const extAddress = deriveRunAddress({
  runId: EXT_DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const REGISTRY_NAME = "ext-reg";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_source_closure_director";
const CALLER_PRINCIPAL_ID = "prn_source_closure_director_creator";
const DEFINITION_ASSET_ID = "ast_source_closure_director_wf";
const SOURCE_ASSET_ID = "ast_source_closure_director_src";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const repoRoot = path.resolve(import.meta.dir, "..", "..");

// A single-step agent workflow whose agent names a director ref by id; the
// factory for that id lives in a dependency package's directors module, not
// here.
function entrySource(spec: {
  workflowId: string;
  stepId: string;
  address: string;
  directorId: string;
}): string {
  return `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: ${JSON.stringify(`${spec.workflowId}-agent`)},
  systemPrompt: "You are the closure-director workflow agent.",
  tools: [],
  capabilities: [],
  director: { id: ${JSON.stringify(spec.directorId)}, config: {} },
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

// A dependency package's directors module: one `defineDirector` factory whose
// director delegates to `createDefaultDirector` with a marker prepended to
// the agent's system prompt, so the captured inference request proves THIS
// director ran. `isAnnotatedDirectorFactory` is structural, so the bundled
// copy of `defineDirector` is accepted.
function directorsModuleSource(spec: { id: string; marker: string }): string {
  return `
import { defineDirector } from "@intx/agent";
import { createDefaultDirector } from "@intx/inference";
export const coding = defineDirector({
  id: ${JSON.stringify(spec.id)},
  configSchema: (config) => config,
  factory: (_config, _env, agent) =>
    createDefaultDirector(${JSON.stringify(spec.marker)} + " " + agent.systemPrompt, [...agent.toolDefinitions], {}),
}).factory;
`;
}

// Bundle a module: inline its `@intx/*` imports to source so the shipped
// `.mjs` carries no bare specifiers. Mirrors the single-member monorepo
// e2e's bundler.
async function bundleModule(
  scratchDir: string,
  source: string,
  basename: string,
): Promise<string> {
  const srcPath = path.join(scratchDir, `${basename}-src.ts`);
  await fs.writeFile(srcPath, source);

  const built = await Bun.build({
    entrypoints: [srcPath],
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
      `bundleModule: Bun.build produced no output for ${basename}`,
    );
  }
  const code = await artifact.text();
  if (code.includes("@intx/")) {
    throw new Error(
      `bundleModule: ${basename} bundle still carries a bare @intx import`,
    );
  }
  return code;
}

function sri(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let sourceCommitSha: string;
let registryServer: ReturnType<typeof Bun.serve> | undefined;
let packument: Packument;
const tarballTempDirs: string[] = [];

const sourceRepoId: RepoId = { kind: "workflow", id: SOURCE_ASSET_ID };

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(
      `closure-director e2e: unexpected attachment request ${assetId}`,
    );
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("closure-director e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

// The install resolves the external dep from this packument (no HTTP on the
// hub side); the sidecar fetches the same packument + tarball from the
// in-process registry at materialize. Both see the same SRI.
const fetchPackument: PackumentFetcher = async (name) => {
  if (name !== EXT_PACKAGE_NAME) {
    throw new Error(
      `closure-director e2e: unexpected packument request ${name}`,
    );
  }
  return packument;
};

// Install one member through the real probe -> gate -> freeze path against
// the shared definition asset.
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
    throw new Error(
      "closure-director e2e: could not open committed reads at commit",
    );
  }
  const approved = await installAndApproveWorkflowDefinition({
    source,
    entry: WORKFLOW_ENTRY,
    assetId: DEFINITION_ASSET_ID,
    approvals: spec.approvals,
    router: env.hub.probeRouter,
    db: h.db,
    reads: committedReadsToSourceTree(committed),
    registryName: REGISTRY_NAME,
    registryConfig: { url: `http://localhost:${String(registryServer?.port)}` },
    fetchPackument,
    resolveAttachment,
  });
  const approval = approved.approval;
  if (!approval.ok) {
    throw new Error(
      `closure-director e2e: install/approve gate did not approve ${spec.packageName} ` +
        `(reason: ${approval.reason}): ${JSON.stringify(approval)}\n${env.sidecarDiagnostics()}`,
    );
  }
  return { ...approved, approval };
}

// Deploy an installed member by source-ref and fire its mail trigger,
// returning once the run reaches a terminal event.
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
    credentialId: "sk-mock",
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

  await seedInferenceCredentials(
    h.db,
    TENANT_ID,
    { [spec.stepId]: [inferenceSource] },
    config,
  );
  env.hub.setPrimaryAllocationIdentity(spec.anchorRunId, spec.address);
  await deployCodeSourcedWorkflow({
    approved: spec.approved,
    source,
    resolveAttachment,
    sidecarAllocationRouter: env.hub.router,
    allocationTarget: {
      allocationId: "allocation-integration-1",
      generation: 1,
    },
    agentAddress: spec.address,
    config,
    sources: { [spec.stepId]: [inferenceSource] },
    db: h.db,
    tenantId: TENANT_ID,
    anchorRunId: spec.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    credentialCipher: createNoopCredentialCipher(),
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
    { diagnostics: env.sidecarDiagnostics },
  );

  await fireMailTrigger(env, spec.address, {
    messageId: `<source-closure-director-${spec.workflowId}@integration.interchange>`,
  });
  const runId = await waitForFirstRunId(env, workflowRunRepoId, {
    diagnostics: env.sidecarDiagnostics,
  });
  const terminal = await waitForWorkflowRunComplete(
    env,
    spec.anchorRunId,
    runId,
    {
      diagnostics: env.sidecarDiagnostics,
    },
  );
  if (terminal.type !== "RunCompleted") {
    const events = await readWorkflowRunEvents(env, spec.anchorRunId, runId);
    throw new Error(
      `closure-director e2e: expected RunCompleted for ${spec.workflowId}, got ${terminal.type}: ${JSON.stringify(terminal.body)}\nevents: ${JSON.stringify(events)}\n${env.sidecarDiagnostics()}`,
    );
  }
}

const approvalsFor = (address: string, directorId: string): ApprovalSet =>
  createApprovalSet([
    "inference.source:anthropic:mock-model",
    "director:@intx/agent/default",
    `director:${directorId}`,
    `mail.address:${address}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
  ]);

// Pull the text out of a captured inference request's `system` field. Narrows
// with `in`/`typeof` guards rather than assertions so a shape drift yields no
// text (and the assertion fails loud) instead of a cast that lies.
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

describe.skipIf(!harnessDbEnvAvailable())(
  "closure-director source-workflow e2e",
  () => {
    beforeAll(async () => {
      scratchDir = await fs.mkdtemp(
        path.join(os.tmpdir(), "source-closure-director-"),
      );
      const memberWorkflowJs = await bundleModule(
        scratchDir,
        entrySource({
          workflowId: MEMBER_WORKFLOW_ID,
          stepId: MEMBER_STEP_ID,
          address: memberAddress,
          directorId: MEMBER_DIRECTOR_ID,
        }),
        "member-entry",
      );
      const memberDirectorsJs = await bundleModule(
        scratchDir,
        directorsModuleSource({
          id: MEMBER_DIRECTOR_ID,
          marker: MEMBER_DIRECTOR_MARKER,
        }),
        "member-directors",
      );
      const extWorkflowJs = await bundleModule(
        scratchDir,
        entrySource({
          workflowId: EXT_WORKFLOW_ID,
          stepId: EXT_STEP_ID,
          address: extAddress,
          directorId: EXT_DIRECTOR_ID,
        }),
        "ext-entry",
      );
      const extDirectorsJs = await bundleModule(
        scratchDir,
        directorsModuleSource({
          id: EXT_DIRECTOR_ID,
          marker: EXT_DIRECTOR_MARKER,
        }),
        "ext-directors",
      );

      // Build the external dep tarball, compute its SRI, and stand up an
      // in-process npm registry that serves its packument + tarball. The
      // tarball's package.json carries `interchange.directors` so the
      // materialized package contributes its director to the closure
      // registry.
      const extBytes = await buildSyntheticNpmPackageTarball(
        (dir) => tarballTempDirs.push(dir),
        {
          packageName: EXT_PACKAGE_NAME,
          version: EXT_PACKAGE_VERSION,
          moduleSource: extDirectorsJs,
          interchange: { directors: "./index.mjs" },
        },
      );
      const extIntegrity = sri(extBytes);

      registryServer = Bun.serve({
        port: 0,
        fetch(req) {
          const url = new URL(req.url);
          if (url.pathname === `/${EXT_PACKAGE_NAME}`) {
            return new Response(JSON.stringify(packument), {
              headers: { "content-type": "application/json" },
            });
          }
          if (url.pathname === EXT_TARBALL_PATH) {
            return new Response(extBytes);
          }
          return new Response("not found", { status: 404 });
        },
      });
      const registryUrl = `http://localhost:${String(registryServer.port)}`;
      packument = {
        name: EXT_PACKAGE_NAME,
        "dist-tags": { latest: EXT_PACKAGE_VERSION },
        versions: {
          [EXT_PACKAGE_VERSION]: {
            name: EXT_PACKAGE_NAME,
            version: EXT_PACKAGE_VERSION,
            dist: {
              tarball: `${registryUrl}${EXT_TARBALL_PATH}`,
              integrity: extIntegrity,
            },
          },
        },
      };

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
        name: "source-closure-director-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });

      env = await startDeployFlowEnv({
        sidecarEnv: {
          SIDECAR_TOOL_REGISTRIES: JSON.stringify([
            { name: REGISTRY_NAME, url: registryUrl },
          ]),
        },
      });

      // Seed the monorepo: a private workspaces root plus three members --
      // the director library and the two apps that consume a director
      // through it (workspace-local) and through the external registry.
      await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
      const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
        HUB_PRINCIPAL,
        sourceRepoId,
        DEFAULT_ASSET_REF,
        {
          files: {
            "package.json": JSON.stringify({
              name: "@wf/closure-director-root",
              private: true,
              workspaces: ["packages/*"],
            }),
            "packages/director-lib/package.json": JSON.stringify({
              name: MEMBER_LIB_PACKAGE_NAME,
              version: PACKAGE_VERSION,
              type: "module",
              interchange: { directors: "./directors.mjs" },
            }),
            "packages/director-lib/directors.mjs": memberDirectorsJs,
            "packages/app-member/package.json": JSON.stringify({
              name: MEMBER_PACKAGE_NAME,
              version: PACKAGE_VERSION,
              type: "module",
              interchange: { workflow: WORKFLOW_ENTRY },
              dependencies: {
                [MEMBER_LIB_PACKAGE_NAME]: "workspace:*",
              },
            }),
            "packages/app-member/workflow.mjs": memberWorkflowJs,
            "packages/app-ext/package.json": JSON.stringify({
              name: EXT_APP_PACKAGE_NAME,
              version: PACKAGE_VERSION,
              type: "module",
              interchange: { workflow: WORKFLOW_ENTRY },
              dependencies: {
                [EXT_PACKAGE_NAME]: `^${EXT_PACKAGE_VERSION}`,
              },
            }),
            "packages/app-ext/workflow.mjs": extWorkflowJs,
          },
          message: "Seed closure-director monorepo source",
        },
      );
      sourceCommitSha = writeResult.commitSha;
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
      if (registryServer !== undefined) await registryServer.stop(true);
      for (const dir of tarballTempDirs.splice(0)) {
        await fs.rm(dir, { recursive: true, force: true });
      }
      if (scratchDir !== undefined) {
        await fs.rm(scratchDir, { recursive: true, force: true });
      }
    });

    test("a workspace-member dependency's director is approved, deployed, and drives the turn", async () => {
      const installed = await installMember({
        packageName: MEMBER_PACKAGE_NAME,
        approvals: approvalsFor(memberAddress, MEMBER_DIRECTOR_ID),
      });

      expect(installed.projection.id).toBe(MEMBER_WORKFLOW_ID);
      const byName = new Map(installed.closure.entries.map((e) => [e.name, e]));
      const libEntry = byName.get(MEMBER_LIB_PACKAGE_NAME);
      if (libEntry?.source.kind !== "asset") {
        throw new Error(
          "closure-director e2e: director lib is not asset-sourced",
        );
      }
      expect(libEntry.source.package.format).toBe("source");
      expect(
        installed.approval.approvedSurface.grants.has(
          `director:${MEMBER_DIRECTOR_ID}`,
        ),
      ).toBe(true);

      await deployAndRun({
        approved: installed,
        packageName: MEMBER_PACKAGE_NAME,
        workflowId: MEMBER_WORKFLOW_ID,
        stepId: MEMBER_STEP_ID,
        anchorRunId: MEMBER_DEPLOYMENT_ID,
        address: memberAddress,
      });

      const systemTexts = env.inference.requests.flatMap(systemBlockTexts);
      expect(
        systemTexts.some((text) => text.includes(MEMBER_DIRECTOR_MARKER)),
      ).toBe(true);
    }, 180_000);

    test("an external registry dependency's director is approved, deployed, and drives the turn", async () => {
      const installed = await installMember({
        packageName: EXT_APP_PACKAGE_NAME,
        approvals: approvalsFor(extAddress, EXT_DIRECTOR_ID),
      });

      expect(installed.projection.id).toBe(EXT_WORKFLOW_ID);
      const byName = new Map(installed.closure.entries.map((e) => [e.name, e]));
      const extEntry = byName.get(EXT_PACKAGE_NAME);
      if (extEntry?.source.kind !== "registry") {
        throw new Error(
          "closure-director e2e: external dep is not a registry entry",
        );
      }
      expect(extEntry.source.registry).toBe(REGISTRY_NAME);
      expect(
        installed.approval.approvedSurface.grants.has(
          `director:${EXT_DIRECTOR_ID}`,
        ),
      ).toBe(true);

      await deployAndRun({
        approved: installed,
        packageName: EXT_APP_PACKAGE_NAME,
        workflowId: EXT_WORKFLOW_ID,
        stepId: EXT_STEP_ID,
        anchorRunId: EXT_DEPLOYMENT_ID,
        address: extAddress,
      });

      const systemTexts = env.inference.requests.flatMap(systemBlockTexts);
      expect(
        systemTexts.some((text) => text.includes(EXT_DIRECTOR_MARKER)),
      ).toBe(true);
    }, 180_000);
  },
);
