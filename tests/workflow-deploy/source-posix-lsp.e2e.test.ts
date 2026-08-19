// A code-sourced workflow runs a real PLUGIN-backed tool package end to end.
//
// This is the first test to drive a genuine `definePlugin` package
// (`@intx/tools-lsp`) through the source-ref deploy path. Unlike a plain tool
// factory -- which a workflow imports and places in `agent.tools`, so Option A
// carries it on `req.agent.toolFactories` for free -- a plugin package
// contributes NO agent-visible factory: its factory reaches the agent only
// through `env.plugins`. The workflow therefore declares it with a per-agent
// `plugins: ["@intx/tools-lsp"]` list, and three separate mechanisms must
// cooperate for the plugin's `lsp` tool to run and authorize:
//
//   1. The deploy-time probe loads the declared plugin's STATIC tool
//      `definitions` from the frozen closure and surfaces a `tool:lsp` grant
//      into the approved grant-walk snapshot (Tier-2 governance -- no floor).
//   2. The run-child materializes the plugin FACTORY from the same closure
//      (no re-download) and feeds it into the existing per-step plugin chain,
//      which threads it onto `env.plugins`; posix's bundle (imported inline by
//      the workflow, so present in `agent.toolFactories`) consumes it and
//      registers the plugin's `lsp` tool.
//   3. At call time the reactor authorizes `tool:lsp` against the frozen
//      snapshot -- the plugin tool's runtime name is the bare `lsp`, matching
//      the walked grant -- and the tool's handler runs in-child.
//
// posix is inlined into the workflow bundle (the entry imports its sidecar
// bundle, which the test bundler rewrites to on-disk source), so the closure
// need only carry `@intx/tools-lsp`. That plugin package is published to an
// in-process npm registry as a SELF-CONTAINED bundle (all of its own deps
// inlined by `Bun.build`), so the closure resolves it to a single registry
// entry with an integrity SRI -- the sidecar materializes exactly those bytes.
//
// The mock inference server drives the agent to call the ungated `lsp` tool
// (posix's own six tools are approval-gated, so calling one would suspend for
// approval; `lsp` is not gated and completes cleanly). The tool has no language
// server configured, so its handler returns "no LSP server available" -- a
// valid tool result -- and the run completes.

import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { dirname } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import * as tar from "tar";

import {
  DEFAULT_ASSET_REF,
  committedReadsToSourceTree,
  deployCodeSourcedWorkflow,
  installAndApproveWorkflowDefinition,
  type RepoId,
} from "@intx/hub-sessions";
import { tenant as tenantTable } from "@intx/db/schema";
import { loadFrozenGrantSnapshot } from "@intx/db";
import type { GrantEffect, GrantWalkSnapshot } from "@intx/types";
import type { WireGrantRule } from "@intx/types/grant-wire";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { WorkflowDefinitionAssetSource } from "@intx/types/workflow-sources";
import type { Packument, PackumentFetcher } from "@intx/tool-packaging";
import { generateId } from "@intx/hub-common";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = generateId("workflowRun");
const STEP_ID = "code";
const AGENT_ID = "coder";
const WORKFLOW_RUN_REF = "refs/heads/main";

const APP_PACKAGE_NAME = "@wf/posix-lsp-app";
const PACKAGE_VERSION = "1.0.0";
const WORKFLOW_ENTRY = "./workflow.mjs";

// The plugin package the closure carries and the run materializes. Its version
// mirrors the real workspace package so the served packument is faithful.
const LSP_PACKAGE_NAME = "@intx/tools-lsp";
const LSP_PACKAGE_VERSION = "0.2.2";
const LSP_TARBALL_PATH = `/${LSP_PACKAGE_NAME}/-/tools-lsp-${LSP_PACKAGE_VERSION}.tgz`;
const REGISTRY_NAME = "plugin-reg";

// The one plugin-contributed tool posix registers from `env.plugins`.
const LSP_TOOL_NAME = "lsp";
// posix's own six tools, all approval-gated -- walked but never invoked here.
const POSIX_TOOL_NAMES = [
  "read_file",
  "write_file",
  "edit_file",
  "run_shell",
  "search_files",
  "grep",
] as const;

const TENANT_ID = "tnt_source_posix_lsp";
const CALLER_PRINCIPAL_ID = "prn_source_posix_lsp";
const DEFINITION_ASSET_ID = "ast_source_posix_lsp_wf";
const SOURCE_ASSET_ID = "ast_source_posix_lsp_src";

const HUB_PRINCIPAL = { kind: "hub" } as const;

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const repoRoot = path.resolve(import.meta.dir, "..", "..");

// The workflow entry imports posix's sidecar bundle (inlined by the bundler)
// and declares the LSP plugin by package name. It does NOT import
// `@intx/tools-lsp` -- a plugin has no agent slot; the `plugins` list is its
// only declaration, and the run-child materializes it from the closure.
const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";
import { posix } from "@intx/tools-posix/sidecar-bundle";

const agent = defineAgent({
  id: ${JSON.stringify(AGENT_ID)},
  systemPrompt: "You are a coding agent with filesystem and language-server tools.",
  tools: [posix],
  plugins: [${JSON.stringify(LSP_PACKAGE_NAME)}],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_posix_lsp",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${JSON.stringify(STEP_ID)}: step({ agent }),
  },
});
`;

// Rewrite every `@intx/*` import to its on-disk source so the bundle is
// self-contained -- the same plugin the source-mixed-closure e2e uses.
const intxToSourcePlugin: Bun.BunPlugin = {
  name: "resolve-intx-to-source",
  setup(build) {
    build.onResolve({ filter: /^@intx\// }, (args) => {
      const fromDir = args.importer.startsWith(`${repoRoot}${path.sep}`)
        ? dirname(args.importer)
        : repoRoot;
      return { path: Bun.resolveSync(args.path, fromDir) };
    });
  },
};

async function bundleWorkflowEntry(scratchDir: string): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "posix-lsp-entry-src.ts");
  await fs.writeFile(entrySrcPath, workflowEntrySource);
  const built = await Bun.build({
    entrypoints: [entrySrcPath],
    target: "bun",
    format: "esm",
    throw: true,
    plugins: [intxToSourcePlugin],
  });
  const artifact = built.outputs[0];
  if (artifact === undefined) {
    throw new Error("bundleWorkflowEntry: Bun.build produced no output");
  }
  const code = await artifact.text();
  // posix is inlined; `@intx/tools-lsp` is declared only via `plugins` and must
  // NOT appear as a bare import in the bundle (it comes from the closure).
  if (/from\s*["'`]@intx\/tools-lsp/.test(code)) {
    throw new Error(
      "bundleWorkflowEntry: the entry must not import @intx/tools-lsp; it is a plugin declared by name",
    );
  }
  return code;
}

// Publish `@intx/tools-lsp` as a self-contained tarball: `Bun.build` bundles
// its sidecar-bundle entry with EVERY dependency inlined (its own `@intx/*`
// deps to source, its npm deps from node_modules), so the closure resolves it
// to a single registry entry with no transitive deps of its own.
async function buildSelfContainedLspTarball(
  scratchDir: string,
): Promise<Uint8Array> {
  const lspEntry = Bun.resolveSync("@intx/tools-lsp/sidecar-bundle", repoRoot);
  const built = await Bun.build({
    entrypoints: [lspEntry],
    target: "bun",
    format: "esm",
    throw: true,
    plugins: [intxToSourcePlugin],
  });
  const artifact = built.outputs[0];
  if (artifact === undefined) {
    throw new Error(
      "buildSelfContainedLspTarball: Bun.build produced no output",
    );
  }
  const bundleJs = await artifact.text();

  const stagingDir = await fs.mkdtemp(path.join(scratchDir, "lsp-pkg-"));
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: LSP_PACKAGE_NAME,
      version: LSP_PACKAGE_VERSION,
      type: "module",
      // The self-contained bundle IS the interchange.tools entry; the loader
      // (probe + run-child) imports it and routes its exported definePlugin
      // factory. No dependencies: everything is inlined.
      interchange: { tools: "./sidecar-bundle.js" },
      exports: { "./sidecar-bundle": "./sidecar-bundle.js" },
    }),
  );
  await fs.writeFile(path.join(packageDir, "sidecar-bundle.js"), bundleJs);

  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  return new Uint8Array(await fs.readFile(tarballPath));
}

function sri(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}

/**
 * Project the frozen grant-walk snapshot into the run's runtime `tool:` grant
 * rows the trigger delivers, mirroring the production
 * `deriveRunRuntimeGrantRows`/`runGrantToWire` tail (as the source-inline-tool
 * e2e does). Every walked tool grant -- posix's gated tools AND the plugin's
 * `tool:lsp` -- becomes one creator-origin `invoke` row with its snapshot
 * effect (ask winning over allow).
 */
function deriveWireRunGrants(snapshot: GrantWalkSnapshot): WireGrantRule[] {
  const effectByResource = new Map<string, GrantEffect>();
  for (const step of snapshot.perStep) {
    const grantEffects = new Map<string, GrantEffect>(
      Object.entries(step.grantEffects),
    );
    for (const grant of step.grants) {
      if (grant.startsWith("tool:")) {
        const effect = grantEffects.get(grant);
        if (effect === undefined) {
          throw new Error(
            `deriveWireRunGrants: tool grant ${JSON.stringify(grant)} has no grantEffects entry`,
          );
        }
        const existing = effectByResource.get(grant);
        if (existing === "ask" || effect === "ask") {
          effectByResource.set(grant, "ask");
        } else if (existing === undefined) {
          effectByResource.set(grant, effect);
        }
      } else if (grant.startsWith("effect:") && !effectByResource.has(grant)) {
        effectByResource.set(grant, "allow");
      }
    }
  }
  return [...effectByResource].map(([resource, effect]) => ({
    id: `run-grant:${resource}`,
    resource,
    action: "invoke",
    effect,
    origin: "creator",
    conditions: null,
    expiresAt: null,
    roleId: null,
    principalId: null,
  }));
}

let env: DeployFlowEnv;
let h: TestDb;
let scratchDir: string;
let sourceCommitSha: string;
let registryServer: ReturnType<typeof Bun.serve> | undefined;
let packument: Packument;

const sourceRepoId: RepoId = { kind: "workflow", id: SOURCE_ASSET_ID };

const resolveAttachment = async (
  assetId: string,
): Promise<{ pack: Uint8Array; ref: string; commitSha: string }> => {
  if (assetId !== SOURCE_ASSET_ID) {
    throw new Error(`posix-lsp e2e: unexpected attachment request ${assetId}`);
  }
  const commitSha = await env.hub.agentRepoStore.repoStore.resolveRef(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  if (commitSha === null) {
    throw new Error("posix-lsp e2e: source asset has no commit");
  }
  const { pack, ref } = await env.hub.agentRepoStore.repoStore.createPack(
    HUB_PRINCIPAL,
    sourceRepoId,
    DEFAULT_ASSET_REF,
  );
  return { pack, ref, commitSha };
};

const fetchPackument: PackumentFetcher = async (name) => {
  if (name !== LSP_PACKAGE_NAME) {
    throw new Error(`posix-lsp e2e: unexpected packument request ${name}`);
  }
  return packument;
};

describe.skipIf(!harnessDbEnvAvailable())(
  "source-ref workflow loads a plugin package and runs its tool",
  () => {
    beforeAll(async () => {
      scratchDir = await fs.mkdtemp(
        path.join(os.tmpdir(), "source-posix-lsp-"),
      );
      const workflowJs = await bundleWorkflowEntry(scratchDir);
      const lspBytes = await buildSelfContainedLspTarball(scratchDir);
      const lspIntegrity = sri(lspBytes);

      registryServer = Bun.serve({
        port: 0,
        fetch(req) {
          const url = new URL(req.url);
          if (url.pathname === `/${LSP_PACKAGE_NAME}`) {
            return new Response(JSON.stringify(packument), {
              headers: { "content-type": "application/json" },
            });
          }
          if (url.pathname === LSP_TARBALL_PATH) {
            return new Response(lspBytes);
          }
          return new Response("not found", { status: 404 });
        },
      });
      const registryUrl = `http://localhost:${String(registryServer.port)}`;
      packument = {
        name: LSP_PACKAGE_NAME,
        "dist-tags": { latest: LSP_PACKAGE_VERSION },
        versions: {
          [LSP_PACKAGE_VERSION]: {
            name: LSP_PACKAGE_NAME,
            version: LSP_PACKAGE_VERSION,
            dist: {
              tarball: `${registryUrl}${LSP_TARBALL_PATH}`,
              integrity: lspIntegrity,
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
        name: "source-posix-lsp-wf",
        creatorPrincipalId: CALLER_PRINCIPAL_ID,
      });

      env = await startDeployFlowEnv({
        sidecarEnv: {
          SIDECAR_TOOL_REGISTRIES: JSON.stringify([
            { name: REGISTRY_NAME, url: registryUrl },
          ]),
        },
        // Drive the agent to call the ungated `lsp` tool the plugin contributes.
        inferenceToolCall: {
          toolName: LSP_TOOL_NAME,
          input: {
            operation: "hover",
            filePath: "index.ts",
            line: 1,
            character: 1,
          },
        },
      });

      await env.hub.agentRepoStore.repoStore.initRepo(sourceRepoId);
      const writeResult = await env.hub.agentRepoStore.repoStore.writeTree(
        HUB_PRINCIPAL,
        sourceRepoId,
        DEFAULT_ASSET_REF,
        {
          files: {
            "package.json": JSON.stringify({
              name: "@wf/posix-lsp-root",
              private: true,
              workspaces: ["packages/*"],
            }),
            "packages/app/package.json": JSON.stringify({
              name: APP_PACKAGE_NAME,
              version: PACKAGE_VERSION,
              type: "module",
              interchange: { workflow: WORKFLOW_ENTRY },
              // The LSP plugin package is a real dependency, so it lands in the
              // frozen closure and is materialized on both the probe and the
              // run. posix is inlined into the bundle, so it needs no entry.
              dependencies: {
                [LSP_PACKAGE_NAME]: `^${LSP_PACKAGE_VERSION}`,
              },
            }),
            "packages/app/workflow.mjs": workflowJs,
          },
          message: "Seed posix-lsp source workflow",
        },
      );
      sourceCommitSha = writeResult.commitSha;
    });

    afterAll(async () => {
      if (env !== undefined) await env.teardown();
      if (h !== undefined) await h.close();
      if (registryServer !== undefined) await registryServer.stop(true);
      if (scratchDir !== undefined) {
        await fs.rm(scratchDir, { recursive: true, force: true });
      }
    });

    test("materializes the plugin from the closure, surfaces tool:lsp, and runs it", async () => {
      const source: WorkflowDefinitionAssetSource = {
        kind: "asset",
        assetId: SOURCE_ASSET_ID,
        package: {
          format: "source",
          commitSha: sourceCommitSha,
          packageName: APP_PACKAGE_NAME,
        },
      };

      const committed =
        await env.hub.agentRepoStore.repoStore.openCommittedReadsAtCommit(
          HUB_PRINCIPAL,
          sourceRepoId,
          sourceCommitSha,
        );
      if (committed === null) {
        throw new Error("posix-lsp e2e: could not open committed reads");
      }

      // The operator approves EVERY grant the walk emits: posix's six gated
      // tools (Option A, from the inlined factory) AND the plugin-contributed
      // `tool:lsp` (Tier-2, surfaced from the plugin's static definitions).
      const approvals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        `tool:${LSP_TOOL_NAME}`,
        ...POSIX_TOOL_NAMES.map((n) => `tool:${n}`),
      ]);

      const approved = await installAndApproveWorkflowDefinition({
        source,
        entry: WORKFLOW_ENTRY,
        assetId: DEFINITION_ASSET_ID,
        approvals,
        router: env.hub.router,
        db: h.db,
        reads: committedReadsToSourceTree(committed),
        registryName: REGISTRY_NAME,
        registryConfig: {
          url: `http://localhost:${String(registryServer?.port)}`,
        },
        fetchPackument,
        resolveAttachment,
      });
      if (!approved.approval.ok) {
        throw new Error(
          `posix-lsp e2e: install did not approve (reason: ${approved.approval.reason}): ${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(approved.projection.id).toBe("wf_posix_lsp");

      // The closure carries the workflow package (source) and the plugin
      // package (registry entry with an integrity SRI).
      const byName = new Map(approved.closure.entries.map((e) => [e.name, e]));
      const lspEntry = byName.get(LSP_PACKAGE_NAME);
      if (lspEntry?.source.kind !== "registry") {
        throw new Error(
          "posix-lsp e2e: the plugin package is not a registry closure entry",
        );
      }
      expect(lspEntry.source.registry).toBe(REGISTRY_NAME);

      // Proof (1): the plugin's `tool:lsp` grant is in the FROZEN snapshot --
      // the Tier-2 walk surfaced the plugin's static tool definition into the
      // approved grant surface. Absent it, the reactor would fail closed on the
      // `lsp` call. posix's own factory tools are present too (Option A).
      if (!approved.approval.ok) {
        throw new Error("posix-lsp e2e: expected an approved definition");
      }
      const snapshot = await loadFrozenGrantSnapshot(
        h.db,
        approved.approval.definitionId,
      );
      if (snapshot === null) {
        throw new Error("posix-lsp e2e: expected a frozen grant snapshot");
      }
      const snapshotToolGrants = snapshot.perStep.flatMap((s) =>
        s.grants.filter((g) => g.startsWith("tool:")),
      );
      expect(snapshotToolGrants).toContain(`tool:${LSP_TOOL_NAME}`);
      expect(snapshotToolGrants).toContain(`tool:${POSIX_TOOL_NAMES[0]}`);

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${String(env.inference.server.port)}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: DEPLOYMENT_ID,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden per step by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      await deployCodeSourcedWorkflow({
        approved,
        source,
        resolveAttachment,
        sidecarRouter: env.hub.router,
        agentAddress: deploymentMailAddress,
        config,
        sources: { [STEP_ID]: [inferenceSource] },
        db: h.db,
        tenantId: TENANT_ID,
        anchorRunId: DEPLOYMENT_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
      });

      const workflowRunRepoId: RepoId = {
        kind: "workflow-run",
        id: deriveDeploymentId(deploymentMailAddress),
      };
      env.registerDeployment({
        anchorRunId: DEPLOYMENT_ID,
        workflowDefinition: {
          id: approved.projection.id,
          triggers: [{ type: "mail", to: deploymentMailAddress }],
          steps: {},
          stepOrder: [...approved.projection.stepOrder],
        },
        workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        mailAddress: deploymentMailAddress,
      });

      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      const runGrants = deriveWireRunGrants(snapshot);
      await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<source-posix-lsp-e2e@integration.interchange>",
        grants: runGrants,
      });

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        timeoutMs: 30_000,
        diagnostics: env.sidecarDiagnostics,
      });
      const terminal = await waitForWorkflowRunComplete(
        env,
        DEPLOYMENT_ID,
        runId,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, DEPLOYMENT_ID, runId);
        const failed = events.find(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
        throw new Error(
          `posix-lsp e2e: expected RunCompleted, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }
      expect(terminal.type).toBe("RunCompleted");

      // Proof (2): the plugin LOADED and posix consumed it. The `lsp` tool only
      // reaches the model if the plugin factory was materialized from the
      // closure, instantiated onto `env.plugins`, and registered by posix's
      // bundle. A run that failed to wire the plugin would offer posix's six
      // tools without `lsp`.
      const firstReq = env.inference.requests[0];
      if (firstReq === undefined) {
        throw new Error("posix-lsp e2e: no inference request captured");
      }
      const toolNames = (firstReq.tools ?? []).map((t) => t.name);
      expect(toolNames).toContain(LSP_TOOL_NAME);
      expect(toolNames).toContain(POSIX_TOOL_NAMES[0]);

      // Proof (3): the `lsp` tool RAN in-child and authorized. The mock emitted
      // a `tool_use` for `lsp`; the reactor authorized `tool:lsp` against the
      // frozen snapshot, the handler executed, and the agent looped back with
      // the tool_result -- so a second inference request landed.
      expect(env.inference.requests.length).toBeGreaterThanOrEqual(2);
    }, 180_000);
  },
);
