// Walking-skeleton end-to-end: install a code-sourced workflow package from
// a test npm registry, probe it, approve+freeze it, deploy it BY SOURCE-REF,
// and assert the deployed workflow runs to completion.
//
// This is the one test that drives every seam of the INTR-416 walking
// skeleton in a single flow, composing the two PRODUCTION entrypoints the
// hub exposes and reimplementing none of their glue:
//
//   installAndApproveWorkflowDefinition(...)  — resolve the frozen closure,
//     probe the live sidecar (airlocked probe child evaluates the package's
//     interchange.workflow entry, projects it to its inert needs-surface,
//     ships the advisory grant set + wire hash), gate the advisory grants
//     against the operator's ApprovalSet, and freeze the recomputed wire hash
//     onto the definition version row.
//
//   deployCodeSourcedWorkflow(...)            — carry the gate's frozen wire
//     hash + inert projection + frozen closure verbatim into a source-ref
//     agent.deploy frame. The sidecar re-materializes the pinned closure
//     (HTTP fetch + SRI verify), re-evaluates the entry, projects live->inert,
//     writes workflow.json, and the child's load-boundary re-verify recomputes
//     the wire hash over that inert projection and matches the frozen anchor.
//
// The workflow is a single toolless agent step triggered by mail; the mock
// inference server returns a deterministic reply, so the run reaches
// RunCompleted on the echo alone. The load-bearing assertion is that terminal
// event: it proves closure resolution, the probe transport + airlocked probe
// child, the hub gate + freeze, source-ref deploy, sidecar apply of the frozen
// closure, child re-verify on load, and per-run grant materialization from the
// frozen set all composed into one run.
//
// The fixture workflow package is built at test time: a self-contained
// `defineWorkflow(...)` entry is bundled with Bun.build (its @intx imports
// inlined to source so the sidecar-materialized closure needs no workspace
// module resolution), tarred as an npm package with an `interchange.workflow`
// entry, and served from a small in-process HTTP registry. The hub reads the
// package's packument through the injected `fetchPackument` seam; the sidecar
// fetches + SRI-verifies the tarball over HTTP from the same registry.

import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { dirname } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { and, eq } from "drizzle-orm";
import * as tar from "tar";

import {
  deployCodeSourcedWorkflow,
  installAndApproveWorkflowDefinition,
  type RepoId,
} from "@intx/hub-sessions";
import {
  workflowDefinitionVersion as workflowDefinitionVersionTable,
  tenant as tenantTable,
} from "@intx/db/schema";
import type {
  Packument,
  PackumentFetcher,
  RegistryConfig,
} from "@intx/tool-packaging";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WorkflowDefinitionRegistrySource } from "@intx/types/workflow-sources";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
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
const DEPLOYMENT_ID = "walking-skeleton-e2e";
const STEP_ID = "run";
const WORKFLOW_RUN_REF = "refs/heads/main";

const REGISTRY_NAME = "walking-skeleton-registry";
const PACKAGE_NAME = "@wf/walking-skeleton";
const PACKAGE_VERSION = "1.0.0";
const PACKAGE_BASENAME = "walking-skeleton";
const WORKFLOW_ENTRY = "./workflow.mjs";

const TENANT_ID = "tnt_walking_skeleton";
const CALLER_PRINCIPAL_ID = "prn_walking_skeleton_creator";
const DEFINITION_ASSET_ID = "ast_walking_skeleton_wf";

const deploymentMailAddress = deriveRunAddress({
  runId: DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});

const repoRoot = path.resolve(import.meta.dir, "..", "..");

// The self-contained entry the fixture package ships as its
// `interchange.workflow`. A single toolless agent step triggered by the
// deployment's mail address; the mock inference reply completes the step.
// Exported as a named binding (not `export default`) so the child loader's
// single-definition selection still finds exactly one WorkflowDefinition.
const workflowEntrySource = `
import { defineWorkflow, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const agent = defineAgent({
  id: "walking-skeleton-agent",
  systemPrompt: "You are the walking-skeleton single-step agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: "wf_walking_skeleton",
  trigger: { type: "mail", to: ${JSON.stringify(deploymentMailAddress)} },
  steps: {
    ${STEP_ID}: step({ agent }),
  },
});
`;

interface WorkflowPackageFixture {
  bytes: Uint8Array;
  integrity: string;
  tarballFilename: string;
}

function sriFromBytes(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}

// Bundle the entry into one self-contained `.mjs` so the sidecar-materialized
// closure (which lays out no workspace `node_modules`) can evaluate it with no
// bare `@intx/*` import left to resolve. @intx specifiers are resolved to
// source via `Bun.resolveSync` under the test's `intx-src` conditions and
// inlined; `node:` builtins stay external (the child's Bun runtime provides
// them); everything else Bun bundles in.
async function bundleWorkflowEntry(scratchDir: string): Promise<string> {
  const entrySrcPath = path.join(scratchDir, "workflow-entry-src.ts");
  await fs.writeFile(entrySrcPath, workflowEntrySource);

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
            // Resolve @intx specifiers against the repo root, where the
            // workspace `node_modules/@intx` symlinks live and the `intx-src`
            // condition points at source. The entry file itself lives in a
            // temp dir with no node_modules chain, so resolving from the
            // importer's dir would fail for the top-level entry.
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
    throw new Error("bundleWorkflowEntry: Bun.build produced no output");
  }
  const code = await artifact.text();
  if (code.includes("@intx/")) {
    throw new Error(
      "bundleWorkflowEntry: bundle still carries a bare @intx import; the " +
        "sidecar closure would fail to resolve it",
    );
  }
  return code;
}

// Tar the bundled entry + a package.json declaring `interchange.workflow` into
// an npm-style tarball (npm strips the leading `package/`), mirroring the
// tool-packaging fixture harness.
async function buildWorkflowPackageFixture(
  scratchDir: string,
): Promise<WorkflowPackageFixture> {
  const workflowJs = await bundleWorkflowEntry(scratchDir);

  const stagingDir = path.join(scratchDir, "staging");
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: PACKAGE_NAME,
      version: PACKAGE_VERSION,
      interchange: { workflow: WORKFLOW_ENTRY },
    }),
  );
  await fs.writeFile(path.join(packageDir, "workflow.mjs"), workflowJs);

  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.readFile(tarballPath);
  return {
    bytes,
    integrity: sriFromBytes(bytes),
    tarballFilename: `${PACKAGE_BASENAME}-${PACKAGE_VERSION}.tgz`,
  };
}

let env: DeployFlowEnv;
let h: TestDb;
let registryServer: ReturnType<typeof Bun.serve> | undefined;
let scratchDir: string;
let fixture: WorkflowPackageFixture;
let tarballUrl: string;

describe("walking skeleton e2e", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "walking-skeleton-"));
    fixture = await buildWorkflowPackageFixture(scratchDir);

    // In-process HTTP registry: serves the fixture tarball bytes so the
    // sidecar's closure materializer can fetch + SRI-verify them. It is
    // consulted only for the tarball GET (the hub reads the packument through
    // the `fetchPackument` seam, no HTTP), so serving the one tarball for any
    // `.tgz` request is sufficient and encoding-robust.
    registryServer = Bun.serve({
      port: 0,
      fetch(req) {
        const url = new URL(req.url);
        if (req.method === "GET" && url.pathname.endsWith(".tgz")) {
          return new Response(fixture.bytes, {
            headers: { "content-type": "application/octet-stream" },
          });
        }
        return new Response("not found", { status: 404 });
      },
    });
    const registryBase = `http://localhost:${String(registryServer.port)}`;
    tarballUrl = `${registryBase}/${PACKAGE_NAME}/-/${fixture.tarballFilename}`;

    h = await createTestDb();

    // The sidecar must be pointed at the fixture registry BEFORE it boots so
    // both the probe and the source-ref deploy materialize the closure from
    // there rather than public npm.
    env = await startDeployFlowEnv({
      sidecarEnv: {
        SIDECAR_TOOL_REGISTRIES: JSON.stringify([
          { name: REGISTRY_NAME, url: registryBase },
        ]),
      },
    });
  });

  afterAll(async () => {
    if (env !== undefined) await env.teardown();
    if (h !== undefined) await h.close();
    if (registryServer !== undefined) await registryServer.stop(true);
    if (scratchDir !== undefined) {
      await fs.rm(scratchDir, { recursive: true, force: true });
    }
  });

  test("install -> probe -> approve -> deploy-by-source-ref -> run-to-completion", async () => {
    // Seed the tenancy + workflow asset the freeze anchors on:
    // installAndApproveWorkflowDefinition's freeze projects a first-class
    // workflow_definition over this asset, so its row (and its tenant) must
    // exist before the gate persists.
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
      name: "walking-skeleton-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    const source: WorkflowDefinitionRegistrySource = {
      kind: "registry",
      registry: REGISTRY_NAME,
    };
    const registryConfig: RegistryConfig = { url: tarballUrl };

    // Hub-side packument seam: the frozen closure resolution reads the pinned
    // version + integrity + tarball URL from here (no HTTP on the hub side).
    const fetchPackument: PackumentFetcher = async (packageName) => {
      if (packageName !== PACKAGE_NAME) {
        throw new Error(`unexpected packument request: ${packageName}`);
      }
      const packument: Packument = {
        name: PACKAGE_NAME,
        versions: {
          [PACKAGE_VERSION]: {
            name: PACKAGE_NAME,
            version: PACKAGE_VERSION,
            dist: {
              tarball: tarballUrl,
              integrity: fixture.integrity,
            },
          },
        },
      };
      return packument;
    };

    // Every advisory grant the probe's capability walk surfaces must be in the
    // operator's approved set or the gate fails closed; this is a superset of
    // what a toolless mail-triggered agent step surfaces.
    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${deploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    // 1) Install + probe + gate + freeze through the production entrypoint.
    const approved = await installAndApproveWorkflowDefinition({
      source,
      pin: `${PACKAGE_NAME}@${PACKAGE_VERSION}`,
      registryConfig,
      entry: WORKFLOW_ENTRY,
      assetId: DEFINITION_ASSET_ID,
      approvals: operatorApprovals,
      router: env.hub.router,
      db: h.db,
      fetchPackument,
    });

    if (!approved.approval.ok) {
      throw new Error(
        `install/approve gate did not approve (reason: ${approved.approval.reason}): ` +
          `${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
      );
    }
    // The probe evaluated the pinned package and projected it to its inert
    // needs-surface; the freeze anchored the recomputed wire hash.
    expect(approved.projection.id).toBe("wf_walking_skeleton");
    expect(approved.projection.stepOrder).toEqual([STEP_ID]);
    expect(approved.closure.topLevel).toEqual([
      { name: PACKAGE_NAME, version: PACKAGE_VERSION },
    ]);
    expect(approved.approval.approvedWireHash.length).toBeGreaterThan(0);

    // The freeze persisted the approved wire hash onto the definition's
    // version row.
    const versionRow = await h.db
      .select({
        approvedWireHash: workflowDefinitionVersionTable.approvedWireHash,
      })
      .from(workflowDefinitionVersionTable)
      .where(
        and(
          eq(
            workflowDefinitionVersionTable.definitionId,
            approved.approval.definitionId,
          ),
          eq(workflowDefinitionVersionTable.version, "1"),
        ),
      )
      .limit(1)
      .then((rows) => rows[0]);
    expect(versionRow?.approvedWireHash).toBe(
      approved.approval.approvedWireHash,
    );

    // 2) Deploy by source-ref through the production entrypoint. The concrete
    // inference source (baseURL -> mock inference) is supplied per step; the
    // fixture agent's declared `anthropic:mock-model` source binds to it.
    const inferenceSource = {
      id: "anthropic:mock-model",
      provider: "anthropic",
      baseURL: `http://localhost:${String(env.inference.server.port)}`,
      apiKey: "sk-mock",
      model: "mock-model",
    };
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: `ins_${DEPLOYMENT_ID}`,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: deploymentMailAddress,
      systemPrompt: "Fallback prompt (overridden per step by the definition)",
      tools: [],
      grants: [],
      sources: [inferenceSource],
      defaultSource: "anthropic:mock-model",
    };

    const deployResult = await deployCodeSourcedWorkflow({
      approved,
      source,
      sidecarRouter: env.hub.router,
      agentAddress: deploymentMailAddress,
      config,
      sources: { [STEP_ID]: [inferenceSource] },
    });
    expect(deployResult.publicKey.length).toBeGreaterThan(0);

    // Register the deployment so the fixture helpers resolve its workflow-run
    // repo, then wait for the deployment address to become routable.
    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(deploymentMailAddress),
    };
    // The live WorkflowDefinition never leaves the airlocked child, so the
    // handle carries an identity-bearing stand-in built from the inert
    // projection; the fixture helpers this test uses only read the repo
    // identity, not the definition body.
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

    // 3) Fire the mail trigger and assert the run reaches RunCompleted.
    await fireMailTrigger(env, deploymentMailAddress, {
      messageId: "<walking-skeleton-e2e@integration.interchange>",
    });

    const runId = await waitForFirstRunId(env, workflowRunRepoId, {
      timeoutMs: 30_000,
      diagnostics: env.sidecarDiagnostics,
    });
    const terminal = await waitForWorkflowRunComplete(
      env,
      DEPLOYMENT_ID,
      runId,
      {
        timeoutMs: 30_000,
        diagnostics: env.sidecarDiagnostics,
      },
    );
    if (terminal.type !== "RunCompleted") {
      throw new Error(
        `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(terminal.type).toBe("RunCompleted");
  }, 90_000);
});
