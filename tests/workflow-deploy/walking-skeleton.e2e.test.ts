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
  workflowRun as workflowRunTable,
  tenant as tenantTable,
} from "@intx/db/schema";
import type {
  Packument,
  PackumentFetcher,
  RegistryConfig,
} from "@intx/tool-packaging";
import type { HarnessConfig } from "@intx/types/runtime";
import type { WorkflowDefinitionRegistrySource } from "@intx/types/workflow-sources";
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
  listRunIds,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

const DEPLOYMENT_DOMAIN = "integration.interchange";
// A run-first anchor id (run_<hex>): the live trigger->run path derives the run
// id from the deployment address via parseRunAddress, which requires the run_
// prefix, so a non-run_ literal would throw "Invalid run address" mid-test.
const DEPLOYMENT_ID = generateId("workflowRun");
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

// Second deployment: an onTrigger-container workflow whose inline section body
// runs a tool-less agent step. This is the acceptance test for source-ref
// per-body inference-source pinning -- the base test above proves the
// single-step source-ref stack; this one proves an onTrigger body runs inference
// end-to-end from its staged `sources.json`, and fails closed when that file is
// removed.
const BODY_PACKAGE_NAME = "@wf/walking-skeleton-body";
const BODY_PACKAGE_VERSION = "1.0.0";
const BODY_PACKAGE_BASENAME = "walking-skeleton-body";
const BODY_WORKFLOW_ID = "wf_walking_skeleton_body";
const BODY_SECTION_ID = "section";
const BODY_STEP_ID = "work";
const BODY_DEFINITION_ASSET_ID = "ast_walking_skeleton_body_wf";
const BODY_DEPLOYMENT_ID = generateId("workflowRun");
const bodyDeploymentMailAddress = deriveRunAddress({
  runId: BODY_DEPLOYMENT_ID,
  domain: DEPLOYMENT_DOMAIN,
});
// The ref the hub stages the body under -- inlineBodyRef(projection.id,
// sectionId) -- and the id the run child re-derives from the re-evaluated
// closure. They must match, or the body's sources.json ENOENTs at runtime.
const BODY_REF = `${BODY_WORKFLOW_ID}__${BODY_SECTION_ID}`;
// The section's body child run id is `<sectionId>__<index>`; index 0 is the
// first fired event, index 1 the second (used by the tamper sub-case).
const BODY_CHILD_RUN_ID_FIRST = `${BODY_SECTION_ID}__0`;
const BODY_CHILD_RUN_ID_SECOND = `${BODY_SECTION_ID}__1`;

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

// The onTrigger-container entry the second fixture ships. A single section
// subscribed to the deployment's mail address; its inline body is a one-step
// tool-less agent, so a fired event spawns a body child that runs inference from
// the staged per-body sources.json.
const bodyWorkflowEntrySource = `
import { defineWorkflow, onTrigger, step } from "@intx/workflow/definition";
import { defineAgent } from "@intx/agent";

const bodyAgent = defineAgent({
  id: "walking-skeleton-body-agent",
  systemPrompt: "You are the source-ref onTrigger body agent.",
  tools: [],
  capabilities: [],
  inference: {
    sources: [{ provider: "anthropic", model: "mock-model" }],
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(BODY_WORKFLOW_ID)},
  trigger: { type: "mail", to: ${JSON.stringify(bodyDeploymentMailAddress)} },
  steps: {
    ${BODY_SECTION_ID}: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(bodyDeploymentMailAddress)} },
      body: defineWorkflow({
        id: "authored-body",
        trigger: { type: "manual" },
        steps: {
          ${BODY_STEP_ID}: step({ agent: bodyAgent }),
        },
      }),
    }),
  },
});
`;

interface WorkflowPackageFixture {
  bytes: Uint8Array;
  integrity: string;
  tarballFilename: string;
}

// The per-package inputs the fixture builder varies, so the base single-step
// package and the onTrigger-body package are built by the same code path.
interface WorkflowPackageSpec {
  entrySource: string;
  packageName: string;
  packageVersion: string;
  packageBasename: string;
  // A unique tag so the two builds get disjoint scratch subdirs and entry files.
  tag: string;
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
async function bundleWorkflowEntry(
  scratchDir: string,
  entrySource: string,
  tag: string,
): Promise<string> {
  const entrySrcPath = path.join(scratchDir, `${tag}-workflow-entry-src.ts`);
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
  spec: WorkflowPackageSpec,
): Promise<WorkflowPackageFixture> {
  const workflowJs = await bundleWorkflowEntry(
    scratchDir,
    spec.entrySource,
    spec.tag,
  );

  const stagingDir = path.join(scratchDir, spec.tag);
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: spec.packageName,
      version: spec.packageVersion,
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
    tarballFilename: `${spec.packageBasename}-${spec.packageVersion}.tgz`,
  };
}

let env: DeployFlowEnv;
let h: TestDb;
let registryServer: ReturnType<typeof Bun.serve> | undefined;
let scratchDir: string;
let fixture: WorkflowPackageFixture;
let tarballUrl: string;
let bodyFixture: WorkflowPackageFixture;
let bodyTarballUrl: string;

describe.skipIf(!harnessDbEnvAvailable())("walking skeleton e2e", () => {
  beforeAll(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), "walking-skeleton-"));
    fixture = await buildWorkflowPackageFixture(scratchDir, {
      entrySource: workflowEntrySource,
      packageName: PACKAGE_NAME,
      packageVersion: PACKAGE_VERSION,
      packageBasename: PACKAGE_BASENAME,
      tag: "base",
    });
    bodyFixture = await buildWorkflowPackageFixture(scratchDir, {
      entrySource: bodyWorkflowEntrySource,
      packageName: BODY_PACKAGE_NAME,
      packageVersion: BODY_PACKAGE_VERSION,
      packageBasename: BODY_PACKAGE_BASENAME,
      tag: "body",
    });

    // In-process HTTP registry: serves each fixture's tarball bytes so the
    // sidecar's closure materializer can fetch + SRI-verify them. It is
    // consulted only for the tarball GET (the hub reads the packument through
    // the `fetchPackument` seam, no HTTP). Route by tarball filename so the two
    // packages resolve to their own bytes.
    registryServer = Bun.serve({
      port: 0,
      fetch(req) {
        const url = new URL(req.url);
        if (req.method === "GET") {
          if (url.pathname.endsWith(bodyFixture.tarballFilename)) {
            return new Response(bodyFixture.bytes, {
              headers: { "content-type": "application/octet-stream" },
            });
          }
          if (url.pathname.endsWith(fixture.tarballFilename)) {
            return new Response(fixture.bytes, {
              headers: { "content-type": "application/octet-stream" },
            });
          }
        }
        return new Response("not found", { status: 404 });
      },
    });
    const registryBase = `http://localhost:${String(registryServer.port)}`;
    tarballUrl = `${registryBase}/${PACKAGE_NAME}/-/${fixture.tarballFilename}`;
    bodyTarballUrl = `${registryBase}/${BODY_PACKAGE_NAME}/-/${bodyFixture.tarballFilename}`;

    h = await createTestDb();

    // Seed the tenancy both tests' freezes anchor on:
    // installAndApproveWorkflowDefinition projects a first-class
    // workflow_definition over each test's asset, so the shared tenant + creator
    // principal must exist before either gate persists. Seeded once here so the
    // two tests are independent of ordering (each still seeds its own asset).
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
    // The tenant + creator principal are seeded once in beforeAll; this test
    // seeds only its own workflow asset (the freeze projects a first-class
    // workflow_definition over it).
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

    const deployResult = await deployCodeSourcedWorkflow({
      approved,
      source,
      sidecarRouter: env.hub.router,
      agentAddress: deploymentMailAddress,
      config,
      sources: { [STEP_ID]: [inferenceSource] },
      db: h.db,
      tenantId: TENANT_ID,
      anchorRunId: DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    expect(deployResult.publicKey.length).toBeGreaterThan(0);

    // The composed entrypoint wrote the deployment's anchor workflow_run row.
    // Run-grant materialization keys off this row by address; without it, no
    // per-run grants would ever materialize for this source-ref deployment.
    const anchorRow = await h.db
      .select({
        address: workflowRunTable.address,
        publicKey: workflowRunTable.publicKey,
        tenantId: workflowRunTable.tenantId,
      })
      .from(workflowRunTable)
      .where(eq(workflowRunTable.id, DEPLOYMENT_ID))
      .limit(1)
      .then((rows) => rows[0]);
    expect(anchorRow?.address).toBe(deploymentMailAddress);
    expect(anchorRow?.publicKey).toBe(deployResult.publicKey);
    expect(anchorRow?.tenantId).toBe(TENANT_ID);

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

  test("deploys an onTrigger body by source-ref and runs it from the staged per-body sources", async () => {
    // The container is the top-level run; body children are `<sectionId>__<n>`.
    const findBodyContainerRunId = async (
      repoId: RepoId,
    ): Promise<string | undefined> => {
      const ids = await listRunIds(env, repoId);
      return ids.find((id) => !id.startsWith(`${BODY_SECTION_ID}__`));
    };
    const findChildCompleted = (
      events: { type: string; body: Record<string, unknown> }[],
      childRunId: string,
    ): { type: string; body: Record<string, unknown> } | undefined =>
      events.find(
        (e) =>
          e.type === "ChildCompleted" && e.body["childRunId"] === childRunId,
      );

    // Seed the body workflow asset (same tenant/principal as the base test).
    await seedAsset(h.db, {
      id: BODY_DEFINITION_ASSET_ID,
      tenantId: TENANT_ID,
      kind: "workflow",
      name: "walking-skeleton-body-wf",
      creatorPrincipalId: CALLER_PRINCIPAL_ID,
    });

    const source: WorkflowDefinitionRegistrySource = {
      kind: "registry",
      registry: REGISTRY_NAME,
    };
    const registryConfig: RegistryConfig = { url: bodyTarballUrl };
    const fetchPackument: PackumentFetcher = async (packageName) => {
      if (packageName !== BODY_PACKAGE_NAME) {
        throw new Error(`unexpected packument request: ${packageName}`);
      }
      return {
        name: BODY_PACKAGE_NAME,
        versions: {
          [BODY_PACKAGE_VERSION]: {
            name: BODY_PACKAGE_NAME,
            version: BODY_PACKAGE_VERSION,
            dist: { tarball: bodyTarballUrl, integrity: bodyFixture.integrity },
          },
        },
      };
    };

    // The body agent's inference source plus the section's mail grants must all
    // be operator-approved, or the gate/freeze and the per-body pin fail closed.
    const operatorApprovals: ApprovalSet = new Set<string>([
      "inference.source:anthropic:mock-model",
      "director:@intx/agent/default",
      `mail.address:${bodyDeploymentMailAddress}`,
      `mail.send:${DEPLOYMENT_DOMAIN}`,
    ]);

    const approved = await installAndApproveWorkflowDefinition({
      source,
      pin: `${BODY_PACKAGE_NAME}@${BODY_PACKAGE_VERSION}`,
      registryConfig,
      entry: WORKFLOW_ENTRY,
      assetId: BODY_DEFINITION_ASSET_ID,
      approvals: operatorApprovals,
      router: env.hub.router,
      db: h.db,
      fetchPackument,
    });
    if (!approved.approval.ok) {
      throw new Error(
        `body install/approve gate did not approve (reason: ${approved.approval.reason}): ` +
          `${JSON.stringify(approved.approval)}\n${env.sidecarDiagnostics()}`,
      );
    }
    // The frozen projection id is the authored workflow id; the body ref the hub
    // stages under is derived from it.
    expect(approved.projection.id).toBe(BODY_WORKFLOW_ID);

    const inferenceSource = {
      id: "anthropic:mock-model",
      provider: "anthropic",
      baseURL: `http://localhost:${String(env.inference.server.port)}`,
      apiKey: "sk-mock",
      model: "mock-model",
    };
    const config: HarnessConfig = {
      sessionId: SESSION_ID,
      agentId: BODY_DEPLOYMENT_ID,
      tenantId: "tenant-1",
      principalId: "prin_integration-1",
      agentAddress: bodyDeploymentMailAddress,
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
      agentAddress: bodyDeploymentMailAddress,
      config,
      sources: { [BODY_SECTION_ID]: [inferenceSource] },
      db: h.db,
      tenantId: TENANT_ID,
      anchorRunId: BODY_DEPLOYMENT_ID,
      deploymentDomain: DEPLOYMENT_DOMAIN,
    });
    expect(deployResult.publicKey.length).toBeGreaterThan(0);

    const workflowRunRepoId: RepoId = {
      kind: "workflow-run",
      id: deriveDeploymentId(bodyDeploymentMailAddress),
    };
    env.registerDeployment({
      anchorRunId: BODY_DEPLOYMENT_ID,
      workflowDefinition: {
        id: approved.projection.id,
        triggers: [{ type: "mail", to: bodyDeploymentMailAddress }],
        steps: {},
        stepOrder: [...approved.projection.stepOrder],
      },
      workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      mailAddress: bodyDeploymentMailAddress,
    });

    // (a) The hub pinned the body's per-step sources and the sidecar staged them
    // under the SHARED body ref -- inlineBodyRef(frozen projection.id,
    // sectionId). Staging under the FROZEN inert id is the id-equivalence proof
    // at the staging layer; the run child below reads under the id it re-derives
    // from the re-evaluated closure, and the body only runs if the two match.
    const bodySourcesPath = path.join(
      env.sidecar.dataDir,
      "assets",
      "workflow",
      BODY_REF,
      "sources.json",
    );
    await waitFor(
      async () => {
        try {
          await fs.access(bodySourcesPath);
          return true;
        } catch {
          return false;
        }
      },
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );
    const stagedSources: unknown = JSON.parse(
      await fs.readFile(bodySourcesPath, "utf8"),
    );
    // The body step's pinned chain is the operator-approved source, keyed by the
    // body's own step id.
    expect(stagedSources).toEqual({ [BODY_STEP_ID]: [inferenceSource] });

    await waitFor(
      () =>
        env.hub.router
          .getRoutableAddresses()
          .includes(bodyDeploymentMailAddress),
      { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
    );

    // (b) Fire the section trigger; it spawns the body child, whose agent step
    // resolves inference from the staged sources.json and runs for real.
    const inferenceBefore = env.inference.requests.length;
    await fireMailTrigger(env, bodyDeploymentMailAddress, {
      messageId: `<${BODY_DEPLOYMENT_ID}-a@integration.interchange>`,
      content: "run the onTrigger body",
    });

    const containerRunId = await (async () => {
      await waitFor(
        async () =>
          (await findBodyContainerRunId(workflowRunRepoId)) !== undefined,
        { timeoutMs: 30_000, diagnostics: env.sidecarDiagnostics },
      );
      const id = await findBodyContainerRunId(workflowRunRepoId);
      if (id === undefined) throw new Error("no container run id");
      return id;
    })();

    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          BODY_DEPLOYMENT_ID,
          containerRunId,
        );
        return (
          findChildCompleted(events, BODY_CHILD_RUN_ID_FIRST) !== undefined
        );
      },
      { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
    );

    // The container recorded the body child completed; the body child's own
    // agent step completed (ran inference), not failed. Body-runs-e2e AND
    // id-equivalence: the body could only read its sources.json and run if the
    // runtime ref matched the frozen ref the hub staged under.
    const containerEvents = await readWorkflowRunEvents(
      env,
      BODY_DEPLOYMENT_ID,
      containerRunId,
    );
    expect(
      findChildCompleted(containerEvents, BODY_CHILD_RUN_ID_FIRST)?.body[
        "terminalStatus"
      ],
    ).toBe("completed");

    const bodyEvents = await readWorkflowRunEvents(
      env,
      BODY_DEPLOYMENT_ID,
      BODY_CHILD_RUN_ID_FIRST,
    );
    const bodyTypes = bodyEvents.map((e) => e.type);
    expect(bodyTypes).not.toContain("StepFailed");
    const stepCompleted = bodyEvents.find(
      (e) => e.type === "StepCompleted" && e.body["stepId"] === BODY_STEP_ID,
    );
    if (stepCompleted === undefined) {
      throw new Error(
        `missing StepCompleted for the body agent step; body events: ${bodyTypes.join(", ")}\n${env.sidecarDiagnostics()}`,
      );
    }
    // The body's inference call actually reached the mock provider.
    expect(env.inference.requests.length).toBeGreaterThan(inferenceBefore);

    // (c) Fails-closed on missing sources: remove the staged sources.json and
    // fire a second event. The new body child (section__1) cannot resolve its
    // inference sources off disk, so readChildStepInferenceSources throws during
    // env build -- BEFORE any inference. The failure surfaces on the CONTAINER
    // run as the section step failing (the body child records no run of its
    // own), so the section settles StepFailed -> RunFailed rather than the body
    // running unpinned inference.
    await fs.rm(bodySourcesPath);
    const inferenceBeforeTamper = env.inference.requests.length;
    await fireMailTrigger(env, bodyDeploymentMailAddress, {
      messageId: `<${BODY_DEPLOYMENT_ID}-b@integration.interchange>`,
      content: "run the onTrigger body with sources removed",
    });

    await waitFor(
      async () => {
        const events = await readWorkflowRunEvents(
          env,
          BODY_DEPLOYMENT_ID,
          containerRunId,
        );
        return events.some(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
      },
      { timeoutMs: 60_000, diagnostics: env.sidecarDiagnostics },
    );
    const tamperEvents = await readWorkflowRunEvents(
      env,
      BODY_DEPLOYMENT_ID,
      containerRunId,
    );
    // The failure is SPECIFICALLY the missing per-body sources.json -- a
    // fail-closed on tamper, not a fallback to unpinned inference.
    const stepFailed = tamperEvents.find((e) => e.type === "StepFailed");
    if (stepFailed === undefined) {
      throw new Error(
        `expected a StepFailed on the fail-closed path; container events: ${tamperEvents.map((e) => e.type).join(", ")}\n${env.sidecarDiagnostics()}`,
      );
    }
    expect(JSON.stringify(stepFailed.body)).toContain(
      "failed to read child inference sources",
    );
    // The second body child never completed successfully.
    expect(
      findChildCompleted(tamperEvents, BODY_CHILD_RUN_ID_SECOND),
    ).toBeUndefined();
    // It failed BEFORE reaching inference: the mock provider saw no new request.
    expect(env.inference.requests.length).toBe(inferenceBeforeTamper);
  }, 180_000);
});
