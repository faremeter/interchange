// End-to-end acceptance for the mediated-credential delivery + authorization
// rail, driven through a REAL credential-consuming tool that a CODE-SOURCED
// workflow carries in its own source closure.
//
// A single-step workflow is deployed BY SOURCE-REF; its agent carries the
// inline `probe` tool from `fixtures/credential-tool-bundle.ts`. When the
// definition declares a `credentialBindings` entry, the deploy resolves the
// binding against the DB-seeded provider + credential, decrypts through the
// credential cipher, and carries the `CredentialDelivery` on the source-ref
// deploy frame; the supervisor pushes it into the child's material cell before
// the step agent builds. When the model drives the `probe` tool, the tool
// resolves its declared handle through the consumer-scoped `credentials`
// capability (Gate 2 authorizes it against a `credential:{id}` / `use` grant),
// obtains an http mediated credential, and fetches a path on the credential's
// pinned origin -- an in-process mock server that records the Authorization
// header it sees.
//
// The consumer both ends key on is the probe factory's `id`: the source-ref arm
// sets the synthetic `StepToolFactory.packageName` to the factory id, and the
// author names `credentialBindings[].package` = that same id, so the delivered
// descriptor's `consumer` and the source tool's runtime consumer match.
//
// The proofs:
//   * Positive: the exact source-resolved secret arrives at the pinned origin
//     as a bearer -- the whole rail (source-binding resolution -> deploy-frame
//     delivery -> child cell -> Gate 2 -> provider shape -> authed fetch)
//     carried it, without the secret ever touching disk or the tool's own API.
//   * Channel-only: a workflow that declares NO binding takes its credential
//     purely over the live `credentials.update` channel; the pushed secret
//     reaches the running child and the shaped handle reads it, proving the
//     channel delivers to a source-workflow tool. (A binding-declaring deploy
//     re-applies its frame material on every run's pre-trigger barrier, so a
//     channel push would be clobbered; the channel-only shape is what proves the
//     live channel.)
//   * Negative: a run whose grant does not authorize the consumer fails the
//     resolve closed -- no request ever reaches the origin.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { toolConsumer } from "@intx/authz";
import { createNoopCredentialCipher } from "@intx/crypto";
import { loadFrozenGrantSnapshot } from "@intx/db";
import { tenant as tenantTable } from "@intx/db/schema";
import type { GrantEffect, GrantWalkSnapshot } from "@intx/types";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { CredentialDelivery } from "@intx/types/sidecar";
import type { WireGrantRule } from "@intx/types/grant-wire";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import {
  seedAsset,
  seedCredential,
  seedPrincipal,
  seedProvider,
} from "@intx/test-harness/seed";

import type { RepoId } from "@intx/hub-sessions";

import {
  SESSION_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import {
  CREDENTIAL_HANDLE,
  PROBE_DEFINITION_NAME,
} from "./fixtures/credential-tool-bundle";
import {
  CREDENTIAL_BINDING_PACKAGE,
  credentialToolWorkflowEntry,
} from "./fixtures/credential-tool-workflow";

/**
 * Project a frozen grant-walk snapshot into the run's runtime `tool:`/`effect:`
 * grant rows, in the `run.grants` wire shape the trigger delivers. Mirrors the
 * production `deriveRunRuntimeGrantRows`/`runGrantToWire` tail (not exported
 * from `@intx/hub-api`): one row per distinct grant across steps, tool effect
 * taken from the step's `grantEffects` with `ask` winning over `allow`, effect
 * grants always `allow`. The rows are principal-agnostic (`principalId: null`),
 * matched by resource + action at the child's grant evaluator. The
 * credential:{id} / use grant is NOT in the snapshot (the credential id is
 * resolved at deploy time, after the freeze), so the caller appends it.
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

const DEPLOYMENT_DOMAIN = "integration.interchange";
const STEP_ID = "step1";
const AGENT_ID = "single-step-credential-tool-agent";

// The consumer identity Gate 2 checks. On the source path the tool's
// `packageName` is the probe factory's `id`; the binding names that same id in
// `package`, so the delivered descriptor's `consumer` and the credential-use
// grant's `{ tool }` condition both equal this.
const CONSUMER = toolConsumer(CREDENTIAL_BINDING_PACKAGE);

// The definition's own tenant, the caller principal that creates the definition
// assets, and the tenant-owned provider + credential the binding resolves
// against. The install/approve freeze and the anchor `workflow_run` insert
// write against these, so they must exist in the real DB before the deploy runs.
const TENANT_ID = "tnt_credential_tool";
const CALLER_PRINCIPAL_ID = "prn_credential_tool";
const PROVIDER_ID = "prv_credential_tool";
const PROVIDER_NAME = "credential-probe-provider";
const CREDENTIAL_ID = "cred_credential_tool";
const CREDENTIAL_NAME = "credential-probe-cred";

const SECRET_INITIAL = "sk-probe-alpha-9d2f";
const SECRET_ROTATED = "sk-probe-bravo-7a41";

// The path the model drives `probe` to fetch, and where the tool writes its
// outcome in the step workspace.
const PROBE_PATH = "/whoami";
const PROBE_SENTINEL = "credential-probe-ran.json";

let env: DeployFlowEnv;
let h: TestDb;

// In-process mock origin the mediated http credential authenticates to. Every
// request's Authorization header is recorded so the test can assert the exact
// delivered secret arrived as a bearer. The provider's `apiBaseUrl` is pinned
// to this origin, so the source-resolved credential's material carries it.
let origin: ReturnType<typeof Bun.serve>;
const originRequests: { path: string; authorization: string | null }[] = [];

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
  origin = Bun.serve({
    port: 0,
    fetch(req) {
      const url = new URL(req.url);
      originRequests.push({
        path: url.pathname,
        authorization: req.headers.get("authorization"),
      });
      return new Response(
        JSON.stringify({ ok: true, seenPath: url.pathname }),
        {
          headers: { "content-type": "application/json" },
        },
      );
    },
  });

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
  // The provider backs an origin-pinned http credential: its `plugin` selects
  // the `http` credential provider that shapes the delivered material into the
  // mediated http handle the probe fetches through, and its `apiBaseUrl` is the
  // pinned origin the handle authenticates to.
  await seedProvider(h.db, {
    id: PROVIDER_ID,
    tenantId: TENANT_ID,
    name: PROVIDER_NAME,
    plugin: "http",
    apiBaseUrl: origin.url.origin,
  });
  await seedCredential(h.db, {
    id: CREDENTIAL_ID,
    tenantId: TENANT_ID,
    providerId: PROVIDER_ID,
    name: CREDENTIAL_NAME,
    secret: SECRET_INITIAL,
  });

  env = await startDeployFlowEnv({
    inferenceToolCall: {
      toolName: PROBE_DEFINITION_NAME,
      input: { path: PROBE_PATH, sentinel: PROBE_SENTINEL },
    },
    // The rail is exercised across more than one deployment's run (positive,
    // rotation, negative), so the tool must be driven on every run, not once.
    inferenceToolCallEachRun: true,
  });
});

afterAll(async () => {
  if (origin !== undefined) await origin.stop(true);
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

/** Build a delivery binding the probe's handle to a credential whose http
 *  material carries `secret`, pinned to the mock origin. Used for the live
 *  `credentials.update` rotation push (the deploy-frame delivery is built by
 *  the source-binding resolution). */
function deliveryWithSecret(secret: string): CredentialDelivery {
  return {
    bindings: [
      {
        handle: CREDENTIAL_HANDLE,
        credentialId: CREDENTIAL_ID,
        consumer: CONSUMER,
      },
    ],
    materials: [
      {
        credentialId: CREDENTIAL_ID,
        providerKey: "http",
        origin: origin.url.origin,
        secret,
      },
    ],
  };
}

/** The run grant that authorizes the probe's consumer to use the credential
 *  (Gate 2). Its `{ tool }` condition scopes it to exactly this consumer. Not
 *  in the frozen snapshot (the credential id is deploy-time), so it is appended
 *  to the snapshot-derived run grants at trigger time. */
const CREDENTIAL_USE_GRANT: WireGrantRule = {
  id: "grant-credential-probe-use",
  resource: `credential:${CREDENTIAL_ID}`,
  action: "use",
  effect: "allow",
  origin: "creator",
  conditions: { tool: CONSUMER },
  expiresAt: null,
  roleId: null,
  principalId: null,
};

const inferenceSource: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "",
  apiKey: "sk-mock",
  model: "mock-model",
};

/**
 * Deploy the single-step credential-probe workflow BY SOURCE-REF. With
 * `withBinding`, the definition declares the credential binding, which the
 * deploy resolves against the DB-seeded provider + credential and delivers on
 * the source-ref frame; without it, the deploy carries no credential material
 * and the probe's credential must arrive over the live `credentials.update`
 * channel. Returns the deployment's routing handle plus the snapshot-derived
 * run grants (the probe's frozen `tool:` grants) the trigger must carry so the
 * tool is authorized to run.
 */
async function deployProbeSource(opts: {
  anchorRunId: string;
  definitionAssetId: string;
  withBinding: boolean;
}): Promise<{
  mailAddress: string;
  workflowRunRepoId: RepoId;
  runGrants: WireGrantRule[];
}> {
  const { anchorRunId, definitionAssetId, withBinding } = opts;
  await seedAsset(h.db, {
    id: definitionAssetId,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: definitionAssetId,
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  const mailAddress = deriveRunAddress({
    runId: anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });

  const source: InferenceSource = {
    ...inferenceSource,
    baseURL: `http://localhost:${String(env.inference.server.port)}`,
  };

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: anchorRunId,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: mailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [source],
    defaultSource: "anthropic:mock-model",
  };

  // The operator approves exactly the surface the source workflow declares:
  // the inline probe's bare `tool:<name>` grant (which authorizes the call at
  // run time through the frozen snapshot) and, when the definition declares a
  // binding, the binding's `credential:<handle>` surface (which the gate holds
  // the binding to).
  const operatorApprovals: ApprovalSet = new Set<string>([
    "inference.source:anthropic:mock-model",
    "director:@intx/agent/default",
    `mail.address:${mailAddress}`,
    `mail.send:${DEPLOYMENT_DOMAIN}`,
    `tool:${PROBE_DEFINITION_NAME}`,
    ...(withBinding ? [`credential:${CREDENTIAL_HANDLE}`] : []),
  ]);

  const entryModule = credentialToolWorkflowEntry({
    stepId: STEP_ID,
    systemPrompt: "You are the source credential-probe agent.",
    address: mailAddress,
    agentId: AGENT_ID,
    ...(withBinding
      ? {
          binding: {
            handle: CREDENTIAL_HANDLE,
            provider: PROVIDER_NAME,
            name: CREDENTIAL_NAME,
          },
        }
      : {}),
  });

  const handle = await deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId,
    anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: mailAddress,
    approvals: operatorApprovals,
    config,
    sources: { [STEP_ID]: [source] },
    // The binding is resolved from the DB and decrypted through the cipher; a
    // no-op cipher passes the seeded secret verbatim. Harmless when no binding
    // is declared (the resolution runs only for a definition that binds one).
    credentialCipher: createNoopCredentialCipher(),
  });
  expect(handle.publicKey).toBeTruthy();

  if (!handle.approved.approval.ok) {
    throw new Error("expected an approved definition");
  }
  const snapshot = await loadFrozenGrantSnapshot(
    h.db,
    handle.approved.approval.definitionId,
  );
  if (snapshot === null) {
    throw new Error("expected a frozen grant snapshot for the definition");
  }
  // The probe's capability walk emitted the bare `tool:probe` grant into the
  // frozen snapshot; assert it is present, then project it into the run grants
  // the trigger delivers -- the source path authorizes the tool call through
  // the snapshot, no floor.
  const snapshotToolGrants = snapshot.perStep.flatMap((s) =>
    s.grants.filter((g) => g.startsWith("tool:")),
  );
  expect(snapshotToolGrants).toContain(`tool:${PROBE_DEFINITION_NAME}`);
  const runGrants = deriveWireRunGrants(snapshot);

  // The source-ref frame round-trips through the real sidecar subprocess (index
  // the pack, check out the pinned subtree, register the address), so
  // routability is asynchronous. Wait for it before firing the trigger.
  await waitFor(
    () => env.hub.router.getRoutableAddresses().includes(mailAddress),
    { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
  );

  return {
    mailAddress,
    workflowRunRepoId: handle.workflowRunRepoId,
    runGrants,
  };
}

/** Fire a run and wait for it to complete, returning its id and terminal. */
async function runOnce(
  anchorRunId: string,
  mailAddress: string,
  workflowRunRepoId: RepoId,
  grants: WireGrantRule[],
  messageId: string,
): Promise<{
  runId: string;
  terminal: Awaited<ReturnType<typeof waitForWorkflowRunComplete>>;
}> {
  await fireMailTrigger(env, mailAddress, { messageId, grants });
  const runId = await waitForFirstRunId(env, workflowRunRepoId, {
    diagnostics: env.sidecarDiagnostics,
    timeoutMs: 20_000,
  });
  const terminal = await waitForWorkflowRunComplete(env, anchorRunId, runId, {
    timeoutMs: 20_000,
    diagnostics: env.sidecarDiagnostics,
  });
  return { runId, terminal };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "single-step credential-consuming tool on the code-sourced path",
  () => {
    test("delivers the source-resolved credential and authenticates to its pinned origin", async () => {
      const anchorRunId = "run_credential-tool-pos";
      const { mailAddress, workflowRunRepoId, runGrants } =
        await deployProbeSource({
          anchorRunId,
          definitionAssetId: "ast_credential_tool_pos",
          withBinding: true,
        });

      const before = originRequests.length;
      const { runId, terminal } = await runOnce(
        anchorRunId,
        mailAddress,
        workflowRunRepoId,
        [...runGrants, CREDENTIAL_USE_GRANT],
        "<credential-tool-pos-1@integration.interchange>",
      );
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, anchorRunId, runId);
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(events.find((e) => e.type === "StepFailed" || e.type === "RunFailed")?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      // THE PROOF: the exact source-resolved secret reached the pinned origin as
      // a bearer, and nothing else did. A tool-less run, or one whose credential
      // never reached the probe, would leave the origin untouched.
      const fresh = originRequests.slice(before);
      expect(fresh.length).toBeGreaterThanOrEqual(1);
      const probeReq = fresh.find((r) => r.path === PROBE_PATH);
      expect(probeReq).toBeDefined();
      expect(probeReq?.authorization).toBe(`Bearer ${SECRET_INITIAL}`);
    });

    test("delivers over the live credentials.update channel to a running child", async () => {
      const anchorRunId = "run_credential-tool-chan";
      // Deploy with NO binding, so the source-ref frame carries no credential
      // material: the probe's credential reaches the child ONLY over the live
      // credentials.update channel. (A binding-declaring deploy re-applies its
      // frame material on every run's pre-trigger barrier, which would clobber a
      // channel push; the channel-only path is what proves the live channel.)
      const { mailAddress, workflowRunRepoId, runGrants } =
        await deployProbeSource({
          anchorRunId,
          definitionAssetId: "ast_credential_tool_chan",
          withBinding: false,
        });

      // Push the credential over the live channel and await the child's ack, so
      // the material is resident in the child's cell before the run's agent
      // builds. The channel-delivered secret is the ROTATED one, distinct from
      // the positive case's frame-delivered secret.
      await env.hub.router.sendCredentialsUpdate(
        mailAddress,
        deliveryWithSecret(SECRET_ROTATED),
      );

      const before = originRequests.length;
      const { runId, terminal } = await runOnce(
        anchorRunId,
        mailAddress,
        workflowRunRepoId,
        [...runGrants, CREDENTIAL_USE_GRANT],
        "<credential-tool-chan-1@integration.interchange>",
      );
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, anchorRunId, runId);
        throw new Error(
          `expected RunCompleted, got ${terminal.type}: ${JSON.stringify(events.find((e) => e.type === "StepFailed" || e.type === "RunFailed")?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      // The channel-delivered secret is what the tool sent -- the live push
      // reached the running child's cell and the shaped handle read it.
      const fresh = originRequests.slice(before);
      const probeReq = fresh.find((r) => r.path === PROBE_PATH);
      expect(probeReq).toBeDefined();
      expect(probeReq?.authorization).toBe(`Bearer ${SECRET_ROTATED}`);
    });

    test("fails closed when no grant authorizes the credential's use", async () => {
      const anchorRunId = "run_credential-tool-neg";
      const { mailAddress, workflowRunRepoId, runGrants } =
        await deployProbeSource({
          anchorRunId,
          definitionAssetId: "ast_credential_tool_neg",
          withBinding: true,
        });

      const before = originRequests.length;
      const inferBefore = env.inference.requests.length;
      // Fire the run WITH the tool grant (so the probe runs) but WITHOUT the
      // credential-use grant: Gate 2 denies the resolve inside the tool, which
      // throws before any request is shaped.
      const { terminal } = await runOnce(
        anchorRunId,
        mailAddress,
        workflowRunRepoId,
        runGrants,
        "<credential-tool-neg-1@integration.interchange>",
      );

      // THE PROOF of fail-closed: the model DID drive the tool (a tool_use turn
      // plus a follow-up turn once its error result landed, so >= 2 inference
      // requests), yet the denied credential never authenticated -- nothing
      // reached the origin. The gate refused the use; the tool did not silently
      // skip it.
      expect(
        env.inference.requests.length - inferBefore,
      ).toBeGreaterThanOrEqual(2);
      expect(originRequests.length).toBe(before);
      expect(terminal.type).toBe("RunCompleted");
    });
  },
);
