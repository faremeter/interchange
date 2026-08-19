// Cross-process custom-adapter integration test (INTR-233).
//
// Proves the whole cross-process pluggability path: an operator-configured
// adapter manifest is serialized into the forked workflow child's substrate
// env, the child deserializes and re-validates it, import()s the custom
// adapter module child-side, and resolves a provider id that no built-in
// supplies. A real hub + real sidecar subprocess + real forked workflow
// child + the mock inference server exercise the exact production wiring;
// nothing here is in-process or mocked at the resolution boundary.
//
// The workflow deploys BY SOURCE-REF (bundle a source entry module into a hub
// asset, probe it, approve+freeze it against a real DB, deploy the source-ref
// frame) through `deployWorkflowSourceForTest` -- the one code-sourced deploy
// front -- rather than the retired live-authored orchestrator path.
//
// POSITIVE: the manifest maps provider "custom-x" to an absolute-path .ts
// fixture adapter (which delegates to the Anthropic adapter so it speaks the
// mock server's wire). A one-step workflow whose source.provider is
// "custom-x" runs to completion in the child and the echoed reply carries
// the inbound body -- the run could only complete if the child resolved
// "custom-x", which is impossible without the manifest crossing the fork and
// being import()-ed child-side. The provider id is the sentinel: a
// built-in-only registry has no "custom-x".
//
// NEGATIVE (the security firewall): a second deployment names a provider the
// manifest does NOT contain. The sidecar deploy router's source-admission
// gate -- built only from the operator manifest plus the linked-in built-ins
// -- rejects it, so the source-ref deploy frame rejects synchronously and
// `deployWorkflowSourceForTest` throws. This proves a provider string (which
// deploy/tenant config does control) cannot conjure an adapter: only
// operator-supplied manifest specifiers can, and they are import()-ed, never
// the provider key.

import path from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress } from "@intx/workflow-deploy";
import { tenant as tenantTable } from "@intx/db/schema";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";

import {
  SESSION_ID,
  SIDECAR_ID,
  deployWorkflowSourceForTest,
  fireMailTrigger,
  readWorkflowRunEvents,
  startDeployFlowEnv,
  waitFor,
  waitForFirstRunId,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type DeployWorkflowSourceForTestHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { singleStepAgentEntry } from "./fixtures/single-step-agent";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const STEP_ID = "step1";

// The provider id the operator manifest maps to the fixture adapter. Not a
// built-in -- resolving it proves the manifest reached the child.
const CUSTOM_PROVIDER = "custom-x";
// A provider id no manifest entry and no built-in supplies -- the negative
// control. The child registry must reject it.
const ABSENT_PROVIDER = "custom-absent";

// Absolute specifier for the fixture adapter module. Absolute paths resolve
// identically regardless of the child's cwd, and bun imports .ts directly.
const FIXTURE_SPECIFIER = path.resolve(
  import.meta.dir,
  "fixtures/custom-inference-adapter.ts",
);

// The definition's own tenant, the caller principal that creates the
// definition assets, and the two `workflow`-kind assets the frozen
// definitions project over (one per deployment). The install/approve freeze
// and the anchor `workflow_run` insert both write against these, so they must
// exist in the real DB before the deploys run.
const TENANT_ID = "tnt_cross_process_custom_adapter";
const CALLER_PRINCIPAL_ID = "prn_cross_process_custom_adapter";
const POSITIVE_ASSET_ID = "ast_cross_process_custom_positive_wf";
const NEGATIVE_ASSET_ID = "ast_cross_process_custom_negative_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
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
    id: POSITIVE_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "cross-process-custom-positive-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });
  await seedAsset(h.db, {
    id: NEGATIVE_ASSET_ID,
    tenantId: TENANT_ID,
    kind: "workflow",
    name: "cross-process-custom-negative-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv({
    inferenceEchoUserMessage: true,
    sidecarEnv: {
      SIDECAR_ADAPTER_MANIFEST: JSON.stringify([
        {
          provider: CUSTOM_PROVIDER,
          specifier: FIXTURE_SPECIFIER,
          export: "makeAdapter",
        },
      ]),
    },
  });
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

// Deploy a one-step workflow BY SOURCE-REF whose single source uses
// `provider`. The entry module pins that provider in the agent's inference
// source, and the operator-pinned per-step source carries it too, so the
// sidecar's source-admission gate and the child's adapter resolution both key
// off the same provider id.
async function deployCustomProviderWorkflow(opts: {
  anchorRunId: string;
  provider: string;
  definitionAssetId: string;
}): Promise<DeployWorkflowSourceForTestHandle> {
  const deploymentMailAddress = deriveRunAddress({
    runId: opts.anchorRunId,
    domain: DEPLOYMENT_DOMAIN,
  });

  const sourceId = `${opts.provider}:mock-model`;
  const inferenceSource: InferenceSource = {
    id: sourceId,
    provider: opts.provider,
    baseURL: `http://localhost:${env.inference.server.port}`,
    apiKey: "sk-mock",
    model: "mock-model",
  };

  const config: HarnessConfig = {
    sessionId: SESSION_ID,
    agentId: opts.anchorRunId,
    tenantId: "tenant-1",
    principalId: "prin_integration-1",
    agentAddress: deploymentMailAddress,
    systemPrompt: "Fallback prompt (overridden per step by the definition)",
    tools: [],
    grants: [],
    sources: [inferenceSource],
    defaultSource: sourceId,
  };

  const entryModule = singleStepAgentEntry({
    stepId: STEP_ID,
    systemPrompt: "You are the cross-process custom-adapter test agent.",
    address: deploymentMailAddress,
    provider: opts.provider,
    agentId: `agent-${opts.anchorRunId}`,
    workflowId: `wf_${opts.anchorRunId}`,
  });

  return deployWorkflowSourceForTest(env, {
    entryModule,
    db: h.db,
    tenantId: TENANT_ID,
    definitionAssetId: opts.definitionAssetId,
    anchorRunId: opts.anchorRunId,
    deploymentDomain: DEPLOYMENT_DOMAIN,
    agentAddress: deploymentMailAddress,
    approvals: "approve-probed",
    config,
    sources: { [STEP_ID]: [inferenceSource] },
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "cross-process custom inference adapter (INTR-233)",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("a manifest custom adapter resolves in the forked child", async () => {
      const anchorRunId = "run_cross-process-custom-positive";
      const body = "Cross-process custom adapter body sentinel-7731.";
      const handle = await deployCustomProviderWorkflow({
        anchorRunId,
        provider: CUSTOM_PROVIDER,
        definitionAssetId: POSITIVE_ASSET_ID,
      });
      expect(handle.publicKey).toBeTruthy();

      const deploymentMailAddress = handle.mailAddress;
      const workflowRunRepoId = handle.workflowRunRepoId;

      // The source-ref frame round-trips through the real sidecar subprocess
      // (index the pack, check out the pinned subtree, register the address),
      // so routability is asynchronous. Wait for it before firing the trigger.
      await waitFor(
        () =>
          env.hub.router.getRoutableAddresses().includes(deploymentMailAddress),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      const mail = await fireMailTrigger(env, deploymentMailAddress, {
        messageId: "<cross-process-custom-positive@integration.interchange>",
        content: body,
      });

      const runId = await waitForFirstRunId(env, workflowRunRepoId, {
        diagnostics: env.sidecarDiagnostics,
        timeoutMs: 20_000,
      });

      const terminal = await waitForWorkflowRunComplete(
        env,
        anchorRunId,
        runId,
        {
          timeoutMs: 20_000,
          diagnostics: env.sidecarDiagnostics,
        },
      );
      if (terminal.type !== "RunCompleted") {
        const events = await readWorkflowRunEvents(env, anchorRunId, runId);
        const failed = events.find(
          (e) => e.type === "StepFailed" || e.type === "RunFailed",
        );
        throw new Error(
          `expected RunCompleted for the custom-adapter run, got ${terminal.type}: ${JSON.stringify(failed?.body)}\n${env.sidecarDiagnostics()}`,
        );
      }

      const events = await readWorkflowRunEvents(env, anchorRunId, runId);
      const startedBody = events.find((e) => e.type === "RunStarted")?.body;
      if (startedBody === undefined) throw new Error("missing RunStarted");
      expect(startedBody["consumedMessageId"]).toBe(mail.messageId);

      // The echoed reply carries the inbound body, proving the custom adapter
      // ran a full inference round-trip in the child (request built, mock SSE
      // parsed) -- not merely that resolution did not throw.
      const reply = readStepReply(stepCompletedBody(events));
      expect(reply.startsWith("echo:")).toBe(true);
      expect(reply).toContain(body);
    });

    test("a provider absent from the manifest is rejected at the source gate", async () => {
      // The firewall: the operator registry holds only the built-ins plus the
      // manifest's "custom-x". A provider id that no manifest entry and no
      // built-in supplies is rejected by the sidecar deploy router's
      // source-admission gate (which reuses `canBuildSource` against that same
      // registry) BEFORE the workflow-process child is spawned. The throw
      // propagates back through the source-ref deploy frame, so
      // `deployWorkflowSourceForTest` rejects synchronously at deploy time
      // rather than failing the first run -- a provider string (which
      // deploy/tenant config controls) cannot conjure an adapter.
      await expect(
        deployCustomProviderWorkflow({
          anchorRunId: "run_cross-process-custom-negative",
          provider: ABSENT_PROVIDER,
          definitionAssetId: NEGATIVE_ASSET_ID,
        }),
      ).rejects.toThrow(new RegExp(`${ABSENT_PROVIDER}.*not registered`));
    });
  },
);

/** Find the single step's `StepCompleted` event body. */
function stepCompletedBody(
  events: { type: string; body: Record<string, unknown> }[],
): Record<string, unknown> {
  const stepCompleted = events.find(
    (e) => e.type === "StepCompleted" && e.body["stepId"] === STEP_ID,
  );
  if (stepCompleted === undefined) {
    throw new Error("missing StepCompleted for the single step");
  }
  return stepCompleted.body;
}

/**
 * Extract the agent's reply string from a `StepCompleted` event body. A small
 * `{ reply, turn }` step output inlines as `inline:<json>`.
 */
function readStepReply(body: Record<string, unknown>): string {
  const output = body["output"];
  if (typeof output !== "object" || output === null || !("ref" in output)) {
    throw new Error(
      `StepCompleted output is not a { ref } record: ${JSON.stringify(output)}`,
    );
  }
  const ref: unknown = output.ref;
  if (typeof ref !== "string") {
    throw new Error(`StepCompleted output ref is not a string: ${String(ref)}`);
  }
  const INLINE_PREFIX = "inline:";
  if (!ref.startsWith(INLINE_PREFIX)) {
    throw new Error(
      `expected an inline output ref for the small step output, got ${ref}`,
    );
  }
  const parsed: unknown = JSON.parse(ref.slice(INLINE_PREFIX.length));
  if (typeof parsed !== "object" || parsed === null || !("reply" in parsed)) {
    throw new Error(
      `step output does not carry a reply field: ${JSON.stringify(parsed)}`,
    );
  }
  const reply: unknown = parsed.reply;
  if (typeof reply !== "string") {
    throw new Error(
      `step output reply is not a string: ${JSON.stringify(parsed)}`,
    );
  }
  return reply;
}
