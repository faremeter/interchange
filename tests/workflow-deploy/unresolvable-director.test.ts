// Unresolvable-director deploy-rejection integration test.
//
// Deploys BY SOURCE-REF a workflow whose step's agent declares a `director`
// ref neither the sidecar's built-in registry nor any closure package can
// resolve, and asserts the install/approve probe REJECTS the deploy. The
// sidecar probe walks the definition's capabilities and fails closed on the
// unresolved director, so `deployWorkflowSourceForTest` rejects with an
// `unresolvable director: <id>` error before any definition is frozen or
// deployed.
//
// This is the defensive case of the capability walk: the probe surfaces the
// unresolved director and refuses to ship an ok probe, so the gate never
// freezes and the deploy never reaches the sidecar. The test also asserts no
// deployment handle was registered for the rejected deploy -- the rejection
// happens before the anchor row insert or the deploy frame.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import { deriveRunAddress, type ApprovalSet } from "@intx/workflow-deploy";
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
  startDeployFlowEnv,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";
import { unresolvableDirectorEntry } from "./fixtures/unresolvable-director";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const DEPLOYMENT_ID = "run_unresolvable-director-1";
const STEP_ID = "step1";
const UNRESOLVABLE_DIRECTOR_ID = "@vendor/missing/director";

// The definition's own tenant, the caller principal that creates the
// definition asset, and the `workflow`-kind asset the frozen definition would
// project over. Seeded for the standard deploy shape even though the probe
// rejects before the freeze reads them.
const TENANT_ID = "tnt_unresolvable_director";
const CALLER_PRINCIPAL_ID = "prn_unresolvable_director";
const DEFINITION_ASSET_ID = "ast_unresolvable_director_wf";

let env: DeployFlowEnv;
let h: TestDb;

beforeAll(async () => {
  if (!harnessDbEnvAvailable()) return;
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
    name: "unresolvable-director-wf",
    creatorPrincipalId: CALLER_PRINCIPAL_ID,
  });

  env = await startDeployFlowEnv();
});

afterAll(async () => {
  if (env !== undefined) await env.teardown();
  if (h !== undefined) await h.close();
});

describe.skipIf(!harnessDbEnvAvailable())(
  "unresolvable-director deploy rejection",
  () => {
    test("sidecar registers with hub", () => {
      expect(env.hub.router.getConnectedSidecars()).toContain(SIDECAR_ID);
    });

    test("deploy rejects when the agent references a director the registry cannot resolve", async () => {
      const deploymentMailAddress = deriveRunAddress({
        runId: DEPLOYMENT_ID,
        domain: DEPLOYMENT_DOMAIN,
      });

      const inferenceSource: InferenceSource = {
        id: "anthropic:mock-model",
        provider: "anthropic",
        baseURL: `http://localhost:${env.inference.server.port}`,
        apiKey: "sk-mock",
        model: "mock-model",
      };

      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${DEPLOYMENT_ID}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: deploymentMailAddress,
        systemPrompt: "Fallback prompt (overridden by the definition)",
        tools: [],
        grants: [],
        sources: [inferenceSource],
        defaultSource: "anthropic:mock-model",
      };

      // The operator approves a broad surface so the deploy cannot fail on a
      // missing inference/mail/director grant: the only remaining rejection
      // vector is the unresolvable director ref the probe fails closed on.
      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${deploymentMailAddress}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        `director:${UNRESOLVABLE_DIRECTOR_ID}`,
      ]);

      const entryModule = unresolvableDirectorEntry({
        stepId: STEP_ID,
        address: deploymentMailAddress,
        directorId: UNRESOLVABLE_DIRECTOR_ID,
      });

      const initialDeploymentCount = env.deployments.size;
      const initialDeployAckCount = env.hub.deployAcks.size;
      const initialStatePackCount = env.hub.statePacks.length;

      let captured: unknown = undefined;
      try {
        await deployWorkflowSourceForTest(env, {
          entryModule,
          db: h.db,
          tenantId: TENANT_ID,
          definitionAssetId: DEFINITION_ASSET_ID,
          anchorRunId: DEPLOYMENT_ID,
          deploymentDomain: DEPLOYMENT_DOMAIN,
          agentAddress: deploymentMailAddress,
          approvals: operatorApprovals,
          config,
          sources: { [STEP_ID]: [inferenceSource] },
        });
      } catch (err) {
        captured = err;
      }

      // The probe rejects with `unresolvable director: <id>`. The rejection is
      // a raw string message from the sidecar, so narrow on the message text
      // rather than an error class.
      expect(captured).toBeDefined();
      const message =
        typeof captured === "string"
          ? captured
          : captured instanceof Error
            ? captured.message
            : String(captured);
      expect(message).toContain("unresolvable director");
      expect(message).toContain(UNRESOLVABLE_DIRECTOR_ID);

      // No partial state: the probe rejects before the anchor row insert, the
      // deploy frame, or `registerDeployment`, so the rejected deploy must not
      // have produced a deployment handle, a deploy-ack, or a state-pack write.
      expect(env.deployments.size).toBe(initialDeploymentCount);
      expect(env.hub.deployAcks.size).toBe(initialDeployAckCount);
      expect(env.hub.statePacks.length).toBe(initialStatePackCount);
    });
  },
);
