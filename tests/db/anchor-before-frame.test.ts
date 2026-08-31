// Anchor-before-frame ordering for the allocation-routed source-ref deploy (real DB).
//
// `deployCodeSourcedWorkflow` must INSERT the deployment's anchor `workflow_run`
// row -- committed on the autocommit handle, so visible to a separate
// connection -- BEFORE it emits the deploy frame. The frame spawns the child,
// whose first events pack races the ack back to the hub; `receiveWorkflowRunPack`
// fails closed with `path_violation` on a missing live anchor. Emit-then-insert
// (the previous order) rejected that first pack and never bootstrapped the log.
//
// The tripwire probes INSIDE the send: the fake `sendAgentDeployToAllocation`, at the moment
// it is called (standing in for the child's first pack arriving while the frame
// is on the wire), runs the REAL `receiveWorkflowRunPack` for the deployment
// address and records whether it was accepted. A post-hoc assertion cannot tell
// the two orderings apart -- the anchor exists under both once deploy returns --
// so only an in-send probe distinguishes fail-before from pass-after. Under the
// fixed insert-then-emit order the committed anchor is visible and the pack is
// accepted; under the old order the probe sees no anchor and this test fails.

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";
import { eq } from "drizzle-orm";

import { defineAgent } from "@intx/agent";
import { createNoopCredentialCipher, generateKeyPair } from "@intx/crypto";
import {
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import {
  createAgentRepoStore,
  createHubSessionLookups,
  deployCodeSourcedWorkflow,
  SessionLaunchError,
  type AgentRepoStore,
  type SidecarAllocationRouter,
} from "@intx/hub-sessions";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedTenants, seedWorkflowRun } from "@intx/test-harness/seed";
import type {
  HarnessConfig,
  InferenceSource,
  KeyPair,
} from "@intx/types/runtime";
import { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import { defineWorkflow, projectLiveToInert, step } from "@intx/workflow";
import {
  deriveRunAddress,
  deriveWorkflowRunRepoId,
} from "@intx/workflow-deploy";

import { seedInferenceCredentials } from "../hub-agent/lib/deploy-flow-env";

const TENANT_ID = "tnt_anchor_before_frame";
const DEFINITION_ID = "def_anchor_before_frame";
const DEPLOYMENT_DOMAIN = "workflow.interchange";
const ANCHOR_RUN_ID = "run_abcdef0123456789abcdef0123456789";
const DEPLOY_ADDRESS = deriveRunAddress({
  runId: ANCHOR_RUN_ID,
  domain: DEPLOYMENT_DOMAIN,
});
const WORKFLOW_RUN_REPO_ID = deriveWorkflowRunRepoId(DEPLOY_ADDRESS);
const WORKFLOW_RUN_REF = "refs/heads/events";
const ACKED_PUBLIC_KEY = "ed25519-anchor-before-frame-pubkey";
const ALLOC_ID = "alloc-anchor-test";
const ALLOC_GENERATION = 1;

const INFERENCE_SOURCE: InferenceSource = {
  id: "src-only",
  provider: "anthropic",
  baseURL: "https://api.example/anthropic",
  credentialId: "secret-only",
  model: "mock-model",
};
const SOURCES = { only: [INFERENCE_SOURCE] };
const CONFIG: HarnessConfig = {
  sessionId: "ses-anchor-before-frame",
  agentId: ANCHOR_RUN_ID,
  tenantId: TENANT_ID,
  principalId: "prin-anchor-before-frame",
  agentAddress: DEPLOY_ADDRESS,
  systemPrompt: "deployment-level",
  tools: [],
  grants: [],
  sources: Object.values(SOURCES).flat(),
  defaultSource: "src-only",
};

// Reproduce the approve output the allocation-routed entrypoint consumes, by hand: the
// gate's ok-arm is a plain object, so a real gate/freeze run is unnecessary to
// exercise the deploy ORDERING under test. Project a trivial single-step
// definition to inert wire form, hash it, and pair it with an empty closure.
async function makeApproveBundle(): Promise<{
  approval: {
    ok: true;
    definitionId: string;
    approvedWireHash: string;
    approvedGrants: ReadonlySet<string>;
    projection: WorkflowProjectionDefinition;
  };
  projection: WorkflowProjectionDefinition;
  closure: ToolPackageManifest;
}> {
  const stubAgent = defineAgent({
    id: "stub",
    systemPrompt: "you stub",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
  });
  const definition = defineWorkflow({
    id: "wf_anchor_before_frame",
    trigger: { type: "manual" },
    steps: { only: step({ agent: stubAgent, after: [] }) },
  });
  const roundTripped: unknown = JSON.parse(
    JSON.stringify(projectLiveToInert(definition)),
  );
  const projection = WorkflowProjectionDefinition(roundTripped);
  if (projection instanceof type.errors) {
    throw new Error(
      `inert projection failed WorkflowProjectionDefinition validation: ${projection.summary}`,
    );
  }
  const approvedWireHash = await computeWireDefinitionHash(projection);
  return {
    approval: {
      ok: true,
      definitionId: DEFINITION_ID,
      approvedWireHash,
      approvedGrants: new Set<string>(),
      projection,
    },
    projection,
    closure: { schemaVersion: "1", topLevel: [], entries: [] },
  };
}

describe.skipIf(!harnessDbEnvAvailable())(
  "anchor-before-frame ordering (real DB)",
  () => {
    let h: TestDb;
    let signingKey: KeyPair;
    const tempDirs: string[] = [];

    beforeAll(async () => {
      h = await createTestDb();
      signingKey = await generateKeyPair();
    });

    afterAll(async () => {
      await h.close();
    });

    beforeEach(async () => {
      await h.reset();
      await seedTenants(h.db, [{ id: TENANT_ID }]);
      // The persisted-definition guard and the anchor row's FK both need this.
      await h.db.insert(workflowDefinition).values({
        id: DEFINITION_ID,
        tenantId: TENANT_ID,
        name: "anchor-before-frame",
      });
    });

    afterEach(async () => {
      for (const dir of tempDirs.splice(0)) {
        await fs.promises
          .rm(dir, { recursive: true, force: true })
          .catch(() => {
            // Best-effort test cleanup.
          });
      }
    });

    // A real hub-session lookups over the real DB, with only the inner repo
    // write stubbed: the anchor gate (status + self-anchor + address) runs for
    // real; the pack bytes are never actually applied.
    async function makeLookups() {
      const dataDir = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), "anchor-before-frame-"),
      );
      tempDirs.push(dataDir);
      const agentRepoStore: AgentRepoStore = {
        ...createAgentRepoStore({ dataDir, signingKey }),
        receiveWorkflowRunPack: async () => [],
      };
      return createHubSessionLookups({ db: h.db, agentRepoStore });
    }

    async function probePack(
      lookups: ReturnType<typeof createHubSessionLookups>,
    ) {
      return lookups.receiveWorkflowRunPack(
        { kind: "workflow-run", id: WORKFLOW_RUN_REPO_ID },
        new Uint8Array(),
        WORKFLOW_RUN_REF,
        "pack-tip",
        {
          kind: "allocated",
          agentAddress: DEPLOY_ADDRESS,
          allocationId: ALLOC_ID,
          anchorRunId: ANCHOR_RUN_ID,
          generation: ALLOC_GENERATION,
        },
      );
    }

    async function seedAllocation(): Promise<void> {
      await h.db.insert(sidecarAllocation).values({
        id: ALLOC_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        provisionerId: "provisioner-anchor-test",
        provisionerApiVersion: 1,
        provisionerBindingFingerprint: "fp-anchor-test",
        status: "allocated",
        generation: ALLOC_GENERATION,
        ensureAcceptedGeneration: ALLOC_GENERATION,
      });
    }

    async function deployWith(router: SidecarAllocationRouter): Promise<void> {
      const { approval, projection, closure } = await makeApproveBundle();
      await seedInferenceCredentials(h.db, TENANT_ID, SOURCES, CONFIG);
      await deployCodeSourcedWorkflow({
        sidecarAllocationRouter: router,
        allocationTarget: {
          allocationId: ALLOC_ID,
          generation: ALLOC_GENERATION,
        },
        agentAddress: DEPLOY_ADDRESS,
        config: CONFIG,
        sources: SOURCES,
        approved: { approval, projection, closure },
        source: { kind: "registry", registry: "npm" },
        db: h.db,
        tenantId: TENANT_ID,
        anchorRunId: ANCHOR_RUN_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        credentialCipher: createNoopCredentialCipher(),
      });
    }

    async function readAnchor(): Promise<
      { status: string; publicKey: string | null } | undefined
    > {
      const [row] = await h.db
        .select({
          status: workflowRun.status,
          publicKey: workflowRun.publicKey,
        })
        .from(workflowRun)
        .where(eq(workflowRun.id, ANCHOR_RUN_ID))
        .limit(1);
      return row;
    }

    // A router whose deploy frame emit throws `cause`, standing in for a send
    // that failed. The `onSend` hook runs first, letting a test mutate the
    // just-inserted anchor to exercise the 0-row tripwire branches.
    function throwingRouter(
      cause: unknown,
      onSend?: () => Promise<void>,
    ): SidecarAllocationRouter {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: the shared deploy path only calls sendAgentDeployToAllocation
      return {
        sendAgentDeployToAllocation: async () => {
          if (onSend !== undefined) await onSend();
          throw cause;
        },
      } as unknown as SidecarAllocationRouter;
    }

    async function expectLeaked(
      promise: Promise<void>,
      leakedAgent: boolean,
    ): Promise<void> {
      let caught: unknown;
      try {
        await promise;
      } catch (err) {
        caught = err;
      }
      if (!(caught instanceof SessionLaunchError)) {
        throw new Error(`expected a SessionLaunchError, got ${String(caught)}`);
      }
      expect(caught.leakedAgent).toBe(leakedAgent);
    }

    test("the child's first pack is accepted because the anchor exists before the frame", async () => {
      const { approval, projection, closure } = await makeApproveBundle();
      const lookups = await makeLookups();

      // The probe stands in for the child's first events pack racing back while
      // the deploy frame is on the wire.
      let probeAccepted: boolean | undefined;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub: the shared deploy path only calls sendAgentDeployToAllocation
      const router = {
        sendAgentDeployToAllocation: async () => {
          // In production the provisioner records the allocation before the
          // deploy runs. Here the anchor row only exists once deploy's
          // pre-emit INSERT has committed -- and the allocation FK references
          // it -- so the seed goes here, at the moment the frame would hit
          // the wire, just ahead of the probe.
          await seedAllocation();
          const result = await probePack(lookups);
          probeAccepted = result.accepted;
          return { publicKey: ACKED_PUBLIC_KEY };
        },
      } as unknown as SidecarAllocationRouter;

      await seedInferenceCredentials(h.db, TENANT_ID, SOURCES, CONFIG);
      await deployCodeSourcedWorkflow({
        sidecarAllocationRouter: router,
        allocationTarget: {
          allocationId: ALLOC_ID,
          generation: ALLOC_GENERATION,
        },
        agentAddress: DEPLOY_ADDRESS,
        config: CONFIG,
        sources: SOURCES,
        approved: { approval, projection, closure },
        source: { kind: "registry", registry: "npm" },
        db: h.db,
        tenantId: TENANT_ID,
        anchorRunId: ANCHOR_RUN_ID,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        credentialCipher: createNoopCredentialCipher(),
      });

      // Fails on the old emit-then-insert order: at probe time the anchor row
      // did not exist yet, so the gate returned path_violation.
      expect(probeAccepted).toBe(true);

      // The successful ack stamped the key onto the now-live anchor.
      const [row] = await h.db
        .select({
          status: workflowRun.status,
          publicKey: workflowRun.publicKey,
        })
        .from(workflowRun)
        .where(eq(workflowRun.id, ANCHOR_RUN_ID))
        .limit(1);
      expect(row?.status).toBe("deployed");
      expect(row?.publicKey).toBe(ACKED_PUBLIC_KEY);
    });

    test("a never-sent frame (frameSent:false) rolls the anchor back and reports no leak", async () => {
      const router = throwingRouter(
        Object.assign(new Error("refused before the send"), {
          frameSent: false,
        }),
      );
      await expectLeaked(deployWith(router), false);
      // Proven never on the wire: the anchor is fully rolled back.
      expect(await readAnchor()).toBeUndefined();
    });

    test("a sent-but-unacked frame (frameSent:true) fences the anchor and reports a leak", async () => {
      const router = throwingRouter(
        Object.assign(new Error("ack timeout"), { frameSent: true }),
      );
      await expectLeaked(deployWith(router), true);
      // A child may be live: the anchor is fenced to failed, not deleted.
      const row = await readAnchor();
      expect(row?.status).toBe("failed");
      expect(row?.publicKey).toBeNull();
    });

    test("an untagged emit error is treated as possibly-live: fence and report a leak", async () => {
      // No frameSent evidence at all -- must not claim a clean rollback.
      const router = throwingRouter(new Error("router misconfiguration"));
      await expectLeaked(deployWith(router), true);
      const row = await readAnchor();
      expect(row?.status).toBe("failed");
    });

    test("an anchor insert collision fails closed before the frame with no leak", async () => {
      // A row already owns this anchor id, so the pre-emit INSERT collides. The
      // frame never goes out; the pre-existing row must be left untouched.
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        address: DEPLOY_ADDRESS,
        status: "running",
      });
      let sendCalled = false;
      const router = throwingRouter(new Error("unused"), async () => {
        sendCalled = true;
      });
      await expectLeaked(deployWith(router), false);
      expect(sendCalled).toBe(false);
      expect((await readAnchor())?.status).toBe("running");
    });

    test("a frameSent:false failure whose anchor already vanished refuses to claim safe", async () => {
      // The guarded delete finds 0 rows: the frameSent:false contract lied, so
      // a child may be live and the failure must NOT be reported as safe.
      const router = throwingRouter(
        Object.assign(new Error("refused before the send"), {
          frameSent: false,
        }),
        async () => {
          await h.db
            .delete(workflowRun)
            .where(eq(workflowRun.id, ANCHOR_RUN_ID));
        },
      );
      await expectLeaked(deployWith(router), true);
    });

    test("a sent-but-unacked failure whose anchor already advanced leaves the live run alone", async () => {
      // The guarded flip finds 0 rows because a trigger already flipped the
      // anchor deployed->running: the deploy actually succeeded, so the live run
      // is left as-is while the ack failure is still reported as a leak.
      const router = throwingRouter(
        Object.assign(new Error("ack timeout"), { frameSent: true }),
        async () => {
          await h.db
            .update(workflowRun)
            .set({ status: "running" })
            .where(eq(workflowRun.id, ANCHOR_RUN_ID));
        },
      );
      await expectLeaked(deployWith(router), true);
      expect((await readAnchor())?.status).toBe("running");
    });

    test("receiveWorkflowRunPack accepts a deployed anchor whose public key is still null", async () => {
      // Pins the gate's publicKey-agnosticism: the anchor-before-frame window is
      // a live "deployed" row with a null key, and the gate must accept a pack
      // against it. A future "require publicKey on the anchor" change turns this
      // red rather than silently re-breaking deploy bootstrap.
      await seedWorkflowRun(h.db, {
        id: ANCHOR_RUN_ID,
        anchorRunId: ANCHOR_RUN_ID,
        tenantId: TENANT_ID,
        address: DEPLOY_ADDRESS,
        status: "deployed",
      });
      await seedAllocation();
      const lookups = await makeLookups();

      const result = await probePack(lookups);
      expect(result.accepted).toBe(true);
    });
  },
);
