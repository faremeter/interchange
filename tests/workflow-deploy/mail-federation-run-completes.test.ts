// Federated deliver path: an agent-to-deployment mail run reaches SUCCESS.
//
// The runId contract has TWO grants writers. The hub-api trigger route is
// covered by `mail-trigger-run-completes-real-route`; this test covers the
// OTHER one: the sidecar's `deliverMailToRecipient`, which materializes a
// receiving deployment's grants when a `mail.outbound` frame (an agent
// sending mail to a workflow deployment) names it. That path derives the
// receiving run's runId from the RECIPIENT address, stages the run's grants
// at `runs/<recipientAddress>/grants.json`, and the supervisor's onRunStart
// barrier reads exactly that path -- so a producer that keys by the mail's
// Message-ID leaves the barrier's path empty and the run fails closed.
//
// One hub, TWO sidecars, one deployment on each -- the sidecar split is
// load-bearing. A mail send is only remote (and so only leaves as a
// `delivered:false` frame that reaches deliverMailToRecipient) when the
// recipient is not registered on the SENDER's own transport; co-locating both
// deployments on one sidecar makes the send local and never exercises the path
// under test.
//   - Deployment B (the RECEIVER) is a completing echo agent on sidecar 2. It
//     is NEVER triggered by the fixture, so its grants come ONLY from the
//     sidecar's real materializer -- if that stages them under the wrong
//     runId, B's onRunStart finds no grants file and B fails closed. B
//     reaching RunCompleted is therefore the whole proof.
//   - Deployment A (the SENDER) is on sidecar 1 and carries the
//     transport-backed `mail_send` tool; its mock inference calls it with
//     `to: <B's address>`, so A's run forwards a real `mail.outbound` frame to
//     the hub, which routes it through
//     `handleMailOutbound -> deliverMailToRecipient(B)`.
//
// The REAL `createMailTriggeredRunGrantsMaterializer` (backed by a migrated
// schema + a real asset service) is wired into the fixture hub's sidecar
// router via the `materializeMailTriggeredRunGrants` option, closing the
// harness gap that let the earlier route test assert only DB rows.
//
// SCOPE: this exercises the RECEIVER seam -- grant materialization + the
// onRunStart barrier -- across a real sidecar transport on ONE hub. It is NOT
// cross-hub coverage: the sender-to-receiver hop stays inside a single hub's
// router, not over a hub-link between two hubs. A dedicated two-hub
// federation-transport test is separate and non-gating; do not read this as
// covering that hop. The harness's dependence on the placement invariant
// below is tracked in INTR-395.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "bun:test";

import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { createGrantStore } from "@intx/db";
import {
  tenant as tenantTable,
  workflowDefinition as workflowDefinitionTable,
  workflowRun as workflowRunTable,
} from "@intx/db/schema";
import { createSSHSignature, generateKeyPair } from "@intx/crypto";
import {
  DEFAULT_ASSET_REF,
  WORKFLOW_JSON_PATH,
  createAssetService,
  createRepoStore,
  workflowAuthorize,
  workflowKindHandler,
  type AuthorizeFn,
  type RepoId,
  type RepoStore,
  type WorkflowRunHubPrincipal,
} from "@intx/hub-sessions";
import { createMailTriggeredRunGrantsMaterializer } from "@intx/hub-api";
import { deriveDeploymentId } from "@intx/sidecar-app/src/workflow-host-wiring";
import type { HarnessConfig, KeyPair } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { WireGrantRule } from "@intx/types/grant-wire";
import {
  createTestDb,
  harnessDbEnvAvailable,
  type TestDb,
} from "@intx/test-harness/db-harness";
import { seedAsset, seedPrincipal } from "@intx/test-harness/seed";
import { defineWorkflow, step, type WorkflowDefinition } from "@intx/workflow";
import {
  createWorkflowDeployOrchestrator,
  deriveRunAddress,
  type ApprovalSet,
  type DeploySingleStepFn,
  type LaunchSessionFn,
  type SendMultiStepDeployFn,
  type WorkflowRepoWriter,
} from "@intx/workflow-deploy";

import {
  SECOND_SIDECAR_ID,
  SECOND_TOKEN,
  SESSION_ID,
  fireMailTrigger,
  startDeployFlowEnv,
  startSidecarSubprocess,
  waitFor,
  waitForReconnect,
  waitForWorkflowRunComplete,
  type DeployFlowEnv,
  type SidecarHandle,
} from "../hub-agent/lib/deploy-flow-env";
import { toLaunchDeployContent } from "./launch-session-bridge";

const DEPLOYMENT_DOMAIN = "integration.interchange";
const WORKFLOW_RUN_REF = "refs/heads/main";

// The RECEIVER, whose grants come only from the sidecar materializer. The
// `run_` prefix is load-bearing and MUST NOT be dropped: the address SHAPE
// selects the materialization path. deliverMailToRecipient only materializes a
// run for a recipient whose address `isRunAddress` recognizes, and that
// predicate keys on the `run_` prefix. A bare id (e.g. `fed-mail-receiver-1` ->
// `fed-mail-receiver-1@...`) fails that predicate, so the mail is routed with
// NO materialization -- and the test still goes GREEN while exercising nothing,
// because the receiver never starts and the RunCompleted assertion is only ever
// reached on the real path. Keep the `run_` prefix.
const RECEIVER_ID = "run_fed-mail-receiver-1";
const RECEIVER_TENANT_ID = "tnt_fed_mail_receiver";
const RECEIVER_CREATOR_PRINCIPAL_ID = "prn_fed_mail_receiver_creator";
const RECEIVER_ASSET_ID = "ast_fed_mail_receiver_wf";
// The SENDER, triggered by the fixture to mail the receiver.
const SENDER_ID = "run_fed-mail-sender-1";

const RECEIVER_STEP_ID = "receive";
const SENDER_STEP_ID = "send";

const MAIL_TOOL = "@intx/tools-mail/sidecar-bundle:mail_send";
const TOOL_PINS: readonly ToolPackagePin[] = [
  { name: "@intx/tools-mail", version: "0.1.2" },
];

const receiverAddress = deriveRunAddress({
  runId: RECEIVER_ID,
  domain: DEPLOYMENT_DOMAIN,
});
const senderAddress = deriveRunAddress({
  runId: SENDER_ID,
  domain: DEPLOYMENT_DOMAIN,
});

// The sender's grant to invoke the mail tool; hand-delivered with its
// fixture trigger (the sender is a plain fixture-triggered run, unlike the
// receiver whose grants are the property under test).
const SENDER_MAIL_GRANT: WireGrantRule = {
  id: "grant-sender-mail",
  resource: `tool:${MAIL_TOOL}`,
  action: "invoke",
  effect: "allow",
  origin: "creator",
  conditions: null,
  expiresAt: null,
  roleId: null,
  principalId: null,
};

// A completing echo agent for the receiver, and a mail-sending agent for
// the sender. Neither declares tools inline; the sender's mail tool comes
// from the pinned bundle + grant.
const receiverAgent = defineAgent({
  id: `agent_${RECEIVER_ID}`,
  systemPrompt: "You are the federated-mail receiver agent.",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});
const senderAgent = defineAgent({
  id: `agent_${SENDER_ID}`,
  systemPrompt: "You are the federated-mail sender agent.",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

const receiverWorkflow: WorkflowDefinition = defineWorkflow({
  id: `wf_${RECEIVER_ID}`,
  trigger: { type: "mail", to: receiverAddress },
  steps: { [RECEIVER_STEP_ID]: step({ agent: receiverAgent }) },
});
const senderWorkflow: WorkflowDefinition = defineWorkflow({
  id: `wf_${SENDER_ID}`,
  trigger: { type: "mail", to: senderAddress },
  steps: { [SENDER_STEP_ID]: step({ agent: senderAgent }) },
});

let env: DeployFlowEnv;
let h: TestDb;
let signingKey: KeyPair;
let repoStore: RepoStore;
let assetService: ReturnType<typeof createAssetService>;
let sidecar2: SidecarHandle | undefined;
const tempDirs: string[] = [];

async function createWorkflowRepoStore(): Promise<RepoStore> {
  const dataDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "fed-mail-completes-"),
  );
  tempDirs.push(dataDir);
  const signer = async (payload: string) =>
    createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey);
  const authorize: AuthorizeFn = (principal, repoId, ref, act) => {
    if (repoId.kind === "workflow") {
      return workflowAuthorize(principal, repoId, ref, act);
    }
    return { allowed: false, reason: `no authorize for ${repoId.kind}` };
  };
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: { workflow: workflowKindHandler },
    authorize,
    signingCallback: () => signer,
  });
}

describe.skipIf(!harnessDbEnvAvailable())(
  "a mail-triggered run started through the sidecar deliver path reaches RunCompleted",
  () => {
    let hasRun = false;

    beforeAll(async () => {
      signingKey = await generateKeyPair();
      h = await createTestDb();
      repoStore = await createWorkflowRepoStore();
      assetService = createAssetService({ db: h.db, repoStore });
      // The REAL materializer the production sidecar router uses, wired into
      // the fixture hub so a `mail.outbound` frame for the receiver drives
      // deliverMailToRecipient through its true grants-write seam.
      const materializer = createMailTriggeredRunGrantsMaterializer({
        db: h.db,
        assetService,
        grantStore: createGrantStore(h.db),
      });
      env = await startDeployFlowEnv({
        transportBackedMailTool: true,
        // The sender's mock inference calls the mail tool with the RECEIVER
        // as `to`; the receiver has no such tool, so its request falls
        // through to echo and its run completes.
        inferenceToolCall: {
          toolName: MAIL_TOOL,
          input: { to: receiverAddress, body: "federated hello" },
        },
        inferenceEchoUserMessage: true,
        materializeMailTriggeredRunGrants: materializer,
      });
    });

    afterAll(async () => {
      if (sidecar2) {
        sidecar2.proc.kill();
        sidecar2 = undefined;
      }
      await env.teardown();
      await h.close();
      for (const d of tempDirs.splice(0)) {
        await fs.promises.rm(d, { recursive: true, force: true });
      }
    });

    beforeEach(async () => {
      await h.reset();
    });

    afterEach(async () => {
      await h.reset();
    });

    test("the receiver's run, grants only from the sidecar materializer, completes", async () => {
      if (hasRun) {
        throw new Error(
          "this suite assumes a single test per shared subprocess env; " +
            "add a new scenario in its own file with its own env instead",
        );
      }
      hasRun = true;

      // The sender and receiver must land on DIFFERENT sidecars: only then is
      // the sender's mail remote to its own transport, so it leaves as a
      // `delivered:false` frame and the hub routes it through
      // deliverMailToRecipient (the path under test) rather than a local
      // persist. findSidecarForNewAgent always places a new deploy on the
      // hub's first-connected sidecar, so capture sidecar 1's handle now, while
      // it is the sole connection, to drive the placement below.
      const startHandles = [...env.hub.liveHandles];
      const sidecar1Handle = startHandles[0];
      if (startHandles.length !== 1 || !sidecar1Handle) {
        throw new Error(
          `expected exactly one connected sidecar at start, got ${startHandles.length}`,
        );
      }
      const hubPort = env.hub.server.port;
      if (hubPort === undefined) {
        throw new Error("hub.server.port is undefined; expected a bound port");
      }

      // Seed the receiver's tenancy + its workflow asset so the materializer
      // can resolve the anchor by address and hydrate the definition.
      await h.db.insert(tenantTable).values({
        id: RECEIVER_TENANT_ID,
        name: RECEIVER_TENANT_ID,
        slug: RECEIVER_TENANT_ID,
        domain: DEPLOYMENT_DOMAIN,
        parentId: null,
      });
      await seedPrincipal(h.db, {
        id: RECEIVER_CREATOR_PRINCIPAL_ID,
        tenantId: RECEIVER_TENANT_ID,
        kind: "user",
        refId: "usr_fed_mail_receiver_creator",
        status: "active",
      });
      await seedAsset(h.db, {
        id: RECEIVER_ASSET_ID,
        tenantId: RECEIVER_TENANT_ID,
        kind: "workflow",
        name: "fed-mail-receiver-wf",
        creatorPrincipalId: RECEIVER_CREATOR_PRINCIPAL_ID,
      });
      await repoStore.initRepo({
        kind: "workflow",
        id: RECEIVER_ASSET_ID,
      });
      await assetService.populateAsset({
        assetId: RECEIVER_ASSET_ID,
        ref: DEFAULT_ASSET_REF,
        principal: { kind: "hub" },
        tree: {
          files: { [WORKFLOW_JSON_PATH]: JSON.stringify(receiverWorkflow) },
          message: "seed receiver workflow.json",
        },
      });
      await h.db.insert(workflowDefinitionTable).values({
        id: `wfd_${RECEIVER_ID}`,
        tenantId: RECEIVER_TENANT_ID,
        name: RECEIVER_ID,
        assetId: RECEIVER_ASSET_ID,
      });
      await h.db.insert(workflowRunTable).values({
        id: RECEIVER_ID,
        tenantId: RECEIVER_TENANT_ID,
        anchorRunId: RECEIVER_ID,
        definitionId: `wfd_${RECEIVER_ID}`,
        address: receiverAddress,
        // The deploy-time birth state; the first mail trigger flips it running.
        status: "deployed",
      });

      // Deploy the receiver onto sidecar 1 while it is the sole connection.
      // The receiver is the PASSIVE party -- it never initiates, it only wakes
      // when the sender's mail arrives -- so it, not the sender, is the one we
      // let absorb the connection churn of the placement dance below.
      await deployWorkflow(receiverWorkflow, RECEIVER_ID, receiverAddress, {
        toolPins: [],
        grants: [],
        approvals: [],
      });

      // Bring up a second sidecar and wait for it to register.
      sidecar2 = await startSidecarSubprocess({
        hubPort,
        registerTempDir: (d) => tempDirs.push(d),
        extraEnv: {
          SIDECAR_ID: SECOND_SIDECAR_ID,
          SIDECAR_TOKEN: SECOND_TOKEN,
        },
      });
      await waitFor(
        () => env.hub.router.getConnectedSidecars().includes(SECOND_SIDECAR_ID),
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Close sidecar 1's original handle so its connections entry is dropped
      // and sidecar 2 becomes the hub's first-connected sidecar. Sidecar 1
      // reconnects and appends AFTER sidecar 2, so once sidecar 2 leads the
      // connection order the next deploy lands on it deterministically,
      // regardless of when sidecar 1 comes back.
      //
      // DO NOT "simplify" this wait away: `getConnectedSidecars()[0] ===
      // SECOND_SIDECAR_ID` is exactly the predicate `findSidecarForNewAgent`
      // evaluates to place a new deploy (first entry of the connections map).
      // Waiting on that precise invariant -- not a sleep, not "sidecar 2 is
      // connected" -- is what makes the placement deterministic rather than
      // racy against sidecar 1's reconnect. Dropping or loosening it silently
      // reintroduces the race and lands the sender on the wrong sidecar (making
      // the send local, which never exercises deliverMailToRecipient). The
      // coupling to this internal placement rule is tracked in INTR-395.
      sidecar1Handle.close();
      await waitFor(
        () => env.hub.router.getConnectedSidecars()[0] === SECOND_SIDECAR_ID,
        { timeoutMs: 20_000, diagnostics: env.sidecarDiagnostics },
      );

      // Deploy the sender; it lands on sidecar 2, the pristine connection that
      // never churns, so the run that must actively fire on trigger is never
      // disrupted.
      await deployWorkflow(senderWorkflow, SENDER_ID, senderAddress, {
        toolPins: TOOL_PINS,
        grants: [SENDER_MAIL_GRANT],
        approvals: [`tool:${MAIL_TOOL}`],
      });

      // Wait for sidecar 1 (the receiver) to reconnect so the receiver is
      // routable again before the sender's mail can arrive, then trigger the
      // sender. Its run signs and forwards a `mail.outbound` frame to the
      // receiver on sidecar 1; because the receiver is not local to sidecar 2's
      // transport, the send is remote and the hub delivers it via
      // deliverMailToRecipient.
      await waitForReconnect(env, receiverAddress);
      await fireMailTrigger(env, senderAddress, {
        messageId: "<fed-mail-sender-kickoff@integration.interchange>",
        grants: [SENDER_MAIL_GRANT],
      });

      // The load-bearing assertion: the RECEIVER's run reaches terminal
      // SUCCESS. Its grants exist only because deliverMailToRecipient staged
      // them at runs/<receiverAddress>/ under the recipient-derived runId. A
      // RunFailed or timeout here is the signature of grants staged under the
      // mail's Message-ID.
      //
      // TRIAGE, if this goes red: this test rides on placement + reconnect
      // settling before it can assert the grants seam, so it has two
      // distinguishable failure points. A timeout at the
      // `getConnectedSidecars()[0]` placement wait or the `waitForReconnect`
      // above is an infrastructure/timing failure, NOT a grants regression --
      // run the dedicated reconnect suites (hub-link-reconnect,
      // reconnect-reemits-parked-correlation) first to disambiguate. Only a
      // reached-here RunFailed or a timeout waiting on the RECEIVER's terminal
      // event below implicates the grants materialization/runId contract this
      // test actually guards.
      const dumpDiag = () =>
        `${env.sidecarDiagnostics()}\n--- sidecar 2 (sender) stderr ---\n${
          sidecar2?.stderr.join("") ?? "(none)"
        }`;
      const terminal = await waitForWorkflowRunComplete(
        env,
        RECEIVER_ID,
        RECEIVER_ID,
        { timeoutMs: 40_000, diagnostics: dumpDiag },
      );
      if (terminal.type !== "RunCompleted") {
        throw new Error(
          `expected RunCompleted for the receiver, got ${terminal.type}: ${JSON.stringify(terminal.body)}\n${dumpDiag()}`,
        );
      }
      expect(terminal.type).toBe("RunCompleted");
    });

    async function deployWorkflow(
      workflow: WorkflowDefinition,
      anchorRunId: string,
      address: string,
      extras: {
        toolPins: readonly ToolPackagePin[];
        grants: WireGrantRule[];
        approvals: string[];
      },
    ): Promise<void> {
      const config: HarnessConfig = {
        sessionId: SESSION_ID,
        agentId: `${anchorRunId}`,
        tenantId: "tenant-1",
        principalId: "prin_integration-1",
        agentAddress: address,
        systemPrompt: "Fallback prompt (overridden per step by orchestrator)",
        tools: [],
        grants: extras.grants,
        sources: [
          {
            id: "anthropic:mock-model",
            provider: "anthropic",
            baseURL: `http://localhost:${String(env.inference.server.port)}`,
            apiKey: "sk-mock",
            model: "mock-model",
          },
        ],
        defaultSource: "anthropic:mock-model",
      };
      const operatorApprovals: ApprovalSet = new Set<string>([
        "inference.source:anthropic:mock-model",
        "director:@intx/agent/default",
        `mail.address:${address}`,
        `mail.send:${DEPLOYMENT_DOMAIN}`,
        ...extras.approvals,
      ]);
      const launchSession: LaunchSessionFn = async (p) => {
        await env.hub.sessionService.stageWorkflowStep({
          agentAddress: p.agentAddress,
          agentId: p.agentId,
          runId: p.runId,
          config: p.config,
          deployContent: toLaunchDeployContent(p.deployContent),
          ...(p.toolPackagePins !== undefined
            ? { toolPackagePins: p.toolPackagePins }
            : {}),
        });
      };
      const sendMultiStepDeploy: SendMultiStepDeployFn = async (params) =>
        env.hub.router.sendAgentDeploy(params.agentAddress, params.config, {
          definition: {
            id: params.definition.id,
            triggers: [...params.definition.triggers],
            stepOrder: [...params.definition.stepOrder],
            steps: params.definition.steps as Record<string, unknown>,
            ...(params.definition.state !== undefined
              ? { state: params.definition.state }
              : {}),
          },
          sources: params.sources,
        });
      const deploySingleStepAtHead: DeploySingleStepFn = (params) =>
        env.hub.sessionService.deploySingleStepAtHead(params);
      const workflowRepo: WorkflowRepoWriter = {
        async writeWorkflowRepo(args) {
          const repoId: RepoId = { kind: "workflow", id: args.workflowRepoId };
          const principal: WorkflowRunHubPrincipal = { kind: "hub" };
          const files: Record<string, string> = {};
          for (const [k, v] of args.files) files[k] = v;
          await env.hub.agentRepoStore.repoStore.writeTree(
            principal,
            repoId,
            DEFAULT_ASSET_REF,
            { files, message: `fed-mail test: ${args.workflowRepoId}` },
          );
        },
      };
      const orchestrator = createWorkflowDeployOrchestrator({
        directorRegistry: createDefaultDirectorRegistry(),
        workflowRepo,
        launchSession,
        sendMultiStepDeploy,
        deploySingleStepAtHead,
      });
      const result = await orchestrator.deployWorkflow({
        workflow,
        config,
        deployContent: { systemPrompt: config.systemPrompt },
        operatorApprovals,
        runId: anchorRunId,
        deploymentDomain: DEPLOYMENT_DOMAIN,
        hubPublicKey: "00".repeat(32),
        ...(extras.toolPins.length > 0
          ? { toolPackagePins: extras.toolPins }
          : {}),
      });
      if (!result.publicKey) {
        throw new Error(
          `deployWorkflow(${anchorRunId}) returned no publicKey\n${env.sidecarDiagnostics()}`,
        );
      }
      env.registerDeployment({
        anchorRunId,
        workflowDefinition: workflow,
        workflowRunRepoId: {
          kind: "workflow-run",
          id: deriveDeploymentId(address),
        },
        workflowRunRef: WORKFLOW_RUN_REF,
        mailAddress: address,
      });
    }
  },
);
