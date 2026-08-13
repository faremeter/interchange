// Workflow-deploy orchestrator.
//
// A deploy validates the workflow, runs the capability walk, and gates on
// operator approval, then routes by step count.
//
// A one-step workflow has no distinct step address: the lone step IS the
// deployment head. It deploys once at the head (`deriveRunAddress`)
// through the single-step hand-off -- the tree staging and the
// `agent.deploy` frame collapse onto one head deploy, with no per-step
// provisioning loop.
//
// A workflow with more than one step derives per-step agent addresses of
// the form `<runId>-<stepId>@<domain>`, instantiates
// one agent-state repo per step keyed by the derived address, and writes
// each step's deploy tree onto its own repo. The derivation is a pure
// function of `(runId, stepId, domain)`, so the
// supervisor reconstructs the same addresses at spawn time without any
// per-deploy state.
//
// The workflow definition envelope plus the walk's per-step grant
// declarations land on a `workflow` repo before any agent-state write
// happens; if the workflow repo write fails, no agent-state repo is
// created.

import type {
  AgentDefinition,
  AnnotatedToolFactory,
  BaseEnv,
  DirectorRegistry,
  InferencePreference,
} from "@intx/agent";
import type {
  HarnessConfig,
  InferenceSource,
  ToolDefinition,
} from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { CredentialDelivery } from "@intx/types/sidecar";
import { formatRunAddress } from "@intx/types";
import {
  STEP_ID_PATTERN,
  type Primitive,
  type WorkflowDefinition,
} from "@intx/workflow/definition";

import {
  createApprovalSetGate,
  type ApprovalDecision,
  type ApprovalSet,
  type CapabilityApprovalGate,
} from "./capability-approval";
import { walkCapabilities, type CapabilityWalkResult } from "./capability-walk";

/**
 * Minimal `DeployContent` shape the orchestrator passes through to
 * `launchSession`. Carried as a structural type so this package does
 * not need a runtime dependency on `@intx/hub-sessions` to name the
 * type. Mirrors the public fields of
 * `packages/hub-sessions/src/agent-repo.ts`'s `DeployContent`.
 */
export interface DeployContent {
  readonly systemPrompt: string;
  readonly toolPackageManifest?: unknown;
  readonly assetMounts?: ReadonlyMap<string, string>;
}

/**
 * The launch-session surface the orchestrator depends on. Matches
 * `SessionService.launchSession` so that method can collapse to a
 * thin caller of `deployWorkflow` without any signature juggling.
 */
export type LaunchSessionFn = (params: {
  agentAddress: string;
  agentId: string;
  runId: string;
  config: HarnessConfig;
  deployContent: DeployContent;
  toolPackagePins?: readonly ToolPackagePin[];
}) => Promise<void>;

/**
 * An extracted onTrigger section body carried inline in the deploy frame:
 * the rewritten `{ ref }`-target definition plus the body's own per-step
 * inference-source pins. The sidecar materializes both alongside each other
 * (`assets/workflow/<bodyRef>/workflow.json` + `sources.json`) so a body
 * child resolves its definition AND its inference sources off disk -- the
 * body child runs in-process (no process env) and its env is lost across a
 * restart, so the sources must be durable and co-located with the body
 * definition, not passed through an ephemeral channel.
 *
 * `sources` is keyed by the body's step ids (matching
 * `definition.stepOrder`), each an ordered non-empty failover chain, exactly
 * as the top-level deploy pins its own steps. Every body step id must have a
 * matching entry, per the wire validator's per-body narrow.
 */
export interface ReferencedBodyDefinition {
  readonly definition: WorkflowDefinition;
  readonly sources: Record<string, InferenceSource[]>;
}

/**
 * Multi-step deploy hand-off. Called once after the per-step
 * provisioning loop has completed; mirrors the wire shape the deploy
 * router consumes (the `agent.deploy` frame's `workflow?` field). The
 * caller-site closure constructs the frame and waits on the sidecar's
 * `agent.deploy.ack`, surfacing the supervisor's principal public key
 * back through the result.
 *
 * The orchestrator does not synthesize the deployment-level address;
 * the caller passes the bus-registered address the sidecar's
 * supervisor will accept on the frame's `agentAddress` field. The
 * orchestrator computes `agentAddress` via `deriveRunAddress`
 * and `agentId` via `deriveRunAgentId`.
 *
 * `sources` is keyed by step id (matching `definition.stepOrder`);
 * every step id must have a matching entry, per the wire validator's
 * narrow.
 */
export type SendMultiStepDeployFn = (params: {
  agentAddress: string;
  agentId: string;
  config: HarnessConfig;
  definition: WorkflowDefinition;
  sources: Record<string, InferenceSource[]>;
  hubPublicKey: string;
  /**
   * Extracted onTrigger section bodies to materialize on the sidecar so a
   * body child resolves by ref. Empty/absent for a workflow with no section.
   */
  referencedDefinitions?: readonly ReferencedBodyDefinition[];
}) => Promise<MultiStepDeployResult>;

/**
 * Single-step deploy hand-off. A one-step workflow has no distinct steps
 * (the lone step IS the head), so it does NOT take the per-step
 * provisioning loop: it deploys once at the head, staging the head's
 * deploy tree AND firing the deployment `agent.deploy` frame that carries
 * the workflow definition and the sole step's source pin. The caller-site
 * closure produces the deploy pack, sends the workflow frame (the sidecar
 * initializes the head repo on receipt), then delivers the pack to the
 * head; it waits on the `agent.deploy.ack` and surfaces the supervisor's
 * principal public key back through the result.
 *
 * This carries the head deploy content and tool pins (which the head-tree
 * staging needs) alongside the definition + sources (which the frame
 * needs) -- the union of what `LaunchSessionFn` and `SendMultiStepDeployFn`
 * carry, because for one step the tree staging and the frame collapse onto
 * a single head deploy.
 */
export type DeploySingleStepFn = (params: {
  agentAddress: string;
  agentId: string;
  runId: string;
  config: HarnessConfig;
  deployContent: DeployContent;
  definition: WorkflowDefinition;
  sources: Record<string, InferenceSource[]>;
  hubPublicKey: string;
  toolPackagePins?: readonly ToolPackagePin[];
  /**
   * Extracted onTrigger section bodies to materialize on the sidecar so a
   * body child resolves by ref. Empty/absent for a workflow with no section.
   */
  referencedDefinitions?: readonly ReferencedBodyDefinition[];
  /**
   * Decrypted credential material for the deployment's tools, delivered on the
   * deploy frame so it is resident before any step runs. Absent when the
   * definition binds no credentials.
   */
  credentials?: CredentialDelivery;
}) => Promise<MultiStepDeployResult>;

/**
 * Result returned by `sendMultiStepDeploy`. Surfaces the sidecar
 * supervisor's principal public key (hex-encoded Ed25519) from the
 * `agent.deploy.ack` frame back through `deployWorkflow` so the
 * orchestrator's caller can persist or verify the deployment's
 * cryptographic identity.
 */
export interface MultiStepDeployResult {
  readonly publicKey: string;
}

/**
 * Result returned by `deployWorkflow`. Surfaces the supervisor public key
 * collected from the sidecar's `agent.deploy.ack` so the caller can stash
 * it alongside the deployment record.
 */
export type DeployWorkflowResult = {
  readonly publicKey: string;
};

/**
 * Minimal interface for writing the workflow repo. The orchestrator
 * writes a single tree containing `workflow.json`,
 * `capability-declarations.json`, and `.gitignore`. The structural type
 * keeps `@intx/workflow-deploy` independent of `@intx/hub-sessions`'s
 * substrate.
 */
export interface WorkflowRepoWriter {
  writeWorkflowRepo(args: {
    workflowRepoId: string;
    files: ReadonlyMap<string, string>;
  }): Promise<void>;
}

export interface WorkflowDeployOrchestratorDeps {
  /**
   * Director registry the capability walk consults. The orchestrator
   * does not synthesize a registry itself; the host wiring (hub) folds
   * in `interchange.directors`-loaded factories before constructing the
   * orchestrator.
   */
  readonly directorRegistry: DirectorRegistry;
  /** Writes the workflow repo's deploy tree. Every deploy calls this once. */
  readonly workflowRepo: WorkflowRepoWriter;
  /**
   * Performs the per-agent deploy + session start. The multi-step branch
   * calls this once per step. In production this is
   * `SessionService.launchSession`; tests pass a tracking stub.
   */
  readonly launchSession: LaunchSessionFn;
  /**
   * Fires the deployment-level `agent.deploy` frame that carries the
   * workflow definition and per-step source pins to the sidecar. The
   * multi-step branch calls this exactly once, after every per-step
   * `agent-state` repo has been provisioned via `launchSession`.
   *
   * Optional so a caller that only exercises the single-step branch does
   * not have to wire a stub. The multi-step branch fails fast with
   * `MultiStepDeployHandoffMissingError` if the dep is absent.
   */
  readonly sendMultiStepDeploy?: SendMultiStepDeployFn;
  /**
   * Deploys a single-step workflow once at the head: stages the head's
   * deploy tree and fires the deployment `agent.deploy` frame in one
   * hand-off (see `DeploySingleStepFn`). The single-step branch calls
   * this exactly once and never runs the per-step `launchSession` loop.
   *
   * Optional for the same reason as `sendMultiStepDeploy`; the single-step
   * branch fails fast with `SingleStepDeployHandoffMissingError` if the
   * dep is absent.
   */
  readonly deploySingleStepAtHead?: DeploySingleStepFn;
}

export interface DeployWorkflowArgs {
  /** The workflow definition the orchestrator validates and deploys. */
  readonly workflow: WorkflowDefinition;
  /**
   * Stable identifier the branch concatenates into derived agent
   * addresses. Required.
   */
  readonly runId?: string;
  /**
   * Mail-domain for the deployment. Required. The multi-step branch
   * derives per-step addresses as
   * `<runId>-<stepId>@<deploymentDomain>`; the single-step
   * branch deploys the lone step at `<runId>@<deploymentDomain>`.
   */
  readonly deploymentDomain?: string;
  /**
   * Harness configuration shared across every step's launch. The
   * orchestrator overrides `agentAddress`, `agentId`, and `systemPrompt`
   * per step in the multi-step branch.
   */
  readonly config: HarnessConfig;
  /**
   * Deploy-tree content shared across every step's launch. The
   * orchestrator overrides `systemPrompt` per step in the multi-step
   * branch from the step's agent definition.
   */
  readonly deployContent: DeployContent;
  /** Tool-package pins to ship with every step's deploy. */
  readonly toolPackagePins?: readonly ToolPackagePin[];
  /**
   * Flat set of grant-shape strings the operator has approved for this
   * deployment. Every grant the capability walk surfaces must be in
   * this set; an unapproved grant fails the deploy with the offending
   * step and missing source.
   */
  readonly operatorApprovals: ApprovalSet;
  /**
   * Hex-encoded hub Ed25519 public key threaded onto the `agent.deploy`
   * frame so the sidecar can verify the deploy-tree commit signatures.
   * Required for both deploy paths (single-step head and multi-step).
   */
  readonly hubPublicKey?: string;
}

export interface WorkflowDeployOrchestrator {
  deployWorkflow(args: DeployWorkflowArgs): Promise<DeployWorkflowResult>;
}

/**
 * Error thrown by `deployWorkflow` when a workflow definition fails the
 * orchestrator's pre-deploy validation. Carries the offending workflow
 * id so the caller's logs name the deployment that was rejected.
 */
export class WorkflowDefinitionInvalidError extends Error {
  readonly workflowId: string;
  constructor(workflowId: string, reason: string) {
    super(
      `workflow definition ${JSON.stringify(workflowId)} is invalid: ${reason}`,
    );
    this.name = "WorkflowDefinitionInvalidError";
    this.workflowId = workflowId;
  }
}

/**
 * Error thrown when the orchestrator must derive a per-step address but
 * the caller did not supply both `runId` and `deploymentDomain`.
 */
export class MultiStepDeploymentArgsMissingError extends Error {
  constructor(missing: string) {
    super(`deploy requires ${missing}; supply both runId and deploymentDomain`);
    this.name = "MultiStepDeploymentArgsMissingError";
  }
}

/**
 * Error thrown when the multi-step branch is reached but the
 * `sendMultiStepDeploy` dependency was not wired. The single-step branch
 * does not consult this dep, so the dep is optional on the deps record;
 * callers that may take the multi-step branch must wire it.
 */
export class MultiStepDeployHandoffMissingError extends Error {
  constructor() {
    super(
      "multi-step deploy requires sendMultiStepDeploy dep; wire it on the orchestrator's WorkflowDeployOrchestratorDeps record",
    );
    this.name = "MultiStepDeployHandoffMissingError";
  }
}

/**
 * Error thrown when the single-step branch is reached but the
 * `deploySingleStepAtHead` dependency was not wired. Parallel to
 * `MultiStepDeployHandoffMissingError`; the multi-step branch does not
 * consult this dep, so it is optional on the deps record.
 */
export class SingleStepDeployHandoffMissingError extends Error {
  constructor() {
    super(
      "single-step deploy requires deploySingleStepAtHead dep; wire it on the orchestrator's WorkflowDeployOrchestratorDeps record",
    );
    this.name = "SingleStepDeployHandoffMissingError";
  }
}

/**
 * Error thrown when the capability-approval gate rejects the deploy.
 * Carries the per-step `pending` delta and the unresolvable director
 * ids so the caller can surface the exact remediation surface to the
 * operator.
 */
export class CapabilityApprovalDeniedError extends Error {
  readonly pending: ReadonlyMap<string, readonly string[]>;
  readonly unresolvedDirectors: readonly string[];
  constructor(decision: Extract<ApprovalDecision, { ok: false }>) {
    super(formatApprovalDeniedMessage(decision));
    this.name = "CapabilityApprovalDeniedError";
    this.pending = decision.pending;
    this.unresolvedDirectors = decision.unresolvedDirectors;
  }
}

function formatApprovalDeniedMessage(
  decision: Extract<ApprovalDecision, { ok: false }>,
): string {
  if (decision.unresolvedDirectors.length > 0) {
    const first = decision.unresolvedDirectors[0];
    return `unresolvable director: ${String(first)}`;
  }
  const firstPending = [...decision.pending.entries()][0];
  if (firstPending === undefined) {
    return "capability approval denied";
  }
  const [stepId, missing] = firstPending;
  const firstGrant = missing[0];
  if (firstGrant === undefined) {
    return `step ${stepId} has zero approved sources`;
  }
  return `step ${stepId} missing approval for ${firstGrant}`;
}

/**
 * Build a `WorkflowDeployOrchestrator`. The orchestrator owns the
 * step-count routing (single-step head vs multi-step derived); its deps
 * own everything else.
 */
export function createWorkflowDeployOrchestrator(
  deps: WorkflowDeployOrchestratorDeps,
): WorkflowDeployOrchestrator {
  const {
    directorRegistry,
    workflowRepo,
    launchSession,
    sendMultiStepDeploy,
    deploySingleStepAtHead,
  } = deps;

  return {
    async deployWorkflow(
      args: DeployWorkflowArgs,
    ): Promise<DeployWorkflowResult> {
      validateWorkflowDefinition(args.workflow);

      const walk = walkCapabilities(args.workflow, directorRegistry);
      const gate: CapabilityApprovalGate = createApprovalSetGate(
        args.operatorApprovals,
      );
      const decision = await gate.evaluate(walk);
      if (!decision.ok) {
        throw new CapabilityApprovalDeniedError(decision);
      }

      // Materialize each onTrigger section's authored inline body into its
      // own workflow asset and rewrite the primitive to a ref, so the runtime
      // spawns the body as a child run resolved by ref. The walk above ran on
      // the inline form so the operator approved the body agents' caps; the
      // stored definition carries `{ ref }` bodies from here on.
      const { workflow: deployed, referencedDefinitions } =
        await extractOnTriggerBodies({
          workflow: args.workflow,
          registry: directorRegistry,
          workflowRepo,
          config: args.config,
          operatorApprovals: args.operatorApprovals,
        });

      await writeWorkflowRepoTree({
        workflow: deployed,
        walk,
        workflowRepo,
      });

      // The deploy hand-off ships the EXTRACTED definition: the runtime runs
      // the `definition` frame carried in the deploy, so it must be the one
      // whose onTrigger bodies are `{ ref }` -- the inline form throws at the
      // runtime. Extraction preserves `stepOrder` and every non-onTrigger
      // step, so branch selection and per-step derivation are unaffected. The
      // extracted body definitions ride the frame too (referencedDefinitions)
      // so the sidecar materializes them on disk for the body child to resolve.
      const deployArgs: DeployWorkflowArgs = { ...args, workflow: deployed };

      // A one-step workflow has no distinct steps: the lone step IS the
      // head. It deploys once at the head (no per-step provisioning loop),
      // so it routes through the dedicated single-step hand-off rather
      // than `runMultiStepBranch`. The multi-step branch is reached only
      // for `stepOrder.length >= 2`.
      if (deployArgs.workflow.stepOrder.length === 1) {
        const result = await runSingleStepAtHead({
          args: deployArgs,
          deploySingleStepAtHead,
          referencedDefinitions,
        });
        return { publicKey: result.publicKey };
      }

      const result = await runMultiStepBranch({
        args: deployArgs,
        launchSession,
        sendMultiStepDeploy,
        referencedDefinitions,
      });
      return { publicKey: result.publicKey };
    },
  };
}

/**
 * Deploy a one-step workflow once at the head. The lone step has no
 * distinct per-step address -- it IS the head (`deriveRunAddress`)
 * -- so this pins the sole step's inference source, builds the head
 * config + deploy content, and hands the whole thing to
 * `deploySingleStepAtHead` in a single call. There is no per-step
 * `launchSession` loop and no separate deployment frame: the tree staging
 * and the `agent.deploy` frame collapse onto one head deploy. The result
 * surfaces the sidecar supervisor's principal public key, same as the
 * multi-step branch.
 */
async function runSingleStepAtHead(args: {
  args: DeployWorkflowArgs;
  deploySingleStepAtHead: DeploySingleStepFn | undefined;
  referencedDefinitions: readonly ReferencedBodyDefinition[];
}): Promise<MultiStepDeployResult> {
  const { args: deploy, deploySingleStepAtHead, referencedDefinitions } = args;
  const runId = deploy.runId;
  const deploymentDomain = deploy.deploymentDomain;
  if (runId === undefined) {
    throw new MultiStepDeploymentArgsMissingError("runId");
  }
  if (deploymentDomain === undefined) {
    throw new MultiStepDeploymentArgsMissingError("deploymentDomain");
  }
  if (deploySingleStepAtHead === undefined) {
    throw new SingleStepDeployHandoffMissingError();
  }
  if (deploy.hubPublicKey === undefined) {
    throw new MultiStepDeploymentArgsMissingError("hubPublicKey");
  }

  // The sole step. `validateWorkflowDefinition` already guaranteed
  // `stepOrder` is non-empty and every entry has a matching `steps`
  // primitive; the index access is re-narrowed here for the compiler.
  const stepId = deploy.workflow.stepOrder[0];
  if (stepId === undefined) {
    throw new WorkflowDefinitionInvalidError(
      deploy.workflow.id,
      "single-step deploy requires a non-empty stepOrder",
    );
  }
  const primitive = deploy.workflow.steps[stepId];
  if (primitive === undefined) {
    throw new WorkflowDefinitionInvalidError(
      deploy.workflow.id,
      `step ${stepId} listed in stepOrder is missing from steps`,
    );
  }
  const stepAgent = extractAgent(primitive);
  // The lone step's chain IS the deploy-wide source chain: a one-step
  // workflow pins its FULL ordered chain so the reactor fails over across it
  // -- whole-workflow failover, identical to the instance path. (The
  // multi-step branch keeps the per-step single-source collapse; failover
  // across distinct steps is not a thing.) Unlike the pre-authorized instance
  // path, the workflow deploy is gated: every source in the chain must be in
  // the operator-approved set, and an unapproved source is a loud rejection
  // rather than a silent skip that would reshape the reviewed chain.
  assertChainHeadIsDefault({
    sources: deploy.config.sources,
    defaultSource: deploy.config.defaultSource,
    workflowId: deploy.workflow.id,
  });
  for (const candidate of deploy.config.sources) {
    if (!isSourceApproved(candidate, deploy.operatorApprovals)) {
      throw new WorkflowDefinitionInvalidError(
        deploy.workflow.id,
        `step ${stepId} inference chain includes ${candidate.provider}:${candidate.model}, which is not in the operator-approved grant set`,
      );
    }
  }

  // The lone step IS the head: one deploy at the deployment address, no
  // per-step derivation. The head's agentId and runId are the same
  // `<runId>` (the minted run id) identity.
  const headAddress = deriveRunAddress({
    runId,
    domain: deploymentDomain,
  });
  const headId = deriveRunAgentId({ runId });
  const headConfig: HarnessConfig = {
    ...deploy.config,
    agentAddress: headAddress,
    agentId: headId,
    ...(stepAgent !== null ? { systemPrompt: stepAgent.systemPrompt } : {}),
  };
  const headDeployContent: DeployContent =
    stepAgent !== null
      ? { ...deploy.deployContent, systemPrompt: stepAgent.systemPrompt }
      : deploy.deployContent;

  // Tool pins for the child's tool materialization: prefer the pins carried on
  // the folded step agent (the definition is the self-contained home for tools
  // under the workflow model), falling back to the deploy-supplied pins for the
  // live-authored instance path. Per-step pins for genuine multi-step workflows
  // are a separate, deferred concern; this path is single-step by construction.
  const headToolPackagePins =
    stepAgent?.toolPackagePins ?? deploy.toolPackagePins;

  return deploySingleStepAtHead({
    agentAddress: headAddress,
    agentId: headId,
    runId: headId,
    config: headConfig,
    deployContent: headDeployContent,
    definition: deploy.workflow,
    // Pin the full ordered chain gated above; the reactor fails over forward
    // across it, matching the instance deploy path.
    sources: { [stepId]: [...deploy.config.sources] },
    hubPublicKey: deploy.hubPublicKey,
    ...(headToolPackagePins !== undefined
      ? { toolPackagePins: headToolPackagePins }
      : {}),
    ...(referencedDefinitions.length > 0 ? { referencedDefinitions } : {}),
  });
}

async function runMultiStepBranch(args: {
  args: DeployWorkflowArgs;
  launchSession: LaunchSessionFn;
  sendMultiStepDeploy: SendMultiStepDeployFn | undefined;
  referencedDefinitions: readonly ReferencedBodyDefinition[];
}): Promise<MultiStepDeployResult> {
  const {
    args: deploy,
    launchSession,
    sendMultiStepDeploy,
    referencedDefinitions,
  } = args;
  const runId = deploy.runId;
  const deploymentDomain = deploy.deploymentDomain;
  if (runId === undefined) {
    throw new MultiStepDeploymentArgsMissingError("runId");
  }
  if (deploymentDomain === undefined) {
    throw new MultiStepDeploymentArgsMissingError("deploymentDomain");
  }
  if (sendMultiStepDeploy === undefined) {
    throw new MultiStepDeployHandoffMissingError();
  }
  if (deploy.hubPublicKey === undefined) {
    throw new MultiStepDeploymentArgsMissingError("hubPublicKey");
  }
  // Pin every step's inference source before launching any session.
  // Threading the pin pass ahead of the launch pass means a step whose
  // source the operator never approved (or whose preferred provider+model
  // is missing from HarnessConfig.sources) rejects the whole deploy
  // before `launchSession` provisions an agent-state repo at the sidecar
  // with no rollback. The pin is a pure function of the workflow + config
  // so the up-front pass is safe to run before any side-effecting work.
  type PreparedStep = {
    stepId: string;
    agentAddress: string;
    agentId: string;
    stepRunId: string;
    config: HarnessConfig;
    deployContent: DeployContent;
  };
  const sources: Record<string, InferenceSource[]> = {};
  const prepared: PreparedStep[] = [];
  for (const stepId of deploy.workflow.stepOrder) {
    const primitive = deploy.workflow.steps[stepId];
    if (primitive === undefined) {
      throw new WorkflowDefinitionInvalidError(
        deploy.workflow.id,
        `step ${stepId} listed in stepOrder is missing from steps`,
      );
    }
    const stepAgent = extractAgent(primitive);
    // A workflow step pins a single source (no per-step failover), wrapped in
    // a one-element list. Per-step failover chains are an instance-only
    // concern; this preserves prior workflow-step behavior.
    sources[stepId] = [
      pickStepInferenceSource({
        stepAgent,
        stepId,
        workflowId: deploy.workflow.id,
        config: deploy.config,
        operatorApprovals: deploy.operatorApprovals,
      }),
    ];
    const agentAddress = deriveStepAddress({
      runId,
      stepId,
      domain: deploymentDomain,
    });
    const agentId = deriveStepAgentId({ runId, stepId });
    const stepRunId = deriveStepRunId({ runId, stepId });
    const stepConfig: HarnessConfig = {
      ...deploy.config,
      agentAddress,
      agentId,
      ...(stepAgent !== null ? { systemPrompt: stepAgent.systemPrompt } : {}),
    };
    const stepDeployContent: DeployContent =
      stepAgent !== null
        ? { ...deploy.deployContent, systemPrompt: stepAgent.systemPrompt }
        : deploy.deployContent;
    prepared.push({
      stepId,
      agentAddress,
      agentId,
      stepRunId,
      config: stepConfig,
      deployContent: stepDeployContent,
    });
  }
  for (const step of prepared) {
    await launchSession({
      agentAddress: step.agentAddress,
      agentId: step.agentId,
      runId: step.stepRunId,
      config: step.config,
      deployContent: step.deployContent,
      ...(deploy.toolPackagePins !== undefined
        ? { toolPackagePins: deploy.toolPackagePins }
        : {}),
    });
  }

  const deploymentAddress = deriveRunAddress({
    runId,
    domain: deploymentDomain,
  });
  const deploymentAgentId = deriveRunAgentId({ runId });
  const deploymentConfig: HarnessConfig = {
    ...deploy.config,
    agentAddress: deploymentAddress,
    agentId: deploymentAgentId,
  };
  return sendMultiStepDeploy({
    agentAddress: deploymentAddress,
    agentId: deploymentAgentId,
    config: deploymentConfig,
    definition: deploy.workflow,
    sources,
    hubPublicKey: deploy.hubPublicKey,
    ...(referencedDefinitions.length > 0 ? { referencedDefinitions } : {}),
  });
}

/**
 * Assert the reactor's forward-only failover invariant on a single-step
 * source chain: the chain is non-empty and its head is the default source.
 * The reactor activates the chain's element 0 and fails over forward with no
 * wrap, so the default must be element 0; a default placed elsewhere would
 * silently no-op failover. Shared by the instance and workflow single-step
 * deploy paths, which both pin a full ordered chain.
 *
 * Throws `WorkflowDefinitionInvalidError` (a client/definition error) so the
 * deploy route can classify an inverted request as a 409 rather than a 502.
 */
export function assertChainHeadIsDefault(args: {
  sources: readonly InferenceSource[];
  defaultSource: string;
  workflowId: string;
}): void {
  if (args.sources.length === 0) {
    throw new WorkflowDefinitionInvalidError(
      args.workflowId,
      "config.sources is empty; at least the default source is required as the chain head",
    );
  }
  if (args.sources[0]?.id !== args.defaultSource) {
    throw new WorkflowDefinitionInvalidError(
      args.workflowId,
      `config.sources[0] (${JSON.stringify(
        args.sources[0]?.id,
      )}) must be the default source ${JSON.stringify(
        args.defaultSource,
      )}; a single-step deploy pins the full ordered chain and the reactor activates the head, so the default must be element 0`,
    );
  }
}

/**
 * Whether an inference source is in the operator-approved grant set, keyed
 * by provider and model. The single definition of "approved source," shared
 * by single-step source selection and the single-step chain gate.
 */
export function isSourceApproved(
  source: InferenceSource,
  operatorApprovals: ApprovalSet,
): boolean {
  return operatorApprovals.has(
    `inference.source:${source.provider}:${source.model}`,
  );
}

/**
 * Pick the per-step `InferenceSource` from the deploy's
 * `HarnessConfig.sources`, cross-checked against the operator-approved
 * grant set.
 *
 * The capability walk emits `inference.source:<provider>:<model>`
 * grants only for the (provider, model) pairs the agent declared. The
 * pinning pass here can otherwise resolve a source the walk never
 * surfaced -- the `HarnessConfig.defaultSource` fallback path for an
 * agent whose preference is unresolvable, or the same fallback for a
 * non-agent step (sleep, gate, awaitSignal, ...) whose primitive
 * carries no preference at all. In both cases the orchestrator must
 * refuse to pin a `(provider, model)` the operator never approved;
 * silently shipping an unapproved source would defeat the capability-
 * walk gate the deploy just passed.
 */
function pickStepInferenceSource(args: {
  stepAgent: AgentDefinition<BaseEnv> | null;
  stepId: string;
  workflowId: string;
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): InferenceSource {
  const preferred = args.stepAgent?.inference.sources[0];
  if (preferred !== undefined) {
    const match = args.config.sources.find(
      (s) => s.provider === preferred.provider && s.model === preferred.model,
    );
    if (match !== undefined && isSourceApproved(match, args.operatorApprovals))
      return match;
  }
  const fallback = args.config.sources.find(
    (s) => s.id === args.config.defaultSource,
  );
  if (
    fallback !== undefined &&
    isSourceApproved(fallback, args.operatorApprovals)
  )
    return fallback;
  const preferredDesc =
    preferred !== undefined
      ? `agent preferred ${preferred.provider}:${preferred.model}`
      : `the step's agent declared no preferred source`;
  const fallbackDesc =
    args.config.defaultSource !== undefined
      ? `the deploy's defaultSource ${JSON.stringify(args.config.defaultSource)} does not resolve to an operator-approved source`
      : `the deploy carries no defaultSource to fall back on`;
  throw new WorkflowDefinitionInvalidError(
    args.workflowId,
    `step ${args.stepId} has no approved inference source: ${preferredDesc} is either missing from HarnessConfig.sources or not in the operator-approved grant set, and ${fallbackDesc}`,
  );
}

/**
 * Pure function: derive a step's agent address from
 * `(runId, stepId, domain)`. Exported so the supervisor can reconstruct
 * the same addresses at spawn time without sharing storage with the
 * orchestrator.
 *
 * The local part IS the run id with the step suffix appended; the runId is
 * already a minted `run_<hex>` carrying the `run_` marker `parseRunAddress`
 * requires at the substrate boundary. The per-step local-part is concat-only
 * because `stepId` is already constrained to `[a-zA-Z0-9_-]+` by the workflow
 * definition validator.
 */
export function deriveStepAddress(args: {
  runId: string;
  stepId: string;
  domain: string;
}): string {
  return formatRunAddress(`${args.runId}-${args.stepId}`, args.domain);
}

/**
 * Derive the per-step agent id (the `agent-state` repo's id and the
 * `HarnessConfig.agentId`). Pure function of `(runId, stepId)`.
 */
export function deriveStepAgentId(args: {
  runId: string;
  stepId: string;
}): string {
  return `${args.runId}-${args.stepId}`;
}

/**
 * Derive the per-step run id. Pure function of `(runId, stepId)`.
 */
export function deriveStepRunId(args: {
  runId: string;
  stepId: string;
}): string {
  return `${args.runId}-${args.stepId}`;
}

/**
 * Derive the deployment-level mail address the supervisor registers on
 * the bus. It is the run id `@` the domain; pure function of `(runId, domain)`.
 *
 * The supervisor uses this address as the inbound mail address for the
 * deployment as a whole; per-step bindings carry their own
 * derived-step addresses.
 */
export function deriveRunAddress(args: {
  runId: string;
  domain: string;
}): string {
  return formatRunAddress(args.runId, args.domain);
}

/**
 * Resolve where a step's deploy tree lives, given the deployment's step
 * count. This is the single owner of the head/step collapse DECISION for
 * a consumer that must choose the address without knowing the deploy
 * shape: a one-step workflow has no distinct steps, so its lone step IS
 * the head (`deriveRunAddress`); a multi-step deployment keeps the
 * head distinct from its per-step addresses (`deriveStepAddress`). The
 * sidecar child reads its deploy tree from the address this returns,
 * keyed only off the deployment mailbox and the host-sourced `stepCount`.
 *
 * The producers do not route through here -- each handles one shape
 * unconditionally: the single-step deploy stages the tree at the head,
 * the multi-step deploy at each per-step address. Because `stepCount` is
 * the deployed definition's `stepOrder.length`, sourced from the host,
 * the consumer's collapse always agrees with whichever producer staged
 * the tree; the two processes never derive divergent addresses.
 */
export function resolveStepAddress(args: {
  runId: string;
  stepId: string;
  domain: string;
  stepCount: number;
}): string {
  return args.stepCount === 1
    ? deriveRunAddress({
        runId: args.runId,
        domain: args.domain,
      })
    : deriveStepAddress(args);
}

/**
 * Derive the deployment-level agent id used on the `agent.deploy`
 * frame's `agentId` field. Pure function of `(runId)`.
 */
export function deriveRunAgentId(args: { runId: string }): string {
  return args.runId;
}

/**
 * Project a workflow-deployment agent address into the substrate-safe
 * id of its workflow-run repo (`{ kind: "workflow-run", id }`). Pure
 * function of the deployment's agent address.
 *
 * The workflow-run repo's `repoId.id` must match `SAFE_REPO_ID`
 * (`/^[a-zA-Z0-9_-]+$/`, the substrate's repo-path-safety contract in
 * `packages/hub-sessions/src/repo-store/types.ts`), and the supervisor
 * principal's `runId` must equal `workflowRunRepoId.id` for the
 * workflow-run kind handler's authz check to pass. That regex rejects
 * `@` and `.`, both of which appear in every agent address, so the
 * address is sanitized by substituting every disallowed character with
 * `-`.
 *
 * The mapping is lossy (two distinct addresses can collapse to the same
 * slug) but deterministic. The sidecar's deploy router keys the
 * workflow-run repo by this slug at write time; the hub's read routes
 * reconstruct the deployment address via `deriveRunAddress` and
 * apply this same derivation so read and write address the same repo.
 * A collision implies two deployments are claiming the same workflow-run
 * surface, which the sidecar's deploy router rejects at deploy time.
 */
export function deriveWorkflowRunRepoId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
}

/**
 * Run the in-orchestrator validation pass against a `WorkflowDefinition`
 * before any deploy-side work happens. `defineWorkflow` already
 * structurally validates definitions at authoring time; this pass
 * defensively re-asserts the deploy-relevant constraints in case the
 * caller hands in a definition synthesized through a different path.
 */
function validateWorkflowDefinition(workflow: WorkflowDefinition): void {
  if (workflow.stepOrder.length === 0) {
    throw new WorkflowDefinitionInvalidError(
      workflow.id,
      "stepOrder must be non-empty",
    );
  }
  for (const stepId of workflow.stepOrder) {
    if (!STEP_ID_PATTERN.test(stepId)) {
      throw new WorkflowDefinitionInvalidError(
        workflow.id,
        `step id ${JSON.stringify(stepId)} must match ${STEP_ID_PATTERN.source}`,
      );
    }
    if (workflow.steps[stepId] === undefined) {
      throw new WorkflowDefinitionInvalidError(
        workflow.id,
        `step ${stepId} listed in stepOrder is missing from steps`,
      );
    }
  }
}

/**
 * Project a primitive to its agent definition when it carries one.
 * Mirrors the same projection the capability walk uses; the multi-step
 * branch consumes the agent's `systemPrompt` to override the launch's
 * deploy-tree prompt per step. Primitives without an agent (sleep,
 * gate, awaitSignal, ...) reuse the deploy-shared prompt.
 */
function extractAgent(primitive: Primitive): AgentDefinition<BaseEnv> | null {
  if (primitive.kind === "step") return primitive.agent;
  if (primitive.kind === "map") return primitive.step.agent;
  return null;
}

/**
 * Deploy each onTrigger section's authored inline body as its own workflow
 * asset and rewrite the primitive to reference it. A section runs its body
 * as a child run resolved by ref -- the same production path childWorkflow
 * uses -- so the deployed definition carries `{ ref }` bodies while the
 * author writes `{ inline }`. The ref is derived deterministically from the
 * parent workflow id and the section's step id, so a redeploy of the same
 * definition produces the same ref. A workflow with no inline section body
 * is returned unchanged with no referenced bodies. Exported for a focused
 * unit test.
 *
 * Returns the rewritten workflow AND each extracted body as a
 * `ReferencedBodyDefinition` ({@link ReferencedBodyDefinition}) -- the body
 * definition plus its own per-step inference-source pins -- so the deploy can
 * both store the body at the hub (via `writeWorkflowRepoTree`) and carry it
 * inline in the deploy frame for the sidecar to materialize (the hub-stored
 * copy is not on the sidecar's disk, so a body child's spawn-child would
 * otherwise fail to resolve the ref, and the body child -- in-process, env
 * lost across a restart -- needs its sources durable on disk beside it). Each
 * body's sources are pinned against the operator-approved set exactly as the
 * top-level steps are, and a tool-bearing body agent is rejected here (see
 * `pinBodySources`).
 */
export async function extractOnTriggerBodies(args: {
  workflow: WorkflowDefinition;
  registry: DirectorRegistry;
  workflowRepo: WorkflowRepoWriter;
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): Promise<{
  workflow: WorkflowDefinition;
  referencedDefinitions: readonly ReferencedBodyDefinition[];
}> {
  const steps: Record<string, Primitive> = { ...args.workflow.steps };
  const referencedDefinitions: ReferencedBodyDefinition[] = [];
  let rewritten = false;
  for (const [stepId, primitive] of Object.entries(steps)) {
    if (primitive.kind !== "onTrigger") continue;
    if (!("inline" in primitive.body)) continue;
    const bodyRef = `${args.workflow.id}__${stepId}`;
    const bodyDefinition: WorkflowDefinition = {
      ...primitive.body.inline,
      id: bodyRef,
    };
    const bodyWalk = walkCapabilities(bodyDefinition, args.registry);
    await writeWorkflowRepoTree({
      workflow: bodyDefinition,
      walk: bodyWalk,
      workflowRepo: args.workflowRepo,
    });
    // Pin the body's own per-step inference sources (gated against the same
    // operator-approved set) and reject any tool-bearing body agent -- both in
    // `pinBodySources`. The pins ride inline so the body child resolves
    // inference off disk, durably across a restart.
    const bodySources = pinBodySources({
      body: bodyDefinition,
      config: args.config,
      operatorApprovals: args.operatorApprovals,
    });
    referencedDefinitions.push({
      definition: bodyDefinition,
      sources: bodySources,
    });
    steps[stepId] = { ...primitive, body: { ref: bodyRef } };
    rewritten = true;
  }
  if (!rewritten) {
    return { workflow: args.workflow, referencedDefinitions: [] };
  }
  return { workflow: { ...args.workflow, steps }, referencedDefinitions };
}

/**
 * Pin every step of an extracted onTrigger body to an operator-approved
 * inference source, mirroring the top-level multi-step per-step pin: a single
 * source wrapped in a one-element failover chain, agent-preferred when
 * approved and available, else the gated `defaultSource`. Non-agent body
 * steps (sleep, awaitSignal, childWorkflow) pin the fallback exactly like the
 * top-level non-agent steps, so the body's `sources` covers every `stepOrder`
 * entry -- the coverage the wire validator's per-body narrow requires.
 *
 * A body agent that declares any tool surface is rejected here
 * (`assertBodyAgentToolless`): INTR-310 wires body agent-step execution but
 * DEFERS staging body tool trees, while the section already unions a body
 * agent's tool grants into its own authorized set -- so a tool-bearing body
 * agent would be authorized for tools whose deploy tree is never staged and
 * would materialize an empty tool set at invoke. Reject at deploy rather than
 * ship that silent-correctness trap.
 */
function pinBodySources(args: {
  body: WorkflowDefinition;
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): Record<string, InferenceSource[]> {
  const sources: Record<string, InferenceSource[]> = {};
  for (const stepId of args.body.stepOrder) {
    const primitive = args.body.steps[stepId];
    if (primitive === undefined) {
      throw new WorkflowDefinitionInvalidError(
        args.body.id,
        `body step ${stepId} listed in stepOrder is missing from steps`,
      );
    }
    const stepAgent = extractAgent(primitive);
    assertBodyAgentToolless(stepAgent, args.body.id, stepId);
    sources[stepId] = [
      pickStepInferenceSource({
        stepAgent,
        stepId,
        workflowId: args.body.id,
        config: args.config,
        operatorApprovals: args.operatorApprovals,
      }),
    ];
  }
  return sources;
}

/**
 * Reject a body agent that declares any tool surface (`toolFactories` or
 * `toolPackagePins`). Body agent tool trees are not yet staged (INTR-310
 * follow-up); the section unions a body agent's tool grants into its own
 * authorized set, so a tool-bearing body agent would be authorized for tools
 * whose deploy tree never landed on the sidecar and would materialize an
 * empty tool set at invoke -- a silent-correctness trap. Fail loud at deploy
 * until body tool trees ship. A toolless body agent, or a non-agent step
 * (`agent === null`), is accepted.
 */
function assertBodyAgentToolless(
  agent: AgentDefinition<BaseEnv> | null,
  bodyId: string,
  stepId: string,
): void {
  if (agent === null) return;
  const toolFactoryCount = agent.toolFactories.length;
  const toolPinCount = agent.toolPackagePins?.length ?? 0;
  if (toolFactoryCount === 0 && toolPinCount === 0) return;
  throw new WorkflowDefinitionInvalidError(
    bodyId,
    `onTrigger body step ${stepId} declares a tool-bearing agent (${String(
      toolFactoryCount,
    )} tool factories, ${String(
      toolPinCount,
    )} tool-package pins); body agent tools are not yet supported (INTR-310 follow-up), and shipping one would authorize the body agent for tools whose deploy tree is never staged`,
  );
}

async function writeWorkflowRepoTree(args: {
  workflow: WorkflowDefinition;
  walk: CapabilityWalkResult;
  workflowRepo: WorkflowRepoWriter;
}): Promise<void> {
  const files = new Map<string, string>();
  files.set("workflow.json", JSON.stringify(args.workflow, null, 2));
  files.set(
    "capability-declarations.json",
    JSON.stringify(serializeWalk(args.walk), null, 2),
  );
  files.set(".gitignore", "");
  await args.workflowRepo.writeWorkflowRepo({
    workflowRepoId: args.workflow.id,
    files,
  });
}

function serializeWalk(walk: CapabilityWalkResult): unknown {
  // `GrantDeclarations.grantEffects` is a `Map`, which `JSON.stringify`
  // would silently emit as `{}` -- corrupting capability-declarations.json
  // into effect-less noise. Convert each Map to a plain object explicitly
  // so the audited declaration carries real per-tool effect data.
  const perStep: Record<string, unknown> = {};
  for (const [stepId, declarations] of walk.perStep) {
    perStep[stepId] = {
      grants: declarations.grants,
      grantEffects: Object.fromEntries(declarations.grantEffects),
    };
  }
  return {
    perStep,
    unresolvedDirectors: walk.unresolvedDirectors,
  };
}

/**
 * Build an `AgentDefinition` from a `HarnessConfig` and a
 * `DeployContent`. `SessionService.deployInstanceAtHead` uses it to wrap
 * a single-agent instance's harness as a one-step workflow and deploy it
 * at the head. The deploy tree itself (`deployContent.systemPrompt`, the
 * harness's `tools` and `grants` arrays) is the source of truth for
 * runtime behaviour; the wrap synthesizes only the surfaces the
 * capability walk needs to gate the deploy against the operator-approval
 * set.
 *
 * The walk inspects `agent.toolFactories[i].id` to emit `tool:<id>`
 * grants. The wrap projects each `HarnessConfig.tools[i].name` onto a
 * synthesized `AnnotatedToolFactory` whose `id` matches; the factory
 * function itself is never invoked on the walk path. Skipping this
 * projection would let the gate admit every deploy regardless of what
 * `HarnessConfig.tools` named, weakening the approval gate.
 */
export function wrapHarnessAsSingleStepWorkflow(args: {
  config: HarnessConfig;
  deployContent: DeployContent;
}): AgentDefinition<BaseEnv> {
  return buildSingleStepAgentDefinition({
    id: args.config.agentId,
    systemPrompt: args.deployContent.systemPrompt,
    inferencePreferences: args.config.sources.map((source) => ({
      provider: source.provider,
      model: source.model,
    })),
    toolFactories: args.config.tools.map(synthesizeWalkToolFactory),
  });
}

/**
 * Assemble a single-step `AgentDefinition` from already-resolved fields. This
 * is the single place the single-step agent shape is constructed, shared by
 * the live-config wrap (`wrapHarnessAsSingleStepWorkflow`) and the offline
 * agent-to-workflow fold synthesis, so the two cannot drift on which fields a
 * wrapped or folded agent carries. Callers pass resolved inputs: the wrap
 * passes walk-only synthesized tool factories and no pins; the fold passes
 * empty tool factories (its tools ride as `toolPackagePins`), the agent's own
 * pins, and its catalog-resolved inference preferences.
 */
export function buildSingleStepAgentDefinition(args: {
  id: string;
  systemPrompt: string;
  inferencePreferences: readonly InferencePreference[];
  toolFactories: readonly AnnotatedToolFactory<BaseEnv>[];
  capabilities?: readonly string[];
  description?: string;
  toolPackagePins?: readonly ToolPackagePin[];
}): AgentDefinition<BaseEnv> {
  return {
    id: args.id,
    systemPrompt: args.systemPrompt,
    toolFactories: args.toolFactories,
    capabilities: args.capabilities ?? [],
    inference: { sources: args.inferencePreferences },
    ...(args.description !== undefined
      ? { description: args.description }
      : {}),
    ...(args.toolPackagePins !== undefined
      ? { toolPackagePins: args.toolPackagePins }
      : {}),
  };
}

/**
 * Synthesize an `AnnotatedToolFactory` from a wire-shaped
 * `ToolDefinition`. The factory's `id` mirrors the tool's `name` so the
 * capability walk emits a `tool:<name>` grant the operator-approval
 * gate can deny. The factory function itself is never invoked on the
 * walk path; the wrap never participates in agent instantiation. If a
 * future caller mistakes this synthesized factory for a real one and
 * invokes it, the throw surfaces the misuse loudly rather than silently
 * fabricating a tool bundle.
 *
 * `validateNamespacedId` (the constructor `defineTool` runs) is
 * deliberately skipped: `HarnessConfig.tools[i].name` is the existing
 * wire shape downstream consumers gate against, and re-validating it
 * here would diverge the single-step wrap's surface from what the
 * harness actually loads. The walk and the gate only consult `.id`, so a bare
 * name still produces a stable grant string.
 */
function synthesizeWalkToolFactory(
  tool: ToolDefinition,
): AnnotatedToolFactory<BaseEnv> {
  const factory = (_env: BaseEnv): never => {
    throw new Error(
      `wrapHarnessAsSingleStepWorkflow synthesized tool factory for ${JSON.stringify(tool.name)} is walk-only; do not instantiate the single-step wrap agent`,
    );
  };
  return Object.assign(factory, {
    id: tool.name,
    requires: Object.freeze([]) as readonly string[],
    definitions: [{ name: tool.name }],
  });
}
