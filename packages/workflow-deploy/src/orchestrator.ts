// Workflow-deploy orchestrator.
//
// A deploy validates the workflow, runs the capability walk, and gates on
// operator approval, then routes by step count.
//
// A one-step workflow has no distinct step address: the lone step IS the
// deployment head. It deploys once at the head (`deriveDeploymentAddress`)
// through the single-step hand-off -- the tree staging and the
// `agent.deploy` frame collapse onto one head deploy, with no per-step
// provisioning loop.
//
// A workflow with more than one step derives per-step agent addresses of
// the form `ins_<deploymentId>-<stepId>@<deploymentDomain>`, instantiates
// one agent-state repo per step keyed by the derived address, and writes
// each step's deploy tree onto its own repo. The derivation is a pure
// function of `(deploymentId, stepId, deploymentDomain)`, so the
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
} from "@intx/agent";
import type {
  HarnessConfig,
  InferenceSource,
  ToolDefinition,
} from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import { parseAgentAddress } from "@intx/types";
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
import {
  walkCapabilities,
  type CapabilityWalkResult,
  type GrantDeclarations,
} from "./capability-walk";

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
  instanceId: string;
  config: HarnessConfig;
  deployContent: DeployContent;
  toolPackagePins?: readonly ToolPackagePin[];
}) => Promise<void>;

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
 * orchestrator computes `agentAddress` via `deriveDeploymentAddress`
 * and `agentId` via `deriveDeploymentAgentId`.
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
  instanceId: string;
  config: HarnessConfig;
  deployContent: DeployContent;
  definition: WorkflowDefinition;
  sources: Record<string, InferenceSource[]>;
  hubPublicKey: string;
  toolPackagePins?: readonly ToolPackagePin[];
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
  readonly kind: "multi-step";
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
  readonly deploymentId?: string;
  /**
   * Mail-domain for the deployment. Required. The multi-step branch
   * derives per-step addresses as
   * `ins_<deploymentId>-<stepId>@<deploymentDomain>`; the single-step
   * branch deploys the lone step at `ins_<deploymentId>@<deploymentDomain>`.
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
 * the caller did not supply both `deploymentId` and `deploymentDomain`.
 */
export class MultiStepDeploymentArgsMissingError extends Error {
  constructor(missing: string) {
    super(
      `deploy requires ${missing}; supply both deploymentId and deploymentDomain`,
    );
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

      await writeWorkflowRepoTree({
        workflow: args.workflow,
        walk,
        workflowRepo,
      });

      // A one-step workflow has no distinct steps: the lone step IS the
      // head. It deploys once at the head (no per-step provisioning loop),
      // so it routes through the dedicated single-step hand-off rather
      // than `runMultiStepBranch`. The multi-step branch is reached only
      // for `stepOrder.length >= 2`.
      if (args.workflow.stepOrder.length === 1) {
        const result = await runSingleStepAtHead({
          args,
          deploySingleStepAtHead,
        });
        return { kind: "multi-step", publicKey: result.publicKey };
      }

      const result = await runMultiStepBranch({
        args,
        launchSession,
        sendMultiStepDeploy,
      });
      return { kind: "multi-step", publicKey: result.publicKey };
    },
  };
}

/**
 * Deploy a one-step workflow once at the head. The lone step has no
 * distinct per-step address -- it IS the head (`deriveDeploymentAddress`)
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
}): Promise<MultiStepDeployResult> {
  const { args: deploy, deploySingleStepAtHead } = args;
  const deploymentId = deploy.deploymentId;
  const deploymentDomain = deploy.deploymentDomain;
  if (deploymentId === undefined) {
    throw new MultiStepDeploymentArgsMissingError("deploymentId");
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
  const source = pickStepInferenceSource({
    stepAgent,
    stepId,
    workflowId: deploy.workflow.id,
    config: deploy.config,
    operatorApprovals: deploy.operatorApprovals,
  });

  // The lone step IS the head: one deploy at the deployment address, no
  // per-step derivation. The head's agentId and instanceId are the same
  // `ins_<deploymentId>` identity.
  const headAddress = deriveDeploymentAddress({
    deploymentId,
    deploymentDomain,
  });
  const headId = deriveDeploymentAgentId({ deploymentId });
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

  return deploySingleStepAtHead({
    agentAddress: headAddress,
    agentId: headId,
    instanceId: headId,
    config: headConfig,
    deployContent: headDeployContent,
    definition: deploy.workflow,
    // A workflow step pins a single source (no per-step failover): wrap the
    // one operator-approved source in a one-element list. The per-step
    // failover chain is intentionally an instance-only concern; a workflow
    // step preserves its prior single-source behavior.
    sources: { [stepId]: [source] },
    hubPublicKey: deploy.hubPublicKey,
    ...(deploy.toolPackagePins !== undefined
      ? { toolPackagePins: deploy.toolPackagePins }
      : {}),
  });
}

async function runMultiStepBranch(args: {
  args: DeployWorkflowArgs;
  launchSession: LaunchSessionFn;
  sendMultiStepDeploy: SendMultiStepDeployFn | undefined;
}): Promise<MultiStepDeployResult> {
  const { args: deploy, launchSession, sendMultiStepDeploy } = args;
  const deploymentId = deploy.deploymentId;
  const deploymentDomain = deploy.deploymentDomain;
  if (deploymentId === undefined) {
    throw new MultiStepDeploymentArgsMissingError("deploymentId");
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
    instanceId: string;
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
      deploymentId,
      stepId,
      deploymentDomain,
    });
    const agentId = deriveStepAgentId({ deploymentId, stepId });
    const instanceId = deriveStepInstanceId({ deploymentId, stepId });
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
      instanceId,
      config: stepConfig,
      deployContent: stepDeployContent,
    });
  }
  for (const step of prepared) {
    await launchSession({
      agentAddress: step.agentAddress,
      agentId: step.agentId,
      instanceId: step.instanceId,
      config: step.config,
      deployContent: step.deployContent,
      ...(deploy.toolPackagePins !== undefined
        ? { toolPackagePins: deploy.toolPackagePins }
        : {}),
    });
  }

  const deploymentAddress = deriveDeploymentAddress({
    deploymentId,
    deploymentDomain,
  });
  const deploymentAgentId = deriveDeploymentAgentId({ deploymentId });
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
  });
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
  const isApproved = (source: InferenceSource) =>
    args.operatorApprovals.has(
      `inference.source:${source.provider}:${source.model}`,
    );
  const preferred = args.stepAgent?.inference.sources[0];
  if (preferred !== undefined) {
    const match = args.config.sources.find(
      (s) => s.provider === preferred.provider && s.model === preferred.model,
    );
    if (match !== undefined && isApproved(match)) return match;
  }
  const fallback = args.config.sources.find(
    (s) => s.id === args.config.defaultSource,
  );
  if (fallback !== undefined && isApproved(fallback)) return fallback;
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
 * `(deploymentId, stepId, deploymentDomain)`. Exported so the
 * supervisor can reconstruct the same addresses at spawn time without
 * sharing storage with the orchestrator.
 *
 * The `ins_` prefix is required by `parseAgentAddress` at the substrate
 * boundary; the per-step local-part is concat-only because `stepId` is
 * already constrained to `[a-zA-Z0-9_-]+` by the workflow definition
 * validator.
 */
export function deriveStepAddress(args: {
  deploymentId: string;
  stepId: string;
  deploymentDomain: string;
}): string {
  return `ins_${args.deploymentId}-${args.stepId}@${args.deploymentDomain}`;
}

/**
 * Derive the per-step agent id (the `agent-state` repo's id and the
 * `HarnessConfig.agentId`). Pure function of `(deploymentId, stepId)`.
 */
export function deriveStepAgentId(args: {
  deploymentId: string;
  stepId: string;
}): string {
  return `ins_${args.deploymentId}-${args.stepId}`;
}

/**
 * Derive the per-step session instance id. Pure function of
 * `(deploymentId, stepId)`.
 */
export function deriveStepInstanceId(args: {
  deploymentId: string;
  stepId: string;
}): string {
  return `ins_${args.deploymentId}-${args.stepId}`;
}

/**
 * Derive the deployment-level mail address the supervisor registers on
 * the bus. The shape mirrors `deriveStepAddress` minus the per-step
 * suffix; pure function of `(deploymentId, deploymentDomain)`.
 *
 * The supervisor uses this address as the inbound mail address for the
 * deployment as a whole; per-step bindings carry their own
 * derived-step addresses.
 */
export function deriveDeploymentAddress(args: {
  deploymentId: string;
  deploymentDomain: string;
}): string {
  return `ins_${args.deploymentId}@${args.deploymentDomain}`;
}

/**
 * Resolve where a step's deploy tree lives, given the deployment's step
 * count. This is the single owner of the head/step collapse DECISION for
 * a consumer that must choose the address without knowing the deploy
 * shape: a one-step workflow has no distinct steps, so its lone step IS
 * the head (`deriveDeploymentAddress`); a multi-step deployment keeps the
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
  deploymentId: string;
  stepId: string;
  deploymentDomain: string;
  stepCount: number;
}): string {
  return args.stepCount === 1
    ? deriveDeploymentAddress({
        deploymentId: args.deploymentId,
        deploymentDomain: args.deploymentDomain,
      })
    : deriveStepAddress(args);
}

/**
 * Derive the deployment-level agent id used on the `agent.deploy`
 * frame's `agentId` field. Pure function of `(deploymentId)`.
 */
export function deriveDeploymentAgentId(args: {
  deploymentId: string;
}): string {
  return `ins_${args.deploymentId}`;
}

/**
 * Project a workflow-deployment agent address into the substrate-safe
 * id of its workflow-run repo (`{ kind: "workflow-run", id }`). Pure
 * function of the deployment's agent address.
 *
 * The workflow-run repo's `repoId.id` must match `SAFE_REPO_ID`
 * (`/^[a-zA-Z0-9_-]+$/`, the substrate's repo-path-safety contract in
 * `packages/hub-sessions/src/repo-store/types.ts`), and the supervisor
 * principal's `deploymentId` must equal `workflowRunRepoId.id` for the
 * workflow-run kind handler's authz check to pass. That regex rejects
 * `@` and `.`, both of which appear in every agent address, so the
 * address is sanitized by substituting every disallowed character with
 * `-`.
 *
 * The mapping is lossy (two distinct addresses can collapse to the same
 * slug) but deterministic. The sidecar's deploy router keys the
 * workflow-run repo by this slug at write time; the hub's read routes
 * reconstruct the deployment address via `deriveDeploymentAddress` and
 * apply this same derivation so read and write address the same repo.
 * A collision implies two deployments are claiming the same workflow-run
 * surface, which the sidecar's deploy router rejects at deploy time.
 */
export function deriveWorkflowRunRepoId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
}

/**
 * The id prefix `generateId("deployment")` stamps on every deployment
 * id (mirrors `@intx/hub-common`'s `PREFIXES.deployment`). Every
 * workflow-derived address — both the deployment-level address from
 * `deriveDeploymentAddress` (`ins_dep_<...>`) and the per-step
 * addresses from `deriveStepAddress` (`ins_dep_<...>-<stepId>`) — wraps
 * a deployment id in the `ins_` instance prefix, so its instance id
 * begins with `ins_dep_`. A plain agent-launch instance id is `ins_` +
 * 32 hex characters, which can never produce that segment, so the
 * prefix is an exact discriminator between the two address families.
 */
const DEPLOYMENT_ID_PREFIX = "dep_";

/**
 * True when `address` is a workflow-derived agent address — either the
 * deployment-level address from `deriveDeploymentAddress` or a per-step
 * address from `deriveStepAddress`. Both wrap a deployment id in the
 * `ins_` instance prefix, so both are recognized here.
 *
 * Used by host-side reactions that must treat workflow-derived
 * addresses (which never carry an `agent_instance` row) differently
 * from launched-agent instance addresses.
 */
export function isWorkflowDerivedAddress(address: string): boolean {
  const parsed = parseAgentAddress(address);
  if (parsed === null) {
    return false;
  }
  return parsed.instanceId.startsWith(`ins_${DEPLOYMENT_ID_PREFIX}`);
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
  const perStep: Record<string, GrantDeclarations> = {};
  for (const [stepId, declarations] of walk.perStep) {
    perStep[stepId] = declarations;
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
  const inferenceSources = args.config.sources.map((source) => ({
    provider: source.provider,
    model: source.model,
  }));
  const toolFactories = args.config.tools.map(synthesizeWalkToolFactory);
  return {
    id: args.config.agentId,
    systemPrompt: args.deployContent.systemPrompt,
    toolFactories,
    capabilities: [],
    inference: {
      sources: inferenceSources,
    },
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
  });
}
