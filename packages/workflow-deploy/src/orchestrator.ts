// Workflow-deploy orchestrator.
//
// =========================================================================
// THE LOAD-BEARING DICHOTOMY
// =========================================================================
//
// Trivial-workflow path preserves the legacy address shape; multi-step
// uses the derived shape; this asymmetry is the agent-deploy uniformity
// claim's escape hatch.
//
// A workflow with exactly one step that arrives with `trivialBindings`
// reuses the deployment's existing `<agentAddress, agentId, instanceId>`
// triple unchanged. The deploy tree lands on the same per-agent
// `agent-state` repo the legacy agent-deploy path writes to, and the
// per-step content (system prompt, tool-package pins, asset attachments)
// flows through `launchSession` with bit-identical wire and on-disk
// shape. The legacy `tests/hub-agent/deploy-flow.test.ts` is the
// invariant the trivial branch must round-trip.
//
// A workflow with more than one step (or one without `trivialBindings`)
// derives per-step agent addresses of the form
// `ins_<deploymentId>-<stepId>@<deploymentDomain>`, instantiates one
// agent-state repo per step keyed by the derived address, and writes
// each step's deploy tree onto its own repo. The derivation is a pure
// function of `(deploymentId, stepId, deploymentDomain)`, so the
// supervisor reconstructs the same addresses at spawn time without any
// per-deploy state.
//
// Both branches first validate the workflow, run the capability walk,
// and gate on operator approval. The workflow definition envelope plus
// the walk's per-step grant declarations land on a `workflow` repo
// before any agent-state write happens; if the workflow repo write
// fails, no agent-state repo is created.
//
// =========================================================================

import type { AgentDefinition, BaseEnv, DirectorRegistry } from "@intx/agent";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
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
  sources: Record<string, InferenceSource>;
  hubPublicKey: string;
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
 * Result returned by `deployWorkflow`. The trivial branch returns
 * `{ kind: "trivial" }`; the multi-step branch surfaces the supervisor
 * public key collected from the sidecar's `agent.deploy.ack` so the
 * caller can stash it alongside the deployment record.
 */
export type DeployWorkflowResult =
  | { readonly kind: "trivial" }
  | { readonly kind: "multi-step"; readonly publicKey: string };

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
  /**
   * Writes the workflow repo's deploy tree. The trivial-branch and the
   * multi-step branch both call this once.
   */
  readonly workflowRepo: WorkflowRepoWriter;
  /**
   * Performs the per-agent deploy + session start. The orchestrator
   * calls this once per step (once total in the trivial branch). In
   * production this is `SessionService.launchSession`; tests pass a
   * tracking stub.
   */
  readonly launchSession: LaunchSessionFn;
  /**
   * Fires the deployment-level `agent.deploy` frame that carries the
   * workflow definition and per-step source pins to the sidecar. The
   * multi-step branch calls this exactly once, after every per-step
   * `agent-state` repo has been provisioned via `launchSession`. The
   * trivial branch never calls it.
   *
   * Optional so existing callers that only exercise the trivial branch
   * (e.g. the legacy agent-deploy passthrough) do not have to wire a
   * stub. The multi-step branch fails fast with
   * `MultiStepDeployHandoffMissingError` if the dep is absent.
   */
  readonly sendMultiStepDeploy?: SendMultiStepDeployFn;
}

export interface DeployWorkflowArgs {
  /** The workflow definition the orchestrator validates and deploys. */
  readonly workflow: WorkflowDefinition;
  /**
   * Pre-existing per-agent address binding. Required for the trivial
   * branch (workflow with exactly one step that wraps an existing
   * agent-deploy). Absent for the multi-step branch -- the orchestrator
   * derives per-step addresses from `deploymentId` and the workflow.
   */
  readonly trivialBindings?: {
    readonly agentAddress: string;
    readonly agentId: string;
    readonly instanceId: string;
  };
  /**
   * Stable identifier the multi-step branch concatenates into derived
   * agent addresses. Required when `trivialBindings` is absent.
   */
  readonly deploymentId?: string;
  /**
   * Mail-domain for the deployment. Required when `trivialBindings` is
   * absent. The multi-step branch derives per-step addresses as
   * `ins_<deploymentId>-<stepId>@<deploymentDomain>`.
   *
   * The plan's `deployWorkflow` shape lists `deploymentId` but elides
   * `deploymentDomain`; the derivation needs both, so the API surfaces
   * the second parameter explicitly. Trivial deployments do not consume
   * it because they reuse the existing `agentAddress`.
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
   * Hex-encoded hub Ed25519 public key the multi-step branch threads
   * onto the `agent.deploy` frame so the sidecar can verify the
   * deploy-tree commit signatures. Required when the multi-step branch
   * is reached; ignored by the trivial branch (whose `launchSession`
   * delegate already carries the hub key through its own wire surface).
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
      `multi-step deploy requires ${missing}; supply both deploymentId and deploymentDomain (or pass trivialBindings for a single-step workflow)`,
    );
    this.name = "MultiStepDeploymentArgsMissingError";
  }
}

/**
 * Error thrown when the multi-step branch is reached but the
 * `sendMultiStepDeploy` dependency was not wired. The trivial branch
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
 * trivial-vs-multi-step decision; its deps own everything else.
 */
export function createWorkflowDeployOrchestrator(
  deps: WorkflowDeployOrchestratorDeps,
): WorkflowDeployOrchestrator {
  const { directorRegistry, workflowRepo, launchSession, sendMultiStepDeploy } =
    deps;

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

      const trivial = isTrivialDeploy(args);
      if (trivial !== null) {
        await launchSession({
          agentAddress: trivial.bindings.agentAddress,
          agentId: trivial.bindings.agentId,
          instanceId: trivial.bindings.instanceId,
          config: args.config,
          deployContent: args.deployContent,
          ...(args.toolPackagePins !== undefined
            ? { toolPackagePins: args.toolPackagePins }
            : {}),
        });
        return { kind: "trivial" };
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
 * Decide whether the deploy takes the trivial branch. A trivial deploy
 * requires a single-step workflow AND a `trivialBindings` triple from
 * the caller. Either condition alone is not enough: multi-step
 * workflows never reuse a single legacy address, and a single-step
 * workflow without `trivialBindings` is the multi-step branch's
 * degenerate case (one derived address). The asymmetry is the
 * load-bearing escape hatch documented in this module's header.
 */
function isTrivialDeploy(
  args: DeployWorkflowArgs,
): { bindings: NonNullable<DeployWorkflowArgs["trivialBindings"]> } | null {
  if (args.workflow.stepOrder.length !== 1) return null;
  if (args.trivialBindings === undefined) return null;
  return { bindings: args.trivialBindings };
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
  const sources: Record<string, InferenceSource> = {};
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
    sources[stepId] = pickStepInferenceSource({
      stepAgent,
      stepId,
      workflowId: deploy.workflow.id,
      config: deploy.config,
      operatorApprovals: deploy.operatorApprovals,
    });
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
 * Derive the deployment-level agent id used on the `agent.deploy`
 * frame's `agentId` field. Pure function of `(deploymentId)`.
 */
export function deriveDeploymentAgentId(args: {
  deploymentId: string;
}): string {
  return `ins_${args.deploymentId}`;
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
 * Build a trivial `AgentDefinition` from a `HarnessConfig` and a
 * `DeployContent`. The orchestrator's trivial branch hands the resulting
 * shape off to consumers that want to inspect an agent definition for a
 * legacy agent-deploy that never went through the workflow surface.
 *
 * The wrap is the load-bearing transformation behind the trivial
 * round-trip claim: the legacy deploy-flow exposes `HarnessConfig` plus
 * `DeployContent`, and `SessionService.launchSession` collapses onto
 * `deployWorkflow` by synthesizing a single-step workflow from those
 * two values via this function. The synthesized agent carries no tool factories, no
 * director ref, and no capabilities at the type level; the deploy tree
 * itself (`deployContent.systemPrompt`, the harness's `tools` and
 * `grants` arrays) is the source of truth for runtime behaviour.
 */
export function wrapHarnessAsTrivialAgent(args: {
  config: HarnessConfig;
  deployContent: DeployContent;
}): AgentDefinition<BaseEnv> {
  const inferenceSources = args.config.sources.map((source) => ({
    provider: source.provider,
    model: source.model,
  }));
  return {
    id: args.config.agentId,
    systemPrompt: args.deployContent.systemPrompt,
    toolFactories: [],
    capabilities: [],
    inference: {
      sources: inferenceSources,
    },
  };
}
