// Workflow-deploy derivation and source-pinning utilities.
//
// The deployment address model is a pure function of `(runId, stepId,
// domain)`: a one-step workflow has no distinct step address -- the lone
// step IS the deployment head (`deriveRunAddress`) -- while a workflow with
// more than one step derives per-step addresses of the form
// `<runId>-<stepId>@<domain>`. Because the derivation is pure, the
// supervisor reconstructs the same addresses at spawn time without any
// per-deploy state, and `resolveStepAddress` is the single owner of the
// head/step collapse decision for a consumer that must choose an address
// from the host-sourced step count alone.
//
// The source-pinning utilities (`pickStepInferenceSource`,
// `buildInertProjectionStepSources`, `isSourceApproved`) resolve each step's
// inference source against the operator-approved grant set, so an unapproved
// source fails the deploy closed rather than slipping past the capability-walk
// gate.

import type {
  AgentDefinition,
  AnnotatedToolFactory,
  BaseEnv,
  InferencePreference,
} from "@intx/agent";
import type { HarnessConfig, InferenceSource } from "@intx/types/runtime";
import type { ToolPackagePin } from "@intx/types/tool-packages";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import { formatRunAddress } from "@intx/types";

import { type ApprovalSet } from "./capability-approval";
import {
  inertLoopBody,
  readInertStepPreference,
} from "./inert-ontrigger-bodies";

/**
 * Minimal structural `DeployContent` shape. Carried as a structural type so
 * this package does not need a runtime dependency on `@intx/hub-sessions` to
 * name the type. Mirrors the public fields of
 * `packages/hub-sessions/src/agent-repo.ts`'s `DeployContent`; the hub's
 * `bridgeOrchestratorDeployContent` narrows this widened shape back to the
 * canonical one at the deploy boundary.
 */
export interface DeployContent {
  readonly systemPrompt: string;
  readonly toolPackageManifest?: unknown;
  readonly assetMounts?: ReadonlyMap<string, string>;
}

/**
 * Error thrown when a workflow definition fails deploy-time validation --
 * an inverted or unapproved inference chain, or a step whose source the
 * operator never approved. Carries the offending workflow id so the caller's
 * logs name the deployment that was rejected.
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
 * The caller passes the step's preferred `(provider, model)` -- the step
 * agent's first declared source, or `null` for a step that declares none
 * (a non-agent step such as sleep/gate/awaitSignal, or an agent with no
 * declared source). The identity is all this needs: the source-ref hub deploy
 * reads it off the frozen inert projection's `modelSources` and feeds it here.
 *
 * The capability walk emits `inference.source:<provider>:<model>`
 * grants only for the (provider, model) pairs the agent declared. The
 * pinning pass here can otherwise resolve a source the walk never
 * surfaced -- the `HarnessConfig.defaultSource` fallback path for a step
 * whose preference is unresolvable, or the same fallback for a step that
 * carries no preference at all. In both cases the source-pinning pass must
 * refuse to pin a `(provider, model)` the operator never approved;
 * silently shipping an unapproved source would defeat the capability-
 * walk gate the deploy just passed.
 *
 * Exported so the source-ref hub deploy can pin its inert onTrigger body
 * steps through the same resolver its top-level steps use.
 */
export function pickStepInferenceSource(args: {
  preferred: { provider: string; model: string } | null;
  stepId: string;
  workflowId: string;
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): InferenceSource {
  const preferred = args.preferred;
  if (preferred !== null) {
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
    preferred !== null
      ? `preferred ${preferred.provider}:${preferred.model}`
      : `the step declared no preferred source`;
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
 * Pin every step of a frozen inert projection to a single approved inference
 * source, producing the `sources` map the source-ref deploy frame carries. The
 * hub holds no live definition, so each step's declared `(provider, model)`
 * preference is read off the inert projection's `modelSources` and resolved
 * through the `pickStepInferenceSource` resolver + operator-approval gate. A
 * step whose preferred source the operator never approved (or that resolves to
 * no approved source at all) throws, failing the whole deploy closed before any
 * frame is sent.
 *
 * The walk RECURSES into `loop` bodies: a loop body runs in-process as a child
 * run sharing the parent's env, so its agent steps resolve their pinned source
 * from this same flat map, keyed by the body step's plain id. Loop-body step
 * ids share a namespace with the top-level steps here; a body step id that
 * collides with another step must resolve to the same source, else the deploy
 * fails closed rather than silently mis-pin. (onTrigger bodies are NOT walked
 * here -- they are lifted to `referencedDefinitions` with their own per-body pin
 * in the deploy composition. childWorkflow bodies are resolved at the child
 * host, not pinned here.)
 *
 * Every step gets one entry (a non-agent step falls back to the approved
 * default), so the sidecar child finds a pinned source for each staged step.
 */
export function buildInertProjectionStepSources(args: {
  projection: WorkflowProjectionDefinition;
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): Record<string, InferenceSource[]> {
  const sources: Record<string, InferenceSource[]> = {};
  const pin = (def: WorkflowProjectionDefinition): void => {
    for (const stepId of def.stepOrder) {
      const stepValue = def.steps[stepId];
      const preferred = readInertStepPreference(
        stepValue,
        "buildInertProjectionStepSources: ",
        stepId,
      );
      const resolved = pickStepInferenceSource({
        preferred,
        stepId,
        workflowId: args.projection.id,
        config: args.config,
        operatorApprovals: args.operatorApprovals,
      });
      const existing = sources[stepId]?.[0];
      if (existing !== undefined) {
        if (!sameInferenceSource(existing, resolved)) {
          throw new WorkflowDefinitionInvalidError(
            args.projection.id,
            `step id ${stepId} resolves to two different inference sources across nested loop bodies; a loop-body step id that collides with another step must resolve to the same source`,
          );
        }
      } else {
        sources[stepId] = [resolved];
      }
      const loopBody = inertLoopBody(stepValue);
      if (loopBody !== null) pin(loopBody);
    }
  };
  pin(args.projection);
  return sources;
}

function sameInferenceSource(a: InferenceSource, b: InferenceSource): boolean {
  return a.id === b.id && a.provider === b.provider && a.model === b.model;
}

/**
 * Pure function: derive a step's run address from
 * `(runId, stepId, domain)`. Exported so the supervisor can reconstruct
 * the same addresses at spawn time without sharing storage with the
 * deploy flow.
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
 * Project a workflow-deployment run address into the substrate-safe
 * id of its workflow-run repo (`{ kind: "workflow-run", id }`). Pure
 * function of the deployment's run address.
 *
 * The workflow-run repo's `repoId.id` must match `SAFE_REPO_ID`
 * (`/^[a-zA-Z0-9_-]+$/`, the substrate's repo-path-safety contract in
 * `packages/hub-sessions/src/repo-store/types.ts`), and the supervisor
 * principal's `runId` must equal `workflowRunRepoId.id` for the
 * workflow-run kind handler's authz check to pass. That regex rejects
 * `@` and `.`, both of which appear in every run address, so the
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
 * Assemble a single-step `AgentDefinition` from already-resolved fields. This
 * is the single place the single-step agent shape is constructed, so the
 * offline agent-to-workflow fold synthesis cannot drift on which fields a
 * folded agent carries. Callers pass resolved inputs: the fold passes empty
 * tool factories (its tools ride as `toolPackagePins`), the agent's own pins,
 * and its catalog-resolved inference preferences.
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
