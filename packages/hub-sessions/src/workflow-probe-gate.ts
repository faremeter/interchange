// Hub-side install/approve orchestration and gate for a code-sourced workflow
// definition.
//
// This is the production glue that turns a code-sourced workflow install into
// an approved, frozen definition WITHOUT executing any author code on the hub.
// Everything here operates over the inert projection and grant set the sidecar
// returns from a probe:
//
//   1. Resolve the frozen dependency closure for the definition's pin.
//   2. Probe the sidecar for the definition's inert needs-surface projection,
//      its advisory grant set, and the content hash the sidecar shipped.
//   3. RECOMPUTE the wire hash over the RECEIVED projection as tamper-evidence:
//      a shipped hash that differs from the hub recompute is rejected, fail
//      closed, no coercion.
//   4. Reject a projection declaring a trigger type the runtime does not
//      implement, so a deployment that could only ever sit inert never gets
//      approved.
//   5. Reject a projection carrying a step the grant walk left no record for,
//      so no deployment can schedule a step whose grants nobody approved.
//   6. Gate the advisory grant set AND the definition's declared grant
//      requirements against the approval policy: an operator `ApprovalSet`
//      requires every grant the probe surfaced and every requirement it
//      declared to be approved or the gate fails, while `approve-probed`
//      approves exactly what the probe surfaced.
//   7. Freeze the approved wire hash onto the definition version row, keyed by
//      the definition's selector, and return the frozen approved grant set.
//
// The frozen approved set is the single source of truth for the definition's
// grants: the deploy path materializes deploy grants as a SUBSET of it (never a
// fresh walk), so a workflow can never acquire at deploy or run time a grant it
// did not have frozen at approval. The wire hash is that freeze's anchor -- the
// grant set is a deterministic projection of the exact content the hash
// addresses, so pinning the hash pins the set.

import { type } from "arktype";
import { and, eq } from "drizzle-orm";

import type { DBExecutor } from "@intx/db";
import { workflowDefinitionVersion } from "@intx/db/schema";
import type {
  ApprovalItem,
  GrantRequirement,
  GrantWalkSnapshot,
} from "@intx/types";
import type { PackumentFetcher, RegistryConfig } from "@intx/tool-packaging";
import type {
  WorkflowSourceAssetMount,
  WorkflowProjectionDefinition,
} from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import type {
  WorkflowDefinitionAssetSource,
  WorkflowDefinitionRegistrySource,
} from "@intx/types/workflow-sources";
import {
  EXECUTABLE_STEP_DESCENT,
  walkStepTree,
} from "@intx/workflow/definition";
import {
  approvalSetFromItems,
  inertNestedBodies,
  isApprovedGrantRequirement,
  type ApprovalSet,
} from "@intx/workflow-deploy";

import {
  buildSourceAssetMounts,
  resolveWorkflowClosure,
  type ResolveAssetAttachmentFn,
} from "./workflow-closure-resolution";
import type { SourceTreeReads } from "./workflow-source-closure";
import { ensureWorkflowDefinitionForAsset } from "./workflow-definition-ensure";
import type { SendProbeArgs, WorkflowProbeResult } from "./ws/sidecar-handler";

type WorkflowProbeRouter = {
  sendProbe(args: SendProbeArgs): Promise<WorkflowProbeResult>;
};

// The version `ensureWorkflowDefinitionForAsset` projects for a fresh
// definition, and therefore the row the approval freeze targets. Kept in step
// with the ensure helper: if that helper ever projects a different initial
// version, this must follow.
const FROZEN_VERSION = "1";

/**
 * The frozen record an approval writes: the definition's asset selector, the
 * approved wire hash (the freeze anchor), the approved surface, and the
 * grant-walk snapshot the run path materializes grants from. The approved
 * surface is a deterministic projection of the content the hash addresses and
 * rides the deploy hand-off in memory; the snapshot is persisted onto the
 * version row so a run derives its grants from the frozen walk without
 * re-reading and re-walking the workflow's `workflow.json`.
 *
 * `approvedGrants` carries both kinds of approved item: the walk's grant-shape
 * strings and the definition's declared grant requirements. The requirements
 * belong in the same record because the run path mints real grant rows from
 * them, so an account of the approval that listed only the walk strings would
 * under-report the authority the definition will actually carry. It is the flat
 * `ApprovalItem` list rather than the gate's partitioned `ApprovalSet` because
 * this record is the input to persistence, and the persisted form is flat.
 */
export type FrozenApproval = {
  readonly assetId: string;
  readonly approvedWireHash: string;
  readonly approvedGrants: readonly ApprovalItem[];
  readonly grantSnapshot: GrantWalkSnapshot;
};

/**
 * Persists a frozen approval and returns the definition it was recorded
 * against. Bound to a `DBExecutor` in production via
 * `createDbFrozenApprovalWriter`; a test double records the call.
 */
export type PersistFrozenApprovalFn = (
  approval: FrozenApproval,
) => Promise<{ definitionId: string }>;

/**
 * The outcome of gating and freezing a probe result. `ok: true` is the frozen
 * approval the deploy hand-off consumes. The `ok: false` arms name the five
 * fail-closed paths: a shipped hash that does not match the hub recompute
 * (tamper-evidence), advisory grants the operator did not approve, declared
 * grant requirements the operator did not approve, a trigger type the runtime
 * does not implement, and an executable step the grant walk left no record for.
 */
export type ProbeGateResult =
  | {
      readonly ok: true;
      readonly definitionId: string;
      readonly approvedWireHash: string;
      readonly approvedSurface: ApprovalSet;
      /**
       * The inert wire projection the freeze hashed. Rides the ok-arm so the
       * deploy hand-off carries the exact content the frozen hash addresses,
       * never a re-projection of a registry that may have moved since approval.
       */
      readonly projection: WorkflowProjectionDefinition;
    }
  | {
      readonly ok: false;
      readonly reason: "wire_hash_mismatch";
      readonly shippedWireHash: string;
      readonly recomputedWireHash: string;
    }
  | {
      readonly ok: false;
      readonly reason: "grants_not_approved";
      readonly unapprovedGrants: readonly string[];
    }
  | {
      readonly ok: false;
      readonly reason: "grant_requirements_not_approved";
      /**
       * The declared requirements the operator's approval does not cover, in
       * the order the probe declared them. Named in full so the operator can
       * see exactly which authority the definition asked to delegate.
       */
      readonly unapprovedGrantRequirements: readonly GrantRequirement[];
    }
  | {
      readonly ok: false;
      readonly reason: "unimplemented_trigger";
      /** The distinct reserved-but-unimplemented trigger types the projection declared. */
      readonly unimplementedTriggerTypes: readonly string[];
    }
  | {
      readonly ok: false;
      readonly reason: "steps_without_grant_record";
      /** Every executable step the grant-walk snapshot accounts for nothing at. */
      readonly stepsWithoutGrantRecord: readonly StepWithoutGrantRecord[];
      /**
       * One sentence naming every miss. A deploy that trips this is a defect in
       * the deploy path rather than in the author's workflow, so the sentence
       * carries what whoever maintains that path needs: the step, the position,
       * and the record that was absent.
       */
      readonly message: string;
    };

/**
 * One step the deployment can execute that the probe's grant-walk snapshot
 * carries no record for.
 */
export type StepWithoutGrantRecord = {
  /** The executable step id with no approved grants behind it. */
  readonly stepId: string;
  /**
   * The chain of step ids the executable walk reached `stepId` through,
   * outermost first and `stepId` itself last. Two nested bodies may
   * legitimately carry the same step id, so the chain -- not the id alone --
   * is what names the position in the closure.
   */
  readonly reachedThrough: readonly string[];
  /**
   * The top-level step whose snapshot record was supposed to account for
   * `stepId`. The capability walk folds every nested body's grants into the
   * record of the top-level step that carries the body, so this names the
   * `perStep` key the absent record would have had.
   */
  readonly recordStepId: string;
};

/**
 * Build the production persistence step of the freeze. Records identity through
 * the selector-keyed ensure helper (a definition keyed by `(assetId,
 * wireHash)`) and writes the approved wire hash and the grant-walk snapshot onto
 * that definition's version row in one transaction. The grant SET is not written
 * to a version-row column -- the approved wire hash already pins the content the
 * grants project from -- so it travels with the returned frozen approval; the
 * snapshot is written because the run path reads it back to materialize grants.
 */
export function createDbFrozenApprovalWriter(
  db: DBExecutor,
): PersistFrozenApprovalFn {
  return async ({ assetId, approvedWireHash, grantSnapshot }) => {
    // Ensure-then-stamp is one freeze: a crash between the two would persist a
    // version row with a NULL `approvedWireHash`, which the schema treats as
    // the legitimate "not yet approved" state -- indistinguishable from an
    // un-approved definition. Wrap both writes in one transaction so the freeze
    // is all-or-nothing.
    return db.transaction(async (tx) => {
      const { definitionId } = await ensureWorkflowDefinitionForAsset(tx, {
        assetId,
        wireHash: approvedWireHash,
      });
      // `FROZEN_VERSION` is hand-coupled to the version the ensure helper
      // projects; if that coupling ever drifts, the update would silently stamp
      // zero rows and no hash would persist. Assert exactly one row so a drift
      // fails loud instead of open.
      const stamped = await tx
        .update(workflowDefinitionVersion)
        .set({ approvedWireHash, grantSnapshot })
        .where(
          and(
            eq(workflowDefinitionVersion.definitionId, definitionId),
            eq(workflowDefinitionVersion.version, FROZEN_VERSION),
          ),
        )
        .returning({ id: workflowDefinitionVersion.id });
      if (stamped.length !== 1) {
        throw new Error(
          `createDbFrozenApprovalWriter: expected to stamp exactly one ${FROZEN_VERSION} version row for definition ${definitionId}, but updated ${String(stamped.length)}`,
        );
      }
      return { definitionId };
    });
  };
}

/**
 * Approve exactly the grant surface the probe reports, without a pre-walked
 * operator `ApprovalSet` to gate against. Under this mode the gate skips the
 * per-grant and per-requirement membership checks and freezes exactly what the
 * probe advertised.
 *
 * This is the code-sourced analogue of the live-authored self-approve: the hub
 * has no live definition to pre-walk, so the probe's advertised grants ARE the
 * declared surface. It does NOT relax tamper-evidence -- the wire-hash check
 * still runs and can still fail closed.
 */
export type ApproveProbedGrants = { readonly mode: "approve-probed" };

/**
 * How the gate turns the probe's advertised surface into an approved one.
 * Either an explicit operator `ApprovalSet` -- every advertised grant and every
 * declared grant requirement must appear in it or the gate fails closed -- or
 * `approve-probed`, which approves exactly the surface the probe reported.
 */
export type ProbeApprovalPolicy = ApprovalSet | ApproveProbedGrants;

function isApproveProbed(
  policy: ProbeApprovalPolicy,
): policy is ApproveProbedGrants {
  return "mode" in policy;
}

/**
 * Trigger types the definition vocabulary declares but the runtime does not
 * implement. `schedule` is reserved: no cron parser and no scheduler exist, so
 * a schedule-triggered deployment would hash, deploy, and then never fire --
 * no error, no log, no failed run. Admitting one at the gate is the only way a
 * deployment reaches that state, so the gate refuses it.
 */
const UNIMPLEMENTED_TRIGGER_TYPES: ReadonlySet<string> = new Set(["schedule"]);

// A projected trigger, typed only to its discriminant. `triggers` rides the
// wire projection as `unknown[]` on purpose (the wire envelope does not own
// the trigger vocabulary), so the discriminant is read through a validator
// rather than an assertion. An entry that carries no string `type` is not a
// trigger this gate has an opinion about and is left to the deploy path.
const ProjectedTriggerType = type({ type: "string" });

/**
 * The distinct unimplemented trigger types a projection declares, in first-seen
 * order. Empty when every declared trigger has an implementation behind it.
 */
function collectUnimplementedTriggerTypes(
  triggers: readonly unknown[],
): readonly string[] {
  const found: string[] = [];
  for (const trigger of triggers) {
    const parsed = ProjectedTriggerType(trigger);
    if (parsed instanceof type.errors) continue;
    if (!UNIMPLEMENTED_TRIGGER_TYPES.has(parsed.type)) continue;
    if (found.includes(parsed.type)) continue;
    found.push(parsed.type);
  }
  return found;
}

const EXECUTABLE_CLOSURE_CONTEXT = "probe gate executable closure: ";

/** One step the executable walk reached, with where it was reached from. */
type ExecutableReach = StepWithoutGrantRecord;

/**
 * Every step the deployment can execute, walked over the frozen inert
 * projection under `EXECUTABLE_STEP_DESCENT`, each carrying the chain it was
 * reached through and the top-level step whose grant record accounts for it.
 *
 * The descent is the canonical one rather than a local re-derivation on
 * purpose: the capability walk folds a nested body's grants into the enclosing
 * top-level step under exactly this descent, so the two agree by construction
 * and a primitive kind added to one side cannot silently fall out of the other.
 *
 * The position each step was reached at is the walk's own `path`, for the same
 * reason. The head of that path is the top-level step the entry descends from,
 * which is exactly the `perStep` key the capability walk folds its grants into.
 */
function collectExecutableReaches(
  projection: WorkflowProjectionDefinition,
): readonly ExecutableReach[] {
  const reaches: ExecutableReach[] = [];
  walkStepTree<unknown, WorkflowProjectionDefinition>({
    tree: projection,
    context: EXECUTABLE_CLOSURE_CONTEXT,
    nestedTrees: (step) => inertNestedBodies(step, EXECUTABLE_STEP_DESCENT),
    visit: ({ stepId, path }) => {
      reaches.push({ stepId, recordStepId: path[0], reachedThrough: path });
    },
  });
  return reaches;
}

/**
 * Every executable step the grant-walk snapshot carries no record for.
 *
 * A step's approved-grant record is the snapshot entry keyed by the top-level
 * step it descends from: the capability walk collects one record per top-level
 * step and folds into it the grants of every step that step can run. So a
 * present record covers a whole executable subtree, and an absent one leaves
 * every step of that subtree with no approved grants at all.
 */
function collectStepsWithoutGrantRecord(
  projection: WorkflowProjectionDefinition,
  snapshot: GrantWalkSnapshot,
): readonly StepWithoutGrantRecord[] {
  const recordedStepIds = new Set(
    snapshot.perStep.map((record) => record.stepId),
  );
  return collectExecutableReaches(projection).filter(
    (reach) => !recordedStepIds.has(reach.recordStepId),
  );
}

function describeStepsWithoutGrantRecord(
  missing: readonly StepWithoutGrantRecord[],
): string {
  const positions = missing
    .map(
      (missed) =>
        `${missed.stepId} (reached through ${missed.reachedThrough.join(" > ")}; expected a grant-walk record keyed by top-level step ${missed.recordStepId})`,
    )
    .join("; ");
  return `the probe's grant-walk snapshot carries no approved-grant record covering ${String(missing.length)} executable step(s): ${positions}. A step outside every record deploys with no approved grants, and its tool calls are refused at run time with nothing on the deploy path reporting it.`;
}

export type GateAndFreezeArgs = {
  /** The `workflow`-kind asset the frozen definition projects over. */
  readonly assetId: string;
  /** The sidecar's inert probe answer: projection, advisory grants, shipped hash. */
  readonly probeResult: WorkflowProbeResult;
  /**
   * The approval policy. An `ApprovalSet` gates the advisory set and the
   * declared grant requirements against the operator-approved items;
   * `approve-probed` approves exactly the surface the probe reported.
   */
  readonly approvals: ProbeApprovalPolicy;
  /** Persistence step for the freeze; `createDbFrozenApprovalWriter` in production. */
  readonly persist: PersistFrozenApprovalFn;
};

/**
 * Gate a probe result and, on approval, freeze it. Operates purely over the
 * inert projection and grant set -- no author code runs here and the capability
 * walk is never re-run.
 *
 * Fails closed on the three security-load-bearing checks before it writes
 * anything: the recomputed wire hash must match the hash the sidecar shipped
 * (tamper-evidence), every advisory grant must be operator-approved, and every
 * declared grant requirement must be operator-approved. It also refuses a
 * projection whose triggers include a reserved-but-unimplemented type -- not a
 * security check, but the layer a pinned closure cannot carry a stale copy of,
 * so it is where a workflow that could only sit inert is caught -- and one
 * whose executable closure reaches a step the grant walk left no record for,
 * which is the layer holding both halves of the probe answer at once.
 * Only then does it freeze the recomputed hash onto the version row and return
 * the approved grant set.
 */
export async function gateAndFreezeProbeResult(
  args: GateAndFreezeArgs,
): Promise<ProbeGateResult> {
  const { assetId, probeResult, approvals, persist } = args;

  // Tamper-evidence: recompute over the RECEIVED projection and compare to the
  // shipped hash. A mismatch means the projection the hub is approving is not
  // the one the sidecar hashed, so reject rather than freeze a hash that does
  // not describe the approved content.
  const recomputedWireHash = await computeWireDefinitionHash(
    probeResult.projection,
  );
  if (recomputedWireHash !== probeResult.wireHash) {
    return {
      ok: false,
      reason: "wire_hash_mismatch",
      shippedWireHash: probeResult.wireHash,
      recomputedWireHash,
    };
  }

  // Reject a trigger type nothing implements. This runs on the projection
  // rather than on the author's definition because `defineWorkflow` is bundled
  // INTO the pinned workflow closure: a closure published before the authoring
  // check carries its own frozen copy and never sees it. The projection's
  // `triggers` are produced by the hub's live->inert projector, so this is the
  // one trigger surface a stale closure cannot carry past. Placed after the
  // wire-hash check so tamper-evidence still decides first -- the projection
  // must be the one the sidecar hashed before its content is reasoned about.
  const unimplementedTriggerTypes = collectUnimplementedTriggerTypes(
    probeResult.projection.triggers,
  );
  if (unimplementedTriggerTypes.length > 0) {
    return {
      ok: false,
      reason: "unimplemented_trigger",
      unimplementedTriggerTypes,
    };
  }

  // Totality: every step the deployment can execute must have an approved-grant
  // record behind it. The two halves of a probe answer are produced
  // independently -- the projection by the hub's live->inert projector, the
  // grant-walk snapshot by the sidecar's capability walk over the live
  // definition -- and nothing until now compared them. A step the walk skipped
  // still projects, still deploys, and is still scheduled; its tool calls are
  // then refused for lack of any grant, and the tool runner turns that refusal
  // into an error tool result rather than a failure, so the run completes
  // having done none of the work. This is the check that makes that
  // unreachable: it is total over the closure and it runs on every deploy,
  // rather than depending on some test happening to invoke a tool from the
  // affected step.
  //
  // Placed after the trigger check and before the operator-policy checks
  // below. A deploy that trips this is a defect in the deploy path, not a
  // decision the operator can make differently, so it must not be reported
  // behind an unapproved-grant message an operator would act on instead.
  //
  // DO NOT move this assertion earlier in this package's history. The record it
  // requires is a claim that the approval covers everything the step can run,
  // and that claim was not kept for a step inside a loop body or a section body
  // until the deploy and runtime producers were made total over the executable
  // closure. Asserted before those producers, the gate would have been
  // enforcing a guarantee the rest of the system did not honour.
  const stepsWithoutGrantRecord = collectStepsWithoutGrantRecord(
    probeResult.projection,
    probeResult.grantWalkSnapshot,
  );
  if (stepsWithoutGrantRecord.length > 0) {
    return {
      ok: false,
      reason: "steps_without_grant_record",
      stepsWithoutGrantRecord,
      message: describeStepsWithoutGrantRecord(stepsWithoutGrantRecord),
    };
  }

  // Gate the advisory grant set. Under an `ApprovalSet` every grant the probe
  // surfaced must appear in the operator's approved set; any miss fails the
  // gate closed. Under `approve-probed` there is no set to gate against -- the
  // probe's surface IS the approved set -- so nothing is ever unapproved.
  const unapprovedGrants = isApproveProbed(approvals)
    ? []
    : probeResult.grants.filter((grant) => !approvals.grants.has(grant));
  if (unapprovedGrants.length > 0) {
    return { ok: false, reason: "grants_not_approved", unapprovedGrants };
  }

  // Gate the DECLARED grant requirements. These ride the walk snapshot rather
  // than the flattened `grants`, and the walk never surfaces them, so the
  // filter above cannot see them -- yet the run path materializes each one into
  // a real grant row on the run principal, and a wildcard row reaches gates no
  // walk-derived row can address. They therefore need the operator's decision
  // on exactly the same terms the advertised grants do. Under `approve-probed`
  // the probe's surface IS the approved surface, so there is nothing to gate
  // against and the requirements ride into the approval below.
  const declaredRequirements = probeResult.grantWalkSnapshot.grantRequirements;
  const unapprovedGrantRequirements = isApproveProbed(approvals)
    ? []
    : declaredRequirements.filter(
        (requirement) => !isApprovedGrantRequirement(approvals, requirement),
      );
  if (unapprovedGrantRequirements.length > 0) {
    return {
      ok: false,
      reason: "grant_requirements_not_approved",
      unapprovedGrantRequirements,
    };
  }

  // Freeze: the approved surface is exactly what the workflow advertised (all
  // of it now operator-approved), pinned to the recomputed hash. Persisting the
  // hash is the freeze; the approved surface is returned for the deploy
  // hand-off. The declared requirements join the walk's grant strings in that
  // surface under both policies, so the record the hand-off and any audit read
  // describes every kind of authority the freeze will mint.
  const approvedGrants: readonly ApprovalItem[] = [
    ...probeResult.grants,
    ...declaredRequirements,
  ];
  const { definitionId } = await persist({
    assetId,
    approvedWireHash: recomputedWireHash,
    approvedGrants,
    grantSnapshot: probeResult.grantWalkSnapshot,
  });

  return {
    ok: true,
    definitionId,
    approvedWireHash: recomputedWireHash,
    approvedSurface: approvalSetFromItems(approvedGrants),
    projection: probeResult.projection,
  };
}

type InstallAndApproveCommonArgs = {
  /** The `interchange.workflow` entry-module path the sidecar evaluates to project the definition. */
  readonly entry: string;
  /** The `workflow`-kind asset the frozen definition projects over. */
  readonly assetId: string;
  /**
   * The approval policy threaded to the gate: an operator `ApprovalSet` to gate
   * the advisory set against, or `approve-probed` to approve exactly the
   * surface the probe reports.
   */
  readonly approvals: ProbeApprovalPolicy;
  /** The sidecar router carrying the probe transport. */
  readonly router: WorkflowProbeRouter;
  /** Executor the freeze writes through. */
  readonly db: DBExecutor;
  /** Optional durable handoff invoked before the approval gate writes. */
  readonly onProbeResult?: (result: WorkflowProbeResult) => Promise<void>;
};

/** Install a definition published to an npm registry. */
export type InstallAndApproveRegistryArgs = InstallAndApproveCommonArgs & {
  readonly source: WorkflowDefinitionRegistrySource;
  /** A `name@range` spec for the workflow definition package. */
  readonly pin: string;
  /** URL and credentials for the registry `source` names. */
  readonly registryConfig: RegistryConfig;
  /** Test seam for packument fetches, threaded to closure resolution. Omitted in production. */
  readonly fetchPackument?: PackumentFetcher;
};

/**
 * Install a definition published as a tarball inside a hub `package-registry`
 * asset. The caller mints the asset-read closures (`readBlob`/`listBlobs`) and
 * `resolveAttachment`; this glue never imports the asset service, so hub-service
 * ownership stays at the caller.
 */
export type InstallAndApproveAssetTarballArgs = InstallAndApproveCommonArgs & {
  readonly source: WorkflowDefinitionAssetSource;
  /** A `name@range` spec for the workflow definition package. */
  readonly pin: string;
  /** Reads a blob at `path` from the asset the definition is sourced from. */
  readonly readBlob: (path: string) => Promise<Uint8Array>;
  /** Lists the blob names directly under `dir` in that asset. */
  readonly listBlobs: (dir: string) => Promise<string[]>;
  /** Resolves each asset the closure references to the pack the probe delivers. */
  readonly resolveAttachment: ResolveAssetAttachmentFn;
};

/**
 * Install a definition whose package lives as a git subtree of a hub asset at a
 * pinned commit. The caller binds a `SourceTreeReads` to that commit and
 * supplies the npm `registryConfig` for the external deps; there is no
 * `name@range` pin (the member is selected from `source.package.packageName`).
 * `resolveAttachment` delivers the same git pack the tarball arm does, so the
 * sidecar checks the subtree out of it.
 */
export type InstallAndApproveAssetSourceArgs = InstallAndApproveCommonArgs & {
  readonly source: WorkflowDefinitionAssetSource;
  /** Git-tree reads pinned to `source.package.commitSha`. */
  readonly reads: SourceTreeReads;
  /**
   * The registry name external deps are stamped with in the frozen closure.
   * Must be a name the sidecar's registry map is keyed by (its npm registry).
   */
  readonly registryName: string;
  /** URL and credentials for the npm registry external deps resolve against. */
  readonly registryConfig: RegistryConfig;
  /** Test seam for packument fetches, threaded to closure resolution. Omitted in production. */
  readonly fetchPackument?: PackumentFetcher;
  /** Resolves each asset the closure references to the pack the probe delivers. */
  readonly resolveAttachment: ResolveAssetAttachmentFn;
};

export type InstallAndApproveArgs =
  | InstallAndApproveRegistryArgs
  | InstallAndApproveAssetTarballArgs
  | InstallAndApproveAssetSourceArgs;

// Both asset arms carry an identical `source` field type, so narrow on the
// source's own `package.format` discriminant rather than adding a redundant
// discriminant to the args.
function isAssetSourceInstallArgs(
  args: InstallAndApproveArgs,
): args is InstallAndApproveAssetSourceArgs {
  return (
    args.source.kind === "asset" && args.source.package.format === "source"
  );
}

function isAssetTarballInstallArgs(
  args: InstallAndApproveArgs,
): args is InstallAndApproveAssetTarballArgs {
  return (
    args.source.kind === "asset" && args.source.package.format === "tarball"
  );
}

/**
 * The frozen hand-off `installAndApproveWorkflowDefinition` produces. It carries
 * the gate outcome plus the two values the source-ref deploy frame needs and
 * must NOT recompute at deploy: the inert `projection` the freeze hashed and the
 * frozen dependency `closure` the pin resolved to. Re-resolving either at deploy
 * would reintroduce the non-determinism the freeze eliminates -- a registry that
 * moved between approve and deploy would pin different bytes and project
 * differently, failing the child re-verify -- so both ride from approve verbatim.
 */
export type InstallAndApproveResult = {
  readonly approval: ProbeGateResult;
  readonly projection: WorkflowProjectionDefinition;
  readonly closure: ToolPackageManifest;
};

/**
 * The install/approve orchestration entrypoint the end-to-end flow drives:
 * resolve the frozen closure, probe the sidecar, then gate and freeze the
 * result. This is production glue, not test-only wiring.
 *
 * The operator-approval decision is an input (`approvals`): the caller supplies
 * either the `ApprovalSet` the operator approved, which the gate holds the
 * advisory set to, or `approve-probed` to approve exactly the surface the probe
 * reports.
 *
 * Returns the gate outcome alongside the inert projection and the frozen
 * closure so the deploy hand-off consumes them verbatim rather than re-probing
 * or re-resolving.
 */
export async function installAndApproveWorkflowDefinition(
  args: InstallAndApproveArgs,
): Promise<InstallAndApproveResult> {
  let closure: ToolPackageManifest;
  let assets: WorkflowSourceAssetMount[];
  if (isAssetSourceInstallArgs(args)) {
    closure = await resolveWorkflowClosure({
      source: args.source,
      reads: args.reads,
      registryName: args.registryName,
      registryConfig: args.registryConfig,
      ...(args.fetchPackument !== undefined
        ? { fetchPackument: args.fetchPackument }
        : {}),
    });
    assets = await buildSourceAssetMounts(closure, args.resolveAttachment);
  } else if (isAssetTarballInstallArgs(args)) {
    closure = await resolveWorkflowClosure({
      source: args.source,
      pin: args.pin,
      readBlob: args.readBlob,
      listBlobs: args.listBlobs,
    });
    assets = await buildSourceAssetMounts(closure, args.resolveAttachment);
  } else {
    closure = await resolveWorkflowClosure({
      source: args.source,
      pin: args.pin,
      registryConfig: args.registryConfig,
      ...(args.fetchPackument !== undefined
        ? { fetchPackument: args.fetchPackument }
        : {}),
    });
    assets = [];
  }

  const probeResult = await args.router.sendProbe({
    source: args.source,
    closure,
    entry: args.entry,
    ...(assets.length > 0 ? { assets } : {}),
  });
  await args.onProbeResult?.(probeResult);

  const approval = await gateAndFreezeProbeResult({
    assetId: args.assetId,
    probeResult,
    approvals: args.approvals,
    persist: createDbFrozenApprovalWriter(args.db),
  });

  return { approval, projection: probeResult.projection, closure };
}
