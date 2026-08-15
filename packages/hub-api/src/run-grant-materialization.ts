// Shared run-grant materialization: the sequence a workflow run's
// authorization is derived and committed through, used by BOTH the
// external trigger route and the hub's mail-triggered run path so the two
// cannot drift.
//
// A run's grant set is the definition-pure runtime grants (the capability
// walk's `tool:`/`effect:` rows) plus the resolved declared grant
// requirements (creator- and invoker-sourced). This module stages those
// rows and commits them idempotently on the run id, minting the run
// principal and anchoring the run row in one transaction.
//
// Delivery (`run.grants` frame, trigger mail / inbound mail forwarding) is
// NOT owned here: the two call sites order those differently for their
// transport, so each orchestrates delivery itself around the shared
// staging and commit below.

import { and, asc, eq } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { type } from "arktype";

import {
  asset,
  grant as grantTable,
  isLiveWorkflowRunStatus,
  principal as principalTable,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type { DB, DBExecutor } from "@intx/db";
import { createWorkflowRunStore, readApprovedGrantSurface } from "@intx/db";
import type { GrantStore, GrantRule } from "@intx/types/authz";
import {
  GrantRequirement,
  isSidecarAllocationDispatchable,
  type ApprovedGrantSurface,
  type GrantEffect,
} from "@intx/types";
import { RunGrantsFrame } from "@intx/types/sidecar";
import {
  enumerateChildWorkflowRefs,
  hydrateDefinition,
  type AssetService,
  type MailTriggeredRunGrantsResult,
  type WorkflowDefinition,
} from "@intx/hub-sessions";
import {
  walkCapabilities,
  resolveRuntimeGrantEffects,
  type CapabilityWalkResult,
} from "@intx/workflow-deploy";
import { createDefaultDirectorRegistry } from "@intx/agent";
import { deriveRunPrincipalId, generateId } from "@intx/hub-common";

import {
  resolveGrantMaterialization,
  type MaterializedGrantRow,
} from "./grant-materialization";

const GrantRequirements = GrantRequirement.array();

// The `tool:<name>` rows carry BARE tool names: the walk reads inline
// `agent.toolFactories`, which have no bundle context. A workflow child gates
// each tool call on `tool:<call.name>`, and every runnable step tool is a
// pinned package the loader namespaces to `<bundleId>:<name>`, so the child
// queries `tool:<bundleId>:<name>`. These bare rows therefore never address a
// pinned tool's runtime gate; they are inert against a pinned call. A pinned
// tool's authority (including its `ask` mark) is supplied instead by the
// sidecar tool-mark floor (`deriveToolMarkFloorGrants`), derived from the
// loaded factory's already-namespaced definitions. The `effect:<cap>` rows are
// different: an action's EffectContext authorizes the bare `effect:<cap>` on
// both sides, so those rows ARE name-matched and operative at run time.

/**
 * Project the capability walk into the run's runtime grant rows -- the
 * `tool:<name>` and `effect:<cap>` grants the runtime enforces fail-closed.
 * Every distinct grant string across all steps becomes one creator-origin
 * `grant` row with `action: invoke`. The run's runtime authority is
 * definition-pure for the deployment's stable top-level run, so the walk
 * output alone determines it.
 *
 * The `tool:`/`effect:` effect resolution is owned by
 * `resolveRuntimeGrantEffects`; a child-workflow parent's pinned surface reads
 * the SAME projection (via `flattenWalkToSurface`), so a folded ceiling and the
 * runtime ceiling cannot drift.
 */
export function deriveRunRuntimeGrantRows(
  walk: CapabilityWalkResult,
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow[] {
  return projectRuntimeGrantRows(
    resolveRuntimeGrantEffects(walk),
    tenantId,
    runPrincipalId,
    now,
  );
}

/**
 * Project a deploy-stamped pinned surface into the run's runtime grant rows.
 * Used for a childWorkflow-bearing definition, whose runtime ceiling is the
 * stamped surface (own ∪ children) rather than a fresh own-walk -- so a child's
 * folded authority actually reaches the run.
 *
 * Iterates `grants` (the authoritative resource list) and requires an effect
 * for each, throwing on a missing one -- mirroring the walk projection's
 * fail-loud invariant (`resolveRuntimeGrantEffects`), which refuses to default
 * a missing effect lest it downgrade an `ask` tool. A well-formed surface (from
 * `flattenWalkToSurface`/`mergeGrantSurfaces`) carries an effect for every
 * grant, so this never fires in practice; reading `grants` rather than
 * `grantEffects` also means a stray effect key with no matching grant is never
 * materialized.
 */
export function deriveRunRuntimeGrantRowsFromSurface(
  surface: ApprovedGrantSurface,
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow[] {
  const entries = surface.grants.map((resource): [string, GrantEffect] => {
    const effect = surface.grantEffects[resource];
    if (effect === undefined) {
      throw new Error(
        `run-grant materialization: pinned surface grant ${JSON.stringify(resource)} has no effect; the stored surface is malformed`,
      );
    }
    return [resource, effect];
  });
  return projectRuntimeGrantRows(entries, tenantId, runPrincipalId, now);
}

function projectRuntimeGrantRows(
  effectByResource: Iterable<readonly [string, GrantEffect]>,
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow[] {
  const rows: MaterializedGrantRow[] = [];
  for (const [resource, effect] of effectByResource) {
    rows.push({
      id: generateId("grant"),
      tenantId,
      principalId: runPrincipalId,
      resource,
      action: "invoke",
      effect,
      conditions: null,
      origin: "creator",
      expiresAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  return rows;
}

/**
 * The runtime grant ceiling for a definition being materialized, or undefined
 * when the definition has no childWorkflow steps (its runtime rows then project
 * from a fresh own-walk, unchanged). A childWorkflow-bearing definition's
 * ceiling is the deploy-stamped pinned surface (own ∪ children): reading it
 * back is how a child's folded authority reaches the run.
 *
 * The surface is read at the definition's CURRENT version. Unlike the walk, it
 * is not a pure function of the definition's content -- it carries an external
 * child contribution and is version-scoped, and `currentVersion` is mutable
 * (a rollback repoints it). So it must be read per run, never cached by
 * `definitionId`, or a superseded version's ceiling could over-grant a later
 * run. Callers with a per-deployment cache freeze only the content-pure
 * `hasChildWorkflow` predicate (`definitionHasChildWorkflow`) and read the
 * surface fresh each run via `readChildWorkflowCeiling`.
 */
export async function resolveChildWorkflowRuntimeCeiling(
  db: DBExecutor,
  definition: WorkflowDefinition,
  definitionId: string,
  currentVersion: string,
): Promise<ApprovedGrantSurface | undefined> {
  if (!definitionHasChildWorkflow(definition)) {
    return undefined;
  }
  return readChildWorkflowCeiling(db, definitionId, currentVersion);
}

/** Whether a definition references any childWorkflow -- a pure function of its
 * content, so a per-deployment cache may freeze it. */
export function definitionHasChildWorkflow(
  definition: WorkflowDefinition,
): boolean {
  return enumerateChildWorkflowRefs(definition).length > 0;
}

/**
 * Read a childWorkflow-bearing definition's stamped pinned surface at a
 * specific version, failing closed when it is absent. `definitionId` is a live
 * foreign key from the run's anchor and `currentVersion` is read off that same
 * definition row, so the version row is present -- a null surface therefore
 * means the deploy never folded and stamped this definition's children (a
 * deploy bug, or a deployment predating the fold). Fail closed loudly rather
 * than run the parent and its children under a ceiling that omits the child
 * authority no operator reviewed. This also gates un-stubbing childWorkflow
 * execution (INTR-310): a run cannot reach a child step without a stamped
 * surface.
 */
export async function readChildWorkflowCeiling(
  db: DBExecutor,
  definitionId: string,
  currentVersion: string,
): Promise<ApprovedGrantSurface> {
  const surface = await readApprovedGrantSurface(
    db,
    definitionId,
    currentVersion,
  );
  if (surface === null) {
    throw new Error(
      `run-grant materialization: childWorkflow-bearing definition ${definitionId} (version ${currentVersion}) has no approved grant surface; its deploy must fold and stamp its children's surfaces before it can run`,
    );
  }
  return surface;
}

/**
 * Project a materialized run grant row into the `run.grants` wire shape --
 * the same `WireGrantRule` encoding the `agent.deploy` frame's
 * `config.grants` ships. A run grant is always principal-scoped and never
 * role-scoped, so `roleId` is null and `principalId` is the run principal.
 */
export function runGrantToWire(
  row: MaterializedGrantRow,
): RunGrantsFrame["stepGrants"][number] {
  return {
    id: row.id,
    resource: row.resource,
    action: row.action,
    effect: row.effect,
    origin: row.origin,
    conditions: row.conditions,
    expiresAt: row.expiresAt,
    roleId: null,
    principalId: row.principalId,
  };
}

export type StageRunGrantsArgs = {
  definition: WorkflowDefinition;
  tenantId: string;
  runPrincipalId: string;
  now: Date;
  /**
   * Declared invoker grants resolved against the launching principal's
   * authority. The mail path passes an empty set (no invoker is on the
   * wire); the external trigger route passes the caller's grants.
   */
  invokerGrants: GrantRule[];
  /** Declared creator grants resolved against the workflow asset's creator. */
  creatorGrants: GrantRule[];
  /**
   * Grant requirements to resolve. The mail path pre-filters this to the
   * non-invoker requirements before calling; the external route passes the
   * definition's requirements unfiltered.
   */
  grantRequirements: readonly GrantRequirement[];
  /**
   * The runtime grant ceiling for a childWorkflow-bearing definition -- the
   * deploy-stamped pinned surface (own ∪ children). When set, the run's
   * runtime `tool:`/`effect:` rows project from it instead of the own-walk, so
   * a child's folded authority reaches the run. Undefined for a definition with
   * no childWorkflow steps (its rows project from the walk, unchanged).
   * Resolved by `resolveChildWorkflowRuntimeCeiling`, which fails closed when a
   * childWorkflow-bearing definition has no stamped surface.
   */
  runtimeCeiling?: ApprovedGrantSurface;
};

export type StageRunGrantsResult =
  | {
      ok: false;
      rejection: { status: 403 | 409; code: string; message: string };
    }
  | {
      ok: true;
      grantRows: MaterializedGrantRow[];
      stepGrants: RunGrantsFrame["stepGrants"];
    };

/**
 * The capability walk for a workflow definition: the deploy-approved lift the
 * run's runtime `tool:`/`effect:` grants project from. Isolated from
 * `stageRunGrants` so a caller that walks a definition once can bind the walk
 * to a stable identity and reuse it, rather than re-walking on every run (the
 * mail-triggered path does exactly this).
 */
export function buildCapabilityWalk(
  definition: WorkflowDefinition,
): CapabilityWalkResult {
  const directorRegistry = createDefaultDirectorRegistry();
  return walkCapabilities(definition, directorRegistry);
}

export type StageRunGrantsFromWalkArgs = Omit<
  StageRunGrantsArgs,
  "definition"
> & {
  /**
   * An already-computed capability walk. `stageRunGrants` supplies a fresh
   * walk of a live definition; the mail-triggered path supplies the walk it
   * froze at the deployment's approved identity, so no re-walk happens per run.
   */
  walk: CapabilityWalkResult;
};

/**
 * Stage a run's grant rows from an ALREADY-COMPUTED capability walk plus the
 * resolved declared requirements. This is the walk-free tail shared by
 * `stageRunGrants` (which walks a live definition then delegates here) and the
 * mail-triggered materializer (which passes a walk cached at the deployment's
 * approved identity). Returns the staged rows and their wire projection, or a
 * rejection when a declared requirement's authority is insufficient. No
 * database write happens here -- `commitRunGrants` performs it once the caller
 * has accepted delivery.
 */
export async function stageRunGrantsFromWalk(
  args: StageRunGrantsFromWalkArgs,
): Promise<StageRunGrantsResult> {
  // A childWorkflow-bearing definition's runtime ceiling is the deploy-stamped
  // pinned surface (own ∪ children); every other definition projects its rows
  // from the walk exactly as before.
  const runtimeGrantRows =
    args.runtimeCeiling !== undefined
      ? deriveRunRuntimeGrantRowsFromSurface(
          args.runtimeCeiling,
          args.tenantId,
          args.runPrincipalId,
          args.now,
        )
      : deriveRunRuntimeGrantRows(
          args.walk,
          args.tenantId,
          args.runPrincipalId,
          args.now,
        );

  const materialization = await resolveGrantMaterialization({
    tenantId: args.tenantId,
    targetPrincipalId: args.runPrincipalId,
    grantRequirements: args.grantRequirements,
    adHocInvokerGrants: [],
    invokerGrants: args.invokerGrants,
    creatorGrants: args.creatorGrants,
    now: args.now,
  });
  if (!materialization.ok) {
    return { ok: false, rejection: materialization.rejection };
  }

  const grantRows = [...runtimeGrantRows, ...materialization.grantRows];
  const stepGrants = grantRows.map((g) => runGrantToWire(g));
  return { ok: true, grantRows, stepGrants };
}

/**
 * Derive and stage a run's grant rows from its definition: the walk's
 * runtime `tool:`/`effect:` grants plus the resolved declared
 * requirements. Walks the live definition and delegates to
 * `stageRunGrantsFromWalk`. No database write happens here --
 * `commitRunGrants` performs it once the caller has accepted delivery.
 */
export async function stageRunGrants(
  args: StageRunGrantsArgs,
): Promise<StageRunGrantsResult> {
  return stageRunGrantsFromWalk({
    walk: buildCapabilityWalk(args.definition),
    tenantId: args.tenantId,
    runPrincipalId: args.runPrincipalId,
    now: args.now,
    invokerGrants: args.invokerGrants,
    creatorGrants: args.creatorGrants,
    grantRequirements: args.grantRequirements,
    ...(args.runtimeCeiling !== undefined
      ? { runtimeCeiling: args.runtimeCeiling }
      : {}),
  });
}

/**
 * Validate a definition's `grantRequirements` at the boundary. Returns the
 * validated array or a rejection carrying the validator summary.
 */
export function parseGrantRequirements(
  definition: WorkflowDefinition,
):
  | { ok: true; requirements: GrantRequirement[] }
  | { ok: false; message: string } {
  const validated = GrantRequirements(definition.grantRequirements ?? []);
  if (validated instanceof type.errors) {
    return {
      ok: false,
      message: `Invalid grant requirements: ${validated.summary}`,
    };
  }
  return { ok: true, requirements: validated };
}

/**
 * Load a workflow asset's `creatorPrincipalId` -- the creator whose
 * authority creator-sourced grant requirements resolve against. Returns
 * `null` when the asset records no creator (the FK is `set null` on
 * principal deletion) or the asset row is absent.
 */
export async function loadAssetCreatorPrincipalId(
  db: DB["db"],
  tenantId: string,
  definitionAssetId: string,
): Promise<string | null> {
  const assetRow = await db.query.asset.findFirst({
    where: and(
      eq(asset.id, definitionAssetId),
      eq(asset.tenantId, tenantId),
      eq(asset.kind, "workflow"),
    ),
  });
  return assetRow?.creatorPrincipalId ?? null;
}

/**
 * Collect a creator's grants only when a creator-sourced requirement
 * exists and the asset records a creator. Mirrors the trigger route: when
 * a creator-sourced requirement exists but the creator is null, the grants
 * stay empty and `resolveGrantMaterialization` fails closed rather than
 * inventing a fallback principal.
 */
export async function collectCreatorGrants(
  grantStore: GrantStore,
  tenantId: string,
  creatorPrincipalId: string | null,
  grantRequirements: readonly GrantRequirement[],
): Promise<GrantRule[]> {
  const hasCreatorReqs = grantRequirements.some((r) => r.source === "creator");
  if (!hasCreatorReqs || creatorPrincipalId === null) return [];
  return grantStore.collectGrants(creatorPrincipalId, tenantId);
}

export type CommitRunGrantsArgs = {
  db: DB["db"];
  tenantId: string;
  anchorRunId: string;
  /**
   * The deployment's definition, resolved by the caller off the anchor run.
   * Anchors the run on its definition. Edge resolves; this interior trusts.
   */
  definitionId: string;
  runId: string;
  runPrincipalId: string;
  now: Date;
  grantRows: MaterializedGrantRow[];
};

export type CommittedRunGrants = {
  runPrincipalId: string;
  stepGrants: RunGrantsFrame["stepGrants"];
};

/**
 * Lock and classify one run row owned by a deployment. A live run -- a
 * "deployed" anchor in its pre-trigger window or a "running" run -- classifies
 * as "running"; the started-vs-not distinction is owned by the durable
 * lifecycle, not this status axis.
 */
export async function lockWorkflowRunState(
  tx: DBExecutor,
  anchorRunId: string,
  runId: string,
): Promise<"absent" | "running" | "terminal"> {
  const [run] = await tx
    .select({ status: workflowRun.status })
    .from(workflowRun)
    .where(
      and(eq(workflowRun.id, runId), eq(workflowRun.anchorRunId, anchorRunId)),
    )
    .limit(1)
    .for("update");
  if (run === undefined) return "absent";
  return isLiveWorkflowRunStatus(run.status) ? "running" : "terminal";
}

/**
 * Lock a deployment's sidecar allocation `FOR UPDATE` and report whether it is
 * still dispatchable. Serializes an exclusive trigger's commit with concurrent
 * allocation transitions so a durable dispatch is never enqueued against an
 * allocation that has moved to a non-dispatchable state.
 */
export async function lockDispatchableAllocation(
  tx: DBExecutor,
  allocationId: string,
  anchorRunId: string,
): Promise<boolean> {
  const [allocation] = await tx
    .select({ status: sidecarAllocation.status })
    .from(sidecarAllocation)
    .where(
      and(
        eq(sidecarAllocation.id, allocationId),
        eq(sidecarAllocation.anchorRunId, anchorRunId),
      ),
    )
    .limit(1)
    .for("update");
  return (
    allocation !== undefined &&
    isSidecarAllocationDispatchable(allocation.status)
  );
}

async function loadCommittedRunGrantsFromExecutor(
  executor: DBExecutor,
  tenantId: string,
  runId: string,
): Promise<CommittedRunGrants | null> {
  const [runPrincipal] = await executor
    .select({ id: principalTable.id })
    .from(principalTable)
    .where(
      and(
        eq(principalTable.tenantId, tenantId),
        eq(principalTable.kind, "workflow"),
        eq(principalTable.refId, runId),
      ),
    )
    .limit(1);
  if (runPrincipal === undefined) return null;

  const rows = await executor
    .select()
    .from(grantTable)
    .where(eq(grantTable.principalId, runPrincipal.id))
    .orderBy(asc(grantTable.id));
  const validated = RunGrantsFrame.assert({
    type: "run.grants",
    agentAddress: "persisted@validation.invalid",
    runId,
    stepGrants: rows.map((row) => ({
      id: row.id,
      resource: row.resource,
      action: row.action,
      effect: row.effect,
      origin: row.origin,
      conditions: row.conditions,
      expiresAt: row.expiresAt,
      roleId: row.roleId,
      principalId: row.principalId,
    })),
  });
  return {
    runPrincipalId: runPrincipal.id,
    stepGrants: validated.stepGrants,
  };
}

/** Load the one canonical grant snapshot already reserved for a stable run. */
export async function loadCommittedRunGrants(
  db: DB["db"],
  tenantId: string,
  runId: string,
): Promise<CommittedRunGrants | null> {
  return loadCommittedRunGrantsFromExecutor(db, tenantId, runId);
}

/**
 * Idempotently reserve a run's principal, run row, and immutable grant rows
 * in one transaction, keyed on the deployment's stable top-level run id.
 *
 * The transaction that wins the unique principal insert owns the grant
 * inserts. A concurrent or later caller returns those exact persisted grants
 * instead of sending its independently staged snapshot. This keeps the
 * database and Git authorization views identical when first deliveries race.
 *
 * On the first commit the `runPrincipalId` is derived deterministically
 * from `(tenantId, runId)` by the caller, so the principal insert and the
 * grant rows that reference it agree on the id even across a retry.
 */
export async function commitRunGrants(
  args: CommitRunGrantsArgs,
  tx?: DBExecutor,
): Promise<RunGrantsFrame["stepGrants"]> {
  const workflowRunStore = createWorkflowRunStore(args.db);
  const commit = async (
    executor: DBExecutor,
  ): Promise<RunGrantsFrame["stepGrants"]> => {
    const existing = await loadCommittedRunGrantsFromExecutor(
      executor,
      args.tenantId,
      args.runId,
    );
    if (existing !== null) return existing.stepGrants;

    const [insertedPrincipal] = await executor
      .insert(principalTable)
      .values({
        id: args.runPrincipalId,
        tenantId: args.tenantId,
        kind: "workflow",
        refId: args.runId,
        status: "active",
        createdAt: args.now,
        updatedAt: args.now,
      })
      .onConflictDoNothing({
        target: [
          principalTable.tenantId,
          principalTable.kind,
          principalTable.refId,
        ],
      })
      .returning({ id: principalTable.id });
    if (insertedPrincipal === undefined) {
      const winner = await loadCommittedRunGrantsFromExecutor(
        executor,
        args.tenantId,
        args.runId,
      );
      if (winner === null) {
        throw new Error(
          `commitRunGrants: principal race for ${args.runId} did not expose the winning grant snapshot`,
        );
      }
      return winner.stepGrants;
    }

    await workflowRunStore.anchorWithPrincipal(
      {
        id: args.runId,
        anchorRunId: args.anchorRunId,
        definitionId: args.definitionId,
        tenantId: args.tenantId,
        principalId: args.runPrincipalId,
        status: "running",
      },
      executor,
    );
    for (const g of args.grantRows) {
      await executor.insert(grantTable).values(g);
    }
    return [...args.grantRows]
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((grant) => runGrantToWire(grant));
  };
  if (tx !== undefined) {
    return commit(tx);
  }
  return args.db.transaction(commit);
}

export type MailTriggeredRunGrantsDeps = {
  db: DB["db"];
  assetService: AssetService;
  grantStore: GrantStore;
};

/**
 * A deployment's deploy-approved grant basis: the frozen capability walk the
 * run's runtime `tool:`/`effect:` grants project from, the creator-sourced grant
 * requirements resolved against it, and whether the definition references any
 * childWorkflow. All three are pure functions of the approved definition
 * content, so they are computed once per deployment and cached; nothing here
 * depends on a live re-read of the asset blob.
 *
 * The childWorkflow runtime ceiling is deliberately NOT cached here: it is the
 * version-scoped pinned surface, and `currentVersion` is mutable (a rollback
 * repoints it), so it is read fresh each run. Caching it by `definitionId`
 * would over-grant a later run under a superseded version's ceiling.
 */
type FrozenRunGrantBasis = {
  readonly walk: CapabilityWalkResult;
  readonly creatorRequirements: readonly GrantRequirement[];
  readonly hasChildWorkflow: boolean;
};

/**
 * Build the mail-triggered run-grants materializer the sidecar router's
 * `mail.outbound` handler invokes for each workflow-deployment recipient.
 *
 * A mail-triggered run derives its grants from the RECEIVING deployment's
 * definition: the walk's `tool:`/`effect:` runtime grants plus the
 * CREATOR-resolved declared requirements. Invoker-sourced requirements are
 * NOT materialized -- no invoker is on the wire -- and the run still
 * launches: a step that needs an invoker grant fails closed at its own
 * authz check. The requirements are pre-filtered to `source !== "invoker"`
 * before staging, so `resolveGrantMaterialization` keeps its
 * reject-on-insufficient-invoker contract intact for the external route.
 *
 * The materializer reserves the stable run and its immutable grants before
 * delivery. A delivery failure can therefore leave a grants-only run, which
 * is intentionally still eligible for its first fire; the durable event log,
 * not the authorization row, is the fired/not-fired authority.
 */
export function createMailTriggeredRunGrantsMaterializer(
  deps: MailTriggeredRunGrantsDeps,
): (args: {
  agentAddress: string;
  runId: string;
}) => Promise<MailTriggeredRunGrantsResult> {
  // Closure-level cache of each deployment's deploy-approved grant basis, keyed
  // by the workflow definition's identity. A definition id is content-addressed
  // -- keyed by `(assetId, wireHash)`, frozen at approval -- and the anchor run
  // carries that id, so the key names the APPROVED definition content, not the
  // mutable asset blob behind it. The first trigger of a deployment hydrates
  // and walks the definition once and freezes the result here; every later
  // trigger consumes the frozen basis WITHOUT re-reading the asset blob or
  // re-walking it. This closes the mutated-asset TOCTOU: rewriting the blob
  // under a stable asset id cannot change a run's grants, because runs bind to
  // the frozen approved walk, never a live re-hydrate.
  const frozenBasisByDefinition = new Map<string, FrozenRunGrantBasis>();

  return async ({ agentAddress, runId }) => {
    const topLevelRun = alias(workflowRun, "mail_triggered_top_level_run");
    const [anchor] = await deps.db
      .select({
        anchorRunId: workflowRun.id,
        tenantId: workflowRun.tenantId,
        definitionId: workflowRun.definitionId,
        definitionAssetId: workflowDefinition.assetId,
        definitionVersion: workflowDefinition.currentVersion,
        anchorStatus: workflowRun.status,
        topLevelRunStatus: topLevelRun.status,
      })
      .from(workflowRun)
      .innerJoin(
        workflowDefinition,
        eq(workflowRun.definitionId, workflowDefinition.id),
      )
      .leftJoin(
        topLevelRun,
        and(
          eq(topLevelRun.id, runId),
          eq(topLevelRun.anchorRunId, workflowRun.id),
        ),
      )
      .where(eq(workflowRun.address, agentAddress))
      .limit(1);
    if (anchor === undefined) return { outcome: "skip" };
    // A "deployed" anchor is live: mail-triggering it IS its first trigger, so
    // it must not be rejected as terminal here.
    //
    // This preflight inspects only the workflow_run.status column and omits the
    // durable-lifecycle terminal check that the HTTP trigger route in
    // workflow-run-trigger.ts applies. The supervisor's durable run-ref guard
    // (rejectTerminalRun / readWorkflowRunLifecycle in supervisor.ts) is the
    // fired/not-fired authority and never re-fires a durably-settled run, so
    // this status check is a lagging fast-fail only. The two preflights diverge
    // inside the status-flip lag window; that asymmetry is tolerable and is
    // tracked for unification in INTR-456.
    if (
      !isLiveWorkflowRunStatus(anchor.anchorStatus) ||
      (anchor.topLevelRunStatus !== null &&
        !isLiveWorkflowRunStatus(anchor.topLevelRunStatus))
    ) {
      return {
        outcome: "rejected",
        status: 409,
        code: "workflow_run_terminal",
        message: `Workflow run ${runId} is terminal and cannot receive more mail`,
      };
    }
    if (anchor.definitionAssetId === null) {
      throw new Error(
        `mail-triggered run ${runId} for ${agentAddress}: anchor run's definition has no asset`,
      );
    }
    const definitionAssetId = anchor.definitionAssetId;
    const tenantId = anchor.tenantId;
    const anchorRunId = anchor.anchorRunId;
    const definitionId = anchor.definitionId;

    const committed = await loadCommittedRunGrants(deps.db, tenantId, runId);
    if (committed !== null) {
      return {
        outcome: "materialized",
        stepGrants: committed.stepGrants,
      };
    }

    let basis = frozenBasisByDefinition.get(definitionId);
    if (basis === undefined) {
      // First trigger of this deployment: read and walk the approved
      // definition exactly once, then freeze the result. Neither the read nor
      // the walk runs again for this definition id.
      const definition = await hydrateDefinition(
        deps.assetService,
        definitionAssetId,
      );

      const parsedRequirements = parseGrantRequirements(definition);
      if (!parsedRequirements.ok) {
        throw new Error(
          `mail-triggered run ${runId} for ${agentAddress}: ${parsedRequirements.message}`,
        );
      }
      // Invoker-sourced requirements are not materialized on the mail path:
      // filter them out BEFORE staging rather than teaching the resolver a
      // skip mode, so the external route keeps resolving invoker grants.
      const creatorRequirements = parsedRequirements.requirements.filter(
        (r) => r.source !== "invoker",
      );
      // Freeze whether this definition references a childWorkflow -- a pure
      // function of its content. The ceiling surface itself is NOT frozen: it
      // is version-scoped and `currentVersion` can be rolled back, so it is
      // read fresh each run below.
      basis = {
        walk: buildCapabilityWalk(definition),
        creatorRequirements,
        hasChildWorkflow: definitionHasChildWorkflow(definition),
      };
      frozenBasisByDefinition.set(definitionId, basis);
    }

    // A childWorkflow-bearing definition's runtime ceiling is read fresh each
    // run at the definition's current version, so a rollback that repoints
    // `currentVersion` cannot leave a later run staged from a superseded
    // (possibly broader) ceiling. Fails closed if the current version carries
    // no stamped surface.
    const runtimeCeiling = basis.hasChildWorkflow
      ? await readChildWorkflowCeiling(
          deps.db,
          definitionId,
          anchor.definitionVersion,
        )
      : undefined;

    // Creator authority is resolved LIVE per run: the definition's grant SHAPE
    // is frozen above, but which grants the creator currently holds is not part
    // of that shape and can change between triggers. This reads the asset row's
    // creator column and the creator's grants -- not the definition blob -- so
    // it is not the re-read the frozen basis eliminates.
    const creatorPrincipalId = await loadAssetCreatorPrincipalId(
      deps.db,
      tenantId,
      definitionAssetId,
    );
    const creatorGrants = await collectCreatorGrants(
      deps.grantStore,
      tenantId,
      creatorPrincipalId,
      basis.creatorRequirements,
    );

    // Derive the run principal id from `(tenantId, runId)`. The runId is the
    // stable deployment address, so all trigger occurrences resolve the same
    // principal and canonical grant snapshot.
    const runPrincipalId = await deriveRunPrincipalId(tenantId, runId);
    const now = new Date();
    const staged = await stageRunGrantsFromWalk({
      walk: basis.walk,
      tenantId: tenantId,
      runPrincipalId,
      now,
      invokerGrants: [],
      creatorGrants,
      grantRequirements: basis.creatorRequirements,
      ...(runtimeCeiling !== undefined ? { runtimeCeiling } : {}),
    });
    if (!staged.ok) {
      return {
        outcome: "rejected",
        status: staged.rejection.status,
        code: staged.rejection.code,
        message: staged.rejection.message,
      };
    }

    const stepGrants = await deps.db.transaction(async (tx) => {
      if (
        (await lockWorkflowRunState(tx, anchorRunId, anchorRunId)) !==
          "running" ||
        (await lockWorkflowRunState(tx, anchorRunId, runId)) === "terminal"
      ) {
        return null;
      }
      return commitRunGrants(
        {
          db: deps.db,
          tenantId,
          anchorRunId,
          definitionId: anchor.definitionId,
          runId,
          runPrincipalId,
          now,
          grantRows: staged.grantRows,
        },
        tx,
      );
    });
    if (stepGrants === null) {
      return {
        outcome: "rejected",
        status: 409,
        code: "workflow_run_terminal",
        message: `Workflow run ${runId} is terminal and cannot receive more mail`,
      };
    }
    return {
      outcome: "materialized",
      stepGrants,
    };
  };
}
