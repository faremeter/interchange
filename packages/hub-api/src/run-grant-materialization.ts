// Shared run-grant materialization: the sequence a workflow run's
// authorization is derived and committed through, used by the external
// trigger route and the hub's mail-triggered run path so the two cannot
// drift. The run's grant set is the definition-pure runtime grants (the
// capability walk's `tool:`/`effect:` rows) plus the resolved declared
// grant requirements (creator- and invoker-sourced). Staging and commit
// live here; each call site orchestrates its own delivery.

import { and, asc, eq } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";

import {
  asset,
  grant as grantTable,
  isLiveWorkflowRunStatus,
  principal as principalTable,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type { DB, DBExecutor, PrincipalKeyStore } from "@intx/db";
import {
  createPrincipalStore,
  createWorkflowRunStore,
  workflowRunExecutability,
  loadFrozenGrantSnapshot,
} from "@intx/db";
import type { GrantStore, GrantRule } from "@intx/types/authz";
import {
  isSidecarAllocationDispatchable,
  type GrantEffect,
  type GrantRequirement,
  type GrantWalkSnapshot,
} from "@intx/types";
import { RunGrantsFrame } from "@intx/types/sidecar";
import { ToolDefinition } from "@intx/types/runtime";
import { type MailTriggeredRunGrantsResult } from "@intx/hub-sessions";
import { deriveRunPrincipalId, generateId } from "@intx/hub-common";

import {
  resolveGrantMaterialization,
  type MaterializedGrantRow,
} from "./grant-materialization";

// The `tool:<name>` rows carry BARE tool names: the walk reads inline
// `agent.toolFactories`, which have no bundle context. A workflow child
// gates each call on `tool:<call.name>` and queries
// `tool:<bundleId>:<name>`, so bare rows never address a pinned tool's
// runtime gate; its authority comes from the sidecar tool-mark floor.
// `effect:<cap>` rows differ: an action's EffectContext authorizes the
// bare name on both sides, so those ARE operative at run time.
const TOOL_GRANT_PREFIX = "tool:";
const EFFECT_GRANT_PREFIX = "effect:";

/**
 * Project the frozen grant-walk snapshot into the run's runtime grant rows --
 * the `tool:<name>` and `effect:<cap>` grants the runtime enforces fail-closed.
 * Every distinct grant string across all steps becomes one creator-origin
 * `grant` row with `action: invoke`.
 *
 * Tool grants carry the effect the tool's static declaration requested (`ask`
 * for approval-gated tools, `allow` otherwise) via each step's `grantEffects`
 * record. `ask` wins over `allow` when steps disagree, so an approval-gated
 * declaration is never silently downgraded. Effect grants are always `allow`
 * and are not routed through `grantEffects` (which covers tool grants only).
 */
export function deriveRunRuntimeGrantRows(
  snapshot: GrantWalkSnapshot,
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow[] {
  const effectByResource = new Map<string, GrantEffect>();
  for (const step of snapshot.perStep) {
    // The snapshot serializes each step's tool-grant-to-effect map as a plain
    // object; rehydrate it to a `Map` for the lookup below.
    const grantEffects = new Map<string, GrantEffect>(
      Object.entries(step.grantEffects),
    );
    for (const grant of step.grants) {
      if (grant.startsWith(TOOL_GRANT_PREFIX)) {
        // A missing `grantEffects` entry means the snapshot's `grants` and
        // `grantEffects` maps have diverged; a defaulted `allow` would
        // silently DOWNGRADE an `ask` tool below its floor, defeating the
        // approval gate. Fail loudly instead.
        const effect = grantEffects.get(grant);
        if (effect === undefined) {
          throw new Error(
            `deriveRunRuntimeGrantRows: tool grant ${JSON.stringify(grant)} has no grantEffects entry; the grant-walk snapshot must carry an effect for every tool grant`,
          );
        }
        const existing = effectByResource.get(grant);
        if (existing === "ask" || effect === "ask") {
          effectByResource.set(grant, "ask");
        } else if (existing === undefined) {
          effectByResource.set(grant, effect);
        }
      } else if (grant.startsWith(EFFECT_GRANT_PREFIX)) {
        // Effect grants are always allow; a repeat across steps is idempotent.
        if (!effectByResource.has(grant)) {
          effectByResource.set(grant, "allow");
        }
      }
    }
  }

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
 * Project a materialized run grant row into the `run.grants` wire shape --
 * the same `WireGrantRule` encoding the `agent.deploy` frame's
 * `config.grants` ships. Run grants are always principal-scoped, so
 * `roleId` is null and `principalId` is the run principal.
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

export type StageRunGrantsFromSnapshotArgs = {
  /** The deploy-approved grant-walk snapshot frozen at approval. */
  snapshot: GrantWalkSnapshot;
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
   * Grant requirements to resolve. The mail path pre-filters the snapshot's
   * requirements to the non-invoker ones before calling; the external route
   * passes the snapshot's requirements unfiltered.
   */
  grantRequirements: readonly GrantRequirement[];
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
 * Stage a run's grant rows from the deploy-approved grant-walk snapshot plus
 * the resolved declared requirements. Returns the staged rows and their wire
 * projection, or a rejection when a declared requirement's authority is
 * insufficient. No database write happens here -- `commitRunGrants` performs
 * it once the caller has accepted delivery.
 */
export async function stageRunGrantsFromSnapshot(
  args: StageRunGrantsFromSnapshotArgs,
): Promise<StageRunGrantsResult> {
  const runtimeGrantRows = deriveRunRuntimeGrantRows(
    args.snapshot,
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
 * exists and the asset records a creator. When the requirement exists but
 * the creator is null, the grants stay empty and
 * `resolveGrantMaterialization` fails closed rather than inventing a
 * fallback principal.
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
  principalKeyStore: PrincipalKeyStore;
  tenantId: string;
  anchorRunId: string;
  /** The deployment's definition, resolved by the caller off the anchor run. */
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
 * Lock one run row owned by a deployment and classify it: "running" while
 * it accepts work, "stopping" once cancellation is requested or its
 * deadline passes, "terminal" once settled. The started-vs-not distinction
 * is owned by the durable lifecycle, not this status axis.
 */
export async function lockWorkflowRunState(
  tx: DBExecutor,
  anchorRunId: string,
  runId: string,
): Promise<"absent" | "running" | "stopping" | "terminal"> {
  const [run] = await tx
    .select({
      status: workflowRun.status,
      expiresAt: workflowRun.expiresAt,
      cancellationRequestedAt: workflowRun.cancellationRequestedAt,
    })
    .from(workflowRun)
    .where(
      and(eq(workflowRun.id, runId), eq(workflowRun.anchorRunId, anchorRunId)),
    )
    .limit(1)
    .for("update");
  if (run === undefined) return "absent";
  const state = workflowRunExecutability(run);
  return state === "executable" ? "running" : state;
}

function unavailableRunResult(
  runId: string,
  state: "stopping" | "terminal",
): MailTriggeredRunGrantsResult {
  return {
    outcome: "rejected",
    status: 409,
    code:
      state === "stopping" ? "workflow_run_stopping" : "workflow_run_terminal",
    message: `Workflow run ${runId} is ${state} and cannot receive more mail`,
  };
}

/**
 * Lock a deployment's sidecar allocation `FOR UPDATE` and report whether it
 * is still dispatchable. Serializes a provisioned trigger's commit with
 * concurrent allocation transitions so a durable dispatch is never enqueued
 * against an allocation that has moved to a non-dispatchable state.
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
 * The tool name an approval names, read from its `toolDefinition` snapshot.
 * The name lives in untyped jsonb; validate it through the `ToolDefinition`
 * arktype rather than reaching in, so a malformed snapshot fails loudly.
 */
export function approvalToolName(
  toolDefinition: Record<string, unknown>,
): string {
  return ToolDefinition.assert(toolDefinition).name;
}

/**
 * Resolve a run's `ask` checkpoint on one tool into a standing effect -- the
 * durable mutation a `scope: "always"` resolution makes: `allow` on
 * approve-always, `deny` on reject-always. The grant stays with the run, so
 * every later read (the child's enforcement floor, the authorization view,
 * the per-dispatch re-establish) sees the standing effect.
 *
 * Guarded to only change a grant currently gated `ask`, so it never overrides
 * an existing `allow`/`deny` and never touches a tool the run does not
 * already hold. Runs against the passed executor so a rolled-back resolve
 * rolls back the grant change with it; a run with no principal is a no-op.
 */
export async function setRunToolGrantEffect(
  executor: DBExecutor,
  tenantId: string,
  runId: string,
  toolName: string,
  effect: "allow" | "deny",
): Promise<void> {
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
  if (runPrincipal === undefined) return;
  await executor
    .update(grantTable)
    .set({ effect, updatedAt: new Date() })
    .where(
      and(
        eq(grantTable.principalId, runPrincipal.id),
        eq(grantTable.resource, `${TOOL_GRANT_PREFIX}${toolName}`),
        eq(grantTable.effect, "ask"),
      ),
    );
}

/**
 * Idempotently reserve a run's principal, run row, and immutable grant rows
 * in one transaction, keyed on the deployment's stable top-level run id.
 *
 * The transaction that wins the unique principal insert owns the grant
 * inserts; a concurrent or later caller returns those exact persisted grants
 * instead of sending its independently staged snapshot, so the database and
 * Git authorization views stay identical when first deliveries race.
 */
export async function commitRunGrants(
  args: CommitRunGrantsArgs,
  tx?: DBExecutor,
): Promise<RunGrantsFrame["stepGrants"]> {
  const workflowRunStore = createWorkflowRunStore(args.db);
  const principalStore = createPrincipalStore(args.db, args.principalKeyStore);
  const commit = async (
    executor: DBExecutor,
  ): Promise<RunGrantsFrame["stepGrants"]> => {
    const existing = await loadCommittedRunGrantsFromExecutor(
      executor,
      args.tenantId,
      args.runId,
    );
    if (existing !== null) return existing.stepGrants;

    const insertedPrincipal = await principalStore.createIfAbsent(
      {
        id: args.runPrincipalId,
        tenantId: args.tenantId,
        kind: "workflow",
        refId: args.runId,
        status: "active",
        createdAt: args.now,
        updatedAt: args.now,
      },
      executor,
    );
    if (insertedPrincipal === null) {
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
  principalKeyStore: PrincipalKeyStore;
  grantStore: GrantStore;
};

/**
 * A deployment's deploy-approved grant basis: the grant-walk snapshot frozen
 * at approval, from which the run's runtime `tool:`/`effect:` grants and its
 * declared requirements both derive. Pure function of the approved definition
 * content, keyed by the definition id, so it is read once and cached.
 */
type FrozenRunGrantBasis = {
  readonly snapshot: GrantWalkSnapshot;
};

/**
 * Build the mail-triggered run-grants materializer the sidecar router's
 * `mail.outbound` handler invokes for each workflow-deployment recipient.
 *
 * A mail-triggered run derives its grants from the RECEIVING deployment's
 * frozen snapshot plus the CREATOR-resolved declared requirements.
 * Invoker-sourced requirements are NOT materialized -- no invoker is on the
 * wire -- and the run still launches: a step that needs an invoker grant
 * fails closed at its own authz check. The snapshot's requirements are
 * pre-filtered to `source !== "invoker"` before staging.
 *
 * The materializer reserves the stable run and its immutable grants before
 * delivery, so a delivery failure can leave a grants-only run that is still
 * eligible for its first fire; the durable event log, not the authorization
 * row, is the fired/not-fired authority.
 */
export function createMailTriggeredRunGrantsMaterializer(
  deps: MailTriggeredRunGrantsDeps,
): (args: {
  agentAddress: string;
  runId: string;
}) => Promise<MailTriggeredRunGrantsResult> {
  // Closure-level cache of each deployment's deploy-approved snapshot, keyed by
  // the workflow definition's identity. A definition id is content-addressed
  // (keyed by `(assetId, wireHash)`, frozen at approval) and the anchor run
  // carries that id, so the key names the APPROVED definition content, not the
  // mutable asset blob behind it. Runs bind to the frozen snapshot, never a
  // live re-hydrate or re-walk, so a rewritten asset blob cannot change a
  // run's grants.
  const frozenBasisByDefinition = new Map<string, FrozenRunGrantBasis>();

  return async ({ agentAddress, runId }) => {
    const topLevelRun = alias(workflowRun, "mail_triggered_top_level_run");
    const [anchor] = await deps.db
      .select({
        anchorRunId: workflowRun.id,
        tenantId: workflowRun.tenantId,
        definitionId: workflowRun.definitionId,
        definitionAssetId: workflowDefinition.assetId,
        anchorStatus: workflowRun.status,
        anchorExpiresAt: workflowRun.expiresAt,
        anchorCancellationRequestedAt: workflowRun.cancellationRequestedAt,
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
    // it must not be rejected as terminal here. The supervisor's durable
    // run-ref guard is the fired/not-fired authority and never re-fires a
    // durably-settled run, so this status check is a lagging fast-fail only.
    const anchorState = workflowRunExecutability({
      status: anchor.anchorStatus,
      expiresAt: anchor.anchorExpiresAt,
      cancellationRequestedAt: anchor.anchorCancellationRequestedAt,
    });
    if (anchorState !== "executable")
      return unavailableRunResult(runId, anchorState);
    if (
      anchor.topLevelRunStatus !== null &&
      !isLiveWorkflowRunStatus(anchor.topLevelRunStatus)
    )
      return unavailableRunResult(runId, "terminal");
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
      // First trigger of this deployment: read the frozen snapshot once, then
      // cache it. The read never runs again for this definition id.
      const snapshot = await loadFrozenGrantSnapshot(deps.db, definitionId);
      if (snapshot === null) {
        // The definition has no approved grant snapshot -- the "not yet
        // approved" state, mirroring a null `approvedWireHash`. Fail closed
        // rather than substitute an empty grant set, which would launch a run
        // with no runtime authority.
        throw new Error(
          `mail-triggered run ${runId} for ${agentAddress}: definition ${definitionId} has no approved grant snapshot`,
        );
      }
      basis = { snapshot };
      frozenBasisByDefinition.set(definitionId, basis);
    }

    // Invoker-sourced requirements are not materialized on the mail path;
    // filter them out before staging so the external route keeps resolving
    // invoker grants.
    const creatorRequirements = basis.snapshot.grantRequirements.filter(
      (r) => r.source !== "invoker",
    );

    // Creator authority is resolved LIVE per run: the snapshot freezes the
    // grant SHAPE, not which grants the creator currently holds, and that can
    // change between triggers. This reads the asset row's creator column and
    // the creator's grants -- not the snapshot.
    const creatorPrincipalId = await loadAssetCreatorPrincipalId(
      deps.db,
      tenantId,
      definitionAssetId,
    );
    const creatorGrants = await collectCreatorGrants(
      deps.grantStore,
      tenantId,
      creatorPrincipalId,
      creatorRequirements,
    );

    // Derive the run principal id from `(tenantId, runId)`: the runId is the
    // stable deployment address, so all trigger occurrences resolve the same
    // principal and canonical grant snapshot.
    const runPrincipalId = await deriveRunPrincipalId(tenantId, runId);
    const now = new Date();
    const staged = await stageRunGrantsFromSnapshot({
      snapshot: basis.snapshot,
      tenantId: tenantId,
      runPrincipalId,
      now,
      invokerGrants: [],
      creatorGrants,
      grantRequirements: creatorRequirements,
    });
    if (!staged.ok) {
      return {
        outcome: "rejected",
        status: staged.rejection.status,
        code: staged.rejection.code,
        message: staged.rejection.message,
      };
    }

    const reserved = await deps.db.transaction(async (tx) => {
      const anchorState = await lockWorkflowRunState(
        tx,
        anchorRunId,
        anchorRunId,
      );
      if (anchorState !== "running")
        return anchorState === "stopping" ? anchorState : "terminal";
      const runState = await lockWorkflowRunState(tx, anchorRunId, runId);
      if (runState === "stopping" || runState === "terminal") return runState;
      return commitRunGrants(
        {
          db: deps.db,
          principalKeyStore: deps.principalKeyStore,
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
    if (reserved === "stopping" || reserved === "terminal")
      return unavailableRunResult(runId, reserved);
    return {
      outcome: "materialized",
      stepGrants: reserved,
    };
  };
}
