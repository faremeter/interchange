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

import { and, asc, eq, sql } from "drizzle-orm";
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
  loadFrozenGrantSnapshot,
  resolveSenderPrincipal,
} from "@intx/db";
import { getLogger } from "@intx/log";
import type {
  GrantStore,
  GrantRule,
  ConditionRegistry,
} from "@intx/types/authz";
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
  evaluateMailAdmission,
  MAIL_ACCEPT_ACTION,
  MAIL_ACCEPT_NAMESPACE,
  mailAcceptRelationToken,
  mailAcceptResource,
  parseMailAcceptResource,
  type MailAcceptCoordinate,
} from "@intx/authz";

import {
  resolveGrantMaterialization,
  type MaterializedGrantRow,
} from "./grant-materialization";

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
const TOOL_GRANT_PREFIX = "tool:";
const EFFECT_GRANT_PREFIX = "effect:";

/**
 * Project the frozen grant-walk snapshot into the run's runtime grant rows --
 * the `tool:<name>` and `effect:<cap>` grants the runtime enforces fail-closed.
 * Every distinct grant string across all steps becomes one creator-origin
 * `grant` row with `action: invoke`. The run's runtime authority is
 * definition-pure for the deployment's stable top-level run, so the snapshot
 * alone determines it.
 *
 * Tool grants carry the effect the tool's static declaration requested (`ask`
 * for approval-gated tools, `allow` otherwise) via each step's `grantEffects`
 * record. A tool in more than one step is emitted once; when two steps disagree
 * on its effect, `ask` wins over `allow` so an approval-gated declaration is
 * never silently downgraded.
 *
 * Effect grants are always `allow` -- the `effect.requires` set names the
 * capability floor an action needs, with no per-effect ask/allow distinction,
 * so they are NOT routed through the `grantEffects` record (which covers tool
 * grants only). An `effect:<cap>` in more than one step is emitted once.
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
    // object; rehydrate it to a `Map` so the lookup below matches the walk's
    // original access pattern.
    const grantEffects = new Map<string, GrantEffect>(
      Object.entries(step.grantEffects),
    );
    for (const grant of step.grants) {
      if (grant.startsWith(TOOL_GRANT_PREFIX)) {
        // Every `tool:` grant the snapshot emits carries a `grantEffects`
        // entry (the tool-mark floor: `ask` for an approval-gated tool,
        // `allow` otherwise). A missing entry means the snapshot's `grants`
        // and `grantEffects` maps have diverged -- a defaulted `allow`
        // here would silently DOWNGRADE an `ask` tool below its floor,
        // defeating the approval gate. Fail loudly instead.
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

// The id-less `mail.accept:<relation>` marker strings the capability walk
// (`collectMailAcceptGrants`) emits for a definition's declared accept-policy.
// There is deliberately no relation-token parser in `@intx/authz`; a marker is
// matched by comparing it against the shared `mailAcceptRelationToken`
// formatter for each known relation. These are computed once so the derivation
// below compares against constants.
const MAIL_ACCEPT_INVOKER_MARKER = mailAcceptRelationToken("invoker");
const MAIL_ACCEPT_SELF_MARKER = mailAcceptRelationToken("self");
const MAIL_ACCEPT_TENANT_MARKER = mailAcceptRelationToken("tenant");
const MAIL_ACCEPT_PARENT_MARKER = mailAcceptRelationToken("parent");
const MAIL_ACCEPT_CHILD_MARKER = mailAcceptRelationToken("child");
const MAIL_ACCEPT_CORRESPONDENT_MARKER =
  mailAcceptRelationToken("correspondent");

const MAIL_ACCEPT_MARKER_PREFIX = `${MAIL_ACCEPT_NAMESPACE}:`;

/**
 * The launch context a run's `mail.accept` accept-grants resolve against. The
 * relational markers name a coordinate that is only concrete at launch: the run
 * itself (`self` -> the deployment's definition), the launching principal
 * (`invoker`), or the run's tenant. `invokerPrincipalId` is null when no invoker
 * principal resolved (e.g. an unresolvable mail sender), in which case no
 * invoker row is emitted.
 */
export type MailAcceptGrantContext = {
  invokerPrincipalId: string | null;
  definitionId: string;
  tenantId: string;
  runPrincipalId: string;
};

/**
 * Resolve one `mail.accept:*` snapshot marker to the concrete
 * `mail.accept:<coord-type>:<id>` resource string it materializes as, or `null`
 * when it must not emit a launch row. Throws on an unrecognized `mail.accept:`
 * marker so a corrupt snapshot fails loudly rather than silently dropping an
 * accept-grant.
 */
function resolveMailAcceptMarker(
  marker: string,
  ctx: MailAcceptGrantContext,
): string | null {
  // An explicit concrete coordinate (`mail.accept:principal:<id>` /
  // `mail.accept:definition:<id>`) is already resolved at deploy time; pass it
  // through unchanged.
  if (parseMailAcceptResource(marker) !== null) {
    return marker;
  }
  switch (marker) {
    case MAIL_ACCEPT_INVOKER_MARKER:
      // No invoker principal resolved (an unresolvable mail sender threads a
      // null invoker). Emit no invoker row rather than invent a coordinate.
      return ctx.invokerPrincipalId === null
        ? null
        : mailAcceptResource("principal", ctx.invokerPrincipalId);
    case MAIL_ACCEPT_SELF_MARKER:
      return mailAcceptResource("definition", ctx.definitionId);
    case MAIL_ACCEPT_TENANT_MARKER:
      return mailAcceptResource("tenant", ctx.tenantId);
    case MAIL_ACCEPT_PARENT_MARKER:
    case MAIL_ACCEPT_CHILD_MARKER:
    case MAIL_ACCEPT_CORRESPONDENT_MARKER:
      // Dynamic relations: the counterparty (a parent, a child, or an
      // established correspondent) is not known from the frozen definition at
      // launch -- it is determined per inbound message (a spawn or a send).
      // Their run-time resolution and enforcement are deferred to their own
      // issues; they are neither materialized here nor evaluated by the gate
      // yet, so declaring one today is inert. Skipped here without error.
      return null;
    default:
      throw new Error(
        `deriveMailAcceptGrantRows: unrecognized mail.accept marker ${JSON.stringify(marker)}; the grant-walk snapshot must emit only known relation tokens or concrete coordinates`,
      );
  }
}

/**
 * Project the frozen grant-walk snapshot's `mail.accept:*` markers into the
 * run's concrete accept-grant rows -- the launch-time half of INTR-510's slim
 * core. This rides the SAME materialization path as
 * `deriveRunRuntimeGrantRows` (which stays pure over `tool:`/`effect:`): a
 * definition's declared accept-relations become concrete
 * `mail.accept:<coord-type>:<id>` grant rows on the run principal so later
 * inbound mail transport can authorize delivery against them.
 *
 * `invoker` resolves to `mail.accept:principal:<invokerPrincipalId>` (skipped
 * when no invoker resolved), `self` to `mail.accept:definition:<definitionId>`,
 * and `tenant` to `mail.accept:tenant:<tenantId>`. Explicit concrete
 * coordinates pass through. `parent`/`child`/`correspondent` are dynamic and
 * not materializable at launch, so they are skipped. Rows are deduplicated by
 * resolved resource string (markers repeat per step, and `self`/`invoker` may
 * collide with an explicit coordinate), mirroring the runtime derivation's
 * per-resource dedup. Every emitted row is a creator-origin `accept`/`allow`
 * grant with no conditions and no expiry.
 */
export function deriveMailAcceptGrantRows(
  snapshot: GrantWalkSnapshot,
  ctx: MailAcceptGrantContext,
  now: Date,
): MaterializedGrantRow[] {
  const resources = new Set<string>();
  for (const step of snapshot.perStep) {
    for (const grant of step.grants) {
      if (!grant.startsWith(MAIL_ACCEPT_MARKER_PREFIX)) continue;
      const resolved = resolveMailAcceptMarker(grant, ctx);
      if (resolved !== null) resources.add(resolved);
    }
  }

  const rows: MaterializedGrantRow[] = [];
  for (const resource of resources) {
    rows.push({
      id: generateId("grant"),
      tenantId: ctx.tenantId,
      principalId: ctx.runPrincipalId,
      resource,
      action: MAIL_ACCEPT_ACTION,
      effect: "allow",
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

export type StageRunGrantsFromSnapshotArgs = {
  /**
   * The deploy-approved grant-walk snapshot frozen at approval. Its per-step
   * grants drive the run's runtime `tool:`/`effect:` rows; its
   * `grantRequirements` are NOT read here -- the caller passes the requirement
   * slice it wants resolved through `grantRequirements` below.
   */
  snapshot: GrantWalkSnapshot;
  tenantId: string;
  runPrincipalId: string;
  now: Date;
  /**
   * The launching principal's id -- the run's invoker. Resolves the snapshot's
   * `mail.accept:invoker` marker to `mail.accept:principal:<id>`. Null when no
   * invoker principal resolved (e.g. an unresolvable mail sender); then no
   * invoker accept-grant row is emitted.
   */
  invokerPrincipalId: string | null;
  /**
   * The deployment's own definition id -- resolves the snapshot's
   * `mail.accept:self` marker to `mail.accept:definition:<definitionId>`.
   */
  definitionId: string;
  /**
   * Declared invoker grants resolved against the launching principal's
   * authority. The external trigger route passes the caller's grants; the mail
   * path passes the authenticated sender's grants (empty when the sender did
   * not resolve to an invoker principal).
   */
  invokerGrants: GrantRule[];
  /** Declared creator grants resolved against the workflow asset's creator. */
  creatorGrants: GrantRule[];
  /**
   * Grant requirements to resolve. Both call sites pass the snapshot's
   * requirements unfiltered, so an invoker-sourced requirement resolves against
   * the passed `invokerGrants` and fails closed when they are insufficient.
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
 * the resolved declared requirements. The snapshot's per-step grants project
 * the run's runtime `tool:`/`effect:` rows; the mail-triggered materializer and
 * the external trigger route both drive this one tail. Returns the staged rows
 * and their wire projection, or a rejection when a declared requirement's
 * authority is insufficient. No database write happens here --
 * `commitRunGrants` performs it once the caller has accepted delivery.
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

  // Ride the same path to project the run's `mail.accept:*` accept-grants from
  // the snapshot's declared relation markers. Kept separate from
  // `deriveRunRuntimeGrantRows`, which stays pure over `tool:`/`effect:`.
  const mailAcceptGrantRows = deriveMailAcceptGrantRows(
    args.snapshot,
    {
      invokerPrincipalId: args.invokerPrincipalId,
      definitionId: args.definitionId,
      tenantId: args.tenantId,
      runPrincipalId: args.runPrincipalId,
    },
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

  const grantRows = [
    ...runtimeGrantRows,
    ...mailAcceptGrantRows,
    ...materialization.grantRows,
  ];
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
  principalKeyStore: PrincipalKeyStore;
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
 * still dispatchable. Serializes a provisioned trigger's commit with concurrent
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
 * The tool name an approval names, read from its `toolDefinition` snapshot. The
 * name lives in untyped jsonb; validate it through the `ToolDefinition` arktype
 * rather than reaching in, so a malformed snapshot fails loudly instead of
 * yielding an unusable name.
 */
export function approvalToolName(
  toolDefinition: Record<string, unknown>,
): string {
  return ToolDefinition.assert(toolDefinition).name;
}

/**
 * Resolve a run's `ask` checkpoint on one tool into a standing effect -- the
 * durable mutation a `scope: "always"` resolution makes. An operator who
 * approves-always sets `allow` (stop asking, let it through); one who
 * rejects-always sets `deny` (stop asking, block it). The grant stays with the
 * run: every later read (the child's enforcement floor, the authorization view,
 * and the per-dispatch re-establish) sees the standing effect, so the tool is
 * not asked again for the life of the run.
 *
 * Guarded to only change a grant currently gated `ask`: the `effect = "ask"`
 * predicate means it only ever resolves the checkpoint, never overrides an
 * existing `allow`/`deny` and never touches a tool the run does not already
 * hold. So a standing resolution can only remove the checkpoint on a capability
 * the deploy already granted-with-a-checkpoint -- in the direction the operator
 * chose. Runs against the passed executor, so the caller mutates inside the
 * resolve transaction and a rolled-back resolve rolls back the grant change with
 * it. A run with no principal (nothing to mutate) is a no-op.
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
  // The hub's condition registry, passed to the admission evaluator so a
  // conditioned operator `mail.accept` deny (collected on the deliver path) is
  // honored rather than skipped. Omitted here means the evaluator throws (fails
  // closed) if it ever meets a conditioned grant; production supplies it.
  registry?: ConditionRegistry;
};

/**
 * A deployment's deploy-approved grant basis: the grant-walk snapshot frozen at
 * approval, from which the run's runtime `tool:`/`effect:` grants and its
 * declared requirements both derive. The snapshot is a pure function of the
 * approved definition content, keyed by the definition id, so it is read once
 * per deployment and cached; nothing here depends on a live re-read or re-walk
 * of the workflow's `workflow.json`.
 */
type FrozenRunGrantBasis = {
  readonly snapshot: GrantWalkSnapshot;
};

// The code a mail-transport admission denial rejects under. Distinct from
// `insufficient_grants` (a declared requirement's authority is missing) and
// `workflow_run_terminal` (the run is settled): this one means the run's
// accept-policy does not admit the authenticated sender.
const MAIL_ADMISSION_DENIED_CODE = "mail_admission_denied";

/**
 * Adapt a staged `MaterializedGrantRow` to the `GrantRule` shape
 * `evaluateMailAdmission` reads. A staged run grant is always a direct
 * principal grant, so `roleId` is null. Only the `mail.accept:*` rows drive
 * the decision; `evaluateMailAdmission` filters to that namespace itself, so
 * the full staged set can be passed through unchanged.
 */
function materializedRowToGrantRule(row: MaterializedGrantRow): GrantRule {
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

/**
 * Build the mail-triggered run-grants materializer the sidecar router's
 * `mail.outbound` handler invokes for each workflow-deployment recipient.
 *
 * A mail-triggered run derives its grants from the RECEIVING deployment's
 * frozen snapshot: the snapshot's `tool:`/`effect:` runtime grants plus the
 * declared requirements resolved against their sources. Creator-sourced
 * requirements resolve against the workflow asset's creator;
 * invoker-sourced requirements resolve against the authenticated mail sender's
 * grants, binding the sender as the run's invoker -- the mail equivalent of the
 * HTTP-trigger route resolving its calling principal as invoker. The sender's
 * `(principalId, tenantId)` are resolved ONCE at the mail seam and threaded in;
 * when the sender did not resolve to a concrete invoker principal both are
 * `null`, no invoker grants are collected, and any invoker-sourced requirement
 * fails closed in `resolveGrantMaterialization`.
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
  senderPrincipalId: string | null;
  senderTenantId: string | null;
  senderCoordinates: MailAcceptCoordinate[] | null;
}) => Promise<MailTriggeredRunGrantsResult> {
  // Closure-level cache of each deployment's deploy-approved snapshot, keyed by
  // the workflow definition's identity. A definition id is content-addressed --
  // keyed by `(assetId, wireHash)`, frozen at approval -- and the anchor run
  // carries that id, so the key names the APPROVED definition content, not the
  // mutable asset blob behind it. The first trigger of a deployment reads the
  // frozen snapshot from the version row once and caches it here; every later
  // trigger consumes the cached snapshot WITHOUT re-reading it. The hub never
  // walks a live definition on this path: a rewritten asset blob under a stable
  // asset id cannot change a run's grants, because runs bind to the frozen
  // snapshot, never a live re-hydrate or re-walk.
  const frozenBasisByDefinition = new Map<string, FrozenRunGrantBasis>();

  return async ({
    agentAddress,
    runId,
    senderPrincipalId,
    senderTenantId,
    senderCoordinates,
  }) => {
    const topLevelRun = alias(workflowRun, "mail_triggered_top_level_run");
    const [anchor] = await deps.db
      .select({
        anchorRunId: workflowRun.id,
        tenantId: workflowRun.tenantId,
        definitionId: workflowRun.definitionId,
        definitionAssetId: workflowDefinition.assetId,
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
      // Deliver-to-existing admission. Gate the sender against the run's
      // COLLECTED grants (not `loadCommittedRunGrants`, which reads only the run
      // principal's direct rows): `collectGrants` also unions any role-owned
      // grants, so an inherited role-/tenant-scoped operator `deny mail.accept:…`
      // is honored by the helper's deny-wins. A `null` sender coordinate set (an
      // unresolvable/ambiguous sender) is never admitted. Same-tenant scoping is
      // inherent in the coordinate ids: a `principal`/`definition` coordinate
      // carries a globally-unique id, and the `tenant` coordinate carries the
      // sender's own tenant, so a cross-tenant sender only matches a rule that
      // explicitly names its ids.
      const recipientGrants = await deps.grantStore.collectGrants(
        committed.runPrincipalId,
        tenantId,
      );
      const admission = await evaluateMailAdmission({
        senderCoordinates,
        recipientGrants,
        ...(deps.registry ? { registry: deps.registry } : {}),
      });
      if (!admission.admit) {
        return {
          outcome: "rejected",
          status: 403,
          code: MAIL_ADMISSION_DENIED_CODE,
          message: `Workflow run ${runId} does not admit mail from this sender`,
        };
      }
      return {
        outcome: "materialized",
        stepGrants: committed.stepGrants,
      };
    }

    let basis = frozenBasisByDefinition.get(definitionId);
    if (basis === undefined) {
      // First trigger of this deployment: read the frozen snapshot from the
      // version row once, then cache it. The read never runs again for this
      // definition id, and no live definition is ever walked here.
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

    // The definition's FULL declared requirements are resolved -- both creator-
    // and invoker-sourced -- so an invoker requirement materializes against the
    // sender's grants rather than being stripped. Passing the unfiltered list
    // keeps `resolveGrantMaterialization`'s reject-on-insufficient-invoker
    // contract intact, so an invoker requirement the sender cannot satisfy fails
    // the run closed instead of launching under-authorized.
    const declaredGrantRequirements = basis.snapshot.grantRequirements;

    // Invoker authority is the authenticated mail sender's grants, binding the
    // sender as the run's invoker. Resolved live per run against the sender's
    // `(principalId, tenantId)` threaded from the seam. A null sender (an
    // unresolvable/ambiguous sender, or none resolved) collects nothing, so an
    // invoker-sourced requirement fails closed above.
    const invokerGrants =
      senderPrincipalId !== null && senderTenantId !== null
        ? await deps.grantStore.collectGrants(senderPrincipalId, senderTenantId)
        : [];

    // Creator authority is resolved LIVE per run: the definition's grant SHAPE
    // is frozen in the snapshot, but which grants the creator currently holds
    // is not part of that shape and can change between triggers. This reads the
    // asset row's creator column and the creator's grants -- not the snapshot
    // -- so it is not the read the frozen basis eliminates.
    const creatorPrincipalId = await loadAssetCreatorPrincipalId(
      deps.db,
      tenantId,
      definitionAssetId,
    );
    const creatorGrants = await collectCreatorGrants(
      deps.grantStore,
      tenantId,
      creatorPrincipalId,
      declaredGrantRequirements,
    );

    // Derive the run principal id from `(tenantId, runId)`. The runId is the
    // stable deployment address, so all trigger occurrences resolve the same
    // principal and canonical grant snapshot.
    const runPrincipalId = await deriveRunPrincipalId(tenantId, runId);
    const now = new Date();
    const staged = await stageRunGrantsFromSnapshot({
      snapshot: basis.snapshot,
      tenantId: tenantId,
      runPrincipalId,
      now,
      // The authenticated mail sender is the run's invoker; the receiving
      // deployment's definition is `self`. Both resolve the run's declared
      // `mail.accept` relation markers to concrete accept-grant rows.
      invokerPrincipalId: senderPrincipalId,
      definitionId,
      invokerGrants,
      creatorGrants,
      grantRequirements: declaredGrantRequirements,
    });
    if (!staged.ok) {
      return {
        outcome: "rejected",
        status: staged.rejection.status,
        code: staged.rejection.code,
        message: staged.rejection.message,
      };
    }

    // Start (first-fire) admission. Gate the sender against the run's STAGED
    // `mail.accept` rows BEFORE committing: an un-admitted sender must not
    // create or fire the run. The staged rows are the definition's declared
    // accept-allows resolved onto this run (`invoker`->the sender's principal,
    // `self`->the definition, `tenant`->the run tenant, plus explicit
    // coordinates); `evaluateMailAdmission` filters them to the `mail.accept`
    // namespace, applies deny-wins, and default-denies when none match. A `null`
    // sender coordinate set is never admitted. (An operator deny applied BEFORE
    // the run principal exists cannot bind here -- first-fire admission is
    // governed by the definition's declared accept-allows; a first-fire operator
    // deny mechanism is a follow-up.)
    // First-fire admission evaluates against the caller's own staged rows
    // before the commit transaction. Two senders racing the first fire of the
    // same run each stage a row naming their own principal as `invoker` and so
    // each admit themselves; one wins the run-principal commit, but the loser's
    // single first mail still delivers even though the committed run binds the
    // winner. The blast radius is bounded -- only the self-referential `invoker`
    // relation, one mail, no authority gained, and every later delivery from the
    // loser hits the deliver-to-existing path gated on the committed grants.
    // Making first-fire admission and the run-principal commit mutually
    // exclusive, so a losing concurrent sender re-enters via deliver-to-existing,
    // is a follow-up.
    const admission = await evaluateMailAdmission({
      senderCoordinates,
      recipientGrants: staged.grantRows.map(materializedRowToGrantRule),
      ...(deps.registry ? { registry: deps.registry } : {}),
    });
    if (!admission.admit) {
      return {
        outcome: "rejected",
        status: 403,
        code: MAIL_ADMISSION_DENIED_CODE,
        message: `Workflow run ${runId} does not admit mail from this sender`,
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

const correspondentLogger = getLogger(["hub-api", "correspondent-grant"]);

export type CorrespondentGrantMinterDeps = {
  db: DB["db"];
  principalKeyStore: PrincipalKeyStore;
};

/**
 * Mint the accept-grant that admits a reply from a party a run mails -- the
 * dynamic `correspondent` relation (design-of-record v4 §D/R3/§H). Launch
 * materialization cannot enumerate future correspondents, so the grant is
 * committed at the send instant instead: when a live run mails a party, a
 * `mail.accept:principal:<recipient>` allow row is added to the SENDING run's
 * principal, so the reply (recipient -> sender) is admitted by the ordinary
 * deliver-to-existing gate with no new admission path. The recipient is
 * resolved through the SAME `resolveSenderPrincipal` the reply resolves
 * through, so the minted principal id is byte-identical to the coordinate the
 * reply presents.
 *
 * Fail-closed and bounded:
 * - The sending definition must carry the approved `mail.accept:correspondent`
 *   marker in its frozen snapshot; absent it, nothing is minted.
 * - A terminal or absent sending run mints nothing (§H terminal-inertness); the
 *   row is only reachable while the run is live, and a re-execution mints a new
 *   principal that cannot inherit it.
 * - A principal-less sending run or an unresolvable/external recipient mints
 *   nothing.
 * - The insert is deduplicated under a `FOR UPDATE` lock on the sending run, so
 *   a repeat mail to the same recipient does not accrue a second row.
 *
 * Best-effort: minting is a side effect of sending and MUST NOT break delivery,
 * so any fault degrades to no grant and is logged, mirroring the send path's
 * frame-key resolver.
 */
export function createCorrespondentGrantMinter(
  deps: CorrespondentGrantMinterDeps,
): (args: {
  senderAddress: string;
  recipientAddress: string;
}) => Promise<void> {
  return async (args) => {
    const senderAddress = args.senderAddress.toLowerCase();
    try {
      await deps.db.transaction(async (tx) => {
        // Lock the SENDING run row. A terminal/absent run mints nothing, and
        // the lock serializes concurrent sends from the same run so the dedup
        // below cannot be raced.
        const [sender] = await tx
          .select({
            status: workflowRun.status,
            principalId: workflowRun.principalId,
            definitionId: workflowRun.definitionId,
            tenantId: workflowRun.tenantId,
          })
          .from(workflowRun)
          .where(eq(sql`lower(${workflowRun.address})`, senderAddress))
          .limit(1)
          .for("update");
        if (sender === undefined) return;
        if (!isLiveWorkflowRunStatus(sender.status)) return;
        if (sender.principalId === null) return;

        // Gate: the sending definition must have been approved for the
        // `correspondent` relation (the marker in its frozen snapshot).
        const snapshot = await loadFrozenGrantSnapshot(tx, sender.definitionId);
        if (snapshot === null) return;
        const authorized = snapshot.perStep.some((step) =>
          step.grants.includes(MAIL_ACCEPT_CORRESPONDENT_MARKER),
        );
        if (!authorized) return;

        const recipient = await resolveSenderPrincipal(
          tx,
          deps.principalKeyStore,
          args.recipientAddress,
        );
        if (recipient === null) return;

        const resource = mailAcceptResource("principal", recipient.principalId);
        // No unique constraint exists on `(principalId, resource, action)` (a
        // principal may hold both an allow and a deny), so dedup with a guarded
        // insert under the run lock rather than an ON CONFLICT.
        const [existing] = await tx
          .select({ id: grantTable.id })
          .from(grantTable)
          .where(
            and(
              eq(grantTable.principalId, sender.principalId),
              eq(grantTable.resource, resource),
              eq(grantTable.action, MAIL_ACCEPT_ACTION),
            ),
          )
          .limit(1);
        if (existing !== undefined) return;

        const now = new Date();
        await tx.insert(grantTable).values({
          id: generateId("grant"),
          tenantId: sender.tenantId,
          principalId: sender.principalId,
          resource,
          action: MAIL_ACCEPT_ACTION,
          effect: "allow",
          conditions: null,
          // A correspondent grant materializes the definition's creator-declared
          // `correspondent` policy, so it shares the `creator` provenance of the
          // launch-time `mail.accept` rows.
          origin: "creator",
          expiresAt: null,
          createdAt: now,
          updatedAt: now,
        });
      });
    } catch (cause) {
      correspondentLogger.error`Failed to mint a correspondent accept-grant (sender ${args.senderAddress} -> recipient ${args.recipientAddress}): ${cause instanceof Error ? cause.message : String(cause)}`;
    }
  };
}
