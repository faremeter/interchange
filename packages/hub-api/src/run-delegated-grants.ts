import { and, asc, eq, inArray, or, sql } from "drizzle-orm";

import {
  approval,
  asset,
  grant as grantTable,
  isLiveWorkflowRunStatus,
  liveWorkflowRunStatuses,
  principal as principalTable,
  principalRole,
  workflowDefinition,
  workflowRun,
  workflowRunLaunchSpec,
} from "@intx/db/schema";
import {
  loadDeploymentGrantSnapshot,
  parseApprovalRow,
  parseGrantRow,
  type DB,
  type DBExecutor,
  type GrantStoreFactory,
} from "@intx/db";
import type { GrantOrigin } from "@intx/types";
import { isMailAcceptNamespaceResource } from "@intx/types/mail-accept";
import { canonicalJsonStringify } from "@intx/types/wire-definition-hash";
import type { GrantRule } from "@intx/types/authz";
import { runtimeGrantRules } from "@intx/workflow-deploy/capabilities";
import { pushRunGrants, type SidecarRouter } from "@intx/hub-sessions";

import {
  INVOKER_GRANT_TTL_MS,
  resolveRequirementGrant,
} from "./grant-materialization";
import {
  approvalToolName,
  bindSelfRunRequirements,
  collectCreatorGrants,
  loadActiveDeployerId,
  loadAssetCreatorPrincipalId,
  TOOL_GRANT_PREFIX,
} from "./run-grant-materialization";

/**
 * Serialize re-derivations of run grants in one tenant for the rest of the
 * transaction.
 *
 * A caller that changes a delegator's grants and then re-derives must take
 * this lock before its own write. Otherwise it holds the written rows while
 * it waits here, and a re-derivation that holds this lock can wait on those
 * rows. The lock is transaction-scoped, so a second call in the same
 * transaction does not block.
 */
export async function lockTenantRunDelegatedGrants(
  tx: DBExecutor,
  tenantId: string,
): Promise<void> {
  await tx.execute(
    sql`select pg_advisory_xact_lock(hashtextextended(${`run-delegated-grants:${tenantId}`}, 0))`,
  );
}

/**
 * The tenant's live deployment anchors whose definition asset was created
 * by, or whose launch spec records as deployer, one of `principalIds`. A
 * deployment not yet triggered is live, so a first trigger that commits
 * while the caller waits on its lock is visible to the caller afterwards.
 * An ended deployment never commits rows again, so it is not returned. No
 * filter on run principal: a deployment not yet triggered has none.
 */
export async function findDelegatedAnchorRunIds(
  tx: DBExecutor,
  tenantId: string,
  principalIds: readonly string[],
): Promise<string[]> {
  if (principalIds.length === 0) return [];
  const rows = await tx
    .select({ id: workflowRun.id })
    .from(workflowRun)
    .innerJoin(
      workflowDefinition,
      eq(workflowDefinition.id, workflowRun.definitionId),
    )
    .leftJoin(asset, eq(asset.id, workflowDefinition.assetId))
    .leftJoin(
      workflowRunLaunchSpec,
      eq(workflowRunLaunchSpec.anchorRunId, workflowRun.id),
    )
    .where(
      and(
        eq(workflowRun.tenantId, tenantId),
        eq(workflowRun.id, workflowRun.anchorRunId),
        inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
        or(
          inArray(asset.creatorPrincipalId, [...principalIds]),
          inArray(workflowRunLaunchSpec.sourceAuthorityPrincipalId, [
            ...principalIds,
          ]),
        ),
      ),
    );
  return rows.map((row) => row.id);
}

export type RederiveRunDelegatedGrantsArgs = {
  grantStoreFor: GrantStoreFactory;
  tenantId: string;
  // Principals whose grants, role grants, status or existence changed.
  principalIds: readonly string[];
  // Anchors to re-derive whatever their delegators are now, for a change
  // that removes the link `findDelegatedAnchorRunIds` follows (a deleted
  // asset creator).
  anchorRunIds: readonly string[];
  now: Date;
};

export type RederivedRun = { anchorRunId: string; address: string | null };

/**
 * Re-derive the requirement rows of every live run that delegates from one
 * of `principalIds` or is named in `anchorRunIds`, so each run holds what its
 * delegators hold now. Returns the runs whose rows changed; the caller sends
 * those runs their grants after its transaction commits.
 *
 * Only the rows a definition's grant requirements produce depend on a
 * delegator. For each requirement:
 *  - the delegator allows it and the run has no row for it: the row is
 *    inserted. An invoker row keeps its first-trigger expiry, and a `tool:`
 *    `ask` row gets back a standing `always` decision the run's approver made;
 *  - the delegator allows it and the run has the row: the row stays as it
 *    is, with its id and any standing decision;
 *  - the delegator does not allow it: the run's `allow` rows for it are
 *    deleted, including an `ask` that a standing approval changed to `allow`.
 *    Its `deny` and `ask` rows stay as floors.
 * A run whose rows change is itself a delegator of the deployments it
 * deployed and of any whose asset it created, so they are re-derived too. A
 * delegator exists before what it delegates to, so the rounds end.
 *
 * Takes the tenant lock, then locks each candidate anchor row, in id order
 * within a round. Only this function locks more than one deployment's
 * anchor in one transaction, and the tenant lock serializes its callers, so
 * a later round locking a lower id cannot form a cycle. Needs READ COMMITTED:
 * reads after a lock wait must see what the lock holder committed.
 */
export async function rederiveRunDelegatedGrants(
  tx: DBExecutor,
  args: RederiveRunDelegatedGrantsArgs,
): Promise<RederivedRun[]> {
  await lockTenantRunDelegatedGrants(tx, args.tenantId);
  const grantStore = args.grantStoreFor(tx);
  const changed = new Map<string, RederivedRun>();

  let principalIds = args.principalIds;
  let seedAnchorRunIds = args.anchorRunIds;
  while (principalIds.length > 0 || seedAnchorRunIds.length > 0) {
    const anchorRunIds = [
      ...new Set([
        ...seedAnchorRunIds,
        ...(await findDelegatedAnchorRunIds(tx, args.tenantId, principalIds)),
      ]),
    ].sort();
    if (anchorRunIds.length === 0) break;
    // `no key update` serializes with the first trigger's `for update`, and
    // with an approval resolve on a provisioned deployment, without blocking
    // the key-share lock a child run row's foreign key takes on its anchor.
    // An approval resolve on a deployment with no allocation does not lock
    // the anchor: a standing decision that commits while this inserts a
    // regained `ask` row leaves that row `ask`, so the run asks again.
    await tx
      .select({ id: workflowRun.id })
      .from(workflowRun)
      .where(inArray(workflowRun.id, anchorRunIds))
      .orderBy(asc(workflowRun.id))
      .for("no key update");

    const changedPrincipalIds: string[] = [];
    for (const anchorRunId of anchorRunIds) {
      const result = await rederiveRun(tx, grantStore, {
        tenantId: args.tenantId,
        anchorRunId,
        now: args.now,
      });
      if (result === null) continue;
      changed.set(anchorRunId, result.run);
      changedPrincipalIds.push(result.runPrincipalId);
    }
    principalIds = changedPrincipalIds;
    seedAnchorRunIds = [];
  }

  return [...changed.values()];
}

export type DelegatorGrantChange<T> = {
  value: T;
  // Principals whose grants, role grants, status or existence the change
  // touched. Empty when the change touched nothing (a 404).
  principalIds: readonly string[];
  // Anchors whose delegator link the change removed (a deleted asset
  // creator).
  anchorRunIds?: readonly string[];
};

export type ChangeDelegatorGrantsDeps = {
  db: DB["db"];
  grantStoreFor: GrantStoreFactory;
  sidecarRouter: Pick<SidecarRouter, "sendRunGrants">;
};

/**
 * Apply a change to delegators' grants and re-derive the running runs it
 * affects, in one transaction, then send each affected live run its grants.
 *
 * The tenant lock is taken before `change` writes, so a re-derivation never
 * waits on rows this transaction wrote while it holds the lock. The runs
 * sent their grants are the runs whose rows the re-derivation changed and
 * the live runs whose own principal is in `principalIds` (an operator change
 * to a run's own rows). The send happens after the commit: the router loads
 * the committed rows itself. A failed send is logged; the next signal,
 * trigger or reconnect sends the grants again.
 *
 * An error thrown by `change` rolls the transaction back and propagates. A
 * caller that maps a database error to a response does so around this call,
 * because the error aborts the transaction.
 */
export async function changeDelegatorGrants<T>(
  deps: ChangeDelegatorGrantsDeps,
  tenantId: string,
  change: (tx: DBExecutor) => Promise<DelegatorGrantChange<T>>,
): Promise<T> {
  const { value, addresses } = await deps.db.transaction(async (tx) => {
    await lockTenantRunDelegatedGrants(tx, tenantId);
    const changed = await change(tx);
    const rederived = await rederiveRunDelegatedGrants(tx, {
      grantStoreFor: deps.grantStoreFor,
      tenantId,
      principalIds: changed.principalIds,
      anchorRunIds: changed.anchorRunIds ?? [],
      now: new Date(),
    });
    const ownRuns =
      changed.principalIds.length === 0
        ? []
        : await tx
            .select({ address: workflowRun.address })
            .from(workflowRun)
            .where(
              and(
                eq(workflowRun.tenantId, tenantId),
                inArray(workflowRun.principalId, [...changed.principalIds]),
                inArray(workflowRun.status, [...liveWorkflowRunStatuses]),
              ),
            );
    const addresses = new Set<string>();
    for (const run of [...rederived, ...ownRuns]) {
      if (run.address !== null) addresses.add(run.address);
    }
    return { value: changed.value, addresses: [...addresses] };
  });
  if (addresses.length > 0) {
    void pushRunGrants(deps.sidecarRouter, addresses);
  }
  return value;
}

/**
 * The tenant's principals that hold `roleId`, so a change to the role's
 * grants re-derives the runs that delegate from them.
 */
export async function findRolePrincipalIds(
  tx: DBExecutor,
  tenantId: string,
  roleId: string,
): Promise<string[]> {
  const rows = await tx
    .select({ id: principalRole.principalId })
    .from(principalRole)
    .innerJoin(principalTable, eq(principalTable.id, principalRole.principalId))
    .where(
      and(
        eq(principalRole.roleId, roleId),
        eq(principalTable.tenantId, tenantId),
      ),
    );
  return rows.map((row) => row.id);
}

// A requirement row's identity on the run. A key has one origin, resource
// and action, so every requirement with that key gets the same delegation
// decision.
function requirementKey(
  origin: GrantOrigin,
  resource: string,
  action: string,
  conditions: Record<string, unknown> | null,
): string {
  return JSON.stringify([
    origin,
    resource,
    action,
    conditions === null ? null : canonicalJsonStringify(conditions),
  ]);
}

async function rederiveRun(
  tx: DBExecutor,
  grantStore: ReturnType<GrantStoreFactory>,
  args: { tenantId: string; anchorRunId: string; now: Date },
): Promise<{ run: RederivedRun; runPrincipalId: string } | null> {
  const { tenantId, anchorRunId, now } = args;
  // A cancel-requested or past-deadline run is still live here: its process
  // can still run, so it must lose a revoked grant too.
  const [run] = await tx
    .select({
      status: workflowRun.status,
      address: workflowRun.address,
      principalId: workflowRun.principalId,
      principalCreatedAt: principalTable.createdAt,
      assetId: workflowDefinition.assetId,
    })
    .from(workflowRun)
    .innerJoin(
      workflowDefinition,
      eq(workflowDefinition.id, workflowRun.definitionId),
    )
    .innerJoin(principalTable, eq(principalTable.id, workflowRun.principalId))
    .where(
      and(eq(workflowRun.id, anchorRunId), eq(workflowRun.tenantId, tenantId)),
    )
    .limit(1);
  if (
    run === undefined ||
    run.principalId === null ||
    !isLiveWorkflowRunStatus(run.status)
  ) {
    return null;
  }
  const runPrincipalId = run.principalId;

  const snapshot = await loadDeploymentGrantSnapshot(tx, anchorRunId);
  if (snapshot === null) {
    throw new Error(
      `deployment ${anchorRunId} has committed run grants but no approved grant snapshot`,
    );
  }
  // The deployment's top-level run id is its anchor id.
  const requirements = bindSelfRunRequirements(
    snapshot.grantRequirements,
    anchorRunId,
  );
  if (requirements.length === 0) return null;

  const creatorPrincipalId =
    run.assetId === null
      ? null
      : await loadAssetCreatorPrincipalId(tx, tenantId, run.assetId);
  const creatorGrants = await collectCreatorGrants(
    grantStore,
    tenantId,
    creatorPrincipalId,
    requirements,
  );
  // With no active deployer, invoker requirements lapse.
  const deployerId = requirements.some((r) => r.source === "invoker")
    ? await loadActiveDeployerId(tx, tenantId, anchorRunId)
    : null;
  const invokerGrants: GrantRule[] =
    deployerId === null
      ? []
      : await grantStore.collectGrants(deployerId, tenantId);
  const ctx = {
    tenantId,
    targetPrincipalId: runPrincipalId,
    creatorGrants,
    invokerGrants,
    now,
    // The first trigger stamps the run principal and the invoker expiry from
    // one `now`, so a re-added invoker row never outlives the first one.
    invokerExpiresAt: new Date(
      run.principalCreatedAt.getTime() + INVOKER_GRANT_TTL_MS,
    ),
  };

  // The deploy approval grants every runtime `tool:`/`effect:` row whatever
  // the delegator holds. A creator requirement with the same key is not
  // what gives the run that capability, so it is left alone.
  const runtimeKeys = new Set(
    runtimeGrantRules(snapshot).map((rule) =>
      requirementKey("creator", rule.resource, rule.action, null),
    ),
  );
  const rows = (
    await tx
      .select()
      .from(grantTable)
      .where(eq(grantTable.principalId, runPrincipalId))
  ).map(parseGrantRow);
  const standingEffects = await loadStandingToolEffects(tx, anchorRunId);

  let rowsChanged = false;
  const seen = new Set<string>();
  for (const req of requirements) {
    const key = requirementKey(
      req.source,
      req.resource,
      req.action,
      req.conditions ?? null,
    );
    // Two requirements that differ only in effect share a key. The first one
    // decides whether a row exists: after a lapse and a regain, a remaining
    // `deny` row stands for the key and the `allow` is not added again.
    // A `mail.accept` row comes from the snapshot's accept markers or from a
    // correspondent the run mailed, never from a delegator. The deploy gate
    // refuses a requirement in that namespace; this keeps a snapshot that
    // carries one from deleting those rows.
    if (
      seen.has(key) ||
      runtimeKeys.has(key) ||
      isMailAcceptNamespaceResource(req.resource)
    ) {
      continue;
    }
    seen.add(key);
    const existing = rows.filter(
      (row) =>
        requirementKey(row.origin, row.resource, row.action, row.conditions) ===
        key,
    );

    const resolved = await resolveRequirementGrant(req, ctx);
    if (resolved.ok) {
      if (existing.length > 0) continue;
      const row = resolved.row;
      if (row.expiresAt !== null && row.expiresAt <= now) continue;
      const standing =
        row.effect === "ask" && row.resource.startsWith(TOOL_GRANT_PREFIX)
          ? standingEffects.get(row.resource.slice(TOOL_GRANT_PREFIX.length))
          : undefined;
      await tx
        .insert(grantTable)
        .values({ ...row, effect: standing ?? row.effect });
      rowsChanged = true;
      continue;
    }

    const lapsed = existing
      .filter((row) => row.effect === "allow")
      .map((row) => row.id);
    if (lapsed.length === 0) continue;
    await tx.delete(grantTable).where(inArray(grantTable.id, lapsed));
    rowsChanged = true;
  }

  if (!rowsChanged) return null;
  return { run: { anchorRunId, address: run.address }, runPrincipalId };
}

// The standing `always` decisions the run's approver made, by tool name. The
// latest resolution wins.
async function loadStandingToolEffects(
  tx: DBExecutor,
  runId: string,
): Promise<Map<string, "allow" | "deny">> {
  const decisions = (
    await tx
      .select()
      .from(approval)
      .where(
        and(
          eq(approval.runId, runId),
          eq(approval.scope, "always"),
          inArray(approval.status, ["approved", "rejected"]),
        ),
      )
      .orderBy(asc(approval.resolvedAt))
  ).map(parseApprovalRow);
  const effects = new Map<string, "allow" | "deny">();
  for (const decision of decisions) {
    effects.set(
      approvalToolName(decision.toolDefinition),
      decision.status === "approved" ? "allow" : "deny",
    );
  }
  return effects;
}
