// Shared run-grant materialization: the sequence a workflow run's
// authorization is derived and committed through, used by BOTH the
// external trigger route and the hub's mail-triggered run path so the two
// cannot drift.
//
// A run's grant set is the definition-pure runtime grants (the capability
// walk's `tool:`/`effect:` rows), the resolved declared grant requirements
// (creator- and invoker-sourced), and one system-origin `credential:{id}` /
// `use` row per distinct credential and consumer on the anchor's resolved
// bindings. Those binding rows are ownership, not delegation: deploy already
// resolved the id and the consumer. A first commit waits until that
// resolution is recorded. This
// module stages those rows and commits them idempotently on the run id,
// granting them to the run principal and linking it to the run row in one
// transaction.
//
// Delivery (`run.grants` frame, trigger mail / inbound mail forwarding) is
// NOT owned here: the two call sites order those differently for their
// transport, so each orchestrates delivery itself around the shared
// staging and commit below.

import { and, asc, eq, gt, isNull, or, sql } from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";

import {
  asset,
  grant as grantTable,
  isLiveWorkflowRunStatus,
  type LegacyMailAcceptRule,
  principal as principalTable,
  workflowRunLaunchSpec,
  sidecarAllocation,
  workflowDefinition,
  workflowRun,
  hubConsumersOf,
  type WorkflowRunCredentialRefs,
} from "@intx/db/schema";
import type {
  DB,
  DBExecutor,
  GrantStoreFactory,
  RunTokenStore,
} from "@intx/db";
import {
  createWorkflowRunStore,
  insertRunPrincipal,
  loadRunPrincipalActiveKey,
  workflowRunExecutability,
  loadDeploymentGrantSnapshot,
  resolveLocalMailParty,
} from "@intx/db";
import { getLogger } from "@intx/log";
import type {
  GrantStore,
  GrantRule,
  ConditionRegistry,
} from "@intx/types/authz";
import { hubCredentialId } from "@intx/types/credential-delivery";
import {
  isSidecarAllocationDispatchable,
  SELF_RUN_RESOURCE,
  type GrantRequirement,
  type GrantWalkSnapshot,
} from "@intx/types";
import { isMailAcceptNamespaceResource } from "@intx/types/mail-accept";
import { MAIL_SEND_NAMESPACE } from "@intx/types/mail-send";
import { reservedGrantNamespace } from "@intx/types/reserved-namespaces";
import { RunGrantsFrame } from "@intx/types/sidecar";
import { ToolDefinition } from "@intx/types/runtime";
import {
  MAIL_ADMISSION_DENIED_CODE,
  type MailTriggeredRunGrantsResult,
} from "@intx/hub-sessions";
import {
  credentialUseGrantRules,
  runtimeGrantRules,
} from "@intx/workflow-deploy/capabilities";
import { deriveRunPrincipalId } from "@intx/hub-common";
import {
  evaluateMailAdmission,
  MAIL_ACCEPT_ACTION,
  MAIL_SEND_ACTION,
  mailAcceptRelationToken,
  mailAcceptResource,
  parseMailAcceptResource,
  type MailAcceptCoordinate,
} from "@intx/authz";

import {
  INVOKER_GRANT_TTL_MS,
  makeGrantRow,
  resolveGrantMaterialization,
  type GrantMaterializationRejection,
  type MaterializedGrantRow,
} from "./grant-materialization";

const log = getLogger(["hub-api", "run-grant-materialization"]);

export const TOOL_GRANT_PREFIX = "tool:";

/**
 * Project the frozen grant-walk snapshot into the run's runtime grant rows:
 * the rules `runtimeGrantRules` derives, stored as the run principal's
 * creator-origin grants.
 */
export function deriveRunRuntimeGrantRows(
  snapshot: GrantWalkSnapshot,
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow[] {
  return runtimeGrantRules(snapshot).map((rule) =>
    makeGrantRow({ tenantId, principalId: runPrincipalId, ...rule, now }),
  );
}

/**
 * One system-origin `credential:{id}` / `use` row per distinct `(credentialId,
 * consumer)` on the anchor's resolved credential bindings. The consumer string
 * is copied onto `{ tool }` as deploy stored it. Inference credentials are not
 * in this list: they have no binding and no consumer.
 */
export function deriveCredentialUseGrantRows(args: {
  bindings: readonly { credentialId: string; consumer: string }[];
  tenantId: string;
  runPrincipalId: string;
  now: Date;
}): MaterializedGrantRow[] {
  return credentialUseGrantRules(args.bindings).map((rule) =>
    makeGrantRow({
      tenantId: args.tenantId,
      principalId: args.runPrincipalId,
      ...rule,
      now: args.now,
    }),
  );
}

/**
 * The `(credentialId, consumer)` pairs a run's credential-use grants cover:
 * the anchor's resolved credential bindings, and one hub credential for each
 * consumer that declares the reserved hub handle.
 */
export function credentialUseBindings(
  refs: WorkflowRunCredentialRefs | null | undefined,
): { credentialId: string; consumer: string }[] {
  return [
    ...(refs?.bindings ?? []),
    ...hubConsumersOf(refs).map((consumer) => ({
      credentialId: hubCredentialId(consumer),
      consumer,
    })),
  ];
}

/**
 * Credential resolution is unrecorded while initialization has not published
 * it. That is a deployment that has neither finished initializing (its run
 * principal holds no key) nor recorded credential refs, or one whose
 * allocation holds an initialization lease. The lease means a deploy has
 * retired the key and the refs still on the row are the previous attempt's.
 * A finished initialization with null refs and no lease means there was no
 * delivery.
 */
export function credentialResolutionUnrecorded(resolution: {
  credentialRefs: WorkflowRunCredentialRefs | null;
  initialized: boolean;
  initializationLeaseId: string | null;
}): boolean {
  if (resolution.initializationLeaseId !== null) return true;
  return resolution.credentialRefs === null && !resolution.initialized;
}

/**
 * Lock the anchor and read the credential resolution a first grant commit
 * must stamp from. The pre-transaction read races initialization, so the
 * commit stages from this row.
 */
export async function lockAnchorCredentialResolution(
  tx: DBExecutor,
  anchorRunId: string,
): Promise<{
  credentialRefs: WorkflowRunCredentialRefs | null;
  initialized: boolean;
  initializationLeaseId: string | null;
} | null> {
  // Lock the anchor only. The allocation lease is read afterwards and is not
  // locked: beginInitialization takes the allocation lock before the anchor,
  // so locking it here would deadlock. The lease and the run principal's key
  // are written in a transaction that holds the anchor lock, which cannot
  // commit while this one holds the anchor.
  const [run] = await tx
    .select({
      tenantId: workflowRun.tenantId,
      credentialRefs: workflowRun.credentialRefs,
    })
    .from(workflowRun)
    .where(
      and(
        eq(workflowRun.id, anchorRunId),
        eq(workflowRun.anchorRunId, anchorRunId),
      ),
    )
    .limit(1)
    .for("update");
  if (run === undefined) return null;
  const [allocation] = await tx
    .select({
      initializationLeaseId: sidecarAllocation.initializationLeaseId,
    })
    .from(sidecarAllocation)
    .where(eq(sidecarAllocation.anchorRunId, anchorRunId))
    .limit(1);
  return {
    credentialRefs: run.credentialRefs,
    initialized:
      (await loadRunPrincipalActiveKey(tx, run.tenantId, anchorRunId)) !== null,
    initializationLeaseId:
      allocation === undefined ? null : allocation.initializationLeaseId,
  };
}

/**
 * Replace any pre-staged credential-use rows with the rows implied by the
 * locked anchor. Returns `not-ready` when resolution is still unrecorded,
 * so the caller inserts no grant rows.
 */
export function grantRowsWithLockedCredentialResolution(
  rows: readonly MaterializedGrantRow[],
  resolution: {
    credentialRefs: WorkflowRunCredentialRefs | null;
    initialized: boolean;
    initializationLeaseId: string | null;
  },
  args: {
    tenantId: string;
    runPrincipalId: string;
    now: Date;
  },
): MaterializedGrantRow[] | "not-ready" {
  if (credentialResolutionUnrecorded(resolution)) {
    return "not-ready";
  }
  const base = rows.filter(
    (row) =>
      !(
        row.origin === "system" &&
        row.action === "use" &&
        row.resource.startsWith("credential:")
      ),
  );
  return [
    ...base,
    ...deriveCredentialUseGrantRows({
      bindings: credentialUseBindings(resolution.credentialRefs),
      tenantId: args.tenantId,
      runPrincipalId: args.runPrincipalId,
      now: args.now,
    }),
  ];
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
const MAIL_ACCEPT_CORRESPONDENT_MARKER =
  mailAcceptRelationToken("correspondent");

const MAIL_ADDRESS_GRANT_PREFIX = "mail.address:";

/**
 * The relation markers a deployment's legacy mail accept rule stands for. A
 * deployment that predates the sender gate holds the rule on its anchor run
 * (see `workflowRun.legacyMailAccept`); its markers add to the ones in the
 * shared definition snapshot.
 */
export function legacyMailAcceptMarkers(
  rule: LegacyMailAcceptRule | null,
): readonly string[] {
  switch (rule) {
    case null:
      return [];
    case "invoker-correspondent":
      return [MAIL_ACCEPT_INVOKER_MARKER, MAIL_ACCEPT_CORRESPONDENT_MARKER];
    case "correspondent":
      return [MAIL_ACCEPT_CORRESPONDENT_MARKER];
    default: {
      const _exhaustive: never = rule;
      throw new Error(
        `unhandled legacy mail accept rule: ${String(_exhaustive)}`,
      );
    }
  }
}

/**
 * The launch context a run's `mail.accept` accept-grants resolve against. The
 * relational markers name a coordinate that is only concrete at launch: the run
 * itself (`self` -> the deployment's workflow asset), its invoker of record,
 * the deployer (`invoker`), or the run's tenant. `invokerPrincipalId` is null
 * when the deployment has no active deployer, in which case no invoker row is
 * emitted. `legacyMailAccept` is the deployment's legacy rule,
 * null for a deployment made after the sender gate.
 */
export type MailAcceptGrantContext = {
  invokerPrincipalId: string | null;
  assetId: string;
  tenantId: string;
  runPrincipalId: string;
  legacyMailAccept: LegacyMailAcceptRule | null;
};

/**
 * Resolve one `mail.accept:*` snapshot marker to the concrete
 * `mail.accept:<coord-type>:<id>` resource string it materializes as, or `null`
 * when it must not emit a launch row. Throws on an unrecognized `mail.accept`
 * marker so a corrupt snapshot fails loudly rather than silently dropping an
 * accept-grant.
 */
function resolveMailAcceptMarker(
  marker: string,
  ctx: MailAcceptGrantContext,
): string | null {
  // An explicit concrete coordinate (`mail.accept:principal:<id>` /
  // `mail.accept:asset:<id>`) is already resolved at deploy time; pass it
  // through unchanged. A coordinate whose id is a `*` pattern does not parse,
  // so it falls to the unrecognized-marker throw below and can never become a
  // grant that admits many senders.
  if (parseMailAcceptResource(marker) !== null) {
    return marker;
  }
  switch (marker) {
    case MAIL_ACCEPT_INVOKER_MARKER:
      // The deployment has no active deployer. Emit no invoker row rather
      // than invent a coordinate.
      return ctx.invokerPrincipalId === null
        ? null
        : mailAcceptResource("principal", ctx.invokerPrincipalId);
    case MAIL_ACCEPT_SELF_MARKER:
      return mailAcceptResource("asset", ctx.assetId);
    case MAIL_ACCEPT_TENANT_MARKER:
      return mailAcceptResource("tenant", ctx.tenantId);
    case MAIL_ACCEPT_CORRESPONDENT_MARKER:
      // `correspondent` is dynamic: the counterparty is not known from the
      // frozen definition at launch -- it is the party the run mails, resolved
      // and minted at the send instant (see `createCorrespondentGrantMinter`).
      // So it materializes no launch row; skipped here without error. Its
      // presence in the snapshot is the deploy-time approval the send-time mint
      // gates on.
      return null;
    default:
      throw new Error(
        `deriveMailAcceptGrantRows: unrecognized mail.accept marker ${JSON.stringify(marker)}; the grant-walk snapshot must emit only known relation tokens or concrete coordinates`,
      );
  }
}

/**
 * Project the frozen grant-walk snapshot's `mail.accept:*` markers into the
 * run's concrete accept-grant rows at launch. This rides the SAME materialization path as
 * `deriveRunRuntimeGrantRows` (which stays pure over `tool:`/`effect:`): a
 * definition's declared accept-relations become concrete
 * `mail.accept:<coord-type>:<id>` grant rows on the run principal so later
 * inbound mail transport can authorize delivery against them.
 *
 * `invoker` resolves to `mail.accept:principal:<invokerPrincipalId>` (skipped
 * when no invoker resolved), `self` to `mail.accept:asset:<assetId>`,
 * and `tenant` to `mail.accept:tenant:<tenantId>`. Explicit concrete
 * coordinates pass through. `correspondent` is dynamic and not materializable
 * at launch (it is minted at the send instant), so it is skipped. Rows are
 * deduplicated by
 * resolved resource string (markers repeat per step, and `self`/`invoker` may
 * collide with an explicit coordinate), mirroring the runtime derivation's
 * per-resource dedup. The markers of the deployment's legacy rule, if any,
 * resolve the same way. Every emitted row is a creator-origin
 * `accept`/`allow` grant with no conditions and no expiry.
 */
export function deriveMailAcceptGrantRows(
  snapshot: GrantWalkSnapshot,
  ctx: MailAcceptGrantContext,
  now: Date,
): MaterializedGrantRow[] {
  const resources = new Set<string>();
  const markers = [
    ...snapshot.perStep.flatMap((step) => step.grants),
    ...legacyMailAcceptMarkers(ctx.legacyMailAccept),
  ];
  for (const grant of markers) {
    if (!isMailAcceptNamespaceResource(grant)) continue;
    const resolved = resolveMailAcceptMarker(grant, ctx);
    if (resolved !== null) resources.add(resolved);
  }

  return [...resources].map((resource) =>
    makeGrantRow({
      tenantId: ctx.tenantId,
      principalId: ctx.runPrincipalId,
      resource,
      action: MAIL_ACCEPT_ACTION,
      effect: "allow",
      conditions: null,
      origin: "creator",
      expiresAt: null,
      now,
    }),
  );
}

/** The resource of a run principal's send grant: every recipient. */
export const RUN_MAIL_SEND_RESOURCE = `${MAIL_SEND_NAMESPACE}:*:*`;

/**
 * The run principal's send grant: one creator-origin `mail.send:*:*` /
 * `send` / `allow` row, staged for every deployment at first trigger. It
 * allows sends to every recipient that has a coordinate: a local party, or an
 * address whose domain is a host name. It does not decide whether a step may
 * send mail at all; the sidecar's `tool:mail_send` and `tool:mail_reply`
 * grants do.
 * A `deny` row narrows it. A narrower `allow` row does not, because this row
 * still allows every recipient that the narrower row does not name. To allow
 * only some recipients, an operator removes this row and adds narrower
 * `allow` rows.
 */
export function deriveRunMailSendGrantRow(
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow {
  return makeGrantRow({
    tenantId,
    principalId: runPrincipalId,
    resource: RUN_MAIL_SEND_RESOURCE,
    action: MAIL_SEND_ACTION,
    effect: "allow",
    conditions: null,
    origin: "creator",
    expiresAt: null,
    now,
  });
}

/**
 * Whether the frozen grant-walk snapshot belongs to a definition with a mail
 * trigger. The capability walk emits a `mail.address:<address>` grant for each
 * mail trigger, so that grant is the snapshot's record of the trigger.
 */
function snapshotHasMailTrigger(snapshot: GrantWalkSnapshot): boolean {
  return snapshot.perStep.some((step) =>
    step.grants.some((grant) => grant.startsWith(MAIL_ADDRESS_GRANT_PREFIX)),
  );
}

/**
 * Project a materialized run grant row into the `run.grants` wire shape. A
 * run grant is always principal-scoped and never
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

/**
 * The requirements a run's delegated rows come from, for both first-trigger
 * staging and re-derivation. Each requirement on the literal
 * `SELF_RUN_RESOURCE` placeholder binds to `workflow-run:<runId>`: the
 * operator approved the placeholder, and binding it before the authority
 * check makes the delegator cover the concrete run id.
 *
 * A requirement in a reserved namespace is dropped. Rows in such a namespace
 * never come from a delegator, and re-derivation could never lapse one. The
 * deploy gate refuses such a requirement, but a snapshot stored before the
 * namespace was reserved can still carry one.
 */
export function delegatedRunRequirements(
  grantRequirements: readonly GrantRequirement[],
  runId: string,
): GrantRequirement[] {
  const delegated: GrantRequirement[] = [];
  for (const req of grantRequirements) {
    const namespace = reservedGrantNamespace(req.resource);
    if (namespace !== null) {
      log.warn`Run ${runId} skips its grant requirement ${req.resource} / ${req.action}: ${namespace.namespace} is a reserved namespace`;
      continue;
    }
    delegated.push(
      req.resource === SELF_RUN_RESOURCE
        ? { ...req, resource: `workflow-run:${runId}` }
        : req,
    );
  }
  return delegated;
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
  /**
   * The stable top-level run id the grants are for. A requirement on
   * `SELF_RUN_RESOURCE` resolves to `workflow-run:<runId>`.
   */
  runId: string;
  runPrincipalId: string;
  now: Date;
  /** The latest expiry of an invoker-delegated `allow` row. */
  invokerExpiresAt: Date;
  /**
   * The deployment's active deployer, its invoker of record. Resolves the
   * snapshot's `mail.accept:invoker` marker to `mail.accept:principal:<id>`.
   * Null when the deployment has no active deployer; then no invoker
   * accept-grant row is emitted.
   */
  invokerPrincipalId: string | null;
  /**
   * The workflow asset that backs the deployment's definition -- resolves the
   * snapshot's `mail.accept:self` marker to `mail.accept:asset:<assetId>`.
   */
  assetId: string;
  /**
   * The deployment's legacy mail accept rule from its anchor run, null for a
   * deployment made after the sender gate. Its markers add to the snapshot's.
   */
  legacyMailAccept: LegacyMailAcceptRule | null;
  /**
   * The grants invoker-sourced requirements resolve against: the deployer's,
   * from `collectDeployerInvokerGrants`, on both trigger paths.
   */
  invokerGrants: GrantRule[];
  /** Declared creator grants resolved against the definition asset's creator. */
  creatorGrants: GrantRule[];
  /**
   * Grant requirements to resolve. Both call sites pass the snapshot's
   * requirements unfiltered, so an invoker-sourced requirement resolves against
   * the passed `invokerGrants` and fails closed when they are insufficient.
   */
  grantRequirements: readonly GrantRequirement[];
  /**
   * Resolved credential-binding descriptors from the anchor run. Each distinct
   * `(credentialId, consumer)` becomes one system-origin credential-use grant.
   * The caller passes an empty array when the anchor has no refs.
   */
  credentialBindings: readonly { credentialId: string; consumer: string }[];
};

export type StageRunGrantsResult =
  | { ok: false; rejection: GrantMaterializationRejection }
  | {
      ok: true;
      grantRows: MaterializedGrantRow[];
      stepGrants: RunGrantsFrame["stepGrants"];
    };

/**
 * Stage a run's grant rows from the deploy-approved grant-walk snapshot, the
 * resolved declared requirements, and the anchor's resolved credential
 * bindings. The snapshot's per-step grants project the run's runtime
 * `tool:`/`effect:` rows and its `mail.accept` rows. Every run gets one
 * `mail.send:*:*` / `send` row (`deriveRunMailSendGrantRow`). Each distinct
 * credential and consumer becomes one system-origin `credential:{id}` / `use`
 * row. The mail-triggered materializer and the external trigger route both
 * drive this one tail.
 * Returns the staged rows and their wire projection, or a rejection when a
 * declared requirement's authority is insufficient. No database write happens
 * here -- `commitRunGrants` performs it once the caller has accepted delivery.
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
      assetId: args.assetId,
      tenantId: args.tenantId,
      runPrincipalId: args.runPrincipalId,
      legacyMailAccept: args.legacyMailAccept,
    },
    args.now,
  );

  const materialization = await resolveGrantMaterialization({
    tenantId: args.tenantId,
    targetPrincipalId: args.runPrincipalId,
    grantRequirements: delegatedRunRequirements(
      args.grantRequirements,
      args.runId,
    ),
    invokerGrants: args.invokerGrants,
    creatorGrants: args.creatorGrants,
    now: args.now,
    invokerExpiresAt: args.invokerExpiresAt,
  });
  if (!materialization.ok) {
    return { ok: false, rejection: materialization.rejection };
  }

  const credentialGrantRows = deriveCredentialUseGrantRows({
    bindings: args.credentialBindings,
    tenantId: args.tenantId,
    runPrincipalId: args.runPrincipalId,
    now: args.now,
  });

  const grantRows = [
    ...runtimeGrantRows,
    ...mailAcceptGrantRows,
    deriveRunMailSendGrantRow(args.tenantId, args.runPrincipalId, args.now),
    ...materialization.grantRows,
    ...credentialGrantRows,
  ];
  const stepGrants = grantRows.map((g) => runGrantToWire(g));
  return { ok: true, grantRows, stepGrants };
}

/**
 * Load a definition asset's `creatorPrincipalId` -- the creator whose
 * authority creator-sourced grant requirements resolve against. Returns
 * `null` when the asset records no creator (the FK is `set null` on
 * principal deletion) or the asset row is absent.
 */
export async function loadAssetCreatorPrincipalId(
  db: DBExecutor,
  tenantId: string,
  definitionAssetId: string,
): Promise<string | null> {
  const assetRow = await db.query.asset.findFirst({
    where: and(eq(asset.id, definitionAssetId), eq(asset.tenantId, tenantId)),
  });
  return assetRow?.creatorPrincipalId ?? null;
}

/**
 * Collect a creator's grants only when a creator-sourced requirement
 * exists and the asset records a creator. When a creator-sourced
 * requirement exists but the creator is null, the grants stay empty and
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

export type DeployerInvokerGrantsResult =
  | { ok: true; invokerGrants: GrantRule[] }
  | { ok: false; rejection: GrantMaterializationRejection };

/**
 * Collect the grants a run's invoker-sourced requirements resolve against.
 * The invoker of record is the deployment's deployer -- the principal its
 * launch spec records as source authority -- not whoever sends the trigger
 * mail, so a run started by another run delegates from the run that deployed
 * it.
 * A deployment with invoker requirements whose deployer is unrecorded or no
 * longer active is unusable and is refused; one without such requirements
 * needs no deployer.
 */
/**
 * The deployment's active deployer -- the launch spec's source authority, its
 * invoker of record -- or null when the launch spec records none, or names a
 * principal outside the deployment's tenant or not active.
 */
export async function loadActiveDeployerId(
  db: DBExecutor,
  tenantId: string,
  anchorRunId: string,
): Promise<string | null> {
  const [deployer] = await db
    .select({ id: principalTable.id, status: principalTable.status })
    .from(workflowRunLaunchSpec)
    .innerJoin(
      principalTable,
      eq(principalTable.id, workflowRunLaunchSpec.sourceAuthorityPrincipalId),
    )
    .where(
      and(
        eq(workflowRunLaunchSpec.anchorRunId, anchorRunId),
        eq(principalTable.tenantId, tenantId),
      ),
    )
    .limit(1);
  return deployer?.status === "active" ? deployer.id : null;
}

export async function collectDeployerInvokerGrants(args: {
  db: DBExecutor;
  grantStore: GrantStore;
  tenantId: string;
  anchorRunId: string;
  grantRequirements: readonly GrantRequirement[];
}): Promise<DeployerInvokerGrantsResult> {
  if (!args.grantRequirements.some((r) => r.source === "invoker")) {
    return { ok: true, invokerGrants: [] };
  }
  const deployerId = await loadActiveDeployerId(
    args.db,
    args.tenantId,
    args.anchorRunId,
  );
  if (deployerId === null) {
    return {
      ok: false,
      rejection: {
        status: 409,
        code: "deployer_unavailable",
        message: `Deployment ${args.anchorRunId} has no active deployer to resolve its invoker requirements against`,
      },
    };
  }
  return {
    ok: true,
    invokerGrants: await args.grantStore.collectGrants(
      deployerId,
      args.tenantId,
    ),
  };
}

export type StageFirstTriggerRunGrantsArgs = {
  snapshot: GrantWalkSnapshot;
  tenantId: string;
  anchorRunId: string;
  runId: string;
  runPrincipalId: string;
  definitionAssetId: string;
  legacyMailAccept: LegacyMailAcceptRule | null;
  credentialBindings: readonly { credentialId: string; consumer: string }[];
  now: Date;
  /** The latest expiry of an invoker-delegated `allow` row. */
  invokerExpiresAt: Date;
};

/**
 * Read a deployment's delegators and stage its run's first grant rows. The
 * creator is the definition asset's creator and the invoker is the
 * deployment's active deployer, both read through `executor` and
 * `grantStore`. The deployer also resolves the run's `mail.accept:invoker`
 * marker. Returns the staged rows, or the rejection when a delegator lacks a
 * requirement's authority or the deployer is unavailable.
 */
export async function stageFirstTriggerRunGrants(
  executor: DBExecutor,
  grantStore: GrantStore,
  args: StageFirstTriggerRunGrantsArgs,
): Promise<StageRunGrantsResult> {
  const grantRequirements = args.snapshot.grantRequirements;
  const invoker = await collectDeployerInvokerGrants({
    db: executor,
    grantStore,
    tenantId: args.tenantId,
    anchorRunId: args.anchorRunId,
    grantRequirements,
  });
  if (!invoker.ok) return invoker;
  const creatorGrants = await collectCreatorGrants(
    grantStore,
    args.tenantId,
    await loadAssetCreatorPrincipalId(
      executor,
      args.tenantId,
      args.definitionAssetId,
    ),
    grantRequirements,
  );
  return stageRunGrantsFromSnapshot({
    snapshot: args.snapshot,
    tenantId: args.tenantId,
    runId: args.runId,
    runPrincipalId: args.runPrincipalId,
    now: args.now,
    invokerExpiresAt: args.invokerExpiresAt,
    invokerPrincipalId: await loadActiveDeployerId(
      executor,
      args.tenantId,
      args.anchorRunId,
    ),
    assetId: args.definitionAssetId,
    legacyMailAccept: args.legacyMailAccept,
    invokerGrants: invoker.invokerGrants,
    creatorGrants,
    grantRequirements,
    credentialBindings: args.credentialBindings,
  });
}

export type CommitRunGrantsArgs = {
  db: DB["db"];
  runTokenStore: RunTokenStore;
  /**
   * The consumers that get a hub run token, from the locked anchor's
   * `credentialRefs.hubConsumers`. Minted only by the run's first commit.
   */
  hubConsumers: readonly string[];
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
 * as "running" while it accepts work and "stopping" once cancellation is
 * requested or its deadline passes; the started-vs-not distinction is owned by
 * the durable lifecycle, not this status axis.
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

/**
 * The run principal a run's first trigger linked to its run row, or null
 * before that trigger commits. Only the run's own workflow principal counts:
 * the link must name the `kind = 'workflow'` principal whose `refId` is the
 * run id.
 */
async function loadLinkedRunPrincipalId(
  executor: DBExecutor,
  tenantId: string,
  runId: string,
): Promise<string | null> {
  const [linked] = await executor
    .select({ id: principalTable.id })
    .from(workflowRun)
    .innerJoin(principalTable, eq(principalTable.id, workflowRun.principalId))
    .where(
      and(
        eq(workflowRun.id, runId),
        eq(workflowRun.tenantId, tenantId),
        eq(principalTable.tenantId, tenantId),
        eq(principalTable.kind, "workflow"),
        eq(principalTable.refId, runId),
      ),
    )
    .limit(1);
  return linked?.id ?? null;
}

export async function loadCommittedRunGrantsFromExecutor(
  executor: DBExecutor,
  tenantId: string,
  runId: string,
): Promise<CommittedRunGrants | null> {
  const runPrincipalId = await loadLinkedRunPrincipalId(
    executor,
    tenantId,
    runId,
  );
  if (runPrincipalId === null) return null;

  const rows = await executor
    .select()
    .from(grantTable)
    .where(eq(grantTable.principalId, runPrincipalId))
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
    runPrincipalId,
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
 * The sidecar router's `loadRunGrants` lookup: a run's committed grants by run
 * id, read at send time, or null when the run has no run row or no run
 * principal (a child run has none).
 */
export function createRunGrantsLoader(deps: {
  db: DB["db"];
}): (runId: string) => Promise<RunGrantsFrame["stepGrants"] | null> {
  return async (runId) => {
    const run = await deps.db.query.workflowRun.findFirst({
      where: eq(workflowRun.id, runId),
      columns: { tenantId: true },
    });
    if (run === undefined) return null;
    const committed = await loadCommittedRunGrants(
      deps.db,
      run.tenantId,
      runId,
    );
    return committed === null ? null : committed.stepGrants;
  };
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
 * rejects-always sets `deny` (stop asking, block it). The caller re-derives the
 * run in the same transaction, which gives the row the expiry of its new
 * effect, or deletes an `allow` the run's delegator no longer holds. Every
 * later read (the child's enforcement floor, the authorization view, and the
 * grants the sidecar router sends ahead of each signal and trigger) sees the
 * result, so the tool is not asked again for the life of the run. Two grant
 * sends for one run can arrive out of order, and then the sidecar holds the
 * older grants until the next send.
 *
 * Guarded to only change a grant currently gated `ask`: the `effect = "ask"`
 * predicate means it only ever resolves the checkpoint, never overrides an
 * existing `allow`/`deny` and never touches a tool the run does not already
 * hold. So a standing resolution can only remove the checkpoint on a capability
 * the deploy already granted-with-a-checkpoint -- in the direction the operator
 * chose. Runs against the passed executor, so the caller mutates inside the
 * resolve transaction and a rolled-back resolve rolls back the grant change with
 * it. A run whose first trigger has not committed (nothing to mutate) is a
 * no-op. Returns the run principal's id when a row changed, and null
 * otherwise.
 */
export async function setRunToolGrantEffect(
  executor: DBExecutor,
  tenantId: string,
  runId: string,
  toolName: string,
  effect: "allow" | "deny",
): Promise<string | null> {
  const runPrincipalId = await loadLinkedRunPrincipalId(
    executor,
    tenantId,
    runId,
  );
  if (runPrincipalId === null) return null;
  const changed = await executor
    .update(grantTable)
    .set({ effect, updatedAt: new Date() })
    .where(
      and(
        eq(grantTable.principalId, runPrincipalId),
        eq(grantTable.resource, `${TOOL_GRANT_PREFIX}${toolName}`),
        eq(grantTable.effect, "ask"),
      ),
    )
    .returning({ id: grantTable.id });
  return changed.length > 0 ? runPrincipalId : null;
}

/**
 * Idempotently commit a run's first trigger: its principal, run tokens,
 * anchor link, and immutable grant rows, keyed on the deployment's stable
 * top-level run id.
 *
 * The caller holds the anchor row `FOR UPDATE` on an executable anchor, which
 * serializes first commits. The anchor's link to its run principal marks the
 * commit: a later caller finds the link and returns the exact persisted
 * grants instead of its independently staged snapshot. This keeps the
 * database and Git authorization views identical when first deliveries race.
 *
 * The `runPrincipalId` is derived deterministically from `(tenantId, runId)`
 * by the caller, so the principal and the grant rows that reference it agree
 * on the id even across a retry. A deployment's run principal exists from
 * deploy; the commit creates one, with no hub key, only for a run that has
 * none. An existing run principal must have that id and be active, so a
 * commit never links a different or deactivated principal. Grants an
 * operator gave the principal before its first trigger stay, and the commit
 * returns all of the principal's rows, which is what every later read
 * returns.
 */
export async function commitRunGrants(
  args: CommitRunGrantsArgs,
  tx: DBExecutor,
): Promise<RunGrantsFrame["stepGrants"]> {
  const workflowRunStore = createWorkflowRunStore(args.db);
  const existing = await loadCommittedRunGrantsFromExecutor(
    tx,
    args.tenantId,
    args.runId,
  );
  if (existing !== null) return existing.stepGrants;

  const insertedPrincipal = await insertRunPrincipal(tx, {
    id: args.runPrincipalId,
    tenantId: args.tenantId,
    runId: args.runId,
    createdAt: args.now,
  });
  if (!insertedPrincipal) {
    await assertReusableRunPrincipal(tx, args);
  }

  await args.runTokenStore.mintIfAbsent(
    { principalId: args.runPrincipalId, consumers: args.hubConsumers },
    tx,
  );
  const linked = await workflowRunStore.anchorWithPrincipal(
    {
      id: args.runId,
      anchorRunId: args.anchorRunId,
      definitionId: args.definitionId,
      tenantId: args.tenantId,
      principalId: args.runPrincipalId,
      status: "running",
    },
    tx,
  );
  if (!linked) {
    const [anchor] = await tx
      .select({
        status: workflowRun.status,
        principalId: workflowRun.principalId,
      })
      .from(workflowRun)
      .where(eq(workflowRun.id, args.runId))
      .limit(1);
    throw new Error(
      `commitRunGrants: anchor ${args.runId} did not link principal ${args.runPrincipalId} (status ${anchor?.status ?? "absent"}, linked principal ${anchor?.principalId ?? "none"})`,
    );
  }
  for (const g of args.grantRows) {
    await tx.insert(grantTable).values(g);
  }
  const committed = await loadCommittedRunGrantsFromExecutor(
    tx,
    args.tenantId,
    args.runId,
  );
  if (committed === null) {
    throw new Error(
      `commitRunGrants: run ${args.runId} has no committed grants after linking principal ${args.runPrincipalId}`,
    );
  }
  return committed.stepGrants;
}

/**
 * Serialize re-derivations of run grants in one tenant for the rest of the
 * transaction.
 *
 * A caller that changes a delegator's grants and then re-derives must take
 * this lock before its own write. Otherwise it holds the written rows while
 * it waits here, and a re-derivation that holds this lock can wait on those
 * rows. A first trigger takes it before its other locks, so a grant change
 * on the run principal commits before the first commit reads the
 * principal's rows, or waits until that commit is done. The scope is the
 * tenant, not the run principal: a grant change does not lock the principal
 * row, and a role change finds the principals it affects only after it
 * writes, so no narrower lock orders both. The lock is transaction-scoped, so
 * a second call in the same transaction does not block.
 */
export async function lockTenantRunDelegatedGrants(
  tx: DBExecutor,
  tenantId: string,
): Promise<void> {
  await tx.execute(
    sql`select pg_advisory_xact_lock(hashtextextended(${`run-delegated-grants:${tenantId}`}, 0))`,
  );
}

export type RunPrincipalBeforeFirstCommit = {
  id: string;
  status: string;
  createdAt: Date;
};

/**
 * A run's principal before its first commit, read in the caller's locked
 * transaction, or null when the run has none yet. A deployment's run
 * principal exists from deploy, so a first trigger reads its status, which
 * an operator can have changed, and its creation time, which anchors the
 * expiry of invoker-delegated `allow` rows.
 */
export async function loadRunPrincipalBeforeFirstCommit(
  tx: DBExecutor,
  tenantId: string,
  runId: string,
): Promise<RunPrincipalBeforeFirstCommit | null> {
  const [row] = await tx
    .select({
      id: principalTable.id,
      status: principalTable.status,
      createdAt: principalTable.createdAt,
    })
    .from(principalTable)
    .where(
      and(
        eq(principalTable.tenantId, tenantId),
        eq(principalTable.kind, "workflow"),
        eq(principalTable.refId, runId),
      ),
    )
    .limit(1);
  return row ?? null;
}

/**
 * The refusal of a first trigger whose run principal an operator suspended or
 * deactivated before the run fired, or null when the run may fire.
 */
export function inactiveRunPrincipalRejection(
  runId: string,
  runPrincipal: RunPrincipalBeforeFirstCommit | null,
): GrantMaterializationRejection | null {
  if (runPrincipal === null || runPrincipal.status === "active") return null;
  return {
    status: 409,
    code: "workflow_run_principal_inactive",
    message: `Workflow run ${runId} has a ${runPrincipal.status} principal and cannot start`,
  };
}

/**
 * The latest expiry of a run's invoker-delegated `allow` rows:
 * `INVOKER_GRANT_TTL_MS` after the run principal is created, which is the
 * deployment's deploy time. A run with no principal yet creates it at `now`.
 */
export function runInvokerExpiresAt(
  runPrincipal: RunPrincipalBeforeFirstCommit | null,
  now: Date,
): Date {
  const createdAt = runPrincipal?.createdAt ?? now;
  return new Date(createdAt.getTime() + INVOKER_GRANT_TTL_MS);
}

async function assertReusableRunPrincipal(
  tx: DBExecutor,
  args: CommitRunGrantsArgs,
): Promise<void> {
  const [existing] = await tx
    .select({ id: principalTable.id, status: principalTable.status })
    .from(principalTable)
    .where(
      and(
        eq(principalTable.tenantId, args.tenantId),
        eq(principalTable.kind, "workflow"),
        eq(principalTable.refId, args.runId),
      ),
    )
    .limit(1);
  if (existing?.id !== args.runPrincipalId) {
    throw new Error(
      `commitRunGrants: run ${args.runId} has principal ${existing?.id ?? "none"}, not the derived ${args.runPrincipalId}`,
    );
  }
  // The caller refuses an inactive run principal before it stages, under the
  // same lock, so reaching here with one is an invariant break.
  if (existing.status !== "active") {
    throw new Error(
      `commitRunGrants: run principal ${args.runPrincipalId} is ${existing.status}, not active`,
    );
  }
}

export type MailTriggeredRunGrantsDeps = {
  db: DB["db"];
  runTokenStore: RunTokenStore;
  grantStoreFor: GrantStoreFactory;
  // The hub's condition registry, passed to the admission evaluator so a
  // conditioned operator `mail.accept` deny (collected on the deliver path) is
  // honored rather than skipped. Omitted here means the evaluator throws (fails
  // closed) if it ever meets a conditioned grant; production supplies it.
  registry?: ConditionRegistry;
};

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
 * frozen snapshot and from that deployment's anchor: the snapshot's
 * `tool:`/`effect:` runtime grants, the declared requirements resolved
 * against the creator and the deployer, and the anchor's resolved
 * credential-binding grants.
 * A first commit returns `notReady` and writes nothing while initialization
 * has not published credential resolution. That is an anchor with neither a
 * public key nor credential refs, or one whose allocation holds an
 * initialization lease. Committing during the lease would freeze the
 * previous bindings.
 * The mail's sender is not the invoker: invoker-sourced requirements resolve
 * against the deployment's deployer, as on the external route.
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
  senderCoordinates: MailAcceptCoordinate[] | null;
}) => Promise<MailTriggeredRunGrantsResult> {
  return async ({ agentAddress, runId, senderCoordinates }) => {
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
        credentialRefs: workflowRun.credentialRefs,
        legacyMailAccept: workflowRun.legacyMailAccept,
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
    // This preflight inspects the anchor row's status, cancellation, and
    // deadline, and omits the durable-lifecycle terminal check that the HTTP
    // trigger route in workflow-run-trigger.ts applies. The supervisor's durable run-ref guard
    // (rejectTerminalRun / readWorkflowRunLifecycle in supervisor.ts) is the
    // fired/not-fired authority and never re-fires a durably-settled run, so
    // this status check is a lagging fast-fail only. The two preflights diverge
    // inside the status-flip lag window; that asymmetry is tolerable and is
    // tracked for unification in INTR-456.
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

    // Deliver-to-existing admission. Gate the sender against the run's
    // COLLECTED grants (not `loadCommittedRunGrants`, which reads only the run
    // principal's direct rows): `collectGrants` returns the run principal's
    // direct grants and the grants of its roles in the run's tenant, so an
    // operator `deny mail.accept:…` on the run principal or one of its roles is
    // honored. It does not walk the tenant chain. A `null` sender coordinate set
    // (an unresolvable/ambiguous sender) is never admitted. Same-tenant scoping
    // is inherent in the coordinate ids: a `principal`/`asset` coordinate
    // carries a globally-unique id, and the `tenant` coordinate carries the
    // sender's own tenant, so a cross-tenant sender only matches a rule that
    // explicitly names its ids.
    const admissionDenied = (): MailTriggeredRunGrantsResult => ({
      outcome: "rejected",
      status: 403,
      code: MAIL_ADMISSION_DENIED_CODE,
      message: `Workflow run ${runId} does not admit mail from this sender`,
    });
    const admitToCommittedRun = async (
      committed: CommittedRunGrants,
    ): Promise<MailTriggeredRunGrantsResult> => {
      const recipientGrants = await deps
        .grantStoreFor(deps.db)
        .collectGrants(committed.runPrincipalId, tenantId);
      const admission = await evaluateMailAdmission({
        senderCoordinates,
        recipientGrants,
        ...(deps.registry ? { registry: deps.registry } : {}),
      });
      if (!admission.admit) return admissionDenied();
      return {
        outcome: "materialized",
        stepGrants: committed.stepGrants,
      };
    };

    const committed = await loadCommittedRunGrants(deps.db, tenantId, runId);
    if (committed !== null) return admitToCommittedRun(committed);

    const snapshot = await loadDeploymentGrantSnapshot(deps.db, anchorRunId);
    if (snapshot === null) {
      // Fail closed rather than substitute an empty grant set, which would
      // launch a run with no runtime authority.
      throw new Error(
        `mail-triggered run ${runId} for ${agentAddress}: deployment ${anchorRunId} has no approved grant snapshot`,
      );
    }

    // Mail starts only a deployment that has a mail trigger. A deployment with
    // another trigger starts through that trigger, and its `mail.accept` rules
    // then admit mail to the run that the start commits. Without this check, a
    // sender that a launch rule admits would start the run and bypass the
    // authorization of the deployment's own trigger.
    if (!snapshotHasMailTrigger(snapshot)) {
      return {
        outcome: "rejected",
        status: 403,
        code: MAIL_ADMISSION_DENIED_CODE,
        message: `Workflow run ${runId} has no mail trigger and does not start by mail`,
      };
    }

    // Derive the run principal id from `(tenantId, runId)`. The runId is the
    // stable deployment address, so all trigger occurrences resolve the same
    // principal and canonical grant snapshot.
    const runPrincipalId = await deriveRunPrincipalId(tenantId, runId);
    const now = new Date();

    // Start (first-fire) admission. Gate the sender against the run's
    // `mail.accept` rows BEFORE committing: an un-admitted sender must not
    // create or fire the run. The rows are the same derivation the staging
    // below writes: the definition's declared accept-allows resolved onto this
    // run (`invoker`->the deployer, `self`->the definition's asset,
    // `tenant`->the run tenant, plus explicit coordinates).
    // `evaluateMailAdmission` filters them to the `mail.accept` namespace,
    // picks one winning grant across the sender's coordinates, and
    // default-denies when none match. A `null` sender coordinate set is never
    // admitted. This check reads only the deploy-approved accept surface; the
    // check under the lock below also reads the grants an operator gave the
    // run principal before its first trigger.
    //
    // The check runs before the grant requirements resolve, so a refused
    // sender is refused as `mail_admission_denied` and costs no grant reads.
    //
    // This check is provisional until the commit below: two admitted senders
    // racing the first fire each pass it. The anchor lock serializes them, and
    // the loser finds the winner's committed run there and is gated as a
    // delivery to that existing run instead.
    //
    // `invoker` names the deployer, not this sender, so which sender wins the
    // race does not change who the rules admit. Reading the deployer is not a
    // grant read: its grants are collected only after admission.
    const deployerPrincipalId = await loadActiveDeployerId(
      deps.db,
      tenantId,
      anchorRunId,
    );
    const acceptRows = deriveMailAcceptGrantRows(
      snapshot,
      {
        invokerPrincipalId: deployerPrincipalId,
        assetId: definitionAssetId,
        tenantId,
        runPrincipalId,
        legacyMailAccept: anchor.legacyMailAccept,
      },
      now,
    );
    const admission = await evaluateMailAdmission({
      senderCoordinates,
      recipientGrants: acceptRows.map(materializedRowToGrantRule),
      ...(deps.registry ? { registry: deps.registry } : {}),
    });
    if (!admission.admit) return admissionDenied();

    const reserved = await deps.db.transaction(async (tx) => {
      await lockTenantRunDelegatedGrants(tx, tenantId);
      const anchorState = await lockWorkflowRunState(
        tx,
        anchorRunId,
        anchorRunId,
      );
      if (anchorState !== "running")
        return anchorState === "stopping" ? anchorState : "terminal";
      const runState = await lockWorkflowRunState(tx, anchorRunId, runId);
      if (runState === "stopping" || runState === "terminal") return runState;
      // A sender that raced this first fire and lost finds the winner's run
      // committed once it holds the anchor lock. `commitRunGrants` would also
      // see it, but only after the credential-resolution gate below, which can
      // park mail to a run that is already live; check here so the loser goes
      // straight to deliver-to-existing admission.
      const raced = await loadCommittedRunGrantsFromExecutor(
        tx,
        tenantId,
        runId,
      );
      if (raced !== null) return { committedByAnotherSender: raced };

      const runPrincipal = await loadRunPrincipalBeforeFirstCommit(
        tx,
        tenantId,
        runId,
      );
      const inactive = inactiveRunPrincipalRejection(runId, runPrincipal);
      if (inactive !== null) {
        return {
          rejected: {
            outcome: "rejected",
            ...inactive,
          } satisfies MailTriggeredRunGrantsResult,
        };
      }

      // The definition's FULL declared requirements are resolved -- both
      // creator- and invoker-sourced -- so an invoker requirement the deployer
      // cannot satisfy fails the run closed instead of launching
      // under-authorized. Creator authority is resolved LIVE per run: the
      // definition's grant SHAPE is frozen in the snapshot, but which grants
      // the creator currently holds is not part of that shape and can change
      // between triggers. The delegators are read under the anchor lock, so a
      // delegator change that holds the lock commits before these reads, and
      // one that waits on the lock finds this run's committed rows.
      const staged = await stageFirstTriggerRunGrants(
        tx,
        deps.grantStoreFor(tx),
        {
          snapshot,
          tenantId,
          anchorRunId,
          runId,
          runPrincipalId,
          definitionAssetId,
          legacyMailAccept: anchor.legacyMailAccept,
          credentialBindings: credentialUseBindings(anchor.credentialRefs),
          now,
          invokerExpiresAt: runInvokerExpiresAt(runPrincipal, now),
        },
      );
      if (!staged.ok) {
        return {
          rejected: {
            outcome: "rejected",
            status: staged.rejection.status,
            code: staged.rejection.code,
            message: staged.rejection.message,
          } satisfies MailTriggeredRunGrantsResult,
        };
      }
      // The provisional admission read the deployer before the lock, and the
      // deployer can stop being active before these reads. Admit only a
      // sender the staged rows the run commits admit: a declared rule starts
      // a run, an operator grant does not. Then admit again with the grants
      // an operator gave the run principal before its first trigger added,
      // so an operator `mail.accept` deny binds from the first fire.
      const stagedRules = staged.grantRows.map(materializedRowToGrantRule);
      const registry = deps.registry ? { registry: deps.registry } : {};
      const stagedAdmission = await evaluateMailAdmission({
        senderCoordinates,
        recipientGrants: stagedRules,
        ...registry,
      });
      if (!stagedAdmission.admit) return { rejected: admissionDenied() };
      if (runPrincipal !== null) {
        const operatorGrants = await deps
          .grantStoreFor(tx)
          .collectGrants(runPrincipal.id, tenantId);
        const operatorAdmission = await evaluateMailAdmission({
          senderCoordinates,
          recipientGrants: [...operatorGrants, ...stagedRules],
          ...registry,
        });
        if (!operatorAdmission.admit) return { rejected: admissionDenied() };
      }

      const resolution = await lockAnchorCredentialResolution(tx, anchorRunId);
      if (resolution === null) {
        throw new Error(
          `mail-triggered run ${runId} for ${agentAddress}: anchor run ${anchorRunId} disappeared while locking credential resolution`,
        );
      }
      const grantRows = grantRowsWithLockedCredentialResolution(
        staged.grantRows,
        resolution,
        { tenantId, runPrincipalId, now },
      );
      if (grantRows === "not-ready") return grantRows;
      return commitRunGrants(
        {
          db: deps.db,
          runTokenStore: deps.runTokenStore,
          hubConsumers: hubConsumersOf(resolution.credentialRefs),
          tenantId,
          anchorRunId,
          definitionId: anchor.definitionId,
          runId,
          runPrincipalId,
          now,
          grantRows,
        },
        tx,
      );
    });
    if (reserved === "not-ready") return { outcome: "notReady" };
    if (reserved === "stopping" || reserved === "terminal")
      return unavailableRunResult(runId, reserved);
    if ("committedByAnotherSender" in reserved)
      return admitToCommittedRun(reserved.committedByAnotherSender);
    if ("rejected" in reserved) return reserved.rejected;
    return {
      outcome: "materialized",
      stepGrants: reserved,
    };
  };
}

const correspondentLogger = getLogger(["hub-api", "correspondent-grant"]);

/**
 * Resolve the principal a correspondent's reply will present, through
 * `resolveLocalMailParty`, which reads no signing key. A user resolves to its
 * principal. A local deployment resolves to its run principal: the linked
 * principal when there is one, else `deriveRunPrincipalId(tenantId, runId)`.
 * Deploy creates the run principal with that same derived id, and the mail
 * seam and the trigger route derive it the same way, so a first fire that
 * commits concurrently cannot make the two disagree.
 *
 * Only an executable anchor qualifies: a stopping or terminal deployment never
 * fires or replies, so a row for it would be dead weight. An anchor without a
 * signing key also qualifies; its reply cannot be signed until it acks, so the
 * row stays inert until then. Anything else resolves to `null`.
 */
async function resolveCorrespondentPrincipalId(
  tx: DBExecutor,
  address: string,
  now: Date,
): Promise<string | null> {
  const party = await resolveLocalMailParty(tx, address);
  if (party === null) return null;
  if (party.kind === "user") return party.principalId;

  const [anchor] = await tx
    .select({
      status: workflowRun.status,
      expiresAt: workflowRun.expiresAt,
      cancellationRequestedAt: workflowRun.cancellationRequestedAt,
    })
    .from(workflowRun)
    .where(eq(workflowRun.id, party.anchorRunId))
    .limit(1);
  if (
    anchor === undefined ||
    workflowRunExecutability(anchor, now) !== "executable"
  ) {
    return null;
  }
  return party.principalId;
}

/**
 * Report whether the sending deployment holds the `correspondent` relation:
 * a legacy rule that includes it, or the approved `mail.accept:correspondent`
 * marker in the grant snapshot of its launch spec.
 */
async function holdsCorrespondentRule(
  executor: DBExecutor,
  sender: {
    runId: string;
    legacyMailAccept: LegacyMailAcceptRule | null;
  },
): Promise<boolean> {
  if (
    legacyMailAcceptMarkers(sender.legacyMailAccept).includes(
      MAIL_ACCEPT_CORRESPONDENT_MARKER,
    )
  ) {
    return true;
  }
  const snapshot = await loadDeploymentGrantSnapshot(executor, sender.runId);
  if (snapshot === null) return false;
  return snapshot.perStep.some((step) =>
    step.grants.includes(MAIL_ACCEPT_CORRESPONDENT_MARKER),
  );
}

export type CorrespondentGrantMinterDeps = {
  db: DB["db"];
};

/**
 * Mint the accept-grant that admits a reply from a party a run mails -- the
 * dynamic `correspondent` relation. Launch
 * materialization cannot enumerate future correspondents, so the grant is
 * committed at the send instant instead: when an executable run mails a party,
 * a `mail.accept:principal:<recipient>` allow row is added to the SENDING run's
 * principal, so the reply (recipient -> sender) is admitted by the ordinary
 * deliver-to-existing gate with no new admission path. The recipient is
 * resolved to the principal id its reply will present (see
 * `resolveCorrespondentPrincipalId`), so the minted grant matches the
 * coordinate the reply carries.
 *
 * Fail-closed and bounded:
 * - The sending deployment must carry the approved `mail.accept:correspondent`
 *   marker in its launch-spec snapshot, or hold a legacy rule that includes
 *   it; absent both, nothing is minted.
 * - A stopping, terminal, or absent sending run mints nothing. The row is only
 *   reachable while the run is executable. The run principal id derives from
 *   the tenant and the stable run id, so the bound is that a terminal run
 *   stays terminal and receives no new grant. A stopping run (cancellation
 *   requested or deadline passed) cannot receive the reply, so a row would be
 *   dead weight.
 * - A principal-less sending run or an unresolvable/external recipient mints
 *   nothing. A local deployment that has not fired yet has no principal on its
 *   anchor row; it is minted for the principal its first fire will create (see
 *   `resolveCorrespondentPrincipalId`), so the reply to a first contact is
 *   admitted.
 * - The insert is deduplicated under a `FOR UPDATE` lock on the sending run, so
 *   a repeat mail to the same recipient does not accrue a second row.
 * - A sending deployment with no `correspondent` relation takes no lock: an
 *   unlocked read finds no rule and returns before the transaction.
 *
 * All recipients of one outbound mail are handled in one transaction: the
 * sending run is locked and its `correspondent` authorization checked once, then
 * each recipient is resolved and inserted under that one lock -- the
 * authorization is invariant across the fan-out, so it is not re-derived per
 * recipient. Each recipient runs in its own savepoint, so a fault on one
 * recipient rolls back only that recipient's grant.
 *
 * Best-effort: minting is a side effect of sending and MUST NOT break delivery,
 * so any fault degrades to no grant and is logged, mirroring the send path's
 * frame-key resolver.
 */
export function createCorrespondentGrantMinter(
  deps: CorrespondentGrantMinterDeps,
): (args: {
  senderAddress: string;
  recipientAddresses: string[];
}) => Promise<void> {
  return async (args) => {
    const senderAddress = args.senderAddress.toLowerCase();
    try {
      // Most outbound mail comes from a deployment with no `correspondent`
      // rule. Check the rule without a lock first, so that mail opens no
      // transaction and does not serialize the run's sends. The locked path
      // checks the rule again, so a mint never depends on this read.
      const [candidate] = await deps.db
        .select({
          runId: workflowRun.id,
          legacyMailAccept: workflowRun.legacyMailAccept,
        })
        .from(workflowRun)
        .where(eq(sql`lower(${workflowRun.address})`, senderAddress))
        .limit(1);
      if (candidate === undefined) return;
      if (!(await holdsCorrespondentRule(deps.db, candidate))) return;

      await deps.db.transaction(async (tx) => {
        // Lock the SENDING run row once. A stopping/terminal/absent run mints
        // nothing, and the lock serializes concurrent sends from the same run
        // so the per-recipient dedup below cannot be raced.
        const [sender] = await tx
          .select({
            status: workflowRun.status,
            runId: workflowRun.id,
            principalId: workflowRun.principalId,
            tenantId: workflowRun.tenantId,
            expiresAt: workflowRun.expiresAt,
            cancellationRequestedAt: workflowRun.cancellationRequestedAt,
            legacyMailAccept: workflowRun.legacyMailAccept,
          })
          .from(workflowRun)
          .where(eq(sql`lower(${workflowRun.address})`, senderAddress))
          .limit(1)
          .for("update");
        if (sender === undefined) return;
        const now = new Date();
        if (workflowRunExecutability(sender, now) !== "executable") return;
        if (sender.principalId === null) return;
        const senderPrincipalId = sender.principalId;

        // Gate: the sending deployment must have been approved for the
        // `correspondent` relation, or hold it as its legacy rule. The sending
        // run is the deployment's anchor, the only run with an address. This
        // holds for every recipient of this mail, so check it once.
        if (!(await holdsCorrespondentRule(tx, sender))) return;

        for (const recipientAddress of args.recipientAddresses) {
          // Each recipient runs in its own savepoint. A recipient that fails to
          // resolve or insert (an ambiguous user address, a query fault) rolls
          // back only its own work, so it never costs a
          // co-recipient of the same mail its grant.
          try {
            await tx.transaction(async (sp) => {
              const recipientPrincipalId =
                await resolveCorrespondentPrincipalId(
                  sp,
                  recipientAddress,
                  now,
                );
              if (recipientPrincipalId === null) return;

              const resource = mailAcceptResource(
                "principal",
                recipientPrincipalId,
              );
              // No unique constraint exists on `(principalId, resource,
              // action)` (a principal may hold both an allow and a deny), so
              // dedup with a guarded insert under the run lock rather than an
              // ON CONFLICT. An expired row no longer admits or refuses, so it
              // does not count.
              const [existing] = await sp
                .select({ id: grantTable.id })
                .from(grantTable)
                .where(
                  and(
                    eq(grantTable.principalId, senderPrincipalId),
                    eq(grantTable.resource, resource),
                    eq(grantTable.action, MAIL_ACCEPT_ACTION),
                    or(
                      isNull(grantTable.expiresAt),
                      gt(grantTable.expiresAt, now),
                    ),
                  ),
                )
                .limit(1);
              if (existing !== undefined) return;

              await sp.insert(grantTable).values(
                makeGrantRow({
                  tenantId: sender.tenantId,
                  principalId: senderPrincipalId,
                  resource,
                  action: MAIL_ACCEPT_ACTION,
                  effect: "allow",
                  conditions: null,
                  // A correspondent grant materializes the definition's
                  // creator-declared `correspondent` policy, so it shares the
                  // `creator` provenance of the launch-time `mail.accept` rows.
                  origin: "creator",
                  expiresAt: null,
                  now,
                }),
              );
            });
          } catch (cause) {
            correspondentLogger.error`Failed to mint a correspondent accept-grant for sender ${args.senderAddress} to recipient ${recipientAddress}: ${cause instanceof Error ? cause.message : String(cause)}`;
          }
        }
      });
    } catch (cause) {
      correspondentLogger.error`Failed to mint correspondent accept-grants for sender ${args.senderAddress}: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
  };
}
