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
  workflowDefinition,
  workflowRun,
} from "@intx/db/schema";
import type { DB, DBExecutor } from "@intx/db";
import { createWorkflowRunStore } from "@intx/db";
import type { GrantStore, GrantRule } from "@intx/types/authz";
import { GrantRequirement, type GrantEffect } from "@intx/types";
import { RunGrantsFrame } from "@intx/types/sidecar";
import {
  workflowDefinitionEnvelopeSchema,
  WORKFLOW_JSON_PATH,
  type AssetService,
  type MailTriggeredRunGrantsResult,
  type WorkflowDefinition,
} from "@intx/hub-sessions";
import {
  walkCapabilities,
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
const TOOL_GRANT_PREFIX = "tool:";
const EFFECT_GRANT_PREFIX = "effect:";

/**
 * Project the capability walk into the run's runtime grant rows -- the
 * `tool:<name>` and `effect:<cap>` grants the runtime enforces fail-closed.
 * Every distinct grant string across all steps becomes one creator-origin
 * `grant` row with `action: invoke`. The run's runtime authority is
 * definition-pure for the deployment's stable top-level run, so the walk
 * output alone determines it.
 *
 * Tool grants carry the effect the tool's static declaration requested (`ask`
 * for approval-gated tools, `allow` otherwise) via the walk's `grantEffects`
 * map. A tool in more than one step is emitted once; when two steps disagree
 * on its effect, `ask` wins over `allow` so an approval-gated declaration is
 * never silently downgraded.
 *
 * Effect grants are always `allow` -- the `effect.requires` set names the
 * capability floor an action needs, with no per-effect ask/allow distinction,
 * so they are NOT routed through the `grantEffects` map (which covers tool
 * grants only). An `effect:<cap>` in more than one step is emitted once.
 */
export function deriveRunRuntimeGrantRows(
  walk: CapabilityWalkResult,
  tenantId: string,
  runPrincipalId: string,
  now: Date,
): MaterializedGrantRow[] {
  const effectByResource = new Map<string, GrantEffect>();
  for (const declarations of walk.perStep.values()) {
    for (const grant of declarations.grants) {
      if (grant.startsWith(TOOL_GRANT_PREFIX)) {
        // Every `tool:` grant the walk emits carries a `grantEffects`
        // entry (the tool-mark floor: `ask` for an approval-gated tool,
        // `allow` otherwise). A missing entry means the walk's `grants`
        // and `grantEffects` maps have diverged -- a defaulted `allow`
        // here would silently DOWNGRADE an `ask` tool below its floor,
        // defeating the approval gate. Fail loudly instead.
        const effect = declarations.grantEffects.get(grant);
        if (effect === undefined) {
          throw new Error(
            `deriveRunRuntimeGrantRows: tool grant ${JSON.stringify(grant)} has no grantEffects entry; the capability walk must emit an effect for every tool grant`,
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
 * Read and hydrate the workflow definition from a workflow asset's
 * `workflow.json`. Validates the structural envelope at this boundary,
 * mirroring the workflow-host child's `loadWorkflowDefinition`: the
 * per-primitive narrows live in the runtime layer that consumes the
 * definition, so the envelope check plus the documented narrow is the
 * canonical hydration shape.
 */
export async function hydrateDefinition(
  assetService: AssetService,
  assetId: string,
): Promise<WorkflowDefinition> {
  const raw = await assetService.readAssetBlob({
    assetId,
    path: WORKFLOW_JSON_PATH,
  });
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(raw));
  } catch (cause) {
    throw new Error(
      `workflow asset ${assetId} ${WORKFLOW_JSON_PATH} is not valid JSON`,
      { cause },
    );
  }
  const validated = workflowDefinitionEnvelopeSchema(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `workflow asset ${assetId} ${WORKFLOW_JSON_PATH} failed envelope validation: ${validated.summary}`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- envelope schema enforces structural shape; per-primitive narrows live in the runtime layer that consumes the definition, matching loadWorkflowDefinition in @intx/workflow-host
  return validated as unknown as WorkflowDefinition;
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
 * Derive and stage a run's grant rows from its definition: the walk's
 * runtime `tool:`/`effect:` grants plus the resolved declared
 * requirements. Returns the staged rows and their wire projection, or a
 * rejection when a declared requirement's authority is insufficient. No
 * database write happens here -- `commitRunGrants` performs the canonical
 * reservation before a caller exposes those grants to a delivery transport.
 */
export async function stageRunGrants(
  args: StageRunGrantsArgs,
): Promise<StageRunGrantsResult> {
  const directorRegistry = createDefaultDirectorRegistry();
  const walk = walkCapabilities(args.definition, directorRegistry);
  const runtimeGrantRows = deriveRunRuntimeGrantRows(
    walk,
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
  deploymentId: string;
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
  deploymentId: string,
  runId: string,
): Promise<"absent" | "running" | "terminal"> {
  const [run] = await tx
    .select({ status: workflowRun.status })
    .from(workflowRun)
    .where(
      and(eq(workflowRun.id, runId), eq(workflowRun.anchorRunId, deploymentId)),
    )
    .limit(1)
    .for("update");
  if (run === undefined) return "absent";
  return isLiveWorkflowRunStatus(run.status) ? "running" : "terminal";
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
        anchorRunId: args.deploymentId,
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
  return async ({ agentAddress, runId }) => {
    const topLevelRun = alias(workflowRun, "mail_triggered_top_level_run");
    const [anchor] = await deps.db
      .select({
        deploymentId: workflowRun.id,
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
    const deploymentId = anchor.deploymentId;

    const committed = await loadCommittedRunGrants(deps.db, tenantId, runId);
    if (committed !== null) {
      return {
        outcome: "materialized",
        stepGrants: committed.stepGrants,
      };
    }

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

    // Derive the run principal id from `(tenantId, runId)`. The runId is the
    // stable deployment address, so all trigger occurrences resolve the same
    // principal and canonical grant snapshot.
    const runPrincipalId = await deriveRunPrincipalId(tenantId, runId);
    const now = new Date();
    const staged = await stageRunGrants({
      definition,
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

    const stepGrants = await deps.db.transaction(async (tx) => {
      if (
        (await lockWorkflowRunState(tx, deploymentId, deploymentId)) !==
          "running" ||
        (await lockWorkflowRunState(tx, deploymentId, runId)) === "terminal"
      ) {
        return null;
      }
      return commitRunGrants(
        {
          db: deps.db,
          tenantId,
          deploymentId,
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
