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

import { and, eq } from "drizzle-orm";
import { type } from "arktype";

import {
  asset,
  grant as grantTable,
  principal as principalTable,
  workflowDeployment,
} from "@intx/db/schema";
import type { DB } from "@intx/db";
import { createWorkflowRunStore } from "@intx/db";
import type { GrantStore, GrantRule } from "@intx/types/authz";
import { GrantRequirement, type GrantEffect } from "@intx/types";
import type { RunGrantsFrame } from "@intx/types/sidecar";
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
 * definition-pure: identical across every run of the deployment, so the walk
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
 * database write happens here -- `commitRunGrants` performs it once the
 * caller has accepted delivery.
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
  runId: string;
  runPrincipalId: string;
  now: Date;
  grantRows: MaterializedGrantRow[];
};

/**
 * Idempotently commit a run's principal, run row, and grant rows in one
 * transaction, keyed on the run id -- stable for a mail run (the
 * Message-ID) and unique for an external trigger.
 *
 * The transaction opens with an already-materialized GUARD: it looks up
 * the principal for `(tenantId, kind: "workflow", refId: runId)` and, if
 * one is present, returns without writing anything. A redelivery is thus a
 * true no-op that neither throws nor duplicates rows -- the grant table
 * has no natural unique key, so re-running the inserts would otherwise
 * append a second copy of every grant. The guard reads inside the
 * transaction, so a concurrent first commit either has not yet inserted
 * the principal (this call proceeds and the principal's unique constraint
 * serializes the race) or has committed it (this call sees it and skips).
 *
 * On the first commit the `runPrincipalId` is derived deterministically
 * from `(tenantId, runId)` by the caller, so the principal insert and the
 * grant rows that reference it agree on the id even across a retry.
 */
export async function commitRunGrants(
  args: CommitRunGrantsArgs,
): Promise<void> {
  const workflowRunStore = createWorkflowRunStore(args.db);
  await args.db.transaction(async (tx) => {
    const existing = await tx
      .select({ id: principalTable.id })
      .from(principalTable)
      .where(
        and(
          eq(principalTable.tenantId, args.tenantId),
          eq(principalTable.kind, "workflow"),
          eq(principalTable.refId, args.runId),
        ),
      )
      .limit(1);
    if (existing.length > 0) return;

    await tx.insert(principalTable).values({
      id: args.runPrincipalId,
      tenantId: args.tenantId,
      kind: "workflow",
      refId: args.runId,
      status: "active",
      createdAt: args.now,
      updatedAt: args.now,
    });
    await workflowRunStore.anchorWithPrincipal(
      {
        id: args.runId,
        deploymentId: args.deploymentId,
        tenantId: args.tenantId,
        principalId: args.runPrincipalId,
        status: "running",
      },
      tx,
    );
    for (const g of args.grantRows) {
      await tx.insert(grantTable).values(g);
    }
  });
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
 * The materializer STAGES only -- it does not write to the database. It
 * returns a discriminated result the caller orders against delivery,
 * committing the run principal, run row, and grants (deterministic
 * principal id, idempotent on the runId) after the mail is accepted. This
 * mirrors the external trigger route's commit-last discipline so a
 * delivery that never lands leaves no orphaned authz state.
 */
export function createMailTriggeredRunGrantsMaterializer(
  deps: MailTriggeredRunGrantsDeps,
): (args: {
  agentAddress: string;
  runId: string;
}) => Promise<MailTriggeredRunGrantsResult> {
  return async ({ agentAddress, runId }) => {
    const deployment = await deps.db.query.workflowDeployment.findFirst({
      where: and(
        eq(workflowDeployment.address, agentAddress),
        eq(workflowDeployment.status, "deployed"),
      ),
    });
    if (deployment === undefined) return { outcome: "skip" };

    const definition = await hydrateDefinition(
      deps.assetService,
      deployment.definitionAssetId,
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
      deployment.tenantId,
      deployment.definitionAssetId,
    );
    const creatorGrants = await collectCreatorGrants(
      deps.grantStore,
      deployment.tenantId,
      creatorPrincipalId,
      creatorRequirements,
    );

    // Derive the run principal id from `(tenantId, runId)` so a redelivery
    // (same Message-ID runId) mints the same principal id and its
    // conflict-noop reinsert leaves the grant rows pointing at the id that
    // is actually present.
    const runPrincipalId = await deriveRunPrincipalId(
      deployment.tenantId,
      runId,
    );
    const now = new Date();
    const staged = await stageRunGrants({
      definition,
      tenantId: deployment.tenantId,
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

    // The DB `workflow_run.deployment_id` is a foreign key to
    // `workflow_deployment.id`, so it carries the deployment row's real id
    // -- NOT the address-derived substrate repo slug, which keys the
    // on-disk workflow-run repo and the wire routing, not this FK.
    const deploymentId = deployment.id;
    return {
      outcome: "materialized",
      stepGrants: staged.stepGrants,
      commit: () =>
        commitRunGrants({
          db: deps.db,
          tenantId: deployment.tenantId,
          deploymentId,
          runId,
          runPrincipalId,
          now,
          grantRows: staged.grantRows,
        }),
    };
  };
}
