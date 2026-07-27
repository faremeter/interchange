// Core of the run-once agent->workflow-definition fold, in two phases.
//
// Phase 1 -- the rows-only fold (`runBackfill`). Lifts each legacy `agent`
// (and each native `workflow`-kind asset) into a `workflow_definition` +
// `workflow_definition_version` row, mirroring the agent's row-level fields --
// including its `model_requirements` manifest, which a folded launch resolves
// against the live catalog without the agent row. This phase is ROWS ONLY: the
// definition BODY (system prompt, tools, inference) is NOT persisted; an
// agent-origin definition's `asset_id` is null and the body is still read from
// the still-live `agent` row at launch until phase 2 freezes it. It needs only
// a `db` handle -- no RepoStore, no asset writes -- which is what keeps it cheap
// to re-run repeatedly while the fold rolls out.
//
// Phase 2 -- body materialization (`materializeFoldedBodies`). The one-time
// cliff run just before the `agent` table is dropped: it freezes each folded
// definition's synthesized `workflow.json` into a `workflow`-kind asset and
// sets `workflow_definition.asset_id`, so the body survives the agent row's
// retirement and a folded definition hydrates exactly like a native one. This
// phase deliberately mints the frozen copy phase 1 avoided -- it needs an
// `AssetService` (a RepoStore) and is not part of `runBackfill`, so the
// transitional re-runs never carry the repo substrate.
//
// The two phases share `assembleFoldedInput`, the single resolution of an
// agent's fields into the synthesizer's input, so the body phase 2 freezes is
// assembled identically to the one phase 1's preflight blesses.
//
// It lives here rather than beside its `bin/db-backfill` entry so both the
// entry and the deploy path can share the same agent->definition fold: the
// preflight runs the exact synthesis the deploy-time hydrate will run, and a
// tests/db suite can exercise it against a real database.

import { and, eq, isNotNull, isNull } from "drizzle-orm";

import type { DB } from "@intx/db";
import {
  createGrantStore,
  parseAgentRow,
  parseAgentVersionRow,
  resolveInferencePreferences,
} from "@intx/db";
import {
  agent,
  agentVersion,
  asset,
  tenant,
  workflowDefinition,
  workflowDefinitionVersion,
} from "@intx/db/schema";
import type { GrantStore } from "@intx/types/authz";
import { generateId } from "@intx/hub-common";
import { ensureWorkflowDefinitionForAsset } from "./workflow-definition-ensure";
import {
  ASSET_NAME_PATTERN,
  AssetServiceError,
  DEFAULT_ASSET_REF,
  type AssetService,
} from "./asset-service";
import type { Principal } from "./repo-store";
import { WORKFLOW_JSON_PATH } from "./workflow-kind";
import {
  deriveDeploymentAddress,
  synthesizeFoldedWorkflow,
  type FoldedWorkflowInput,
} from "@intx/workflow-deploy";

// The hub principal authorized to write a `workflow`-kind asset repo
// (`workflowAuthorize` grants `kind: "hub"` full write). Materialization writes
// the frozen body as the hub, matching the native workflow deploy path.
const HUB_PRINCIPAL: Principal = { kind: "hub" };

/** An agent that cannot be folded, with the reason its dry-run synthesis threw. */
export interface UndeployableAgent {
  readonly agentId: string;
  readonly name: string;
  readonly reason: string;
}

/**
 * Thrown by the preflight when one or more agents cannot be folded. Carries the
 * complete manifest so the whole failure set surfaces at once, and signals that
 * NO rows were written -- the disposition of the listed agents (fix, delete, or
 * consciously skip) is an operator decision made with this manifest in hand.
 */
export class BackfillPreflightError extends Error {
  constructor(readonly undeployable: readonly UndeployableAgent[]) {
    super(
      `agent fold preflight: ${undeployable.length} agent(s) cannot be folded; no rows written`,
    );
    this.name = "BackfillPreflightError";
  }
}

export interface BackfillSummary {
  readonly agentsFolded: number;
  readonly agentsSkipped: number;
  readonly workflowAssetsFolded: number;
  readonly workflowAssetsSkipped: number;
}

/** A folded definition whose body could not be materialized, with the reason. */
export interface UnmaterializableDefinition {
  readonly definitionId: string;
  readonly agentId: string;
  readonly name: string;
  readonly reason: string;
}

/**
 * Thrown by `materializeFoldedBodies` when one or more folded definitions could
 * not have their body frozen. Carries the complete manifest so the whole
 * failure set surfaces in one pass. Unlike the rows-only preflight this is NOT
 * all-or-nothing: definitions that did materialize stay committed (freezing a
 * body is additive and independent), so this reports the stragglers the
 * operator must resolve before the agent table can be dropped, not a rollback.
 */
export class MaterializeError extends Error {
  constructor(
    readonly unmaterializable: readonly UnmaterializableDefinition[],
  ) {
    super(
      `agent fold materialize: ${unmaterializable.length} folded definition(s) could not be materialized`,
    );
    this.name = "MaterializeError";
  }
}

export interface MaterializeSummary {
  readonly bodiesMaterialized: number;
  readonly bodiesSkipped: number;
}

/**
 * Fold every legacy agent and native workflow asset into definition rows.
 * Idempotent: re-running skips agents/assets that already have a definition
 * (guarded by the `origin_agent_id` / `asset_id` query plus their unique
 * indexes). Aborts loud via `BackfillPreflightError` -- writing nothing -- if
 * any agent is undeployable.
 */
export async function runBackfill(db: DB["db"]): Promise<BackfillSummary> {
  const agentResult = await foldAgents(db);
  const workflowResult = await foldWorkflowAssets(db);
  return { ...agentResult, ...workflowResult };
}

async function foldAgents(
  db: DB["db"],
): Promise<Pick<BackfillSummary, "agentsFolded" | "agentsSkipped">> {
  const agents = (await db.select().from(agent)).map(parseAgentRow);

  // Only agents that do not already have a definition are in scope. Scoping the
  // preflight to these -- rather than every agent -- keeps a re-run from
  // aborting because an already-folded agent's source data has since degraded
  // (an offering or creator grant removed) so that it would no longer
  // synthesize; such an agent is never re-folded, so it must not gate the ones
  // that still need folding.
  const foldedOriginIds = new Set(
    (
      await db
        .select({ originAgentId: workflowDefinition.originAgentId })
        .from(workflowDefinition)
        .where(isNotNull(workflowDefinition.originAgentId))
    ).map((row) => row.originAgentId),
  );
  const pending = agents.filter((a) => !foldedOriginIds.has(a.id));
  const agentsSkipped = agents.length - pending.length;

  // Preflight: prove every pending agent can be folded before writing anything.
  // An agent whose deploy-time synthesis would throw (null system prompt, or
  // model requirements that resolve to no source), or that has no version rows
  // to mirror, cannot be folded into a coherent definition; inserting one would
  // manufacture a row that can never hydrate or a definition with no version.
  // Collect the whole failing set and, if it is non-empty, abort having written
  // nothing. The versions are read here and reused below so the preflight and
  // the write agree on exactly which rows land.
  const grantStore = createGrantStore(db);
  const domains = await tenantDomains(db);
  const undeployable: UndeployableAgent[] = [];
  const versionsByAgent = new Map<
    string,
    ReturnType<typeof parseAgentVersionRow>[]
  >();
  for (const a of pending) {
    try {
      await synthesizeForPreflight(db, grantStore, domains, a);
      const versions = (
        await db
          .select()
          .from(agentVersion)
          .where(eq(agentVersion.agentId, a.id))
      ).map(parseAgentVersionRow);
      if (versions.length === 0) {
        throw new Error("agent has no agent_version rows to mirror");
      }
      versionsByAgent.set(a.id, versions);
    } catch (err) {
      undeployable.push({
        agentId: a.id,
        name: a.name,
        reason: err instanceof Error ? err.message : String(err),
      });
    }
  }
  if (undeployable.length > 0) {
    throw new BackfillPreflightError(undeployable);
  }

  let agentsFolded = 0;
  for (const a of pending) {
    const versions = versionsByAgent.get(a.id);
    if (versions === undefined) {
      throw new Error(`internal: no preflighted versions for agent ${a.id}`);
    }
    const definitionId = generateId("workflowDefinition");
    // One transaction per agent: the definition and its versions land together
    // or not at all, and a failure on one agent leaves the already-folded ones
    // committed (the run is re-runnable). Rows-only writes are what make this
    // single-store transaction possible.
    await db.transaction(async (tx) => {
      await tx.insert(workflowDefinition).values({
        id: definitionId,
        tenantId: a.tenantId,
        creatorPrincipalId: a.creatorPrincipalId,
        assetId: null,
        originAgentId: a.id,
        name: a.name,
        description: a.description,
        grantRequirements: a.grantRequirements,
        // Mirror the agent's model requirements onto the definition so a folded
        // launch resolves its inference sources without the agent row. Copied
        // verbatim (null stays null) -- the launch re-resolves it against the
        // live catalog each launch, exactly as the agent path did.
        modelRequirements: a.modelRequirements,
        currentVersion: a.currentVersion,
        status: a.status,
      });
      for (const v of versions) {
        await tx.insert(workflowDefinitionVersion).values({
          id: generateId("workflowDefinitionVersion"),
          definitionId,
          version: v.version,
          status: v.status,
        });
      }
    });
    agentsFolded += 1;
  }
  return { agentsFolded, agentsSkipped };
}

/**
 * Resolve a legacy agent's fields into the synthesizer's input: the impure
 * resolution (inference preferences against the tenant catalog + creator
 * grants, the deployment mail address) the pure `synthesizeFoldedWorkflow`
 * requires supplied. Shared by the preflight (which synthesizes-to-throw) and
 * materialization (which synthesizes-to-persist) so the body a launch
 * eventually hydrates is assembled identically to the one the preflight
 * blesses. Throws when the requirements resolve to no source (or collapse
 * ambiguously) or the tenant has no domain.
 */
async function assembleFoldedInput(
  db: DB["db"],
  grantStore: GrantStore,
  domains: Map<string, string>,
  a: ReturnType<typeof parseAgentRow>,
): Promise<FoldedWorkflowInput> {
  // A null requirement list flows through as `[]`, which is exactly what the
  // resolver rejects (`no_requirements`) -- the fail-loud path, not a fallback.
  const requirements = a.modelRequirements ?? [];
  const creatorGrants = await grantStore.collectGrantsInChain(
    a.creatorPrincipalId,
    a.tenantId,
  );
  const inferencePreferences = await resolveInferencePreferences(
    db,
    a.tenantId,
    requirements,
    creatorGrants,
  );

  const domain = domains.get(a.tenantId);
  if (domain === undefined) {
    throw new Error(`tenant ${a.tenantId} has no domain`);
  }

  return {
    workflowId: `wf_${a.id}`,
    mailAddress: deriveDeploymentAddress({
      deploymentId: a.id,
      deploymentDomain: domain,
    }),
    systemPrompt: a.systemPrompt,
    description: a.description,
    inferencePreferences,
    toolPackagePins: a.toolPackages,
    ...(a.grantRequirements !== null
      ? { grantRequirements: a.grantRequirements }
      : {}),
  };
}

/**
 * A faithful dry run of the deploy-time transform, so an agent that cannot be
 * synthesized surfaces in the preflight rather than at deploy. Runs the
 * synthesizer over the assembled input (which throws when the requirements
 * resolve to no source) -- it throws too on a null system prompt. The
 * synthesized definition is discarded -- the fold persists no body; the value
 * of running it here is the throw.
 */
async function synthesizeForPreflight(
  db: DB["db"],
  grantStore: GrantStore,
  domains: Map<string, string>,
  a: ReturnType<typeof parseAgentRow>,
): Promise<void> {
  synthesizeFoldedWorkflow(
    await assembleFoldedInput(db, grantStore, domains, a),
  );
}

async function tenantDomains(db: DB["db"]): Promise<Map<string, string>> {
  const rows = await db
    .select({ id: tenant.id, domain: tenant.domain })
    .from(tenant);
  return new Map(rows.map((r) => [r.id, r.domain]));
}

async function foldWorkflowAssets(
  db: DB["db"],
): Promise<
  Pick<BackfillSummary, "workflowAssetsFolded" | "workflowAssetsSkipped">
> {
  const assets = await db
    .select()
    .from(asset)
    .where(eq(asset.kind, "workflow"));

  let workflowAssetsFolded = 0;
  let workflowAssetsSkipped = 0;
  for (const workflowAsset of assets) {
    // A native workflow already carries its body in the asset it points at, so
    // this only projects a definition row (plus version "1") over it, via the
    // shared create-if-absent helper the deploy path uses -- one source for the
    // definition's shape. Each projection is its own transaction so a partial
    // failure never leaves a definition without its version.
    const { created } = await db.transaction((tx) =>
      ensureWorkflowDefinitionForAsset(tx, workflowAsset.id),
    );
    if (created) {
      workflowAssetsFolded += 1;
    } else {
      workflowAssetsSkipped += 1;
    }
  }
  return { workflowAssetsFolded, workflowAssetsSkipped };
}

/**
 * The deterministic `workflow`-kind asset name a folded agent's body is stored
 * under. Keyed on the globally-unique agent id, so it never collides across
 * tenants and a re-run resolves the same asset. `createAsset` requires
 * lowercase-kebab; agent ids are `agt_<hex>`, so replacing `_` with `-` yields
 * a valid name. Asserting the shape here fails loud if a future id shape ever
 * violates the assumption, rather than surfacing three layers in as a generic
 * `invalid_name`.
 */
function foldedAssetName(agentId: string): string {
  const name = `folded-${agentId.replace(/_/g, "-")}`;
  if (!ASSET_NAME_PATTERN.test(name)) {
    throw new Error(
      `cannot derive a workflow asset name for agent ${agentId}: ` +
        `${JSON.stringify(name)} is not a valid lowercase-kebab asset name`,
    );
  }
  return name;
}

/**
 * Resolve the `workflow`-kind asset that holds a folded agent's body, creating
 * it if absent. Find-or-create rather than create-then-catch: the normal
 * re-run path looks the asset up by its deterministic `(tenant, kind, name)`
 * and reuses it, so a crash between a prior run's asset creation and its
 * `asset_id` write is recovered without minting a second asset row. The
 * `duplicate_asset` catch is demoted to a guard against a concurrent racer
 * between this lookup and the create.
 */
async function findOrCreateWorkflowAsset(
  db: DB["db"],
  assetService: AssetService,
  a: ReturnType<typeof parseAgentRow>,
): Promise<string> {
  const name = foldedAssetName(a.id);

  const existing = await db
    .select({ id: asset.id })
    .from(asset)
    .where(
      and(
        eq(asset.tenantId, a.tenantId),
        eq(asset.kind, "workflow"),
        eq(asset.name, name),
      ),
    )
    .limit(1);
  const found = existing[0];
  if (found !== undefined) {
    return found.id;
  }

  try {
    const created = await assetService.createAsset({
      tenantId: a.tenantId,
      kind: "workflow",
      name,
      displayName: a.name,
      creatorPrincipalId: a.creatorPrincipalId,
    });
    return created.id;
  } catch (err) {
    if (err instanceof AssetServiceError && err.reason === "duplicate_asset") {
      // A racer created it between the lookup and the create; read back the
      // winner rather than fail.
      const raced = await db
        .select({ id: asset.id })
        .from(asset)
        .where(
          and(
            eq(asset.tenantId, a.tenantId),
            eq(asset.kind, "workflow"),
            eq(asset.name, name),
          ),
        )
        .limit(1);
      const winner = raced[0];
      if (winner === undefined) {
        throw new Error(
          `duplicate_asset for folded workflow ${name} but no row found on read-back`,
        );
      }
      return winner.id;
    }
    throw err;
  }
}

/**
 * Freeze each folded definition's body into a `workflow`-kind asset -- the
 * one-time cliff run just before the `agent` table is dropped. For every folded
 * definition whose `asset_id` is still null, re-assemble the synthesizer input
 * (identically to the preflight), synthesize the `workflow.json`, write it into
 * the asset repo, and bind the definition to that asset. Idempotent: a
 * definition that already has an `asset_id` is skipped, and the asset write plus
 * the `asset_id` set are ordered so a re-run recovers a partial prior run.
 *
 * Bodies are materialized independently; a failure on one collects into the
 * `MaterializeError` manifest without rolling back the ones that succeeded.
 *
 * A folded definition may mirror several `workflow_definition_version` rows, but
 * this writes a single `workflow.json` at the asset's HEAD: there is no
 * version->blob mapping anywhere (hydrate reads a fixed path at HEAD with no
 * version argument), and the agent row only ever had one current body, so the
 * historical versions never carried distinct bodies to freeze. All versions
 * hydrate from the one HEAD body.
 *
 * The frozen body snapshots the agent's credential-*authorized* candidate set
 * at fold time. After the launch path is rewired to hydrate from the asset, a
 * folded agent behaves like a native definition: its candidate set is fixed and
 * only credentials re-attach per deploy. This is the intended convergence, but
 * it means a credential grant added *after* the fold no longer surfaces the way
 * it does for a live agent instance, which re-resolves its full candidate set
 * on every launch. That is an accepted semantic change, not a regression.
 */
export async function materializeFoldedBodies(
  db: DB["db"],
  assetService: AssetService,
): Promise<MaterializeSummary> {
  const pending = await db
    .select({ definitionId: workflowDefinition.id, agentRow: agent })
    .from(workflowDefinition)
    .innerJoin(agent, eq(agent.id, workflowDefinition.originAgentId))
    .where(
      and(
        isNull(workflowDefinition.assetId),
        isNotNull(workflowDefinition.originAgentId),
      ),
    );

  const alreadyMaterialized = await db
    .select({ id: workflowDefinition.id })
    .from(workflowDefinition)
    .where(
      and(
        isNotNull(workflowDefinition.assetId),
        isNotNull(workflowDefinition.originAgentId),
      ),
    );
  const bodiesSkipped = alreadyMaterialized.length;

  const grantStore = createGrantStore(db);
  const domains = await tenantDomains(db);
  const unmaterializable: UnmaterializableDefinition[] = [];
  let bodiesMaterialized = 0;

  for (const row of pending) {
    const a = parseAgentRow(row.agentRow);
    try {
      const input = await assembleFoldedInput(db, grantStore, domains, a);
      const definition = synthesizeFoldedWorkflow(input);
      const body = JSON.stringify(definition, null, 2);
      const assetId = await findOrCreateWorkflowAsset(db, assetService, a);
      await assetService.populateAsset({
        assetId,
        ref: DEFAULT_ASSET_REF,
        tree: {
          files: { [WORKFLOW_JSON_PATH]: body },
          message: `Materialize folded agent ${a.id} workflow body`,
        },
        principal: HUB_PRINCIPAL,
      });
      // Bind the definition to the asset LAST: the body is proven written
      // before the definition points at it, so a crash before this leaves the
      // definition unmaterialized (recovered next run) rather than pointing at
      // an asset whose body never landed.
      await db
        .update(workflowDefinition)
        .set({ assetId })
        .where(eq(workflowDefinition.id, row.definitionId));
      bodiesMaterialized += 1;
    } catch (err) {
      unmaterializable.push({
        definitionId: row.definitionId,
        agentId: a.id,
        name: a.name,
        reason: err instanceof Error ? err.message : String(err),
      });
    }
  }

  if (unmaterializable.length > 0) {
    throw new MaterializeError(unmaterializable);
  }

  return { bodiesMaterialized, bodiesSkipped };
}
