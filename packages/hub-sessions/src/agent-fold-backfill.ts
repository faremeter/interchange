// Core of the run-once agent->workflow-definition fold.
//
// Lifts each legacy `agent` (and each native `workflow`-kind asset) into a
// `workflow_definition` + `workflow_definition_version` row. The fold is ROWS
// ONLY: the definition body is NOT persisted here. An agent-origin definition's
// `asset_id` is null and its body is synthesized on the fly at deploy time from
// the still-live `agent` row (via `synthesizeFoldedWorkflow`), so there is one
// source of truth during the transition and no drifting frozen copy. This
// module therefore needs only a `db` handle -- no RepoStore, no asset writes.
//
// It lives here rather than beside its `bin/db-backfill` entry so both the
// entry and the deploy path can share the same agent->definition fold: the
// preflight runs the exact synthesis the deploy-time hydrate will run, and a
// tests/db suite can exercise it against a real database.

import { eq, isNotNull, isNull } from "drizzle-orm";

import type { DB } from "@intx/db";
import {
  createGrantStore,
  parseAgentRow,
  parseAgentVersionRow,
  resolveDefinitionIdForAsset,
  resolveInferencePreferences,
} from "@intx/db";
import {
  agent,
  agentVersion,
  asset,
  tenant,
  workflowDefinition,
  workflowDefinitionVersion,
  workflowDeployment,
  workflowRun,
} from "@intx/db/schema";
import type { GrantStore } from "@intx/types/authz";
import { generateId } from "@intx/hub-common";
import { ensureWorkflowDefinitionForAsset } from "./workflow-definition-ensure";
import {
  deriveDeploymentAddress,
  synthesizeFoldedWorkflow,
} from "@intx/workflow-deploy";

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
  /** Deployment-anchored runs given their deployment's definitionId. */
  readonly nativeRunsAnchored: number;
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
  // Runs the definitions the two folds above created, so it must come last.
  const runResult = await backfillNativeRunDefinitions(db);
  return { ...agentResult, ...workflowResult, ...runResult };
}

/**
 * Give each deployment-anchored run its deployment's definitionId, so a run
 * carries its definition directly rather than only through its deployment.
 * Folded (agent-origin) runs already set it, so they are skipped by the
 * `definition_id IS NULL` guard, which also makes a re-run a no-op. A run whose
 * deployment's asset was never folded resolves to nothing and is left null --
 * the honest value; it keeps anchoring on `deploymentId` until that asset is
 * folded. Definitions per asset are few, so their lookups are cached.
 */
async function backfillNativeRunDefinitions(
  db: DB["db"],
): Promise<Pick<BackfillSummary, "nativeRunsAnchored">> {
  const pending = await db
    .select({
      runId: workflowRun.id,
      definitionAssetId: workflowDeployment.definitionAssetId,
    })
    .from(workflowRun)
    .innerJoin(
      workflowDeployment,
      eq(workflowRun.deploymentId, workflowDeployment.id),
    )
    .where(isNull(workflowRun.definitionId));

  const definitionByAsset = new Map<string, string | null>();
  let nativeRunsAnchored = 0;

  for (const { runId, definitionAssetId } of pending) {
    let definitionId = definitionByAsset.get(definitionAssetId);
    if (definitionId === undefined) {
      definitionId = await resolveDefinitionIdForAsset(db, definitionAssetId);
      definitionByAsset.set(definitionAssetId, definitionId);
    }
    if (definitionId === null) {
      continue;
    }
    await db
      .update(workflowRun)
      .set({ definitionId })
      .where(eq(workflowRun.id, runId));
    nativeRunsAnchored += 1;
  }

  return { nativeRunsAnchored };
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
 * A faithful dry run of the deploy-time transform, so an agent that cannot be
 * synthesized surfaces in the preflight rather than at deploy. Resolves the
 * inference preferences (throws when the requirements resolve to no source) and
 * runs the synthesizer (throws on a null system prompt). The synthesized
 * definition is discarded -- the fold persists no body; the value of running it
 * here is the throw.
 */
async function synthesizeForPreflight(
  db: DB["db"],
  grantStore: GrantStore,
  domains: Map<string, string>,
  a: ReturnType<typeof parseAgentRow>,
): Promise<void> {
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

  synthesizeFoldedWorkflow({
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
  });
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
