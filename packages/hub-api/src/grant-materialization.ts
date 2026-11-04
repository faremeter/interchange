import { authorizeActionUntil } from "@intx/authz";
import type { GrantRule } from "@intx/types/authz";
import type { GrantEffect, GrantOrigin, GrantRequirement } from "@intx/types";
import { generateId } from "@intx/hub-common";

// A materialized grant row, ready to insert onto a target principal.
export type MaterializedGrantRow = {
  id: string;
  tenantId: string;
  principalId: string;
  resource: string;
  action: string;
  effect: GrantEffect;
  conditions: Record<string, unknown> | null;
  origin: GrantOrigin;
  expiresAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
};

// Build a materialized grant row: mint the id and stamp createdAt/updatedAt from
// a single `now`, so every materialization site -- creator/invoker delegation
// and the launch route's tenant-owned credential bindings -- constructs the
// identical row shape one way.
export function makeGrantRow(fields: {
  tenantId: string;
  principalId: string;
  resource: string;
  action: string;
  effect: GrantEffect;
  conditions: Record<string, unknown> | null;
  origin: GrantOrigin;
  expiresAt: Date | null;
  now: Date;
}): MaterializedGrantRow {
  return {
    id: generateId("grant"),
    tenantId: fields.tenantId,
    principalId: fields.principalId,
    resource: fields.resource,
    action: fields.action,
    effect: fields.effect,
    conditions: fields.conditions,
    origin: fields.origin,
    expiresAt: fields.expiresAt,
    createdAt: fields.now,
    updatedAt: fields.now,
  };
}

export type ResolveGrantMaterializationArgs = {
  tenantId: string;
  // The principal the materialized grants are written onto.
  targetPrincipalId: string;
  // The definition's grant requirements, already parsed and validated by the
  // caller. Each is resolved against the creator's or invoker's authority.
  grantRequirements: readonly GrantRequirement[];
  // The invoker's collected grants. Used to authorize invoker-sourced
  // requirements.
  invokerGrants: GrantRule[];
  // The creator's collected grants. Used to authorize creator-sourced
  // requirements. Empty when the definition declares no creator requirements.
  creatorGrants: GrantRule[];
  // Timestamp stamped onto materialized rows. A row that has expired by `now`
  // is not materialized.
  now: Date;
  // The latest expiry of an invoker-sourced `allow` row.
  invokerExpiresAt: Date;
};

// A rejection carries the wire error shape and HTTP status the caller returns
// verbatim. `insufficient_grants` is a 403; `not_launchable`,
// `deployer_unavailable`, and `workflow_run_principal_inactive` are 409s.
export type GrantMaterializationRejection = {
  status: 403 | 409;
  code:
    | "insufficient_grants"
    | "not_launchable"
    | "deployer_unavailable"
    | "workflow_run_principal_inactive";
  message: string;
};

export type ResolveGrantMaterializationResult =
  | { ok: true; grantRows: MaterializedGrantRow[] }
  | { ok: false; rejection: GrantMaterializationRejection };

// Invoker-delegated `allow` rows are short-lived: they expire a day after the
// run principal is created, at deploy, at the latest, and nothing renews them.
// An invoker `deny` or `ask` row is a floor and does not expire.
export const INVOKER_GRANT_TTL_MS = 24 * 60 * 60 * 1000;

export type ResolveRequirementGrantContext = Omit<
  ResolveGrantMaterializationArgs,
  "grantRequirements"
>;

/**
 * The expiry of a requirement's row with `effect`. An `allow` row ends at
 * `allowExpiresAt`, when the delegator's grants stop allowing the
 * requirement; an invoker `allow` row ends no later than `invokerExpiresAt`.
 * A `deny` or `ask` row is a floor: it restricts the run, so it has no
 * expiry.
 */
export function requirementRowExpiresAt(
  allowExpiresAt: Date | null,
  effect: GrantEffect,
): Date | null {
  return effect === "allow" ? allowExpiresAt : null;
}

export type ResolveRequirementGrantResult =
  | { ok: true; row: MaterializedGrantRow; allowExpiresAt: Date | null }
  | { ok: false; rejection: GrantMaterializationRejection };

function earlier(a: Date | null, b: Date): Date {
  return a !== null && a < b ? a : b;
}

// Only system/role/creator grants can be delegated: an invoker-origin allow
// cannot be re-delegated. An invoker-origin deny or ask still restricts what
// the invoker holds, so it stays in the set.
function delegatableInvokerGrants(invokerGrants: GrantRule[]): GrantRule[] {
  return invokerGrants.filter(
    (g) => g.origin !== "invoker" || g.effect === "deny" || g.effect === "ask",
  );
}

/**
 * Decide one grant requirement against its delegator's authority and build
 * the row to materialize for it.
 *
 * Pure over its inputs: it runs the creator- or invoker-sourced delegation
 * check with `authorizeActionUntil` against the supplied collected grants. A
 * delegation check has no request to evaluate a condition against, so it
 * evaluates conditions pessimistically: a delegator's conditioned deny or ask
 * refuses the requirement, and its conditioned allow does not satisfy it. A
 * run never holds more than its delegator holds without conditions.
 *
 * It takes the invoker's grants unfiltered and applies the invoker-origin
 * filter itself (see `delegatableInvokerGrants`).
 *
 * The row's expiry follows its effect (see `requirementRowExpiresAt`). The
 * result also carries the `allow` expiry, so a caller that changes the row's
 * effect can give it the expiry of the new effect.
 *
 * Returns a rejection instead of a row when the delegator lacks the authority
 * the requirement demands (403 `insufficient_grants`), or when the
 * requirement carries an unrecognized source (409 `not_launchable`).
 */
export async function resolveRequirementGrant(
  req: GrantRequirement,
  ctx: ResolveRequirementGrantContext,
): Promise<ResolveRequirementGrantResult> {
  const effect = req.effect ?? "allow";

  if (req.source === "creator") {
    const decision = await authorizeActionUntil(
      ctx.creatorGrants,
      req.resource,
      req.action,
      { conditions: "pessimistic" },
    );
    if (!decision.ok) {
      return {
        ok: false,
        rejection: {
          status: 403,
          code: "insufficient_grants",
          message: `Creator lacks authority to delegate ${req.resource}/${req.action} (${decision.reason})`,
        },
      };
    }
    const allowExpiresAt = decision.expiresAt;
    return {
      ok: true,
      row: makeGrantRow({
        tenantId: ctx.tenantId,
        principalId: ctx.targetPrincipalId,
        resource: req.resource,
        action: req.action,
        effect,
        conditions: req.conditions ?? null,
        origin: "creator",
        expiresAt: requirementRowExpiresAt(allowExpiresAt, effect),
        now: ctx.now,
      }),
      allowExpiresAt,
    };
  }

  if (req.source === "invoker") {
    const decision = await authorizeActionUntil(
      delegatableInvokerGrants(ctx.invokerGrants),
      req.resource,
      req.action,
      { conditions: "pessimistic" },
    );
    if (!decision.ok) {
      return {
        ok: false,
        rejection: {
          status: 403,
          code: "insufficient_grants",
          message: `Invoker lacks authority for ${req.resource}/${req.action} (${decision.reason})`,
        },
      };
    }
    const allowExpiresAt = earlier(decision.expiresAt, ctx.invokerExpiresAt);
    return {
      ok: true,
      row: makeGrantRow({
        tenantId: ctx.tenantId,
        principalId: ctx.targetPrincipalId,
        resource: req.resource,
        action: req.action,
        effect,
        conditions: req.conditions ?? null,
        origin: "invoker",
        expiresAt: requirementRowExpiresAt(allowExpiresAt, effect),
        now: ctx.now,
      }),
      allowExpiresAt,
    };
  }

  const _exhaustive: never = req.source;
  return {
    ok: false,
    rejection: {
      status: 409,
      code: "not_launchable",
      message: `Unknown grant requirement source: ${String(_exhaustive)}`,
    },
  };
}

/**
 * Resolve an agent's grant requirements into the grant rows to materialize on
 * the target principal.
 *
 * Decides each requirement with `resolveRequirementGrant` and stops at the
 * first rejection, so the caller writes every row or none. An invoker-sourced
 * `allow` row expires at `invokerExpiresAt` at the latest, and a row that has
 * already expired by `now` is left out: it would grant nothing.
 * Nothing is written to the database; the caller inserts the returned rows.
 */
export async function resolveGrantMaterialization({
  grantRequirements,
  ...ctx
}: ResolveGrantMaterializationArgs): Promise<ResolveGrantMaterializationResult> {
  const grantRows: MaterializedGrantRow[] = [];
  for (const req of grantRequirements) {
    const resolved = await resolveRequirementGrant(req, ctx);
    if (!resolved.ok) return resolved;
    const { expiresAt } = resolved.row;
    if (expiresAt !== null && expiresAt <= ctx.now) continue;
    grantRows.push(resolved.row);
  }

  return { ok: true, grantRows };
}
