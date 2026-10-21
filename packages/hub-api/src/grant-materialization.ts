import { authorizeAction } from "@intx/authz";
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
  // Timestamp stamped onto materialized rows and used to compute the invoker
  // grant expiry.
  now: Date;
};

// A rejection carries the wire error shape and HTTP status the caller returns
// verbatim. `insufficient_grants` is a 403; `not_launchable` and
// `deployer_unavailable` are 409s.
export type GrantMaterializationRejection = {
  status: 403 | 409;
  code: "insufficient_grants" | "not_launchable" | "deployer_unavailable";
  message: string;
};

export type ResolveGrantMaterializationResult =
  | { ok: true; grantRows: MaterializedGrantRow[] }
  | { ok: false; rejection: GrantMaterializationRejection };

// Invoker-delegated grants are short-lived: they expire a day after the run's
// first trigger, and nothing renews them, so a delegation outlives a change
// to the deployer's grants by a day at most.
export const INVOKER_GRANT_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * Resolve an agent's grant requirements into the grant rows to materialize on
 * the target principal.
 *
 * Pure over its inputs: it runs creator- and invoker-sourced delegation
 * checks with `authorizeAction` against the supplied collected grants and
 * accumulates the rows to insert. A delegation check has no request to
 * evaluate a condition against, so it evaluates conditions pessimistically:
 * a delegator's conditioned deny or ask refuses the requirement, and its
 * conditioned allow does not satisfy it. The rows are frozen, so a run never
 * holds more than its delegator holds without conditions.
 *
 * Only `system`/`role`/`creator` grants can be delegated, so invoker-origin
 * allows are filtered out of the invoker's delegatable set before resolving
 * invoker-sourced requirements. An invoker-origin deny or ask stays in the
 * set, because it still restricts the invoker. Nothing is written to the
 * database; the caller inserts the returned rows.
 *
 * Returns a rejection instead of rows when the creator or invoker lacks the
 * authority a requirement demands (403 `insufficient_grants`), or when a
 * requirement carries an unrecognized source (409 `not_launchable`).
 */
export async function resolveGrantMaterialization({
  tenantId,
  targetPrincipalId,
  grantRequirements,
  invokerGrants,
  creatorGrants,
  now,
}: ResolveGrantMaterializationArgs): Promise<ResolveGrantMaterializationResult> {
  // Only system/role/creator grants can be delegated: an invoker-origin allow
  // cannot be re-delegated. An invoker-origin deny or ask still restricts what
  // the invoker holds, so it stays in the set.
  const delegatableInvokerGrants = invokerGrants.filter(
    (g) => g.origin !== "invoker" || g.effect === "deny" || g.effect === "ask",
  );

  const invokerExpiresAt = new Date(now.getTime() + INVOKER_GRANT_TTL_MS);

  const grantRows: MaterializedGrantRow[] = [];

  for (const req of grantRequirements) {
    const effect = req.effect ?? "allow";

    if (req.source === "creator") {
      const decision = await authorizeAction(
        creatorGrants,
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
      grantRows.push(
        makeGrantRow({
          tenantId,
          principalId: targetPrincipalId,
          resource: req.resource,
          action: req.action,
          effect,
          conditions: req.conditions ?? null,
          origin: "creator",
          expiresAt: null,
          now,
        }),
      );
    } else if (req.source === "invoker") {
      const decision = await authorizeAction(
        delegatableInvokerGrants,
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
      grantRows.push(
        makeGrantRow({
          tenantId,
          principalId: targetPrincipalId,
          resource: req.resource,
          action: req.action,
          effect,
          conditions: req.conditions ?? null,
          origin: "invoker",
          expiresAt: invokerExpiresAt,
          now,
        }),
      );
    } else {
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
  }

  return { ok: true, grantRows };
}
