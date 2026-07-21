import { evaluateGrants } from "@intx/authz";
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

// An ad-hoc grant the invoker asks to delegate at launch, resolved against
// the invoker's own authority. Structurally the launch request's
// `invokerGrants` entries.
export type AdHocInvokerGrant = {
  resource: string;
  action: string;
  effect?: GrantEffect;
  conditions?: Record<string, unknown> | null;
};

export type ResolveGrantMaterializationArgs = {
  tenantId: string;
  // The principal the materialized grants are written onto.
  targetPrincipalId: string;
  // The definition's grant requirements, already parsed and validated by the
  // caller. Each is resolved against the creator's or invoker's authority.
  grantRequirements: readonly GrantRequirement[];
  // Ad-hoc grants the invoker asks to delegate at launch, resolved against
  // the invoker's authority in addition to the definition's requirements.
  adHocInvokerGrants: readonly AdHocInvokerGrant[];
  // The invoker's collected grants. Used to authorize invoker-sourced
  // requirements and ad-hoc invoker grants.
  invokerGrants: GrantRule[];
  // The creator's collected grants. Used to authorize creator-sourced
  // requirements. Empty when the definition declares no creator requirements.
  creatorGrants: GrantRule[];
  // Timestamp stamped onto materialized rows and used to compute the invoker
  // grant expiry.
  now: Date;
};

// A rejection carries the wire error shape and HTTP status the caller returns
// verbatim. `insufficient_grants` is a 403; `not_launchable` is a 409.
export type GrantMaterializationRejection = {
  status: 403 | 409;
  code: "insufficient_grants" | "not_launchable";
  message: string;
};

export type ResolveGrantMaterializationResult =
  | { ok: true; grantRows: MaterializedGrantRow[] }
  | { ok: false; rejection: GrantMaterializationRejection };

// Invoker-delegated grants are short-lived: they expire a day after launch so
// a delegation does not outlive the session that motivated it.
export const INVOKER_GRANT_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * Resolve an agent's grant requirements and ad-hoc invoker grants into the
 * grant rows to materialize on the target principal.
 *
 * Pure over its inputs: it runs creator- and invoker-sourced delegation
 * checks with `evaluateGrants` against the supplied collected grants and
 * accumulates the rows to insert. Only `system`/`role`/`creator` grants can be
 * delegated, so invoker-origin grants are filtered out of the invoker's
 * delegatable set before resolving invoker-sourced requirements. Nothing is
 * written to the database; the caller inserts the returned rows.
 *
 * Returns a rejection instead of rows when the creator or invoker lacks the
 * authority a requirement demands (403 `insufficient_grants`), or when a
 * requirement carries an unrecognized source (409 `not_launchable`).
 */
export async function resolveGrantMaterialization({
  tenantId,
  targetPrincipalId,
  grantRequirements,
  adHocInvokerGrants,
  invokerGrants,
  creatorGrants,
  now,
}: ResolveGrantMaterializationArgs): Promise<ResolveGrantMaterializationResult> {
  // Only system/role/creator grants can be delegated. Invoker-sourced
  // grants cannot be transitively re-delegated.
  const delegatableInvokerGrants = invokerGrants.filter(
    (g) => g.origin !== "invoker",
  );

  const invokerExpiresAt = new Date(now.getTime() + INVOKER_GRANT_TTL_MS);

  const grantRows: MaterializedGrantRow[] = [];

  for (const req of grantRequirements) {
    const effect = req.effect ?? "allow";

    if (req.source === "creator") {
      const result = await evaluateGrants(
        creatorGrants,
        req.resource,
        req.action,
      );
      if (result.effect !== "allow") {
        return {
          ok: false,
          rejection: {
            status: 403,
            code: "insufficient_grants",
            message: `Creator lacks authority to delegate ${req.resource}/${req.action}`,
          },
        };
      }
      grantRows.push({
        id: generateId("grant"),
        tenantId,
        principalId: targetPrincipalId,
        resource: req.resource,
        action: req.action,
        effect,
        conditions: req.conditions ?? null,
        origin: "creator",
        expiresAt: null,
        createdAt: now,
        updatedAt: now,
      });
    } else if (req.source === "invoker") {
      const result = await evaluateGrants(
        delegatableInvokerGrants,
        req.resource,
        req.action,
      );
      if (result.effect !== "allow") {
        return {
          ok: false,
          rejection: {
            status: 403,
            code: "insufficient_grants",
            message: `Invoker lacks authority for ${req.resource}/${req.action}`,
          },
        };
      }
      grantRows.push({
        id: generateId("grant"),
        tenantId,
        principalId: targetPrincipalId,
        resource: req.resource,
        action: req.action,
        effect,
        conditions: req.conditions ?? null,
        origin: "invoker",
        expiresAt: invokerExpiresAt,
        createdAt: now,
        updatedAt: now,
      });
    } else {
      return {
        ok: false,
        rejection: {
          status: 409,
          code: "not_launchable",
          message: `Unknown grant requirement source: ${req.source}`,
        },
      };
    }
  }

  // Process ad-hoc invoker grants from the launch request.
  for (const ig of adHocInvokerGrants) {
    const effect = ig.effect ?? "allow";
    const result = await evaluateGrants(
      delegatableInvokerGrants,
      ig.resource,
      ig.action,
    );
    if (result.effect !== "allow") {
      return {
        ok: false,
        rejection: {
          status: 403,
          code: "insufficient_grants",
          message: `Invoker lacks authority for ${ig.resource}/${ig.action}`,
        },
      };
    }
    grantRows.push({
      id: generateId("grant"),
      tenantId,
      principalId: targetPrincipalId,
      resource: ig.resource,
      action: ig.action,
      effect,
      conditions: ig.conditions ?? null,
      origin: "invoker",
      expiresAt: invokerExpiresAt,
      createdAt: now,
      updatedAt: now,
    });
  }

  return { ok: true, grantRows };
}
