import { isNamespaceResource } from "@intx/types/authz";

import type { ConditionRegistry, GrantRule, MatchedGrant } from "./types";
import { EFFECT_PRIORITY, evaluateGrants } from "./evaluate";
import { patternSpecificity } from "./specificity";
import {
  coordResource,
  coordTypeTier,
  type CoordGrammar,
  type Coordinate,
} from "./coord";

/**
 * Pick the grant that decides a coordinate-namespace check.
 *
 * This composes the existing grant-evaluation engine; it does not
 * reimplement authorization.
 *
 * - The grants are filtered to `grammar.namespace` before evaluation. A
 *   broad `*` or `mail.*` grant matches a resource in the namespace under
 *   `matchPattern`, so without the filter such a grant would decide every
 *   check in the namespace.
 * - Each coordinate is a distinct resource string; `matchPattern` has no
 *   alternation, so each coordinate is evaluated on its own. The grants that
 *   match any coordinate then form ONE set, the same as a principal's direct
 *   and role grants form one set in `evaluateGrants`, and one grant wins. The
 *   order is: the coordinate tier the grant names (its position in
 *   `grammar.coordTypes`, with a grant that names no coord type such as
 *   `<namespace>:*` lowest), then the specificity of the coordinate pattern,
 *   then effect priority (deny > ask > allow). The action and the spelling of
 *   a namespace-wide pattern do not count, so a deny that matches the
 *   coordinate beats an allow of the same coordinate pattern whatever action
 *   either spells.
 * - The tier comes from the literal type segment of the grant. A grant whose
 *   type segment holds a wildcard (`<namespace>:principal*`,
 *   `<namespace>:*`) names no coord type, so it ranks as namespace-wide,
 *   below every grant that names a coordinate. A wildcard id does not lower
 *   the tier.
 *
 * Without a condition `registry` the check cannot tell whether a grant's
 * condition holds. So when no registry is supplied and any grant in the
 * namespace carries conditions, this THROWS rather than guess, and the
 * misconfiguration surfaces loudly.
 *
 * Returns the winning grant, or `null` when no grant in the namespace
 * matched any coordinate.
 */
export async function evaluateCoordinateGrants<T extends string>(args: {
  grammar: CoordGrammar<T>;
  coordinates: readonly Coordinate<T>[];
  grants: GrantRule[];
  registry: ConditionRegistry | undefined;
}): Promise<MatchedGrant | null> {
  const { grammar, coordinates, grants, registry } = args;

  const namespaceGrants = grants.filter((g) =>
    isNamespaceResource(grammar.namespace, g.resource),
  );

  if (registry === undefined) {
    const conditioned = namespaceGrants.find(
      (g) => g.conditions !== null && Object.keys(g.conditions).length > 0,
    );
    if (conditioned !== undefined) {
      throw new Error(
        `${grammar.namespace} grant ${JSON.stringify(conditioned.resource)} carries conditions but no condition registry was supplied; pass a registry or author ${grammar.namespace} grants unconditioned`,
      );
    }
  }

  // With no registry, the guard above leaves no conditioned grant, so the
  // pessimistic mode gives the same result as a registry would.
  const opts = { conditions: registry ?? ("pessimistic" as const) };

  // A namespace-wide grant matches every coordinate, so key the pooled set on
  // grant id to count it once.
  const matched = new Map<string, MatchedGrant>();
  for (const coord of coordinates) {
    const result = await evaluateGrants(
      namespaceGrants,
      coordResource(grammar, coord.coordType, coord.id),
      grammar.action,
      opts,
    );
    for (const g of result.matchingGrants) matched.set(g.id, g);
  }

  let winner: MatchedGrant | null = null;
  for (const g of matched.values()) {
    if (winner === null || outranks(grammar, g, winner)) winner = g;
  }
  return winner;
}

// A grant only matches coordinates of the type it names, so the type segment
// of its resource is also the tier of the coordinate it matched.
function grantTier<T extends string>(
  grammar: CoordGrammar<T>,
  resource: string,
): number {
  const coordType = resource.split(":")[1];
  return coordType === undefined ? 0 : coordTypeTier(grammar, coordType);
}

// The specificity of the coordinate a grant names: its resource without the
// namespace prefix and the `:` separators. The action does not count: every
// grant here already matches the grammar's one action, so how it spells that
// action (`send` or `*`) says nothing about what it covers. The separators do
// not count either, so `<namespace>:*` and `<namespace>:*:*` rank the same.
function coordSpecificity<T extends string>(
  grammar: CoordGrammar<T>,
  resource: string,
): number {
  const coordPart = resource
    .slice(grammar.namespace.length + 1)
    .replaceAll(":", "");
  // The bare namespace names no coordinate and matches none; it must not
  // score as an exact match.
  return coordPart === "" ? 0 : patternSpecificity(coordPart);
}

function outranks<T extends string>(
  grammar: CoordGrammar<T>,
  a: MatchedGrant,
  b: MatchedGrant,
): boolean {
  const tierDiff =
    grantTier(grammar, a.resource) - grantTier(grammar, b.resource);
  if (tierDiff !== 0) return tierDiff > 0;
  const specDiff =
    coordSpecificity(grammar, a.resource) -
    coordSpecificity(grammar, b.resource);
  if (specDiff !== 0) return specDiff > 0;
  return EFFECT_PRIORITY[a.effect] > EFFECT_PRIORITY[b.effect];
}
