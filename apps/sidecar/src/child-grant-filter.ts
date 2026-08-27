// Cap a spawned child's inherited grants at the capabilities the child body
// itself declares.
//
// A spawned child runs under the authority of the run that spawned it, but it
// must never hold MORE than the child body's own definition declares. Without
// this cap a child authorizes against the full parent grant set, so a
// parent-held `effect:`/`credential:` grant (neither is tool-gated) or a
// bare-name `tool:` collision the child body never declared would authorize a
// capability the child was never written to use. The parent stays the ceiling
// (a child can never exceed it); the child body's declared capability set is
// the shape it is narrowed to -- the same "a run's authority is bounded by its
// declared capabilities" model the top level enforces through its own walk.

import type { DirectorRegistry } from "@intx/agent";
import { matchPattern } from "@intx/authz";
import type { WorkflowDefinition } from "@intx/workflow";
import {
  walkCapabilities,
  type PluginToolDefinitions,
} from "@intx/workflow-deploy";

/**
 * The flat union of every grant-shape resource string the child body declares,
 * across all its steps AND every inline nested body (loop, onTrigger,
 * childWorkflow) the capability walk folds into its enclosing step.
 *
 * Pass the child's PRE-rewrite definition -- whose grandchildren are still
 * inline. The walk skips a `{ ref }` body, so a rewritten definition (children
 * lifted to refs) would omit a grandchild's declared resources; the union must
 * include them, because the caller uses this set to cap the grants it persists
 * as the ceiling a grandchild spawn filters against in turn.
 */
export function collectDeclaredResources(
  definition: WorkflowDefinition,
  directors: DirectorRegistry,
  pluginDefs: PluginToolDefinitions,
): ReadonlySet<string> {
  const walk = walkCapabilities(definition, directors, pluginDefs);
  const declared = new Set<string>();
  for (const step of walk.perStep.values()) {
    for (const grant of step.grants) {
      declared.add(grant);
    }
  }
  return declared;
}

/**
 * Filter a parent's grant rules down to what a spawned child body declares.
 *
 * The result caps the child at the parent: it only ever REMOVES parent rules,
 * never adds or widens one. The filter errs toward keeping, because dropping is
 * the only unsafe direction --
 *
 *  - Every `deny` and `ask` rule is kept unconditionally. Both only ever
 *    restrict, so dropping one WEAKENS safety. An `ask` floor in particular is
 *    load-bearing: `evaluateGrants` ranks `ask` above `allow` at equal
 *    specificity so a workflow cannot declare its way under an approval gate,
 *    and filtering the `ask` out would punch straight through that gate.
 *  - An `allow` rule is kept only when its resource PATTERN covers at least one
 *    resource the child body declares (via `matchPattern`, so a wildcard grant
 *    like `tool:*` survives when the child declares any matching `tool:`). An
 *    `allow` for a resource the child never declares -- a parent-only
 *    `effect:`/`credential:`/`tool:` -- is dropped: that is the escalation this
 *    cap closes.
 *  - Any entry that is not an `allow` rule with a string `resource` is kept. A
 *    non-rule entry is inert at evaluation (`evaluateGrants` reads `.resource`),
 *    so keeping it never widens authority, and keeping-on-doubt can never
 *    wrongly drop a restriction.
 *
 * Action is deliberately not part of the coverage test. The declared set is
 * resource strings; a parent rule whose action never matches a child query is
 * inert (`evaluateGrants` re-gates on action at decision time), so testing the
 * resource alone can only OVER-keep an inert `allow`, never wrongly drop one
 * the child legitimately needs.
 */
export function filterGrantsToDeclaredResources(
  parentGrants: readonly unknown[],
  declared: ReadonlySet<string>,
): readonly unknown[] {
  return parentGrants.filter((grant) => keepGrantForDeclared(grant, declared));
}

function keepGrantForDeclared(
  grant: unknown,
  declared: ReadonlySet<string>,
): boolean {
  if (!isAllowRuleWithResource(grant)) {
    return true;
  }
  for (const resource of declared) {
    if (matchPattern(grant.resource, resource)) {
      return true;
    }
  }
  return false;
}

function isAllowRuleWithResource(
  grant: unknown,
): grant is { effect: "allow"; resource: string } {
  if (typeof grant !== "object" || grant === null) {
    return false;
  }
  if (!("effect" in grant) || !("resource" in grant)) {
    return false;
  }
  return grant.effect === "allow" && typeof grant.resource === "string";
}
