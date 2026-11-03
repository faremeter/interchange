// The credentials snapshot a local run authorizes against.
//
// A deployed source-ref run authorizes every step against one flat grant set:
// the hub projects the probe's capability walk into runtime `tool:` and
// `effect:` rules, adds a `credential:` use rule for each resolved binding,
// and the sidecar gives every step id the same set. This module builds that
// set from the live definition, with no hub and no sidecar, so a local run
// evaluates tool, effect and credential grants against the same rules as a
// deployed step. Requirement grants need hub state, so the caller passes them.
// Nothing here imports a Node module.

import type { DirectorRegistry, ToolDeclaration } from "@intx/agent";
import type { GrantRule } from "@intx/authz";

import type { CredentialDelivery } from "@intx/types/credential-delivery";
import { serializeGrantWalk } from "@intx/types/grant-snapshot";
import { executableStepIds, type WorkflowDefinition } from "@intx/workflow";
import {
  credentialUseGrantRules,
  runtimeGrantRules,
  walkCapabilities,
  type RuntimeGrantRule,
} from "@intx/workflow-deploy/capabilities";

import type { CredentialsSnapshot } from "./supervisor/credentials";

export type LocalCredentialsSnapshotOpts = {
  /** The live definition the run executes. */
  readonly definition: WorkflowDefinition;
  /** The director registry the run resolves directors against. */
  readonly directors: DirectorRegistry;
  /**
   * The tool declarations of each plugin an agent declares, keyed by plugin
   * name. Required when any agent declares a plugin: the walk refuses a plugin
   * it has no declarations for.
   */
  readonly pluginDefinitions?: ReadonlyMap<string, readonly ToolDeclaration[]>;
  /**
   * The credential delivery the run's material cell holds. Each binding adds
   * the `credential:` use rule its consumer needs.
   */
  readonly delivery?: CredentialDelivery;
  /**
   * Further grants every step holds, such as the requirement grants a hub
   * resolves from its principals.
   */
  readonly grants?: readonly GrantRule[];
};

/**
 * Build the credentials snapshot a deployed source-ref run of `definition`
 * would authorize against. Every step id the definition can execute (top
 * level, loop bodies and inline onTrigger and childWorkflow bodies) holds the
 * same grants, as in a deployed run. A local childWorkflow child therefore
 * authorizes against the parent's full set, where a deployed child gets a
 * narrower capped set.
 *
 * Throws when the walk cannot resolve a director, as the deploy probe does.
 */
export function createLocalCredentialsSnapshot(
  opts: LocalCredentialsSnapshotOpts,
): CredentialsSnapshot {
  const walk = walkCapabilities(
    opts.definition,
    opts.directors,
    opts.pluginDefinitions ?? new Map(),
  );
  const [unresolved] = walk.unresolvedDirectors;
  if (unresolved !== undefined) {
    throw new Error(`unresolvable director: ${unresolved}`);
  }
  const rules = [
    ...runtimeGrantRules(serializeGrantWalk(walk, undefined)),
    ...credentialUseGrantRules(opts.delivery?.bindings ?? []),
  ];
  const grants: readonly GrantRule[] = [
    ...rules.map(localGrantRule),
    ...(opts.grants ?? []),
  ];
  return {
    steps: executableStepIds(opts.definition).map((stepId) => ({
      stepId,
      grants,
      // A local snapshot is never pushed over a control channel, so it carries
      // no content hash to cross-check.
      contentHash: "",
    })),
  };
}

function localGrantRule(rule: RuntimeGrantRule, index: number): GrantRule {
  return {
    id: `local-grant:${String(index)}`,
    ...rule,
    roleId: null,
    principalId: null,
  };
}
