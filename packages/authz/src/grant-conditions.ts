import { isMailSendNamespaceResource } from "@intx/types/mail-send";

import { validateConditions } from "./conditions";
import { MAIL_SEND_CONDITION_REGISTRY } from "./mail-send-condition-registry";
import { matchPattern } from "./patterns";
import { RUN_CHILD_CONDITION_REGISTRY } from "./run-child-condition-registry";
import { CREDENTIAL_USE_CONDITIONS } from "./tool-consumer";
import type { ConditionRegistry } from "./types";

const CREDENTIAL_PREFIX = "credential:";

export type GrantConditionTarget = {
  readonly resource: string;
  readonly action: string;
  readonly conditions: Record<string, unknown> | null;
  /**
   * Whether the grant sits directly on a workflow run's principal. Such a row
   * ships to the run as a step grant, so the run child and the credential
   * gate evaluate it as well as the hub. A role grant never ships to a run,
   * because the run loads only the rows on its own principal; if that
   * changes, a role grant needs this check too.
   */
  readonly onRunPrincipal: boolean;
};

// The literal text a pattern's matches must start with: everything before
// its first `*`, or the whole pattern when it has none.
function literalPrefix(pattern: string): string {
  const star = pattern.indexOf("*");
  return star === -1 ? pattern : pattern.slice(0, star);
}

function canMatchCredential(resource: string): boolean {
  const prefix = literalPrefix(resource);
  if (!resource.includes("*")) return prefix.startsWith(CREDENTIAL_PREFIX);
  return (
    prefix.startsWith(CREDENTIAL_PREFIX) || CREDENTIAL_PREFIX.startsWith(prefix)
  );
}

function canMatchNonCredential(resource: string): boolean {
  return !literalPrefix(resource).startsWith(CREDENTIAL_PREFIX);
}

/**
 * Check a grant's conditions against every registry that will evaluate the
 * grant, not only the hub's. The hub evaluates every grant, with
 * `hubRegistry`. The hub's mail send check evaluates a `mail.send` grant on
 * any holder with a narrower registry. A row on a workflow run's principal
 * is also evaluated by the run child, for any resource its pattern can match
 * outside `credential:`, and by the credential gate, for `credential:<id>` /
 * `use`. Each of those has a narrower registry, and a condition outside it
 * throws there.
 *
 * Returns one problem per bad entry. A problem only a narrower registry
 * reports names the place that rejects it.
 */
export function validateGrantConditions(
  target: GrantConditionTarget,
  hubRegistry: Readonly<ConditionRegistry>,
): string[] {
  const problems = validateConditions(target.conditions, hubRegistry);

  const narrower: [string, Readonly<ConditionRegistry>][] = [];
  if (isMailSendNamespaceResource(target.resource)) {
    narrower.push(["a mail send", MAIL_SEND_CONDITION_REGISTRY]);
  }
  if (target.onRunPrincipal) {
    if (canMatchNonCredential(target.resource)) {
      narrower.push(["a workflow run", RUN_CHILD_CONDITION_REGISTRY]);
    }
    if (
      canMatchCredential(target.resource) &&
      matchPattern(target.action, "use")
    ) {
      narrower.push(["credential use", CREDENTIAL_USE_CONDITIONS]);
    }
  }

  for (const [place, registry] of narrower) {
    for (const problem of validateConditions(target.conditions, registry)) {
      if (!problems.includes(problem)) {
        problems.push(`${place} cannot evaluate this grant: ${problem}`);
      }
    }
  }
  return problems;
}
