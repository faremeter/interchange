// The credentials capability a deployed action handler resolves its declared
// handles through, built by the harness action credentials factory over the
// run child's live material cell and the action step's grants.

import {
  createActionCredentialsFactory,
  type CredentialMaterialCell,
  type CredentialProviderRegistry,
} from "@intx/harness";
import type { GrantRule } from "@intx/types/authz";
import type { ActionCredentialsFactory } from "@intx/workflow";

/**
 * The per-run credential inputs a build resolves a step's credentials from.
 * The live material cell and the grants resolver ride in from the run child
 * (per-run state); the provider registry is sidecar-static and combined in at
 * the invoke boundary. Absent when no credential material was threaded, which
 * leaves the step's inference reader and credential capabilities unwired.
 */
export interface SidecarStepCredentialContext {
  readonly materialCell: CredentialMaterialCell;
  /**
   * The step's grants, resolved live by base step id. The credentials
   * snapshot types them `unknown[]`; `stepGrantsResolver` casts them to
   * `GrantRule[]` where Gate 2 reads them.
   */
  readonly resolveStepGrants: (stepId: string) => readonly unknown[];
  readonly providers: CredentialProviderRegistry;
}

/**
 * A thunk over one step's grants in the `GrantRule` shape Gate 2 reads. The
 * credentials snapshot carries grants as `unknown[]`; this is the one place
 * the host casts them.
 */
export function stepGrantsResolver(
  context: SidecarStepCredentialContext,
  stepId: string,
): () => readonly GrantRule[] {
  return () =>
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- resolveStepGrants returns unknown[]; Gate 2 reads GrantRule
    context.resolveStepGrants(stepId) as readonly GrantRule[];
}

/**
 * Build the action credentials factory over a run child's credential context,
 * through the harness builder. The run id is not needed to find a step's
 * grants: every step in a sidecar run child's credentials snapshot carries the
 * same grants (the run's flat grant set, or a child run's capped set). A step
 * id can repeat inside one context, for example a loop-body step and a body
 * step, and the lookup still finds those grants.
 */
export function createSidecarActionCredentialsFactory(
  context: SidecarStepCredentialContext,
): ActionCredentialsFactory {
  return createActionCredentialsFactory({
    materialCell: context.materialCell,
    resolveStepGrants: ({ stepId }) => stepGrantsResolver(context, stepId)(),
    providers: context.providers,
  });
}
