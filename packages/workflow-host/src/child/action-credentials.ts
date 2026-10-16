// The credentials capability a deployed action handler resolves its declared
// handles through. Each invocation builds one capability for the handler's
// consumer identity, `action:<handler>`, from the run's live material cell and
// the action step's grants, through the same builder tool packages use. The
// action invoker disposes it when the handler settles.

import { actionConsumer } from "@intx/authz";
import {
  buildConsumerCredentialCapability,
  createCredentialCapability,
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
 * Build the action credentials factory over a run's credential context.
 *
 * The capability is built per invocation, so each invocation reads its own
 * step's grants and the bindings in the cell at that moment, and the invoker
 * can dispose it without affecting the next invocation. The grants are read
 * only when the handler declares a handle or has a binding.
 *
 * The declared-versus-bound reconcile checks one direction: a declared handle
 * with no binding fails the build, before the handler runs. A binding for a
 * handle no action step with this handler declared is refused by the approval
 * gate. The capability serves every handle bound to `action:<handler>`, so
 * steps that share a handler share its handles.
 */
export function createActionCredentialsFactory(
  context: SidecarStepCredentialContext,
): ActionCredentialsFactory {
  return ({ handler, credentials, authzContext }) => {
    const stepId = authzContext.stepId;
    if (stepId === undefined) {
      throw new Error(
        `action ${handler} cannot build its credentials capability: the authz context carries no step id`,
      );
    }
    const consumer = actionConsumer(handler);
    const capability = buildConsumerCredentialCapability(
      consumer,
      credentials.map((handle) => ({ handle })),
      {
        materialCell: context.materialCell,
        resolveGrants: stepGrantsResolver(context, stepId),
        providers: context.providers,
      },
    );
    // No declaration and no binding: there is nothing to resolve, so Gate 2
    // never runs and empty grants are correct. `resolve` fails closed naming
    // the consumer and the handle.
    return (
      capability ??
      createCredentialCapability({
        consumer,
        bindings: new Map(),
        providers: context.providers,
        grants: [],
      })
    );
  };
}
