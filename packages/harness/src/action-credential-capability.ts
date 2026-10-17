// The credentials capability a workflow action handler resolves its declared
// handles through. Each invocation builds one capability for the handler's
// consumer identity, `action:<handler>`, from the live material cell and the
// action step's grants, through the same builder tool packages use. The action
// invoker disposes it when the handler settles.
//
// The request and result types are structural. `@intx/workflow` owns the
// `ActionCredentialsFactory` type, and this package does not depend on it, so
// the factory here is assignable to that type without an import. A host that
// wires it in checks the assignment where both packages are in scope.

import { actionConsumer } from "@intx/authz";
import type { HostCredentialCapability } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";

import { createCredentialCapability } from "./credential-capability";
import type { CredentialProviderRegistry } from "./credential-providers";
import {
  buildConsumerCredentialCapability,
  type CredentialMaterialCell,
} from "./step-credential-capabilities";

/**
 * The run inputs an action credentials factory builds a capability from: the
 * live material cell, the grants of the step an invocation runs in, and the
 * provider registry that shapes a credential into a mediated handle.
 */
export interface ActionCredentialWiring {
  readonly materialCell: CredentialMaterialCell;
  /**
   * The grants of the step an invocation runs in, for Gate 2. It receives the
   * run id as well as the step id, so one factory can serve a parent run and
   * its child runs, whose step ids can repeat. It runs only when the handler
   * declares a handle or has a binding.
   */
  readonly resolveStepGrants: (step: {
    readonly runId?: string;
    readonly stepId: string;
  }) => readonly GrantRule[];
  readonly providers: CredentialProviderRegistry;
}

/** One action invocation's request for a credentials capability. */
export interface ActionCredentialRequest {
  readonly handler: string;
  readonly credentials: readonly string[];
  readonly authzContext: {
    readonly runId?: string;
    readonly stepId?: string;
  };
}

/**
 * Build the action credentials factory over a run's credential wiring.
 *
 * The capability is built per invocation, so each invocation reads its own
 * step's grants and the bindings in the cell at that moment, and the invoker
 * can dispose it without affecting the next invocation.
 *
 * The declared-versus-bound reconcile checks one direction: a declared handle
 * with no binding fails the build, before the handler runs. A binding for a
 * handle no action step with this handler declared is refused by the approval
 * gate. The capability serves every handle bound to `action:<handler>`, so
 * steps that share a handler share its handles.
 */
export function createActionCredentialsFactory(
  wiring: ActionCredentialWiring,
): (request: ActionCredentialRequest) => HostCredentialCapability {
  return ({ handler, credentials, authzContext }) => {
    const { runId, stepId } = authzContext;
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
        materialCell: wiring.materialCell,
        resolveGrants: () =>
          wiring.resolveStepGrants(
            runId === undefined ? { stepId } : { runId, stepId },
          ),
        providers: wiring.providers,
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
        providers: wiring.providers,
        grants: [],
      })
    );
  };
}
