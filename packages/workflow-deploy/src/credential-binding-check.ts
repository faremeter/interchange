// Check a frozen inert projection's credential bindings against the consumers
// the workflow actually runs, before any credential is resolved.
//
// Deploy resolves only the root definition's `credentialBindings`, and a
// binding is delivered to the consumer it names. So a binding on a nested body
// is never resolved, a binding whose consumer the workflow never runs is never
// used, two bindings for one consumer and handle leave the delivered one to
// chance, and an action that declares a handle with no binding fails before
// its handler runs. This check refuses all of these at approval, over the
// projection the gate freezes, so a typo in a binding fails the deploy instead
// of leaving a step without its credential.
//
// Scope of the rules:
//   - The declared-handle rule covers actions only; tool packages carry no
//     handle declarations in the inert projection.
//   - Tool consumers are keyed on the tool factory id, the consumer identity
//     on the source-ref lineage, the only lineage that delivers bindings. A
//     `package` binding for a plugin package, or for a factory used only in a
//     `{ ref }` body, names no consumer this deployment runs and is refused.
//
// The projection types its steps as `unknown`, so every field read here is
// validated first. A malformed frozen projection is a projector defect, not an
// author error, so it throws rather than becoming a violation.

import { type } from "arktype";
import type { CredentialBinding } from "@intx/types";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import {
  EXECUTABLE_STEP_DESCENT,
  walkStepTree,
} from "@intx/workflow/definition";

import {
  inertNestedBodies,
  readInertStepToolFactoryIds,
} from "./inert-ontrigger-bodies";

const CONTEXT = "credential binding check: ";

/** One credential binding problem the check found. */
export type CredentialBindingViolation =
  | {
      /** An action declares a handle that no root binding names for it. */
      readonly kind: "unbound_declared_handle";
      readonly handler: string;
      readonly handle: string;
    }
  | {
      /** A nested body definition carries `credentialBindings`. */
      readonly kind: "nested_body_bindings";
      readonly bodyId: string;
      /** The step ids the walk reached the body's carrying step through. */
      readonly reachedThrough: readonly string[];
    }
  | {
      /** A `package` binding names no tool factory the workflow runs. */
      readonly kind: "unknown_binding_package";
      readonly package: string;
      readonly handle: string;
    }
  | {
      /** An `action` binding names a handler no declaring action step runs. */
      readonly kind: "unknown_binding_action";
      readonly action: string;
      readonly handle: string;
    }
  | {
      /** An `action` binding names a handle the handler never declared. */
      readonly kind: "undeclared_binding_handle";
      readonly action: string;
      readonly handle: string;
    }
  | {
      /** Two root bindings name the same consumer and handle. */
      readonly kind: "duplicate_binding";
      readonly consumer: string;
      readonly handle: string;
    };

const ActionStep = type({ kind: "'action'", handler: "string" });
const DeclaredHandles = type("string[]");
const HasBindingsKey = type({ credentialBindings: "unknown" });

/** The consumer a binding names, as the refusal message reports it. */
function bindingConsumerLabel(binding: CredentialBinding): string {
  return binding.package !== undefined
    ? `package ${binding.package}`
    : `action ${binding.action}`;
}

/**
 * Every credential binding problem in `projection`, in walk order. An empty
 * list means every root binding names a consumer the workflow runs, once,
 * every declared action handle is bound, and no nested body carries bindings.
 */
export function checkCredentialBindings(
  projection: WorkflowProjectionDefinition,
): readonly CredentialBindingViolation[] {
  const violations: CredentialBindingViolation[] = [];
  const toolFactoryIds = new Set<string>();
  const declaredByHandler = new Map<string, Set<string>>();

  walkStepTree<unknown, WorkflowProjectionDefinition>({
    tree: projection,
    context: CONTEXT,
    nestedTrees: (step) => inertNestedBodies(step, EXECUTABLE_STEP_DESCENT),
    visit: ({ stepId, step, path }) => {
      // Read each body off the step that carries it, so a body is checked
      // once and whatever its step count.
      for (const body of inertNestedBodies(step, EXECUTABLE_STEP_DESCENT)) {
        if (!(HasBindingsKey(body) instanceof type.errors)) {
          violations.push({
            kind: "nested_body_bindings",
            bodyId: body.id,
            reachedThrough: path,
          });
        }
      }

      for (const id of readInertStepToolFactoryIds(step, CONTEXT, stepId)) {
        toolFactoryIds.add(id);
      }

      const asAction = ActionStep(step);
      if (asAction instanceof type.errors) return;
      if (
        typeof step !== "object" ||
        step === null ||
        !("credentials" in step)
      ) {
        return;
      }
      const declared = DeclaredHandles(step.credentials);
      if (declared instanceof type.errors) {
        throw new Error(
          `${CONTEXT}action step ${path.join(" > ")} declares credentials that are not a list of strings: ${declared.summary}`,
        );
      }
      const handles =
        declaredByHandler.get(asAction.handler) ?? new Set<string>();
      for (const handle of declared) handles.add(handle);
      declaredByHandler.set(asAction.handler, handles);
    },
  });

  const bindings: readonly CredentialBinding[] =
    projection.credentialBindings ?? [];

  for (const [handler, handles] of declaredByHandler) {
    for (const handle of handles) {
      const bound = bindings.some(
        (binding) => binding.action === handler && binding.handle === handle,
      );
      if (!bound) {
        violations.push({ kind: "unbound_declared_handle", handler, handle });
      }
    }
  }

  const seen = new Set<string>();
  for (const binding of bindings) {
    const consumer = bindingConsumerLabel(binding);
    const key = `${consumer}\u0000${binding.handle}`;
    if (seen.has(key)) {
      violations.push({
        kind: "duplicate_binding",
        consumer,
        handle: binding.handle,
      });
    }
    seen.add(key);

    if (binding.package !== undefined) {
      if (!toolFactoryIds.has(binding.package)) {
        violations.push({
          kind: "unknown_binding_package",
          package: binding.package,
          handle: binding.handle,
        });
      }
      continue;
    }
    const declared = declaredByHandler.get(binding.action);
    if (declared === undefined) {
      violations.push({
        kind: "unknown_binding_action",
        action: binding.action,
        handle: binding.handle,
      });
    } else if (!declared.has(binding.handle)) {
      violations.push({
        kind: "undeclared_binding_handle",
        action: binding.action,
        handle: binding.handle,
      });
    }
  }

  return violations;
}

/** One sentence for a violation, for a refusal message. */
export function describeCredentialBindingViolation(
  violation: CredentialBindingViolation,
): string {
  switch (violation.kind) {
    case "unbound_declared_handle":
      return `action ${violation.handler} declares credential handle "${violation.handle}", but no root binding names it for that handler`;
    case "nested_body_bindings":
      return `body ${violation.bodyId} (carried by step ${violation.reachedThrough.join(" > ")}) declares credentialBindings; remove them, because only the root definition's bindings are resolved`;
    case "unknown_binding_package":
      return `the binding for handle "${violation.handle}" names package ${violation.package}, which no tool factory in the workflow has`;
    case "unknown_binding_action":
      return `the binding for handle "${violation.handle}" names action handler ${violation.action}, which no action step that declares credentials runs`;
    case "undeclared_binding_handle":
      return `the binding for handle "${violation.handle}" names action handler ${violation.action}, which does not declare that handle`;
    case "duplicate_binding":
      return `two bindings name ${violation.consumer} and handle "${violation.handle}"`;
    default: {
      const _exhaustive: never = violation;
      return `unknown credential binding violation: ${JSON.stringify(_exhaustive)}`;
    }
  }
}
