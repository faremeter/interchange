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
//   - The declared-handle rule covers actions and the tool factories that
//     declare credentials through `defineTool`. A factory that declares no
//     credentials is not checked, so its bindings may name any handle; one
//     that declares a list is held to it, both ways.
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
import { HUB_CREDENTIAL_HANDLE } from "@intx/types/credential-delivery";
import { actionConsumer, toolConsumer } from "@intx/authz";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";
import {
  EXECUTABLE_STEP_DESCENT,
  walkStepTree,
} from "@intx/workflow/definition";

import {
  inertNestedBodies,
  readInertStepToolFactories,
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
      /** A tool factory declares a handle that no root binding names for it. */
      readonly kind: "unbound_declared_tool_handle";
      readonly factory: string;
      readonly handle: string;
    }
  | {
      /** A `package` binding names a handle its declaring factory omits. */
      readonly kind: "undeclared_tool_binding_handle";
      readonly package: string;
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
      /** An authored binding names the reserved hub handle. */
      readonly kind: "reserved_handle_binding";
      readonly consumer: string;
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
  const { violations, toolFactoryIds, declaredByFactory, declaredByHandler } =
    collectDeclarations(projection);

  const bindings: readonly CredentialBinding[] =
    projection.credentialBindings ?? [];

  // Deploy binds the reserved hub handle itself, so a declaration of it needs
  // no authored binding.
  for (const [handler, handles] of declaredByHandler) {
    for (const handle of handles) {
      if (handle === HUB_CREDENTIAL_HANDLE) continue;
      const bound = bindings.some(
        (binding) => binding.action === handler && binding.handle === handle,
      );
      if (!bound) {
        violations.push({ kind: "unbound_declared_handle", handler, handle });
      }
    }
  }

  for (const [factory, handles] of declaredByFactory) {
    for (const handle of handles) {
      if (handle === HUB_CREDENTIAL_HANDLE) continue;
      const bound = bindings.some(
        (binding) => binding.package === factory && binding.handle === handle,
      );
      if (!bound) {
        violations.push({
          kind: "unbound_declared_tool_handle",
          factory,
          handle,
        });
      }
    }
  }

  const seen = new Set<string>();
  for (const binding of bindings) {
    const consumer = bindingConsumerLabel(binding);
    if (binding.handle === HUB_CREDENTIAL_HANDLE) {
      violations.push({
        kind: "reserved_handle_binding",
        consumer,
        handle: binding.handle,
      });
      continue;
    }
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
        continue;
      }
      const declaredByTool = declaredByFactory.get(binding.package);
      if (declaredByTool !== undefined && !declaredByTool.has(binding.handle)) {
        violations.push({
          kind: "undeclared_tool_binding_handle",
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

/**
 * The consumers that declare the reserved hub handle anywhere in the workflow
 * -- the root and every nested body, which all run as the one run principal
 * -- as `tool:<package>` and `action:<handler>`, sorted and without repeats. Each
 * gets a run token for the hub REST API when the run principal is created.
 */
export function collectHubConsumers(
  projection: WorkflowProjectionDefinition,
): readonly string[] {
  const { declaredByFactory, declaredByHandler } =
    collectDeclarations(projection);
  const consumers = new Set<string>();
  for (const [factory, handles] of declaredByFactory) {
    if (handles.has(HUB_CREDENTIAL_HANDLE))
      consumers.add(toolConsumer(factory));
  }
  for (const [handler, handles] of declaredByHandler) {
    if (handles.has(HUB_CREDENTIAL_HANDLE))
      consumers.add(actionConsumer(handler));
  }
  return [...consumers].sort();
}

type Declarations = {
  violations: CredentialBindingViolation[];
  toolFactoryIds: Set<string>;
  declaredByFactory: Map<string, Set<string>>;
  declaredByHandler: Map<string, Set<string>>;
};

/**
 * Walk the executable tree once for the tool factories it runs, the handles
 * declaring factories and action handlers declare, and the nested bodies that
 * wrongly carry bindings.
 */
function collectDeclarations(
  projection: WorkflowProjectionDefinition,
): Declarations {
  const violations: CredentialBindingViolation[] = [];
  const toolFactoryIds = new Set<string>();
  const declaredByFactory = new Map<string, Set<string>>();
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

      for (const factory of readInertStepToolFactories(step, CONTEXT, stepId)) {
        toolFactoryIds.add(factory.id);
        if (factory.credentialHandles === undefined) continue;
        const handles = declaredByFactory.get(factory.id) ?? new Set<string>();
        for (const handle of factory.credentialHandles) handles.add(handle);
        declaredByFactory.set(factory.id, handles);
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

  return { violations, toolFactoryIds, declaredByFactory, declaredByHandler };
}

/** One sentence for a violation, for a refusal message. */
export function describeCredentialBindingViolation(
  violation: CredentialBindingViolation,
): string {
  switch (violation.kind) {
    case "unbound_declared_handle":
      return `action ${violation.handler} declares credential handle "${violation.handle}", but no root binding names it for that handler`;
    case "unbound_declared_tool_handle":
      return `tool factory ${violation.factory} declares credential handle "${violation.handle}", but no root binding names it for that factory`;
    case "undeclared_tool_binding_handle":
      return `the binding for handle "${violation.handle}" names package ${violation.package}, which declares its credentials and does not declare that handle`;
    case "nested_body_bindings":
      return `body ${violation.bodyId} (carried by step ${violation.reachedThrough.join(" > ")}) declares credentialBindings; remove them, because only the root definition's bindings are resolved`;
    case "unknown_binding_package":
      return `the binding for handle "${violation.handle}" names package ${violation.package}, which no tool factory in the workflow has`;
    case "unknown_binding_action":
      return `the binding for handle "${violation.handle}" names action handler ${violation.action}, which no action step that declares credentials runs`;
    case "undeclared_binding_handle":
      return `the binding for handle "${violation.handle}" names action handler ${violation.action}, which does not declare that handle`;
    case "reserved_handle_binding":
      return `the binding for ${violation.consumer} names the reserved handle "${violation.handle}"; remove it, because deploy binds that handle itself`;
    case "duplicate_binding":
      return `two bindings name ${violation.consumer} and handle "${violation.handle}"`;
    default: {
      const _exhaustive: never = violation;
      return `unknown credential binding violation: ${JSON.stringify(_exhaustive)}`;
    }
  }
}
