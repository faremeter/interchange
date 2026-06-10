// Singular shorthand for the trivial single-step case.
//
// A workflow whose entire body is one agent's send-and-respond loop
// fans through the same `WorkflowDefinition` shape as the plural form
// after normalization. Authors write:
//
//   defineWorkflow({
//     id: "support",
//     agent: support,
//     trigger: { type: "mail", to: "support@..." },
//   });
//
// which is structurally equivalent to:
//
//   defineWorkflow({
//     id: "support",
//     trigger: { type: "mail", to: "support@..." },
//     steps: { default: step({ agent: support }) },
//   });
//
// The two forms must produce deep-equal `WorkflowDefinition` values
// so authors who later switch from one to the other (or who hash and
// compare definitions) see no spurious differences.

import type { BaseEnv } from "@intx/agent";

import { step } from "./primitives";
import type { Primitive } from "./primitives";
import type { Trigger } from "./triggers";
import type { StateSchema } from "./primitives";

export interface SingularShorthand<EnvReq extends BaseEnv> {
  id: string;
  agent: import("@intx/agent").AgentDefinition<EnvReq>;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  state?: { schema?: StateSchema };
}

export interface PluralShape {
  id: string;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  steps: Record<string, Primitive>;
  state?: { schema?: StateSchema };
}

export function normalizeSingularShorthand<EnvReq extends BaseEnv>(
  config: SingularShorthand<EnvReq>,
): PluralShape {
  return {
    id: config.id,
    ...(config.trigger !== undefined ? { trigger: config.trigger } : {}),
    ...(config.triggers !== undefined ? { triggers: config.triggers } : {}),
    steps: { default: step({ agent: config.agent }) },
    ...(config.state !== undefined ? { state: config.state } : {}),
  };
}
