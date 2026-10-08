// Singular shorthand for the trivial single-step case: a workflow whose whole
// body is one agent's loop normalizes to the plural form as
// `steps: { default: step({ agent }) }`. The two forms must produce
// deep-equal `WorkflowDefinition` values, so authors who switch between them
// (or hash and compare definitions) see no spurious differences.

import type { BaseEnv } from "@intx/agent";
import type {
  CredentialBinding,
  GrantRequirement,
  SidecarCapabilityPolicy,
} from "@intx/types";
import type { InboundMailPolicy } from "@intx/types/runtime";

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
  grantRequirements?: readonly GrantRequirement[];
  credentialBindings?: readonly CredentialBinding[];
  sidecarPlacement?: SidecarCapabilityPolicy;
  inboundMailPolicy?: InboundMailPolicy;
}

export interface PluralShape {
  id: string;
  trigger?: Trigger;
  triggers?: readonly Trigger[];
  steps: Record<string, Primitive>;
  state?: { schema?: StateSchema };
  grantRequirements?: readonly GrantRequirement[];
  credentialBindings?: readonly CredentialBinding[];
  sidecarPlacement?: SidecarCapabilityPolicy;
  inboundMailPolicy?: InboundMailPolicy;
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
    ...(config.grantRequirements !== undefined
      ? { grantRequirements: config.grantRequirements }
      : {}),
    ...(config.credentialBindings !== undefined
      ? { credentialBindings: config.credentialBindings }
      : {}),
    ...(config.sidecarPlacement !== undefined
      ? { sidecarPlacement: config.sidecarPlacement }
      : {}),
    ...(config.inboundMailPolicy !== undefined
      ? { inboundMailPolicy: config.inboundMailPolicy }
      : {}),
  };
}
