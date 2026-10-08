import { type } from "arktype";

import { CredentialBinding } from "./credentials";
import { GrantRequirement } from "./grants";
import { InboundMailPolicy } from "./inbound-mail-policy";
import { SidecarCapabilityPolicy } from "./sidecar-capabilities";

/**
 * Structural arktype validator for the `workflow.json` envelope. The
 * substrate checks the cross-cutting shape of `WorkflowDefinition`
 * (presence and primitive type of `id`, `triggers`, `steps`,
 * `stepOrder`) but does not re-derive `defineWorkflow`'s DAG-level
 * validation here — primitive-level shape, default-input application,
 * and `after`-ref resolution belong to the runtime layer that hydrates
 * the definition. The codebase push uses this validator to detect an
 * ambiguous tree that also carries an envelope-valid `workflow.json`,
 * and the hydrate-time definition loaders reuse it to validate a
 * materialized definition before instantiation.
 */
const StepsObject = type("Record<string, unknown>").narrow((value, ctx) => {
  if (Array.isArray(value)) {
    return ctx.mustBe("a JSON object, not an array");
  }
  return true;
});

const StateObject = type("Record<string, unknown>").narrow((value, ctx) => {
  if (Array.isArray(value)) {
    return ctx.mustBe("a JSON object, not an array");
  }
  return true;
});

export const workflowDefinitionEnvelopeSchema = type({
  id: "string > 0",
  triggers: "unknown[]",
  steps: StepsObject,
  stepOrder: "string[]",
  "state?": StateObject,
  // `grantRequirements` passes through the envelope whether or not it is
  // declared here: arktype's `.onUndeclaredKey("ignore")` below is
  // passthrough, not stripping (only `"delete"` strips), so the hydrate read
  // sees the field either way. Declaring it here VALIDATES declared
  // requirements at the deploy boundary — a malformed `source` is rejected
  // rather than passed through unchecked — as defense in depth alongside the
  // trigger route's own `GrantRequirements` re-validation. Compose the
  // exported `GrantRequirement` arktype rather than restating its shape so the
  // envelope and the definition stay in lockstep.
  "grantRequirements?": GrantRequirement.array(),
  // `credentialBindings` is validated here too -- same defense-in-depth
  // rationale as grantRequirements above: a malformed binding (bad locator,
  // authority, or handle) is rejected at the deploy boundary rather than
  // passed through to launch-time resolution unchecked.
  "credentialBindings?": CredentialBinding.array(),
  "sidecarPlacement?": SidecarCapabilityPolicy,
  // `inboundMailPolicy` is validated here too -- same defense-in-depth
  // rationale as credentialBindings above: a malformed policy (an unknown
  // outcome key or a value that is not reject/admit) is rejected at the deploy
  // boundary rather than passed through to later admission resolution
  // unchecked.
  "inboundMailPolicy?": InboundMailPolicy,
}).onUndeclaredKey("ignore");
