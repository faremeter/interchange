// Composition edge for the workflow child.
//
// Closes the child factory over the grant cap, and re-exports the substrate-config key list the binary
// forwards. The binary imports this module and the workflow-host child entry.

import {
  collectDeclaredCredentialConsumers,
  collectDeclaredResources,
  filterGrantsToDeclaredResources,
} from "@intx/workflow-deploy/child-grants";
import {
  SIDECAR_SUBSTRATE_CONFIG_KEYS,
  createSidecarSubstrateFactory,
} from "@intx/workflow-host/child";

export { SIDECAR_SUBSTRATE_CONFIG_KEYS };

export const createSubstrate = createSidecarSubstrateFactory({
  collectDeclaredResources,
  collectDeclaredCredentialConsumers,
  filterGrantsToDeclaredResources,
});
