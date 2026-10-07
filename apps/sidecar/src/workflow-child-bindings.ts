// Composition edge for the workflow child.
//
// Closes the child factory over the sidecar's tool materializer and the
// grant cap, and re-exports the substrate-config key list the binary
// forwards. The binary imports this module and the workflow-host barrel.

import {
  collectDeclaredCredentialConsumers,
  collectDeclaredResources,
  filterGrantsToDeclaredResources,
} from "@intx/workflow-deploy";
import {
  SIDECAR_SUBSTRATE_CONFIG_KEYS,
  createSidecarSubstrateFactory,
} from "@intx/workflow-host/child";

import { materializeStepTools } from "./step-tool-materialization";

export { SIDECAR_SUBSTRATE_CONFIG_KEYS };

export const createSubstrate = createSidecarSubstrateFactory({
  materializeStepTools,
  collectDeclaredResources,
  collectDeclaredCredentialConsumers,
  filterGrantsToDeclaredResources,
});
