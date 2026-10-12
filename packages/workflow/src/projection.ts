/** Inspect workflow declarations without loading the execution runtime. */
export { collectDeclaredPluginNames } from "./declared-plugins";
export { inlineBodyRef } from "./inline-body-ref";
export {
  projectLiveToInert,
  computeLiveDefinitionHash,
  type InertWorkflowDefinition,
} from "./live-inert-projector";
