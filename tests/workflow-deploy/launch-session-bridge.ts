import {
  bridgeOrchestratorDeployContent,
  type DeployContent as LaunchDeployContent,
} from "@intx/hub-sessions";
import type { DeployContent as OrchestratorDeployContent } from "@intx/workflow-deploy";

/**
 * Bridge the orchestrator's `DeployContent` to hub-sessions'
 * `DeployContent` at the `launchSession` boundary.
 *
 * `@intx/workflow-deploy` widens `toolPackageManifest` to `unknown` so
 * the package does not need a runtime dep on `@intx/hub-sessions` (see
 * the `DeployContent` docblock in
 * `packages/workflow-deploy/src/orchestrator.ts`). This delegates to the
 * same `bridgeOrchestratorDeployContent` the production multi-step
 * callback uses, which validates and narrows `toolPackageManifest` back
 * to the canonical shape rather than casting `unknown`.
 */
export function toLaunchDeployContent(
  deployContent: OrchestratorDeployContent,
): LaunchDeployContent {
  return bridgeOrchestratorDeployContent(deployContent);
}
