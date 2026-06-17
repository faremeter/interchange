import type { DeployContent as LaunchDeployContent } from "@intx/hub-sessions";
import type { DeployContent as OrchestratorDeployContent } from "@intx/workflow-deploy";

/**
 * Bridge the orchestrator's `DeployContent` to hub-sessions'
 * `DeployContent` at the `launchSession` boundary.
 *
 * `@intx/workflow-deploy` widens `toolPackageManifest` to `unknown`
 * so the package does not need a runtime dep on `@intx/hub-sessions`
 * (see the `DeployContent` docblock in
 * `packages/workflow-deploy/src/orchestrator.ts`). That docblock
 * declares the orchestrator type a structural mirror of hub-sessions'
 * `DeployContent`, so the narrowing is sound -- but unprovable to
 * the type checker. This helper centralizes the one allowed
 * `eslint-disable` so each `LaunchSessionFn` callback in the test
 * fixtures does not need its own.
 */
export function toLaunchDeployContent(
  deployContent: OrchestratorDeployContent,
): LaunchDeployContent {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- see file docblock; the orchestrator's DeployContent docblock declares this structural mirror
  return deployContent as LaunchDeployContent;
}
