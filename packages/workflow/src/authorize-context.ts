// Workflow-runtime per-call authz context.
//
// `AuthorizeContext` is the concrete shape the workflow runtime supplies to
// the agent harness's `AuthorizeFn` at every authz call. It lives here, not
// in `@intx/inference` or `@intx/agent`, because `stepId`, `attempt`, and
// `runId` are workflow-runtime vocabulary; the inference and agent layers
// stay workflow-unaware and receive the context through the generic third
// argument that defaults to `unknown`.

import type { AuthorizeFn } from "@intx/agent";

/**
 * Per-call context the workflow runtime attaches to every authz call.
 * Fields are optional because the same callback type carries both
 * in-workflow and bare-caller invocations.
 */
export interface AuthorizeContext {
  /** The id of the workflow step that originated the authz call. */
  stepId?: string;
  /** The 1-indexed attempt number for the originating step. */
  attempt?: number;
  /** The id of the workflow run the originating step belongs to. */
  runId?: string;
}

/**
 * Workflow-typed authorize callback. The agent harness's `BaseEnv`
 * carries `AuthorizeFn<unknown>`; the workflow runtime carries this
 * stricter shape on its own bookkeeping.
 */
export type WorkflowAuthorizeFn = AuthorizeFn<AuthorizeContext>;
