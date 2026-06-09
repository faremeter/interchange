// Authz-based BeforeToolExtension.
//
// Creates an extension that authorizes tool calls against a policy before
// execution. The caller provides a pre-bound authorize function that
// encapsulates store, principal, tenant, and condition registry details.
//
// Effects:
//   allow  → tool proceeds
//   deny   → tool blocked
//   ask    → tool blocked (gate-based approval deferred to a future commit)
//   null   → tool blocked (fail-closed: no grants matched)
//
// The action is always "invoke" — all tool calls are invocations. If
// additional action granularity is needed later, the action becomes a
// parameter.
//
// Signal propagation into the authorize function is deferred — the caller
// can capture the signal in their closure if cancellation is needed.
//
// The onDecision callback must not throw. If it does, the exception is
// logged but swallowed so it cannot interfere with the authorization
// decision or mask the original error.

import type { BeforeToolExtension } from "@intx/types/runtime";
import type { Effect } from "@intx/types/authz";

export type AuthzMatchedGrant = {
  id: string;
  resource: string;
  action: string;
  effect: Effect;
  origin: "system" | "role" | "creator" | "invoker";
  specificity: number;
};

export type AuthzCallResult = {
  effect: Effect | null;
  matchingGrants: AuthzMatchedGrant[];
  resolvedBy: AuthzMatchedGrant | null;
};

export type AuthzDecision = {
  callId: string;
  tool: string;
  resource: string;
  action: string;
  effect: Effect | null;
  resolvedBy: AuthzMatchedGrant | null;
  matchingGrants: AuthzMatchedGrant[];
  blocked: boolean;
  blockReason: string | undefined;
  error: string | undefined;
};

export type AuthzExtensionOptions<Ctx = unknown> = {
  authorize: (
    resource: string,
    action: string,
    context: Ctx,
  ) => Promise<AuthzCallResult>;
  onDecision?: (decision: AuthzDecision) => void;
};

type BlockEffect = "deny" | "ask" | null;

function formatBlockReason(
  effect: BlockEffect,
  resource: string,
  action: string,
): string {
  switch (effect) {
    case "deny":
      return `Denied by policy: ${resource}/${action}`;
    case "ask":
      return `Requires approval: ${resource}/${action}`;
    case null:
      return `No matching grants for ${resource}/${action}`;
  }
}

function safeOnDecision(
  callback: ((decision: AuthzDecision) => void) | undefined,
  decision: AuthzDecision,
): void {
  if (callback === undefined) return;
  try {
    callback(decision);
  } catch {
    // onDecision must not throw. If it does, swallow the exception so
    // it cannot interfere with the authorization decision or mask the
    // original error from authorize().
  }
}

export function createAuthzExtension<Ctx = unknown>(
  opts: AuthzExtensionOptions<Ctx>,
): BeforeToolExtension {
  // The reactor does not know workflow concepts; per-call context is the
  // caller's domain. The third arg is plumbing here -- if the caller
  // needs to attach context (workflow step, tenant id, request id), they
  // do so by closure on the authorize function. The empty object is the
  // safe default at this layer.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the inference layer has no domain knowledge to construct a Ctx; callers that need a populated context use closure capture on the authorize function (see @intx/workflow's AuthorizeContext)
  const emptyContext = Object.freeze({}) as Ctx;
  return {
    async beforeTool(call) {
      const resource = `tool:${call.name}`;
      const action = "invoke";

      let result: AuthzCallResult;
      try {
        result = await opts.authorize(resource, action, emptyContext);
      } catch (cause) {
        const msg = cause instanceof Error ? cause.message : String(cause);
        const decision: AuthzDecision = {
          callId: call.id,
          tool: call.name,
          resource,
          action,
          effect: null,
          resolvedBy: null,
          matchingGrants: [],
          blocked: true,
          blockReason: `Authorization failed: ${msg}`,
          error: msg,
        };
        safeOnDecision(opts.onDecision, decision);
        throw cause;
      }

      const blocked = result.effect !== "allow";
      const blockReason =
        result.effect === "deny" ||
        result.effect === "ask" ||
        result.effect === null
          ? formatBlockReason(result.effect, resource, action)
          : undefined;

      const decision: AuthzDecision = {
        callId: call.id,
        tool: call.name,
        resource,
        action,
        effect: result.effect,
        resolvedBy: result.resolvedBy,
        matchingGrants: result.matchingGrants,
        blocked,
        blockReason,
        error: undefined,
      };
      safeOnDecision(opts.onDecision, decision);

      return blockReason;
    },
  };
}
