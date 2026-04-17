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

import type { BeforeToolExtension } from "@interchange/types/runtime";
import type { Effect } from "@interchange/types/authz";

export type AuthzMatchedGrant = {
  id: string;
  resource: string;
  action: string;
  effect: Effect;
  source: "system" | "role" | "creator" | "invoker";
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

export type AuthzExtensionOptions = {
  authorize: (resource: string, action: string) => Promise<AuthzCallResult>;
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

export function createAuthzExtension(
  opts: AuthzExtensionOptions,
): BeforeToolExtension {
  return {
    async beforeTool(call) {
      const resource = `tool:${call.name}`;
      const action = "invoke";

      let result: AuthzCallResult;
      try {
        result = await opts.authorize(resource, action);
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
      const blockReason = blocked
        ? formatBlockReason(result.effect as BlockEffect, resource, action)
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
