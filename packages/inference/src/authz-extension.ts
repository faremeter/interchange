// Authz-based BeforeToolExtension.
//
// Creates an extension that authorizes tool calls against a policy before
// execution. The caller provides a pre-bound authorize function that
// encapsulates store, principal, tenant, and condition registry details.
//
// Effects:
//   allow  → tool proceeds
//   deny   → tool blocked
//   ask    → tool suspended (parked awaiting an external approval decision)
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

import type {
  BeforeToolExtension,
  PendingOperation,
} from "@intx/types/runtime";
import type { Effect } from "@intx/types/authz";

// Default deadline for an approval suspension when the caller does not supply
// one. Matches the reactor's DEFAULT_GATE_TIMEOUT_MS (one hour); the value is
// duplicated rather than imported to avoid a dependency from the pure-policy
// extension onto the reactor module.
const DEFAULT_APPROVAL_TIMEOUT_MS = 3_600_000;

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
  /**
   * Deadline applied to an approval suspension, in milliseconds from the
   * moment the `ask` effect is hit. Defaults to `DEFAULT_APPROVAL_TIMEOUT_MS`.
   */
  approvalTimeoutMs?: number;
};

type BlockEffect = "deny" | null;

function formatBlockReason(
  effect: BlockEffect,
  resource: string,
  action: string,
): string {
  switch (effect) {
    case "deny":
      return `Denied by policy: ${resource}/${action}`;
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

      // An `ask` effect suspends the call rather than blocking it, so it is
      // neither cleanly blocked nor allowed: the decision records
      // `blocked: false` with no block reason. Only `deny`/null (fail-closed)
      // are blocks.
      const blockReason =
        result.effect === "deny" || result.effect === null
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
        blocked: blockReason !== undefined,
        blockReason,
        error: undefined,
      };
      safeOnDecision(opts.onDecision, decision);

      if (blockReason !== undefined) {
        return { type: "block", reason: blockReason };
      }

      if (result.effect === "ask") {
        // Mint the correlationId once here so it is the single source of
        // identity for both the gate and the persisted operation. The
        // reactor persists the operation, so this id survives a restart.
        const correlationId = crypto.randomUUID();
        const timeoutAt =
          Date.now() + (opts.approvalTimeoutMs ?? DEFAULT_APPROVAL_TIMEOUT_MS);
        const gateId = `pending-${correlationId}`;
        const pendingOp: PendingOperation = {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId,
          timeoutAt,
          suspendedCall: call,
        };
        return {
          type: "suspend",
          gate: { type: "approval", gateId, correlationId, timeoutAt },
          pendingOp,
        };
      }

      return { type: "allow" };
    },
  };
}
