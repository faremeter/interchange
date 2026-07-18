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
  ApprovalSnapshot,
  BeforeToolExtension,
  PendingOperation,
  ToolDefinition,
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
  /**
   * Tool definitions the extension can be asked to authorize, used to build the
   * approver-facing snapshot at the `ask` branch. Presence is a contract: when
   * supplied, every tool this extension authorizes must appear here, and an
   * `ask` for a tool that does not is a wiring defect that throws. Omitted
   * entirely, the extension produces no snapshot — a mode for callers that
   * never register a suspension with the hub.
   */
  toolDefinitions?: readonly ToolDefinition[];
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

  // One-shot bypass tokens, keyed on ToolCall.id. A token authorizes a single
  // re-dispatch of an already-approved call to skip the `ask` gate it would
  // otherwise re-hit. Held in memory only, within the resumed reactor cycle
  // that grants and consumes it: a durable allow would outlive the cycle and
  // defeat the one-shot intent, and a crash between grant and consume simply
  // re-drives from the durable log and re-grants.
  const approvedOnce = new Set<string>();

  // Name → definition lookup for building the approval snapshot at the `ask`
  // branch. `undefined` (not merely empty) means the caller wired no tool
  // definitions and wants no snapshot; a defined map means every authorizable
  // tool must be present, so a lookup miss is a wiring defect that throws. The
  // sentinel keeps those two contracts distinguishable at the lookup site.
  const toolDefinitionsByName =
    opts.toolDefinitions !== undefined
      ? new Map(opts.toolDefinitions.map((def) => [def.name, def]))
      : undefined;

  return {
    grantOneShot(id) {
      approvedOnce.add(id);
    },
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

      // A one-shot token only authorizes bypassing an `ask` gate. If the
      // resolved effect is anything else, the grant changed underneath the
      // token: drop it and let the normal path decide, rather than silently
      // allowing a call the policy no longer parks.
      if (approvedOnce.has(call.id) && result.effect !== "ask") {
        approvedOnce.delete(call.id);
      }

      if (blockReason !== undefined) {
        return { type: "block", reason: blockReason };
      }

      if (result.effect === "ask") {
        // A prior approval authorized this exact call to run once. Consume the
        // token (delete-on-read) and allow it through instead of suspending,
        // so a re-dispatched approved call does not re-park on its own gate.
        if (approvedOnce.has(call.id)) {
          approvedOnce.delete(call.id);
          return { type: "allow" };
        }

        // Mint the correlationId once here so it is the single source of
        // identity for both the gate and the persisted operation. The
        // reactor persists the operation, so this id survives a restart.
        const correlationId = crypto.randomUUID();
        const timeoutAt =
          Date.now() + (opts.approvalTimeoutMs ?? DEFAULT_APPROVAL_TIMEOUT_MS);
        const gateId = `pending-${correlationId}`;

        // Build the approver-facing snapshot when tool definitions are wired.
        // A wired extension must have a definition for every tool it can
        // authorize, so a miss is a wiring defect rather than a fallback. An
        // unwired extension produces no snapshot: such callers never register
        // the suspension with the hub, so the downstream required-snapshot
        // validator never sees them.
        let approvalSnapshot: ApprovalSnapshot | undefined;
        if (toolDefinitionsByName !== undefined) {
          const def = toolDefinitionsByName.get(call.name);
          if (def === undefined) {
            throw new Error(
              `Tool "${call.name}" was authorized with effect "ask" but has ` +
                `no definition in the resolved tool set; the approval ` +
                `snapshot cannot be built. This is a wiring defect: every ` +
                `tool the authz extension can authorize must be present in ` +
                `toolDefinitions.`,
            );
          }
          approvalSnapshot = {
            name: call.name,
            description: def.description,
            inputSchema: def.inputSchema,
            arguments: call.arguments,
          };
        }

        const pendingOp: PendingOperation = {
          correlationId,
          kind: "approval",
          registeredAt: Date.now(),
          gateId,
          timeoutAt,
          suspendedCall: call,
          ...(approvalSnapshot !== undefined ? { approvalSnapshot } : {}),
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
