// Authz-based BeforeToolExtension. Authorizes tool calls against a pre-bound
// authorize function (store, principal, tenant, and condition registry are
// the caller's domain). Effects: allow proceeds, deny blocks, ask suspends
// pending approval, null blocks fail-closed. Action is always "invoke".

import type {
  ApprovalSnapshot,
  BeforeToolExtension,
  PendingOperation,
  ToolDefinition,
} from "@intx/types/runtime";
import type { Effect } from "@intx/types/authz";

// Default approval deadline; duplicated from the reactor's
// DEFAULT_GATE_TIMEOUT_MS to avoid a dependency onto the reactor module.
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
   * Tool definitions used to build the approver-facing snapshot at an `ask`.
   * When supplied, every authorizable tool must appear here (a miss throws).
   * Omitted, no snapshot is produced.
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
    // Swallow: an onDecision throw must not mask the authorize() error.
  }
}

export function createAuthzExtension<Ctx = unknown>(
  opts: AuthzExtensionOptions<Ctx>,
): BeforeToolExtension {
  // Per-call context is the caller's domain; callers attach it by closure on
  // the authorize function. The empty object is the safe default here.
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the inference layer has no domain knowledge to construct a Ctx; callers that need a populated context use closure capture on the authorize function (see @intx/workflow's AuthorizeContext)
  const emptyContext = Object.freeze({}) as Ctx;

  // One-shot bypass tokens keyed on ToolCall.id: a re-dispatched approved
  // call skips its `ask` gate once. Memory-only — a durable allow would
  // outlive the cycle, and a crash re-drives from the durable log anyway.
  const approvedOnce = new Set<string>();

  // Name → definition lookup for the approval snapshot. `undefined` means no
  // snapshot is wanted; a defined map must contain every authorizable tool,
  // so a lookup miss throws.
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

      // `ask` suspends rather than blocks: only deny/null produce a block
      // reason; ask records blocked: false.
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

      // A token only bypasses an `ask` gate; if the effect changed, drop it
      // and let the normal path decide.
      if (approvedOnce.has(call.id) && result.effect !== "ask") {
        approvedOnce.delete(call.id);
      }

      if (blockReason !== undefined) {
        return { type: "block", reason: blockReason };
      }

      if (result.effect === "ask") {
        // A prior approval authorized this call once: consume the token and
        // allow it instead of re-parking on the same gate.
        if (approvedOnce.has(call.id)) {
          approvedOnce.delete(call.id);
          return { type: "allow" };
        }

        // Single correlation id shared by the gate and the persisted
        // operation; the reactor persists it, so it survives a restart.
        const correlationId = crypto.randomUUID();
        const timeoutAt =
          Date.now() + (opts.approvalTimeoutMs ?? DEFAULT_APPROVAL_TIMEOUT_MS);
        const gateId = `pending-${correlationId}`;

        // Build the approver-facing snapshot when tool definitions are wired;
        // a miss is a wiring defect. Unwired extensions never register the
        // suspension with the hub, so no snapshot is needed.
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
