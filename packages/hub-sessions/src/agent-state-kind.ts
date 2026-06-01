import { type } from "arktype";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  ValidatePushResult,
} from "./repo-store";

export type AgentStateHubPrincipal = { readonly kind: "hub" };

export type AgentStateSidecarPrincipal = {
  readonly kind: "sidecar";
  readonly agentId: string;
};

export type AgentStatePrincipal =
  | AgentStateHubPrincipal
  | AgentStateSidecarPrincipal;

const SidecarPrincipal = type({
  kind: "'sidecar'",
  agentId: "string",
});

export const AGENT_STATE_DEPLOY_REF = "refs/heads/deploy";

const ALLOWED_STATE_TOP_LEVEL = new Set([
  "state",
  ".gitignore",
  "turns.jsonl",
  "prompt.jsonl",
  "response.jsonl",
  "manifest.jsonl",
  "metadata.json",
  "tool-output",
]);

export const agentStateKindHandler: KindHandler = {
  kind: "agent-state",
  directoryPrefix: "agents",
  validatePush: ({ topLevelTreePaths }): ValidatePushResult => {
    const offender = topLevelTreePaths.find(
      (p) => !ALLOWED_STATE_TOP_LEVEL.has(p),
    );
    if (offender !== undefined) {
      return {
        ok: false,
        reason: `tree contains disallowed top-level path: ${offender}`,
      };
    }
    if (!topLevelTreePaths.some((p) => p !== ".gitignore")) {
      return {
        ok: false,
        reason: "tree must include at least one state-bearing top-level entry",
      };
    }
    return { ok: true };
  },
  onRefUpdated: () => {
    // No consumer at this kind today; the substrate's hook surface is
    // uniform across kinds and future kinds will use it.
  },
};

export const agentStateAuthorize: AuthorizeFn = (
  principal: Principal,
  repoId,
  ref,
  action,
) => {
  if (principal.kind === "hub") {
    // Full access at this kind. Hub-side reads (getDeployRef,
    // createDeployPack) and writes (writeDeployTree) all flow through
    // here, so changing this branch tightens behavior in non-obvious
    // places.
    return { allowed: true };
  }

  if (principal.kind === "sidecar") {
    const parsed = SidecarPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `sidecar principal is malformed: ${parsed.summary}`,
      };
    }
    if (repoId.kind !== "agent-state" || repoId.id !== parsed.agentId) {
      return {
        allowed: false,
        reason: `sidecar ${parsed.agentId} cannot access ${repoId.kind}/${repoId.id}`,
      };
    }
    switch (action) {
      case "receivePack":
        return { allowed: true };
      case "createPack":
      case "resolveRef":
        if (ref !== AGENT_STATE_DEPLOY_REF) {
          return {
            allowed: false,
            reason: `sidecar may only read ${AGENT_STATE_DEPLOY_REF}, not ${ref}`,
          };
        }
        return { allowed: true };
      case "init":
        return {
          allowed: false,
          reason: "init is not authorize-gated for agent-state",
        };
      case "writeTree":
        return {
          allowed: false,
          reason: `action ${action} is hub-only for agent-state`,
        };
      default: {
        const _exhaustive: never = action;
        return {
          allowed: false,
          reason: `unhandled action: ${String(_exhaustive)}`,
        };
      }
    }
  }

  return {
    allowed: false,
    reason: `unknown principal kind: ${principal.kind}`,
  };
};
