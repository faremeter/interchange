import { type } from "arktype";
import { glob, repoActionToGrantVerb } from "@intx/hub-common";
import {
  UserPrincipal,
  type AuthorizeFn,
  type KindHandler,
  type Principal,
  type ValidatePushResult,
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

// Mirror of the sidecar's agent-state write surface. The isogit
// ContextStore (`packages/storage-isogit/src/store.ts`) writes exactly
// these top-level entries: `turns.jsonl`, `prompt.jsonl`,
// `response.jsonl`, `manifest.jsonl`, `metadata.json`, the `tool-output/`
// blob directory, and the `state/` directory holding `state/audit/` and
// `state/errors/`. `.gitignore` is seeded once by `initSidecarRepo`.
// Adding a new top-level write on the sidecar side requires adding the
// entry here in the same change — receivePack will silently
// `path_violation` the push otherwise. The receivePack path
// (`pack-receive.ts:validateTree`) walks every top-level tree entry —
// files and directories alike — through this allowlist; widening it
// to anything unowned by the sidecar's writer would let a malicious
// pack smuggle non-state content into the repo.
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

const ALLOWED_DEPLOY_TOP_LEVEL = new Set(["deploy", ".gitignore"]);

export const agentStateKindHandler: KindHandler = {
  kind: "agent-state",
  directoryPrefix: "agents",
  validatePush: ({ ref, topLevelTreePaths }): ValidatePushResult => {
    // The deploy ref carries hub-authored prompt content under
    // `deploy/`; every other ref carries sidecar-pushed agent state
    // and must stay confined to the state-bearing allowlist.
    const allowed =
      ref === AGENT_STATE_DEPLOY_REF
        ? ALLOWED_DEPLOY_TOP_LEVEL
        : ALLOWED_STATE_TOP_LEVEL;
    const offender = topLevelTreePaths.find((p) => !allowed.has(p));
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
      case "resolveRef":
        return { allowed: true };
      case "createPack":
        if (ref !== AGENT_STATE_DEPLOY_REF) {
          return {
            allowed: false,
            reason: `sidecar may only fetch ${AGENT_STATE_DEPLOY_REF}, not ${ref}`,
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

  if (principal.kind === "user") {
    // The route layer has already pre-resolved the grant verdict and
    // attached it as `authz`. The substrate does NOT re-query the
    // grant store here; it (a) checks the bearer-token's claims
    // bound the requested (ref, action) and have not expired, and
    // (b) sanity-checks that the pre-resolved verdict targets this
    // exact resource and grant verb. Both gates must pass before the
    // verdict's `effect` is honoured.
    const parsed = UserPrincipal(principal);
    if (parsed instanceof type.errors) {
      return {
        allowed: false,
        reason: `user principal is malformed: ${parsed.summary}`,
      };
    }
    if (repoId.kind !== "agent-state") {
      return {
        allowed: false,
        reason: `user authorize received non-agent-state repo ${repoId.kind}/${repoId.id}`,
      };
    }
    if (!parsed.tokenClaims.actions.includes(action)) {
      return {
        allowed: false,
        reason: `token does not grant action ${action}`,
      };
    }
    // `ref === "*"` is the substrate's sentinel for the bulk read
    // performed by `listRefs`. Per-ref filtering is the advertise-refs
    // layer's responsibility, so the bulk read is gated on action and
    // expiry alone.
    if (ref !== "*" && !glob.match(parsed.tokenClaims.refPattern, ref)) {
      return {
        allowed: false,
        reason: `token refPattern ${parsed.tokenClaims.refPattern} does not match ${ref}`,
      };
    }
    if (Date.now() >= parsed.tokenClaims.expiresAt) {
      return {
        allowed: false,
        reason: `token expired at ${parsed.tokenClaims.expiresAt}`,
      };
    }
    const expectedResource = `agent-state:${repoId.id}`;
    if (parsed.authz.resource !== expectedResource) {
      return {
        allowed: false,
        reason: `authz verdict resource ${parsed.authz.resource} does not match ${expectedResource}`,
      };
    }
    const expectedGrantVerb = repoActionToGrantVerb(action);
    if (parsed.authz.grantVerb !== expectedGrantVerb) {
      return {
        allowed: false,
        reason: `authz verdict grantVerb ${parsed.authz.grantVerb} does not match ${expectedGrantVerb}`,
      };
    }
    if (parsed.authz.effect === "allow") {
      return { allowed: true };
    }
    return {
      allowed: false,
      reason: `authz verdict denied for ${expectedResource} ${expectedGrantVerb}`,
    };
  }

  // Fail closed on any kind not handled above. The tenant-level
  // `workflow` principal kind (`@intx/types` principalKinds) is a
  // grant owner, not an agent-state bearer, and never carries an
  // agent-state repo push here -- so it is intentionally left denied.
  return {
    allowed: false,
    reason: `unknown principal kind: ${principal.kind}`,
  };
};
