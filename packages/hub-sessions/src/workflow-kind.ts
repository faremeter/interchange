// KindHandler for the `workflow` asset kind.
//
// A workflow asset is a git repo that holds a single `WorkflowDefinition`
// envelope plus its capability-walk output. The deploy tree shape is:
//
//   - `workflow.json` — the full `WorkflowDefinition` envelope. The
//     content is parsed and structurally validated at push time; deeper
//     primitive-shape and DAG validation belongs to the runtime layer
//     that instantiates the definition (`defineWorkflow`).
//   - `capability-declarations.json` — the per-step capability-walk
//     output. Content shape is owned by the capability-walk module; the
//     substrate only verifies the file parses as a JSON object.
//   - `.gitignore` — supplied by the asset routes' genesis init body.
//
// Any top-level entry outside this set fails the push.
//
// Authz:
//   - hub principal: full access.
//   - sidecar principal: read-only (createPack, resolveRef).
//   - user principal: gated by bearer-token claims and the route
//     layer's pre-resolved authz verdict, mirroring the convention
//     used by skill assets.

import { type } from "arktype";
import { getLogger } from "@intx/log";
import { glob, repoActionToGrantVerb } from "@intx/hub-common";
import {
  UserPrincipal,
  type AuthorizeFn,
  type KindHandler,
  type Principal,
  type ValidatePushResult,
} from "./repo-store";

const logger = getLogger(["hub-sessions", "workflow-kind"]);

export type WorkflowHubPrincipal = { readonly kind: "hub" };

export type WorkflowSidecarPrincipal = {
  readonly kind: "sidecar";
  readonly agentId: string;
};

export type WorkflowPrincipal = WorkflowHubPrincipal | WorkflowSidecarPrincipal;

export const WORKFLOW_JSON_PATH = "workflow.json";
export const CAPABILITY_DECLARATIONS_JSON_PATH = "capability-declarations.json";
export const WORKFLOW_GITIGNORE_PATH = ".gitignore";

const ALLOWED_TOP_LEVEL = new Set<string>([
  WORKFLOW_JSON_PATH,
  CAPABILITY_DECLARATIONS_JSON_PATH,
  WORKFLOW_GITIGNORE_PATH,
]);

/**
 * Structural arktype validator for the `workflow.json` envelope. The
 * substrate checks the cross-cutting shape of `WorkflowDefinition`
 * (presence and primitive type of `id`, `triggers`, `steps`,
 * `stepOrder`) but does not re-derive `defineWorkflow`'s DAG-level
 * validation here — primitive-level shape, default-input application,
 * and `after`-ref resolution belong to the runtime layer that hydrates
 * the definition. Push-time validation rejects the obvious wrongs
 * (missing top-level fields, wrong primitive types) so a tree that
 * could not possibly hydrate into a `WorkflowDefinition` never reaches
 * the deploy ref.
 */
const StepsObject = type("Record<string, unknown>").narrow((value, ctx) => {
  if (Array.isArray(value)) {
    return ctx.mustBe("a JSON object, not an array");
  }
  return true;
});

const StateObject = type("Record<string, unknown>").narrow((value, ctx) => {
  if (Array.isArray(value)) {
    return ctx.mustBe("a JSON object, not an array");
  }
  return true;
});

export const workflowDefinitionEnvelopeSchema = type({
  id: "string > 0",
  triggers: "unknown[]",
  steps: StepsObject,
  stepOrder: "string[]",
  "state?": StateObject,
}).onUndeclaredKey("ignore");

/**
 * Capability-declarations.json is held to "is a JSON object" at this
 * commit; the per-step structure is owned by the capability-walk
 * module that authors the file. `Record<string, unknown>` on its own
 * accepts arrays under arktype's structural-object semantics, so the
 * push validator pairs it with an array-rejection narrow.
 */
const CapabilityDeclarationsObject = type("Record<string, unknown>").narrow(
  (value, ctx) => {
    if (Array.isArray(value)) {
      return ctx.mustBe("a JSON object, not an array");
    }
    return true;
  },
);

const SidecarPrincipal = type({
  kind: "'sidecar'",
  agentId: "string",
});

async function readJSONBlob(
  path: string,
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<{ ok: true; value: unknown } | { ok: false; reason: string }> {
  let raw: Uint8Array;
  try {
    raw = await readBlob(path);
  } catch (cause) {
    return {
      ok: false,
      reason: `${path} could not be read from the tree: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  const text = new TextDecoder().decode(raw);
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (cause) {
    return {
      ok: false,
      reason: `${path} is not valid JSON: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }
  return { ok: true, value: parsed };
}

export const workflowKindHandler: KindHandler = {
  kind: "workflow",
  directoryPrefix: "assets/workflow",
  async validatePush({
    repoId,
    ref,
    topLevelTreePaths,
    readBlob,
  }): Promise<ValidatePushResult> {
    for (const entry of topLevelTreePaths) {
      if (!ALLOWED_TOP_LEVEL.has(entry)) {
        return {
          ok: false,
          reason: `unexpected top-level entry ${JSON.stringify(entry)}; allowed: "${WORKFLOW_JSON_PATH}", "${CAPABILITY_DECLARATIONS_JSON_PATH}", "${WORKFLOW_GITIGNORE_PATH}"`,
        };
      }
    }

    // A workflow asset without `workflow.json` is structurally
    // incoherent: there is nothing for the deploy orchestrator to
    // hydrate. Reject so the push surfaces the missing envelope at
    // the boundary rather than at hydrate time.
    if (!topLevelTreePaths.includes(WORKFLOW_JSON_PATH)) {
      return {
        ok: false,
        reason: `tree is missing required ${WORKFLOW_JSON_PATH}`,
      };
    }

    const workflowOutcome = await readJSONBlob(WORKFLOW_JSON_PATH, readBlob);
    if (!workflowOutcome.ok) {
      logger.debug`workflow validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${workflowOutcome.reason}`;
      return { ok: false, reason: workflowOutcome.reason };
    }
    const validated = workflowDefinitionEnvelopeSchema(workflowOutcome.value);
    if (validated instanceof type.errors) {
      const reason = `${WORKFLOW_JSON_PATH} failed validation: ${validated.summary}`;
      logger.debug`workflow validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${reason}`;
      return { ok: false, reason };
    }

    if (topLevelTreePaths.includes(CAPABILITY_DECLARATIONS_JSON_PATH)) {
      const capOutcome = await readJSONBlob(
        CAPABILITY_DECLARATIONS_JSON_PATH,
        readBlob,
      );
      if (!capOutcome.ok) {
        logger.debug`workflow validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${capOutcome.reason}`;
        return { ok: false, reason: capOutcome.reason };
      }
      const capValidated = CapabilityDeclarationsObject(capOutcome.value);
      if (capValidated instanceof type.errors) {
        const reason = `${CAPABILITY_DECLARATIONS_JSON_PATH} must be a JSON object: ${capValidated.summary}`;
        logger.debug`workflow validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${reason}`;
        return { ok: false, reason };
      }
    }

    return { ok: true };
  },
  onRefUpdated() {
    // No cached index today. Consumers read the workflow.json and
    // capability-declarations.json through the substrate's blob-read
    // API at session time.
  },
};

export const workflowAuthorize: AuthorizeFn = (
  principal: Principal,
  repoId,
  ref,
  action,
) => {
  if (repoId.kind !== "workflow") {
    return {
      allowed: false,
      reason: `workflow authorize received non-workflow repo ${repoId.kind}/${repoId.id}`,
    };
  }

  if (principal.kind === "hub") {
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
    switch (action) {
      case "createPack":
      case "resolveRef":
        return { allowed: true };
      case "init":
      case "writeTree":
      case "receivePack":
        return {
          allowed: false,
          reason: `sidecars may only read workflow assets, not ${action}`,
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
    const expectedResource = `asset:${repoId.id}`;
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
  // grant owner, not a git/asset bearer, and never carries a workflow
  // repo push here -- so it is intentionally left denied.
  return {
    allowed: false,
    reason: `unknown principal kind: ${principal.kind}`,
  };
};
