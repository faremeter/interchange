// KindHandler for the `workflow` asset kind.
//
// A workflow asset is a codebase: a top-level `package.json` declaring an
// `interchange.workflow` entry module plus arbitrary source files. The sidecar
// materializes the codebase into a closure and evaluates the pinned entry to the
// definition. `validatePush` requires the `package.json`; a tree that lacks one
// is rejected. The legacy `workflow.json` envelope form is no longer accepted at
// the push boundary.
//
// Source files are unconstrained, but the push validates the manifest's shape
// and the entry-path's containment, and refuses an envelope-only
// `capability-declarations.json`, a committed `node_modules`, and an ambiguous
// tree that also carries an envelope-valid `workflow.json`, so one asset resolves
// to exactly one definition. The codebase shape accepts both a single package and
// a `workspaces` monorepo; for a monorepo the push validates only the root's
// well-formedness and leaves per-member validation to the resolver.
//
// Authz:
//   - hub principal: full access.
//   - sidecar principal: read-only (createPack, resolveRef).
//   - user principal: gated by bearer-token claims and the route
//     layer's pre-resolved authz verdict, mirroring the convention
//     used by skill assets.

import { type } from "arktype";
import { getLogger } from "@intx/log";
import {
  CredentialBinding,
  GrantRequirement,
  SidecarCapabilityPolicy,
} from "@intx/types";
import { PackageJSON, isContainedEntryPath } from "@intx/types/package-json";
import {
  authorizeUserPrincipal,
  type AuthorizeFn,
  type KindHandler,
  type Principal,
  type RepoId,
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
export const PACKAGE_JSON_PATH = "package.json";
export const NODE_MODULES_PATH = "node_modules";
export const PNPM_WORKSPACE_PATH = "pnpm-workspace.yaml";

/**
 * Structural arktype validator for the `workflow.json` envelope. The
 * substrate checks the cross-cutting shape of `WorkflowDefinition`
 * (presence and primitive type of `id`, `triggers`, `steps`,
 * `stepOrder`) but does not re-derive `defineWorkflow`'s DAG-level
 * validation here — primitive-level shape, default-input application,
 * and `after`-ref resolution belong to the runtime layer that hydrates
 * the definition. The codebase push uses this validator to detect an
 * ambiguous tree that also carries an envelope-valid `workflow.json`,
 * and the hydrate-time definition loaders reuse it to validate a
 * materialized definition before instantiation.
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
  // `grantRequirements` passes through the envelope whether or not it is
  // declared here: arktype's `.onUndeclaredKey("ignore")` below is
  // passthrough, not stripping (only `"delete"` strips), so the hydrate read
  // sees the field either way. Declaring it here VALIDATES declared
  // requirements at the deploy boundary — a malformed `source` is rejected
  // rather than passed through unchecked — as defense in depth alongside the
  // trigger route's own `GrantRequirements` re-validation. Compose the
  // exported `GrantRequirement` arktype rather than restating its shape so the
  // envelope and the definition stay in lockstep.
  "grantRequirements?": GrantRequirement.array(),
  // `credentialBindings` is validated here too -- same defense-in-depth
  // rationale as grantRequirements above: a malformed binding (bad locator,
  // authority, or handle) is rejected at the deploy boundary rather than
  // passed through to launch-time resolution unchecked.
  "credentialBindings?": CredentialBinding.array(),
  "sidecarPlacement?": SidecarCapabilityPolicy,
}).onUndeclaredKey("ignore");

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

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function rejectPush(
  repoId: RepoId,
  ref: string,
  reason: string,
): ValidatePushResult {
  logger.debug`workflow validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${reason}`;
  return { ok: false, reason };
}

/**
 * Validate the codebase shape: a top-level `package.json` declaring a contained
 * `interchange.workflow` entry (single package), or a `workspaces` monorepo
 * whose root well-formedness is checked and whose members are deferred to the
 * resolver, plus arbitrary source files. Entered when the tree carries a
 * `package.json`. Source files are unconstrained, but the push refuses the
 * envelope-only `capability-declarations.json`, a committed `node_modules`, and
 * an ambiguous tree that also carries an envelope-valid `workflow.json`, so one
 * asset resolves to exactly one definition.
 *
 * The push validates STRUCTURE only. It never imports or evaluates the entry
 * module -- that runs author code and is the sidecar's sandboxed job. The
 * entry-path containment check is the string-level half of the loader's rule
 * (`isContainedEntryPath`); the realpath-based symlink half runs at load time
 * against the materialized directory, which the hub does not have here.
 */
async function validateWorkflowCodebasePush(
  repoId: RepoId,
  ref: string,
  topLevelTreePaths: string[],
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<ValidatePushResult> {
  if (topLevelTreePaths.includes(CAPABILITY_DECLARATIONS_JSON_PATH)) {
    return rejectPush(
      repoId,
      ref,
      `${CAPABILITY_DECLARATIONS_JSON_PATH} is an envelope-only artifact and cannot appear in a codebase workflow asset`,
    );
  }
  if (topLevelTreePaths.includes(NODE_MODULES_PATH)) {
    return rejectPush(
      repoId,
      ref,
      `a committed top-level ${NODE_MODULES_PATH} directory is not allowed; the sidecar materializes dependencies from the resolved closure`,
    );
  }

  // A `workflow.json` that also parses as a valid envelope makes the asset
  // advertise two definitions; reject that. A `workflow.json` present but not a
  // valid envelope is an ordinary source file and is allowed.
  if (topLevelTreePaths.includes(WORKFLOW_JSON_PATH)) {
    const envelopeOutcome = await readJSONBlob(WORKFLOW_JSON_PATH, readBlob);
    if (
      envelopeOutcome.ok &&
      !(
        workflowDefinitionEnvelopeSchema(envelopeOutcome.value) instanceof
        type.errors
      )
    ) {
      return rejectPush(
        repoId,
        ref,
        `tree carries both ${PACKAGE_JSON_PATH} and an envelope-valid ${WORKFLOW_JSON_PATH}; a workflow asset must be a codebase or an envelope, not both`,
      );
    }
  }

  const pkgOutcome = await readJSONBlob(PACKAGE_JSON_PATH, readBlob);
  if (!pkgOutcome.ok) {
    return rejectPush(repoId, ref, pkgOutcome.reason);
  }

  // `PackageJSON` does not declare `workspaces`, so it is read off the raw
  // parsed value. A monorepo is a distinct codebase shape: the workflow lives
  // in one member, selected at resolve time by `packageName`, so this gate does
  // NOT descend into members or require a root `interchange.workflow`. It
  // validates ROOT well-formedness only -- `workspaces` is an array of glob
  // strings -- and defers per-member validation to the resolver, which re-reads
  // every member and fails loud there (one enumeration owner, not two).
  if (isJSONObject(pkgOutcome.value) && "workspaces" in pkgOutcome.value) {
    const workspaces = pkgOutcome.value["workspaces"];
    if (
      !Array.isArray(workspaces) ||
      !workspaces.every((w) => typeof w === "string")
    ) {
      return rejectPush(
        repoId,
        ref,
        `${PACKAGE_JSON_PATH} "workspaces" must be an array of glob strings; the object form ({ packages, catalog, catalogs }) is not supported`,
      );
    }
    return { ok: true };
  }

  // A pnpm monorepo declares its members in `pnpm-workspace.yaml`, not the
  // package.json `workspaces` field, so a pnpm root has no `workspaces` and
  // would fall through to the single-package check below. Reject that layout at
  // the boundary with a clear message rather than letting it fail obscurely at
  // resolve time (full pnpm support is tracked in INTR-461).
  if (topLevelTreePaths.includes(PNPM_WORKSPACE_PATH)) {
    return rejectPush(
      repoId,
      ref,
      `tree declares a ${PNPM_WORKSPACE_PATH}; the pnpm workspace layout is not supported -- declare members via a package.json "workspaces" array`,
    );
  }

  const pkg = PackageJSON(pkgOutcome.value);
  if (pkg instanceof type.errors) {
    return rejectPush(
      repoId,
      ref,
      `${PACKAGE_JSON_PATH} failed validation: ${pkg.summary}`,
    );
  }

  const entry = pkg.interchange?.workflow;
  if (entry === undefined || entry === "") {
    return rejectPush(
      repoId,
      ref,
      `${PACKAGE_JSON_PATH} must declare a non-empty "interchange.workflow" entry`,
    );
  }
  if (!isContainedEntryPath(entry)) {
    return rejectPush(
      repoId,
      ref,
      `"interchange.workflow" entry ${JSON.stringify(entry)} must be a package-relative path that does not escape the package`,
    );
  }

  return { ok: true };
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
    if (topLevelTreePaths.includes(PACKAGE_JSON_PATH)) {
      return validateWorkflowCodebasePush(
        repoId,
        ref,
        topLevelTreePaths,
        readBlob,
      );
    }
    return rejectPush(
      repoId,
      ref,
      `a workflow asset must be a codebase declaring a ${PACKAGE_JSON_PATH} with an "interchange.workflow" entry; the ${WORKFLOW_JSON_PATH} envelope form is no longer supported`,
    );
  },
  onRefUpdated() {
    // No cached index today. Consumers read the asset's tree through the
    // substrate's blob-read API at session time.
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
    return authorizeUserPrincipal({
      principal,
      repoId,
      ref,
      action,
      resourcePrefix: "asset",
    });
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
