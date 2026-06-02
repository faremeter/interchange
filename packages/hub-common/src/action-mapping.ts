import type { RepoAction } from "@intx/types/sidecar";

/**
 * Translations between the HTTP request shape, the `RepoAction`
 * vocabulary used by the repo-store substrate, and the verb
 * vocabulary used by the grant store. This module is the single
 * source of truth: both the bearer middleware (which queries authz
 * with `repoActionToGrantVerb(httpToRepoAction(req))`) and the kind
 * handler (which receives the resolved `RepoAction`) import from
 * here, so the two layers cannot drift.
 */

export type HTTPRequestShape = {
  method: string;
  path: string;
  query: Record<string, string | undefined>;
};

const UPLOAD_PACK_SERVICE = "git-upload-pack";
const RECEIVE_PACK_SERVICE = "git-receive-pack";

/**
 * Resolve a smart-HTTP request to the `RepoAction` it requires. The
 * match is on the trailing smart-HTTP suffix so the same logic works
 * for every mount prefix (asset routes, agent-state routes, etc.).
 *
 * Throws when the request shape is not a recognised smart-HTTP
 * endpoint; callers are expected to gate this behind their route
 * matcher.
 */
export function httpToRepoAction(req: HTTPRequestShape): RepoAction {
  const path = req.path;
  const method = req.method.toUpperCase();

  if (method === "GET" && hasSuffix(path, "/info/refs")) {
    const service = req.query.service;
    if (service === undefined) {
      throw new Error(
        "unrecognised git smart-HTTP request: /info/refs missing service query",
      );
    }
    if (service !== UPLOAD_PACK_SERVICE && service !== RECEIVE_PACK_SERVICE) {
      throw new Error(
        `unrecognised git smart-HTTP request: unknown service ${service}`,
      );
    }
    return "resolveRef";
  }

  if (method === "POST" && hasSuffix(path, "/git-upload-pack")) {
    return "createPack";
  }

  if (method === "POST" && hasSuffix(path, "/git-receive-pack")) {
    return "receivePack";
  }

  throw new Error(`unrecognised git smart-HTTP request: ${method} ${path}`);
}

function hasSuffix(path: string, suffix: string): boolean {
  return path === suffix || path.endsWith(suffix);
}

/**
 * Translate a `RepoAction` to the grant verb used by the authz
 * grant store. Every `RepoAction` has exactly one verb; an
 * exhaustive switch makes the compiler enforce coverage when the
 * `RepoAction` union grows.
 */
export function repoActionToGrantVerb(action: RepoAction): string {
  switch (action) {
    case "init":
      return "create";
    case "writeTree":
    case "receivePack":
      return "write";
    case "createPack":
    case "resolveRef":
      return "read";
  }
}

/**
 * Mint-API friendly aliases that expand to one or more
 * `RepoAction`s. Callers that issue a token specify `actions:
 * ["can_read"]` or `actions: ["can_push"]` rather than enumerating
 * the underlying repo-store verbs.
 */
export const RepoActionAliases = {
  can_read: ["createPack", "resolveRef"],
  can_push: ["receivePack"],
} as const satisfies Record<string, readonly RepoAction[]>;

/**
 * Expand a mint-API actions string into the underlying RepoActions.
 * Accepts both aliases (`can_read`, `can_push`) and bare RepoAction
 * names. Unknown strings throw — callers receive the rejection at
 * the mint boundary rather than producing an empty grant set.
 */
export function expandRepoActionAlias(name: string): RepoAction[] {
  const alias = lookupAlias(name);
  if (alias !== null) return [...alias];
  const action = lookupRepoAction(name);
  if (action !== null) return [action];
  throw new Error(`unknown RepoAction alias: ${name}`);
}

function lookupAlias(name: string): readonly RepoAction[] | null {
  if (name === "can_read") return RepoActionAliases.can_read;
  if (name === "can_push") return RepoActionAliases.can_push;
  return null;
}

function lookupRepoAction(name: string): RepoAction | null {
  switch (name) {
    case "init":
    case "writeTree":
    case "receivePack":
    case "createPack":
    case "resolveRef":
      return name;
    default:
      return null;
  }
}
