import type { RepoAction } from "@intx/types/sidecar";

/**
 * Single source of truth mapping smart-HTTP requests to `RepoAction`s
 * and `RepoAction`s to grant verbs, so the bearer middleware and the
 * repo-store kind handler cannot drift.
 */

export type HTTPRequestShape = {
  method: string;
  path: string;
  query: Record<string, string | undefined>;
};

const UPLOAD_PACK_SERVICE = "git-upload-pack";
const RECEIVE_PACK_SERVICE = "git-receive-pack";

/**
 * Resolve a smart-HTTP request to the `RepoAction` it requires, matching
 * on the trailing suffix so it works under any mount prefix. Throws for
 * unrecognised endpoints; callers gate this behind their route matcher.
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
 * Every `RepoAction` maps to exactly one grant verb; the exhaustive
 * switch makes the compiler enforce coverage as the union grows.
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
 * Mint-API aliases expanding to one or more `RepoAction`s, so callers
 * issue `["can_read"]` instead of enumerating the underlying verbs.
 */
export const RepoActionAliases = {
  can_read: ["createPack", "resolveRef"],
  can_push: ["receivePack"],
} as const satisfies Record<string, readonly RepoAction[]>;

/**
 * Expand a mint-API actions string (alias or bare `RepoAction` name) to
 * RepoActions. Unknown strings throw, so the mint boundary rejects
 * rather than granting an empty set.
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
