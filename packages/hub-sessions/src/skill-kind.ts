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

const logger = getLogger(["hub-sessions", "skill-kind"]);

export type SkillHubPrincipal = { readonly kind: "hub" };

export type SkillSidecarPrincipal = {
  readonly kind: "sidecar";
  readonly agentId: string;
};

export type SkillPrincipal = SkillHubPrincipal | SkillSidecarPrincipal;

/**
 * arktype schema for the SKILL.md frontmatter. Required fields are
 * `name` and `description`; the Claude Code superset of optional fields
 * (`when_to_use`, `allowed-tools`, `paths`, `model`, ...) is accepted
 * via `onUndeclaredKey("ignore")` but not enforced.
 *
 * The forbidden-name narrow rejects `"anthropic"` and `"claude"`
 * because those values are reserved by the upstream agentskills.io
 * spec for vendor-owned skill packs.
 */
export const skillFrontmatterSchema = type({
  name: type(/^[a-z0-9]+(-[a-z0-9]+)*$/)
    .and("string<=64")
    .narrow((n: string, ctx) => {
      if (n === "anthropic" || n === "claude") {
        return ctx.mustBe(`not the reserved name "anthropic" or "claude"`);
      }
      return true;
    }),
  description: type("1 <= string <= 1024").and(type(/^(?!.*<[^>]+>).*$/s)),
}).onUndeclaredKey("ignore");

export type SkillFrontmatter = typeof skillFrontmatterSchema.infer;

export type SkillIndexEntry = {
  /** Equal to the containing directory name and to `frontmatter.name`. */
  name: string;
  description: string;
  /** Full parsed frontmatter, including any accepted optional fields. */
  frontmatter: Record<string, unknown>;
  /** Path of the skill subdirectory relative to the asset mount, with trailing slash. */
  workspaceSubpath: string;
};

type CacheKey = string;

function cacheKey(assetId: string, ref: string): CacheKey {
  return `${assetId}\u0000${ref}`;
}

const skillIndex = new Map<CacheKey, SkillIndexEntry[]>();
const pendingIndex = new Map<CacheKey, SkillIndexEntry[]>();

/**
 * Returns the parsed skill index for `(assetId, ref)`, or an empty
 * array if no index has been populated yet. The index is refreshed by
 * the kind handler's `onRefUpdated` hook after each successful write.
 */
export function getSkillIndex(assetId: string, ref: string): SkillIndexEntry[] {
  return skillIndex.get(cacheKey(assetId, ref)) ?? [];
}

const FRONTMATTER_DELIMITER = "---";

type ParsedSkillMd = {
  frontmatter: Record<string, unknown>;
};

const YamlMapping = type("Record<string, unknown>");

function parseSkillMd(body: string): ParsedSkillMd {
  const lines = body.split(/\r?\n/);
  if (lines[0] !== FRONTMATTER_DELIMITER) {
    throw new Error("SKILL.md is missing YAML frontmatter delimiter");
  }
  let endIdx = -1;
  for (let i = 1; i < lines.length; i += 1) {
    if (lines[i] === FRONTMATTER_DELIMITER) {
      endIdx = i;
      break;
    }
  }
  if (endIdx === -1) {
    throw new Error("SKILL.md frontmatter has no closing delimiter");
  }
  const yamlText = lines.slice(1, endIdx).join("\n");
  const parsed: unknown = Bun.YAML.parse(yamlText);
  const validated = YamlMapping(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `SKILL.md frontmatter must be a YAML mapping: ${validated.summary}`,
    );
  }
  return { frontmatter: validated };
}

type ParseOutcome =
  | { ok: true; entry: SkillIndexEntry }
  | { ok: false; reason: string };

async function parseSkillEntry(
  subdir: string,
  readBlob: (path: string) => Promise<Uint8Array>,
): Promise<ParseOutcome> {
  const skillPath = `${subdir}/SKILL.md`;
  let raw: Uint8Array;
  try {
    raw = await readBlob(skillPath);
  } catch (cause) {
    return {
      ok: false,
      reason: `skill ${subdir} is missing SKILL.md: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }

  const body = new TextDecoder().decode(raw);
  let parsed: ParsedSkillMd;
  try {
    parsed = parseSkillMd(body);
  } catch (cause) {
    return {
      ok: false,
      reason: `skill ${subdir} frontmatter parse failed: ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    };
  }

  const result = skillFrontmatterSchema(parsed.frontmatter);
  if (result instanceof type.errors) {
    return {
      ok: false,
      reason: `skill ${subdir} frontmatter is invalid: ${result.summary}`,
    };
  }

  if (result.name !== subdir) {
    return {
      ok: false,
      reason: `skill ${subdir} frontmatter.name ${JSON.stringify(
        result.name,
      )} does not match directory name`,
    };
  }

  return {
    ok: true,
    entry: {
      name: result.name,
      description: result.description,
      frontmatter: parsed.frontmatter,
      workspaceSubpath: `${subdir}/`,
    },
  };
}

async function buildSkillIndex(
  topLevelTreePaths: string[],
  readBlob: (path: string) => Promise<Uint8Array>,
  listDir: (path: string) => Promise<string[]>,
): Promise<
  { ok: true; entries: SkillIndexEntry[] } | { ok: false; reason: string }
> {
  const entries: SkillIndexEntry[] = [];
  // Sort so the index ordering is deterministic across reads.
  const candidates = [...topLevelTreePaths].sort();
  for (const candidate of candidates) {
    // Skill subdirectories are arbitrarily named (`<skill-name>/`), so
    // there is no enumerable allowlist of names the way the workflow,
    // package-registry, and agent-state handlers use. The structural
    // distinction is tree-vs-blob: a top-level tree entry is a skill
    // subdir, a top-level blob (e.g. `.gitignore` seeded by the
    // genesis init) is not.
    //
    // Probe via `listDir`. The substrate's two implementations
    // disagree on the not-a-tree signal — the receivePack closures
    // throw, the writeTree closures return an empty array — so this
    // probe accepts either signal as "not a directory" and skips the
    // entry. Git does not store empty trees, so an empty `listDir`
    // result is equivalent to "this name is not a directory in the
    // prospective tree".
    let children: string[];
    try {
      children = await listDir(candidate);
    } catch {
      continue;
    }
    if (children.length === 0) continue;
    const outcome = await parseSkillEntry(candidate, readBlob);
    if (!outcome.ok) {
      return { ok: false, reason: outcome.reason };
    }
    entries.push(outcome.entry);
  }
  return { ok: true, entries };
}

export const skillKindHandler: KindHandler = {
  kind: "skill",
  directoryPrefix: "assets/skill",
  async validatePush({
    repoId,
    ref,
    topLevelTreePaths,
    readBlob,
    listDir,
  }): Promise<ValidatePushResult> {
    // Drop any staged entry from a previous attempt first. The
    // substrate calls validatePush before advancing the ref, so a prior
    // validation that was accepted but never followed by onRefUpdated
    // (e.g. the commit step threw after validation succeeded) would
    // otherwise leave a stale entry that a later rejected attempt
    // would silently inherit.
    const key = cacheKey(repoId.id, ref);
    pendingIndex.delete(key);

    const result = await buildSkillIndex(topLevelTreePaths, readBlob, listDir);
    if (!result.ok) {
      logger.debug`skill validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${result.reason}`;
      return { ok: false, reason: result.reason };
    }
    // Stage the parsed index against (repoId.id, ref). The substrate
    // calls onRefUpdated immediately after the ref is advanced and we
    // promote the staged entry into the live cache there.
    pendingIndex.set(key, result.entries);
    return { ok: true };
  },
  onRefUpdated({ repoId, ref }) {
    const key = cacheKey(repoId.id, ref);
    const staged = pendingIndex.get(key);
    if (staged === undefined) {
      throw new Error(
        `skillKindHandler.onRefUpdated: no validated tree pending for ${repoId.id} @ ${ref}`,
      );
    }
    pendingIndex.delete(key);
    skillIndex.set(key, staged);
  },
};

const SidecarPrincipal = type({
  kind: "'sidecar'",
  agentId: "string",
});

export const skillAuthorize: AuthorizeFn = (
  principal: Principal,
  repoId,
  ref,
  action,
) => {
  if (repoId.kind !== "skill") {
    return {
      allowed: false,
      reason: `skill authorize received non-skill repo ${repoId.kind}/${repoId.id}`,
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
          reason: `sidecars may only read skill assets, not ${action}`,
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
    // `ref === "*"` is the substrate's sentinel for a bulk read used
    // by `listRefs`. The advertise-refs layer applies the per-ref
    // refPattern filter on the listing it returns, so gating the bulk
    // read on refPattern here would prevent any token with a
    // narrow-pattern (e.g. `refs/heads/main`) from listing refs at
    // all. Only the action and expiry are enforced for the bulk read;
    // every other call site passes a concrete ref and is gated below.
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

  return {
    allowed: false,
    reason: `unknown principal kind: ${principal.kind}`,
  };
};
