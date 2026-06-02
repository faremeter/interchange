/**
 * Agent-state smart-HTTP route group.
 *
 * Two URL grammars exposed under two Hono sub-apps:
 *
 *   /api/tenants/:tenantId/agents/instances/:insId/state.git/...
 *     -> RepoId { kind: "agent-state", id: insId }
 *     (per-instance runtime state, written by the sidecar's first
 *      state pack)
 *
 *   /api/tenants/:tenantId/agents/definitions/:agtId/state.git/...
 *     -> RepoId { kind: "agent-state", id: agtId }
 *     (per-definition repo with hub-written deploy artifacts; the
 *      `deploy/` prefix is populated by writeDeployTree at instance
 *      launch)
 *
 * Both grammars are READ-ONLY over HTTP. Upload-pack
 * (`info/refs?service=git-upload-pack` and `POST /git-upload-pack`)
 * runs behind the same bearer middleware the asset routes use, with
 * a pre-resolved authz verdict on the constructed UserPrincipal.
 * Receive-pack
 * (`info/refs?service=git-receive-pack` and `POST /git-receive-pack`)
 * is denied at the edge with pkt-line-framed responses so a
 * `git push -v` parses the protocol-level rejection even when no
 * Authorization header is present. The receive-pack denial
 * middleware is mounted BEFORE bearer middleware in the app layer;
 * the substrate's `handleReceivePack` is NOT imported here — agent
 * state never accepts writes over HTTP.
 *
 * Both resolvers verify the instance / definition row belongs to
 * `:tenantId` and 404 otherwise. The per-instance repo is
 * lazily-materialised: on a never-pushed instance, `listRefs`
 * returns the empty list and the advertise layer emits the
 * `capabilities^{}` empty-repo record so a stock `git clone`
 * succeeds against an empty tree rather than 404ing.
 */

import { and, eq } from "drizzle-orm";
import { Hono, type Context } from "hono";
import { createMiddleware } from "hono/factory";
import type { MiddlewareHandler } from "hono";

import { authorize } from "@intx/authz";
import { agent as agentTable, agentInstance } from "@intx/db/schema";
import type { DB } from "@intx/db";
import { repoActionToGrantVerb } from "@intx/hub-common";
import { getLogger } from "@intx/log";
import type {
  RefEntry,
  RepoId,
  RepoStore,
  UserPrincipal,
} from "@intx/hub-sessions";
import type { RepoAction } from "@intx/types/sidecar";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";

import type { TenantEnv } from "../context";
import type { GitTokenClaims } from "../middleware/git-token-auth";
import {
  advertiseUploadPack,
  type RefSource,
} from "../git-http/advertise-refs";
import {
  handleUploadPack,
  type UploadPackRepoStore,
} from "../git-http/upload-pack";
import { writePktLine, writeFlush } from "../git-http/pkt-line";

const log = getLogger(["hub", "agent-state-git"]);

// ----- Receive-pack denial: pkt-line responses -----------------------
//
// The advertise denial body is locked to:
//
//   # service=git-receive-pack\n0000ERR agent-state is read-only over HTTP\n
//
// The leading `# service=` line and the `0000` flush packet mirror the
// shape stock git emits for a successful advertise; the trailing
// `ERR ...` substring is what `git push -v` surfaces as the visible
// rejection reason. The body is emitted verbatim (not pkt-line
// framed beyond the literal `0000` flush in the middle).
const RECEIVE_PACK_ADVERTISE_DENY_BODY =
  "# service=git-receive-pack\n0000ERR agent-state is read-only over HTTP\n";

// The POST denial reason that surfaces in the `unpack` and per-ref
// `ng` pkt-lines. Short, stable, support-recognisable.
const RECEIVE_PACK_POST_DENY_REASON = "agent-state-readonly";

const RECEIVE_PACK_RESULT_CONTENT_TYPE =
  "application/x-git-receive-pack-result";
const RECEIVE_PACK_ADVERTISEMENT_CONTENT_TYPE =
  "application/x-git-receive-pack-advertisement";

function parseHex4(buf: Uint8Array, off: number): number {
  let v = 0;
  for (let i = 0; i < 4; i++) {
    const c = buf[off + i];
    if (c === undefined) {
      throw new Error("truncated pkt-line: short header");
    }
    let d: number;
    if (c >= 0x30 && c <= 0x39) {
      d = c - 0x30;
    } else if (c >= 0x61 && c <= 0x66) {
      d = c - 0x61 + 10;
    } else if (c >= 0x41 && c <= 0x46) {
      d = c - 0x41 + 10;
    } else {
      throw new Error("malformed pkt-line length");
    }
    v = (v << 4) | d;
  }
  return v;
}

/**
 * Minimal parse of the receive-pack request body to extract the list
 * of ref names. The full request grammar includes old/new shas and
 * optional capability tail; we only care about the ref name for the
 * `ng <ref> <reason>` lines. Capabilities and packfile bytes that
 * follow the flush packet are ignored.
 */
function extractReceivePackRefs(body: Uint8Array): string[] {
  const decoder = new TextDecoder();
  const refs: string[] = [];
  let off = 0;
  while (off + 4 <= body.length) {
    const length = parseHex4(body, off);
    off += 4;
    if (length === 0) {
      // flush: end of commands
      break;
    }
    if (length < 4) {
      throw new Error(`reserved pkt-line length: ${length}`);
    }
    const bodyLen = length - 4;
    if (off + bodyLen > body.length) {
      throw new Error("truncated receive-pack pkt-line body");
    }
    const line = decoder.decode(body.subarray(off, off + bodyLen));
    off += bodyLen;
    // strip optional capability tail after \0 and trailing \n
    const nulIdx = line.indexOf("\0");
    const head = nulIdx === -1 ? line : line.substring(0, nulIdx);
    const trimmed = head.endsWith("\n") ? head.slice(0, -1) : head;
    const parts = trimmed.split(" ");
    if (parts.length < 3) continue;
    const ref = parts.slice(2).join(" ");
    if (ref.length > 0) refs.push(ref);
  }
  return refs;
}

async function writeReceivePackDenyReport(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  refs: readonly string[],
): Promise<void> {
  await writePktLine(writer, `unpack ${RECEIVE_PACK_POST_DENY_REASON}\n`);
  for (const ref of refs) {
    await writePktLine(writer, `ng ${ref} ${RECEIVE_PACK_POST_DENY_REASON}\n`);
  }
  await writeFlush(writer);
}

function buildReceivePackPostDenyStream(
  refs: readonly string[],
): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      const sink = new WritableStream<Uint8Array>({
        write(chunk) {
          controller.enqueue(chunk);
        },
      });
      const writer = sink.getWriter();
      try {
        await writeReceivePackDenyReport(writer, refs);
        await writer.close();
        controller.close();
      } catch (cause) {
        await writer.abort(cause).catch(() => undefined);
        controller.error(cause);
      }
    },
  });
}

// ----- Receive-pack deny middleware ---------------------------------
//
// Mounted ahead of the bearer middleware. Intercepts:
//   - GET .../info/refs?service=git-receive-pack -> 403 with locked body
//   - POST .../git-receive-pack                  -> 200 pkt-line denial
// Other requests (upload-pack info/refs and POST) call next() so the
// subsequent bearer + upload-pack handlers run normally.

function urlPathEndsWith(path: string, suffix: string): boolean {
  return path === suffix || path.endsWith(suffix);
}

/**
 * Hono middleware that denies any receive-pack request reaching the
 * agent-state route surface, regardless of bearer-token presence.
 * Mount BEFORE `createGitTokenAuth` so unauthenticated `git push -v`
 * sees the locked pkt-line ERR rather than a 401.
 */
export function createAgentStateReceivePackDeny(): MiddlewareHandler {
  return createMiddleware(async (c, next) => {
    const method = c.req.method.toUpperCase();
    const path = c.req.path;

    if (method === "GET" && urlPathEndsWith(path, "/info/refs")) {
      const service = c.req.query("service");
      if (service === "git-receive-pack") {
        log.info("receive-pack advertise denied {path}", { path });
        return new Response(RECEIVE_PACK_ADVERTISE_DENY_BODY, {
          status: 403,
          headers: {
            "content-type": RECEIVE_PACK_ADVERTISEMENT_CONTENT_TYPE,
            "cache-control": "no-cache",
          },
        });
      }
      // upload-pack advertise: fall through.
      await next();
      return;
    }

    if (method === "POST" && urlPathEndsWith(path, "/git-receive-pack")) {
      log.info("receive-pack POST denied {path}", { path });
      let refs: string[] = [];
      try {
        const body = new Uint8Array(await c.req.raw.arrayBuffer());
        refs = extractReceivePackRefs(body);
      } catch (err) {
        // Malformed body still gets a deny report with no per-ref lines.
        log.info("receive-pack POST: body parse failed {err}", {
          err: err instanceof Error ? err.message : String(err),
        });
        refs = [];
      }
      const stream = buildReceivePackPostDenyStream(refs);
      return new Response(stream, {
        status: 200,
        headers: {
          "content-type": RECEIVE_PACK_RESULT_CONTENT_TYPE,
          "cache-control": "no-cache",
        },
      });
    }

    await next();
  });
}

// ----- Pre-resolved authz + UserPrincipal construction --------------

function dateToNumber(d: Date): number {
  return d.getTime();
}

async function resolveAuthzVerdict(args: {
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  principalId: string;
  tenantId: string;
  agentStateId: string;
  action: RepoAction;
}): Promise<UserPrincipal["authz"]> {
  const resource = `agent-state:${args.agentStateId}`;
  const grantVerb = repoActionToGrantVerb(args.action);
  const verdict = await authorize(
    args.grantStore,
    args.principalId,
    args.tenantId,
    resource,
    grantVerb,
    args.conditionRegistry,
  );
  return {
    effect: verdict.effect === "allow" ? "allow" : "deny",
    resource,
    grantVerb,
  };
}

function buildUserPrincipal(args: {
  principalId: string;
  tenantId: string;
  authz: UserPrincipal["authz"];
  claims: GitTokenClaims;
}): UserPrincipal {
  return {
    kind: "user",
    principalId: args.principalId,
    tenantId: args.tenantId,
    authz: args.authz,
    tokenClaims: {
      refPattern: args.claims.refPattern,
      actions: args.claims.actions,
      expiresAt: dateToNumber(args.claims.expiresAt),
    },
  };
}

// ----- Substrate adapters -------------------------------------------

function makeRefSource(
  repoStore: RepoStore,
  principal: UserPrincipal,
): RefSource {
  return {
    async listRefs(_p, repoId): Promise<RefEntry[]> {
      return repoStore.listRefs(principal, repoId);
    },
  };
}

function makeUploadPackStore(
  repoStore: RepoStore,
  principal: UserPrincipal,
): UploadPackRepoStore {
  return {
    async listRefs(_p, repoId): Promise<RefEntry[]> {
      return repoStore.listRefs(principal, repoId);
    },
    async getRepoDir(_p, repoId): Promise<string> {
      return repoStore.getRepoDir(repoId);
    },
  };
}

// ----- Resolver shape -----------------------------------------------

type AgentStateRouteMode = "instance" | "definition";

type SmartHttpResolved = {
  principal: UserPrincipal;
  repoId: RepoId;
};

type ResolveResult =
  | { ok: true; resolved: SmartHttpResolved }
  | {
      ok: false;
      status: 400 | 401 | 403 | 404;
      code: string;
      message: string;
    };

async function resolveAgentStateId(
  db: DB["db"],
  mode: AgentStateRouteMode,
  tenantId: string,
  paramId: string,
): Promise<{ ok: true; id: string } | { ok: false; reason: string }> {
  if (mode === "instance") {
    const row = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.id, paramId),
        eq(agentInstance.tenantId, tenantId),
      ),
    });
    if (row === undefined) {
      return { ok: false, reason: `no instance ${paramId} in tenant` };
    }
    return { ok: true, id: row.id };
  }
  const row = await db.query.agent.findFirst({
    where: and(eq(agentTable.id, paramId), eq(agentTable.tenantId, tenantId)),
  });
  if (row === undefined) {
    return { ok: false, reason: `no agent definition ${paramId} in tenant` };
  }
  return { ok: true, id: row.id };
}

type ResolveSmartHttpDeps = {
  db: DB["db"];
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
};

async function resolveSmartHttp(
  deps: ResolveSmartHttpDeps,
  c: Context<TenantEnv>,
  mode: AgentStateRouteMode,
  action: RepoAction,
): Promise<ResolveResult> {
  const tenantRow = c.get("tenant");
  const principalRow = c.get("principal");
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bearer middleware populates git-token-claims as a variable; the typed env shape is not visible at this nested layer
  const claims = c.get("git-token-claims" as never) as
    | GitTokenClaims
    | undefined;
  if (claims === undefined) {
    return {
      ok: false,
      status: 401,
      code: "unauthorized",
      message: "bearer middleware did not populate git-token-claims",
    };
  }
  if (!claims.actions.includes(action)) {
    return {
      ok: false,
      status: 403,
      code: "forbidden",
      message: `token claims do not include action ${action}`,
    };
  }
  const paramName = mode === "instance" ? "instanceId" : "agentId";
  const paramId = c.req.param(paramName);
  if (paramId === undefined) {
    return {
      ok: false,
      status: 400,
      code: "bad_request",
      message: `missing :${paramName} in URL`,
    };
  }
  const tenantId = tenantRow.id;
  const resolved = await resolveAgentStateId(deps.db, mode, tenantId, paramId);
  if (!resolved.ok) {
    return {
      ok: false,
      status: 404,
      code: "not_found",
      message: resolved.reason,
    };
  }
  const authz = await resolveAuthzVerdict({
    grantStore: deps.grantStore,
    conditionRegistry: deps.conditionRegistry,
    principalId: principalRow.id,
    tenantId,
    agentStateId: resolved.id,
    action,
  });
  if (authz.effect !== "allow") {
    log.info(
      "agent-state authz denied {tenantId} {mode}={id} principal={principalId}",
      {
        tenantId,
        mode,
        id: resolved.id,
        principalId: principalRow.id,
      },
    );
    return {
      ok: false,
      status: 403,
      code: "forbidden",
      message: "authz denied",
    };
  }
  const principal = buildUserPrincipal({
    principalId: principalRow.id,
    tenantId,
    authz,
    claims,
  });
  const repoId: RepoId = { kind: "agent-state", id: resolved.id };
  return { ok: true, resolved: { principal, repoId } };
}

// ----- Upload-pack route factory ------------------------------------

export type CreateAgentStateGitRoutesDeps = {
  db: DB["db"];
  repoStore: RepoStore;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
};

function createAgentStateGitRoutes(
  deps: CreateAgentStateGitRoutesDeps,
  mode: AgentStateRouteMode,
): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();
  const paramSeg = mode === "instance" ? ":instanceId" : ":agentId";

  app.get(`/${paramSeg}/state.git/info/refs`, async (c) => {
    const service = c.req.query("service");
    if (service !== "git-upload-pack") {
      // The receive-pack case is handled by the deny middleware above;
      // anything else is a bad request.
      return c.json(
        {
          error: {
            code: "bad_request",
            message: "info/refs requires service=git-upload-pack",
          },
        },
        400,
      );
    }
    const r = await resolveSmartHttp(deps, c, mode, "resolveRef");
    if (!r.ok) {
      return c.json({ error: { code: r.code, message: r.message } }, r.status);
    }
    const refSource = makeRefSource(deps.repoStore, r.resolved.principal);
    const stream = await advertiseUploadPack(
      refSource,
      r.resolved.principal,
      r.resolved.repoId,
    );
    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "application/x-git-upload-pack-advertisement",
        "cache-control": "no-cache",
      },
    });
  });

  app.post(`/${paramSeg}/state.git/git-upload-pack`, async (c) => {
    const r = await resolveSmartHttp(deps, c, mode, "createPack");
    if (!r.ok) {
      return c.json({ error: { code: r.code, message: r.message } }, r.status);
    }
    return handleUploadPack(
      makeUploadPackStore(deps.repoStore, r.resolved.principal),
      r.resolved.principal,
      r.resolved.repoId,
      c.req.raw,
    );
  });

  return app;
}

export function createAgentStateInstanceGitRoutes(
  deps: CreateAgentStateGitRoutesDeps,
): Hono<TenantEnv> {
  return createAgentStateGitRoutes(deps, "instance");
}

export function createAgentStateDefinitionGitRoutes(
  deps: CreateAgentStateGitRoutesDeps,
): Hono<TenantEnv> {
  return createAgentStateGitRoutes(deps, "definition");
}

/**
 * Smart-HTTP paths excluded from the OpenAPI document. The agent-state
 * routes serve binary git wire vocabulary; advertising them in the
 * generated spec would invite client codegen to treat them as JSON
 * endpoints.
 */
export const AGENT_STATE_OPENAPI_EXCLUDE_GLOBS = [
  "/api/tenants/*/agents/instances/*/state.git/info/refs",
  "/api/tenants/*/agents/instances/*/state.git/git-upload-pack",
  "/api/tenants/*/agents/instances/*/state.git/git-receive-pack",
  "/api/tenants/*/agents/definitions/*/state.git/info/refs",
  "/api/tenants/*/agents/definitions/*/state.git/git-upload-pack",
  "/api/tenants/*/agents/definitions/*/state.git/git-receive-pack",
] as const;
