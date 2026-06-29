import { and, eq } from "drizzle-orm";
import { createMiddleware } from "hono/factory";
import type { Context, Env, MiddlewareHandler } from "hono";

import type { DB } from "@intx/db";
import { parseGitTokenRow } from "@intx/db";
import { gitToken, principal, tenant } from "@intx/db/schema";
import { PAT_PREFIX, SVC_PREFIX } from "@intx/hub-common";
import { getLogger } from "@intx/log";
import type { RepoAction } from "@intx/hub-sessions";

import type { AppEnv, PrincipalRow, TenantRow } from "../context";

const log = getLogger(["hub", "git-token"]);

const WWW_AUTHENTICATE_HEADER = "WWW-Authenticate";
const WWW_AUTHENTICATE_VALUE = 'Basic realm="Interchange"';

/**
 * Claims carried on a successfully authenticated git token. These
 * mirror the typed columns on the `git_token` row but are the only
 * shape downstream middleware should rely on; the row itself is not
 * exposed.
 */
export type GitTokenClaims = {
  resource: string;
  refPattern: string;
  actions: RepoAction[];
  expiresAt: Date;
};

export type GitTokenAuthEnv = Env & {
  Variables: {
    principal: PrincipalRow;
    tenant: TenantRow;
    "git-token-claims": GitTokenClaims;
  };
};

/**
 * Hono environment for routes mounted under the bearer middleware.
 * Combines the tenant-resolution variables with the bearer auth
 * variables so the smart-HTTP route handlers can read
 * `git-token-claims` straight from `c.get(...)` without an `as` cast.
 */
export type TenantGitTokenEnv = Env & {
  Variables: AppEnv["Variables"] & {
    tenant: TenantRow;
    principal: PrincipalRow;
    "git-token-claims": GitTokenClaims;
  };
};

export type CreateGitTokenAuthDeps = {
  db: DB["db"];
};

/**
 * Bearer-token authentication for the smart-HTTP git endpoints.
 * Parses `Authorization: Basic` (password is the secret; username is
 * ignored but logged) or `Authorization: Bearer`, validates the
 * `itx_pat_` / `itx_svc_` prefix shape, hashes the secret with
 * SHA-256, looks up the matching `git_token` row, and resolves the
 * principal and tenant before passing to the next middleware.
 *
 * On success, the middleware sets `principal`, `tenant`, and
 * `git-token-claims` on the request context.
 */
export function createGitTokenAuth({
  db,
}: CreateGitTokenAuthDeps): MiddlewareHandler<GitTokenAuthEnv> {
  return createMiddleware<GitTokenAuthEnv>(async (c, next) => {
    const authHeader = c.req.header("authorization");
    const parsed = parseAuthorizationHeader(authHeader);
    if (parsed === null) {
      log.info("git-token auth: missing or unparseable Authorization header");
      return unauthorized(c, "Authentication required");
    }

    const { secret, basicUsername } = parsed;
    if (basicUsername !== null) {
      log.info(
        "git-token auth: Basic username received (ignored for gating) {username}",
        { username: basicUsername },
      );
    }

    if (!hasKnownPrefix(secret)) {
      log.info("git-token auth: malformed token prefix");
      return unauthorized(c, "Authentication required");
    }

    const tokenHash = await sha256(secret);

    const tokenRowRaw = await db.query.gitToken.findFirst({
      where: eq(gitToken.tokenHashSha256, tokenHash),
    });

    if (!tokenRowRaw) {
      log.info("git-token auth: unknown token");
      return unauthorized(c, "Authentication required");
    }

    const tokenRow = parseGitTokenRow(tokenRowRaw);

    if (tokenRow.revokedAt !== null) {
      log.info("git-token auth: token revoked {tokenId}", {
        tokenId: tokenRow.id,
      });
      return forbidden(c, "token_revoked", "Token has been revoked");
    }

    const now = new Date();
    if (tokenRow.expiresAt.getTime() <= now.getTime()) {
      log.info("git-token auth: token expired {tokenId}", {
        tokenId: tokenRow.id,
      });
      return forbidden(c, "token_expired", "Token has expired");
    }

    const urlTenantId = c.req.param("tenantId");

    if (tokenRow.tenantId !== null) {
      if (urlTenantId !== undefined && urlTenantId !== tokenRow.tenantId) {
        log.info(
          "git-token auth: tenant mismatch {tokenId} token={tokenTenantId} url={urlTenantId}",
          {
            tokenId: tokenRow.id,
            tokenTenantId: tokenRow.tenantId,
            urlTenantId,
          },
        );
        return forbidden(
          c,
          "tenant_mismatch",
          "Token is not valid for this tenant",
        );
      }
    }

    const resolvedTenantId =
      tokenRow.tenantId !== null ? tokenRow.tenantId : urlTenantId;
    if (resolvedTenantId === undefined) {
      log.info(
        "git-token auth: tenant cannot be resolved (token not tenant-bound and URL has no :tenantId) {tokenId}",
        { tokenId: tokenRow.id },
      );
      return forbidden(
        c,
        "tenant_mismatch",
        "Token is not valid for this request",
      );
    }

    const tenantRow = await db.query.tenant.findFirst({
      where: eq(tenant.id, resolvedTenantId),
    });

    if (!tenantRow) {
      log.info("git-token auth: tenant not found {tenantId}", {
        tenantId: resolvedTenantId,
      });
      return forbidden(
        c,
        "tenant_mismatch",
        "Token is not valid for this tenant",
      );
    }

    const principalRow = await resolvePrincipal(db, tokenRow, resolvedTenantId);

    if (!principalRow) {
      log.info(
        "git-token auth: principal not found {tokenId} userId={userId} tenantId={tenantId}",
        {
          tokenId: tokenRow.id,
          userId: tokenRow.userId,
          tenantId: resolvedTenantId,
        },
      );
      return forbidden(
        c,
        "principal_not_found",
        "No principal is registered for this token in the target tenant",
      );
    }

    if (principalRow.status !== "active") {
      log.info(
        "git-token auth: principal suspended {principalId} status={status}",
        {
          principalId: principalRow.id,
          status: principalRow.status,
        },
      );
      return forbidden(c, "principal_suspended", "Principal is not active");
    }

    log.info(
      "git-token auth: success {tokenId} principal={principalId} tenant={tenantId}",
      {
        tokenId: tokenRow.id,
        principalId: principalRow.id,
        tenantId: tenantRow.id,
      },
    );

    c.set("principal", principalRow);
    c.set("tenant", tenantRow);
    c.set("git-token-claims", {
      resource: tokenRow.resource,
      refPattern: tokenRow.refPattern,
      actions: tokenRow.actions,
      expiresAt: tokenRow.expiresAt,
    });

    await next();
  });
}

type ParsedAuthorization = {
  secret: string;
  basicUsername: string | null;
};

function parseAuthorizationHeader(
  header: string | undefined,
): ParsedAuthorization | null {
  if (header === undefined) return null;

  const trimmed = header.trim();
  if (trimmed.length === 0) return null;

  const spaceIdx = trimmed.indexOf(" ");
  if (spaceIdx === -1) return null;

  const scheme = trimmed.slice(0, spaceIdx).toLowerCase();
  const rest = trimmed.slice(spaceIdx + 1).trim();
  if (rest.length === 0) return null;

  if (scheme === "bearer") {
    return { secret: rest, basicUsername: null };
  }

  if (scheme === "basic") {
    const decoded = decodeBase64Utf8(rest);
    if (decoded === null) return null;
    const colonIdx = decoded.indexOf(":");
    if (colonIdx === -1) return null;
    const username = decoded.slice(0, colonIdx);
    const password = decoded.slice(colonIdx + 1);
    if (password.length === 0) return null;
    return { secret: password, basicUsername: username };
  }

  return null;
}

function decodeBase64Utf8(input: string): string | null {
  try {
    return Buffer.from(input, "base64").toString("utf8");
  } catch {
    return null;
  }
}

function hasKnownPrefix(secret: string): boolean {
  return secret.startsWith(PAT_PREFIX) || secret.startsWith(SVC_PREFIX);
}

async function sha256(input: string): Promise<Uint8Array> {
  return new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(input)),
  );
}

async function resolvePrincipal(
  db: DB["db"],
  tokenRow: ReturnType<typeof parseGitTokenRow>,
  resolvedTenantId: string,
): Promise<PrincipalRow | undefined> {
  if (tokenRow.principalId !== null) {
    return await db.query.principal.findFirst({
      where: eq(principal.id, tokenRow.principalId),
    });
  }

  return await db.query.principal.findFirst({
    where: and(
      eq(principal.tenantId, resolvedTenantId),
      eq(principal.kind, "user"),
      eq(principal.refId, tokenRow.userId),
    ),
  });
}

function unauthorized(c: Context, message: string): Response {
  c.header(WWW_AUTHENTICATE_HEADER, WWW_AUTHENTICATE_VALUE);
  return c.json({ error: { code: "unauthorized", message } }, 401);
}

function forbidden(
  c: Context,
  code:
    | "token_revoked"
    | "token_expired"
    | "tenant_mismatch"
    | "principal_not_found"
    | "principal_suspended",
  message: string,
): Response {
  return c.json({ error: { code, message } }, 403);
}
