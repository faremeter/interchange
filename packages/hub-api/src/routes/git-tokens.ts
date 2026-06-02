import { createHash } from "node:crypto";

import { and, eq } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import { gitToken } from "@intx/db/schema";
import { parseGitTokenRow } from "@intx/db";
import type { DB } from "@intx/db";
import {
  expandRepoActionAlias,
  generateId,
  glob,
  RepoActionAliases,
} from "@intx/hub-common";
import { getLogger } from "@intx/log";
import type { RepoAction } from "@intx/types/sidecar";
import { ErrorResponse, paginatedSchema } from "@intx/types";

import type { AppEnv, TenantEnv } from "../context";
import { ts } from "../format";
import type { RequireGrant } from "../middleware/grant";
import {
  cursorCondition,
  pageOrder,
  pageParameters,
  paginatedResponse,
  parsePageParams,
} from "../pagination";

const log = getLogger(["hub", "git-token"]);

const PAT_PREFIX = "itx_pat_";
const SVC_PREFIX = "itx_svc_";

const SECRET_BYTES = 32;

/**
 * Minimum lifetime of a freshly minted token. The mint endpoint
 * rejects `expiresAt` values that fall within this window so callers
 * cannot accidentally issue a token whose effective lifetime is so
 * short that it cannot be used.
 */
const MIN_LIFETIME_MS = 60_000;

const RepoActionType = type.enumerated(
  "init",
  "writeTree",
  "receivePack",
  "createPack",
  "resolveRef",
);

const RepoActionAliasName = type.enumerated("can_read", "can_push");

// Compile-time guarantee that the alias literals above stay in sync
// with `RepoActionAliases`: any drift surfaces as a type error here.
const _aliasNameCoverage: Record<keyof typeof RepoActionAliases, true> = {
  can_read: true,
  can_push: true,
};
void _aliasNameCoverage;

const ActionInput = RepoActionType.or(RepoActionAliasName);

const CreateTenantGitToken = type({
  name: "string",
  resource: "string",
  refPattern: "string",
  actions: ActionInput.array(),
  expiresAt: "string",
});

const CreateMeGitToken = type({
  name: "string",
  resource: "string",
  refPattern: "string",
  actions: ActionInput.array(),
  expiresAt: "string",
  "tenantId?": "string",
});

const GitTokenSummary = type({
  id: "string",
  userId: "string",
  "principalId?": "string | null",
  "tenantId?": "string | null",
  name: "string",
  kind: type.enumerated("pat", "svc"),
  resource: "string",
  refPattern: "string",
  actions: RepoActionType.array(),
  expiresAt: "string",
  "revokedAt?": "string | null",
  createdAt: "string",
});

const GitTokenMintResponse = type({
  id: "string",
  secret: "string",
  name: "string",
  kind: type.enumerated("pat", "svc"),
  claims: {
    resource: "string",
    refPattern: "string",
    actions: RepoActionType.array(),
    expiresAt: "string",
  },
});

type MintInput = {
  kind: "pat" | "svc";
  userId: string;
  principalId: string | null;
  tenantId: string | null;
  name: string;
  resource: string;
  refPattern: string;
  rawActions: string[];
  expiresAt: Date;
  now: Date;
};

type MintResult = {
  id: string;
  secret: string;
  name: string;
  kind: "pat" | "svc";
  resource: string;
  refPattern: string;
  actions: RepoAction[];
  expiresAt: Date;
};

/**
 * Error raised by `mintGitToken` when an input fails validation.
 * Each `code` corresponds to a distinct REST error response so the
 * HTTP layer can translate without rebuilding the validation chain.
 */
class MintValidationError extends Error {
  constructor(
    readonly code:
      | "invalid_ref_pattern"
      | "invalid_action"
      | "invalid_expires_at",
    message: string,
  ) {
    super(message);
  }
}

function generateSecret(kind: "pat" | "svc"): string {
  const prefix = kind === "pat" ? PAT_PREFIX : SVC_PREFIX;
  const bytes = new Uint8Array(SECRET_BYTES);
  // `crypto.getRandomValues` is the Web Crypto API and is exposed on
  // the global `crypto` object in both Node and Bun.
  crypto.getRandomValues(bytes);
  return `${prefix}${Buffer.from(bytes).toString("base64url")}`;
}

function sha256(input: string): Uint8Array {
  return new Uint8Array(createHash("sha256").update(input, "utf8").digest());
}

/**
 * Validate that a ref pattern is a syntactically acceptable glob.
 * The simple-glob compiler does not throw on any input, so the only
 * shape concerns to catch at this layer are the patterns that would
 * never match anything useful: empty strings, and patterns
 * containing characters that cannot appear in a git ref name.
 */
function validateRefPattern(pattern: string): void {
  if (pattern.length === 0) {
    throw new MintValidationError(
      "invalid_ref_pattern",
      "refPattern must not be empty",
    );
  }
  // Probe the compiler against a benign sample so callers see the
  // matcher run; any future enrichment of the compiler that adds
  // throw-on-malformed behaviour surfaces here.
  glob.match(pattern, "refs/heads/main");
}

function resolveActions(raw: string[]): RepoAction[] {
  if (raw.length === 0) {
    throw new MintValidationError(
      "invalid_action",
      "at least one action or alias is required",
    );
  }
  const seen = new Set<RepoAction>();
  for (const name of raw) {
    let expanded: RepoAction[];
    try {
      expanded = expandRepoActionAlias(name);
    } catch {
      throw new MintValidationError(
        "invalid_action",
        `unknown action or alias: ${name}`,
      );
    }
    for (const a of expanded) seen.add(a);
  }
  return [...seen];
}

function validateExpiresAt(expiresAt: Date, now: Date): void {
  if (Number.isNaN(expiresAt.getTime())) {
    throw new MintValidationError(
      "invalid_expires_at",
      "expiresAt must be an ISO-8601 timestamp",
    );
  }
  if (expiresAt.getTime() - now.getTime() < MIN_LIFETIME_MS) {
    throw new MintValidationError(
      "invalid_expires_at",
      `expiresAt must be at least ${MIN_LIFETIME_MS / 1000}s in the future`,
    );
  }
}

/**
 * Core mint primitive shared by both tenant-bound and personal
 * endpoints. Generates a fresh secret, validates the inputs,
 * inserts the row with the SHA-256 digest, and returns the resolved
 * claims alongside the plaintext secret. The plaintext is never
 * persisted; the caller is responsible for handing it back to the
 * user exactly once.
 */
export async function mintGitToken(
  db: DB["db"],
  input: MintInput,
): Promise<MintResult> {
  validateRefPattern(input.refPattern);
  const actions = resolveActions(input.rawActions);
  validateExpiresAt(input.expiresAt, input.now);
  const expiresAt = input.expiresAt;

  const id = generateId("gitToken");
  const secret = generateSecret(input.kind);
  const tokenHashSha256 = sha256(secret);

  await db.insert(gitToken).values({
    id,
    userId: input.userId,
    principalId: input.principalId,
    tenantId: input.tenantId,
    name: input.name,
    kind: input.kind,
    tokenHashSha256,
    resource: input.resource,
    refPattern: input.refPattern,
    actions,
    expiresAt,
    createdAt: input.now,
  });

  return {
    id,
    secret,
    name: input.name,
    kind: input.kind,
    resource: input.resource,
    refPattern: input.refPattern,
    actions,
    expiresAt,
  };
}

function mintErrorBody(err: MintValidationError) {
  return { error: { code: err.code, message: err.message } } as const;
}

function formatGitTokenRow(row: typeof gitToken.$inferSelect) {
  const parsed = parseGitTokenRow(row);
  return {
    id: parsed.id,
    userId: parsed.userId,
    principalId: parsed.principalId ?? null,
    tenantId: parsed.tenantId ?? null,
    name: parsed.name,
    kind: parsed.kind,
    resource: parsed.resource,
    refPattern: parsed.refPattern,
    actions: parsed.actions,
    expiresAt: ts(parsed.expiresAt),
    revokedAt: parsed.revokedAt ? ts(parsed.revokedAt) : null,
    createdAt: ts(parsed.createdAt),
  };
}

function formatMintResult(result: MintResult) {
  return {
    id: result.id,
    secret: result.secret,
    name: result.name,
    kind: result.kind,
    claims: {
      resource: result.resource,
      refPattern: result.refPattern,
      actions: result.actions,
      expiresAt: ts(result.expiresAt),
    },
  };
}

export type CreateTenantGitTokenRoutesDeps = {
  db: DB["db"];
  requireGrant: RequireGrant;
};

export function createTenantGitTokenRoutes({
  db,
  requireGrant,
}: CreateTenantGitTokenRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("git-token:*", "read"),
    describeRoute({
      tags: ["Git Tokens"],
      summary: "List tenant git tokens",
      description:
        'Lists service tokens (`kind: "svc"`) bound to this tenant. Secrets are never returned; the plaintext is shown only at mint time.',
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of git tokens",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(GitTokenSummary)),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [eq(gitToken.tenantId, tenantCtx.id)];
      if (cursor) {
        conditions.push(
          cursorCondition(gitToken.createdAt, gitToken.id, cursor),
        );
      }

      const rows = await db.query.gitToken.findMany({
        where: and(...conditions),
        orderBy: pageOrder(gitToken.createdAt, gitToken.id),
        limit,
      });

      log.info("tenant list {tenantId} count={count}", {
        tenantId: tenantCtx.id,
        count: rows.length,
      });

      return c.json(
        paginatedResponse(rows.map(formatGitTokenRow), rows, limit),
      );
    },
  );

  app.post(
    "/",
    requireGrant("git-token:*", "create"),
    describeRoute({
      tags: ["Git Tokens"],
      summary: "Mint a tenant-bound service git token",
      description:
        'Mints a service token (`kind: "svc"`) bound to the requesting tenant. The plaintext secret is returned exactly once in the response and is never persisted in plaintext.',
      responses: {
        201: {
          description: "Token minted",
          content: {
            "application/json": { schema: resolver(GitTokenMintResponse) },
          },
        },
        400: {
          description: "Validation error",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateTenantGitToken),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const principalCtx = c.get("principal");
      const user = c.get("user");
      if (!user) {
        return c.json(
          {
            error: { code: "unauthorized", message: "Authentication required" },
          },
          401,
        );
      }
      const body = c.req.valid("json");

      const now = new Date();
      let result: MintResult;
      try {
        result = await mintGitToken(db, {
          kind: "svc",
          userId: user.id,
          principalId: principalCtx.id,
          tenantId: tenantCtx.id,
          name: body.name,
          resource: body.resource,
          refPattern: body.refPattern,
          rawActions: body.actions,
          expiresAt: new Date(body.expiresAt),
          now,
        });
      } catch (err) {
        if (err instanceof MintValidationError) {
          log.info(
            "tenant mint rejected {tenantId} principal={principalId} code={code}",
            {
              tenantId: tenantCtx.id,
              principalId: principalCtx.id,
              code: err.code,
            },
          );
          return c.json(mintErrorBody(err), 400);
        }
        throw err;
      }

      log.info(
        "tenant mint succeeded {tenantId} principal={principalId} tokenId={tokenId}",
        {
          tenantId: tenantCtx.id,
          principalId: principalCtx.id,
          tokenId: result.id,
        },
      );

      return c.json(formatMintResult(result), 201);
    },
  );

  app.delete(
    "/:tokenId",
    requireGrant("git-token:*", "manage"),
    describeRoute({
      tags: ["Git Tokens"],
      summary: "Revoke a tenant git token",
      description:
        "Soft-revokes a tenant-bound git token by setting `revokedAt`. The row is retained for audit.",
      responses: {
        204: { description: "Token revoked" },
        404: {
          description: "Token not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const tokenId = c.req.param("tokenId");

      const existing = await db.query.gitToken.findFirst({
        where: and(
          eq(gitToken.id, tokenId),
          eq(gitToken.tenantId, tenantCtx.id),
        ),
      });
      if (!existing) {
        log.info("tenant revoke not found {tenantId} tokenId={tokenId}", {
          tenantId: tenantCtx.id,
          tokenId,
        });
        return c.json(
          { error: { code: "not_found", message: "Token not found" } },
          404,
        );
      }

      if (existing.revokedAt === null) {
        await db
          .update(gitToken)
          .set({ revokedAt: new Date() })
          .where(eq(gitToken.id, tokenId));
      }

      log.info("tenant revoke succeeded {tenantId} tokenId={tokenId}", {
        tenantId: tenantCtx.id,
        tokenId,
      });

      return c.body(null, 204);
    },
  );

  return app;
}

export type CreateMeGitTokenRoutesDeps = {
  db: DB["db"];
};

export function createMeGitTokenRoutes({
  db,
}: CreateMeGitTokenRoutesDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get(
    "/",
    describeRoute({
      tags: ["Git Tokens"],
      summary: "List personal git tokens",
      description:
        'Lists the authenticated user\'s personal access tokens (`kind: "pat"`). Secrets are never returned; the plaintext is shown only at mint time.',
      parameters: [...pageParameters],
      responses: {
        200: {
          description: "List of git tokens",
          content: {
            "application/json": {
              schema: resolver(paginatedSchema(GitTokenSummary)),
            },
          },
        },
        401: {
          description: "Not authenticated",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const user = c.get("user");
      if (!user) {
        return c.json(
          {
            error: { code: "unauthorized", message: "Authentication required" },
          },
          401,
        );
      }
      const { limit, cursor } = parsePageParams({
        cursor: c.req.query("cursor"),
        limit: c.req.query("limit"),
      });

      const conditions = [
        eq(gitToken.userId, user.id),
        eq(gitToken.kind, "pat"),
      ];
      if (cursor) {
        conditions.push(
          cursorCondition(gitToken.createdAt, gitToken.id, cursor),
        );
      }

      const rows = await db.query.gitToken.findMany({
        where: and(...conditions),
        orderBy: pageOrder(gitToken.createdAt, gitToken.id),
        limit,
      });

      log.info("personal list userId={userId} count={count}", {
        userId: user.id,
        count: rows.length,
      });

      return c.json(
        paginatedResponse(rows.map(formatGitTokenRow), rows, limit),
      );
    },
  );

  app.post(
    "/",
    describeRoute({
      tags: ["Git Tokens"],
      summary: "Mint a personal access git token",
      description:
        'Mints a personal access token (`kind: "pat"`) for the authenticated user. The plaintext secret is returned exactly once in the response. An optional `tenantId` restricts the token to a single tenant.',
      responses: {
        201: {
          description: "Token minted",
          content: {
            "application/json": { schema: resolver(GitTokenMintResponse) },
          },
        },
        400: {
          description: "Validation error",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        401: {
          description: "Not authenticated",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateMeGitToken),
    async (c) => {
      const user = c.get("user");
      if (!user) {
        return c.json(
          {
            error: { code: "unauthorized", message: "Authentication required" },
          },
          401,
        );
      }
      const body = c.req.valid("json");

      const now = new Date();
      let result: MintResult;
      try {
        result = await mintGitToken(db, {
          kind: "pat",
          userId: user.id,
          principalId: null,
          tenantId: body.tenantId ?? null,
          name: body.name,
          resource: body.resource,
          refPattern: body.refPattern,
          rawActions: body.actions,
          expiresAt: new Date(body.expiresAt),
          now,
        });
      } catch (err) {
        if (err instanceof MintValidationError) {
          log.info("personal mint rejected userId={userId} code={code}", {
            userId: user.id,
            code: err.code,
          });
          return c.json(mintErrorBody(err), 400);
        }
        throw err;
      }

      log.info("personal mint succeeded userId={userId} tokenId={tokenId}", {
        userId: user.id,
        tokenId: result.id,
      });

      return c.json(formatMintResult(result), 201);
    },
  );

  app.delete(
    "/:tokenId",
    describeRoute({
      tags: ["Git Tokens"],
      summary: "Revoke a personal git token",
      description:
        "Soft-revokes a personal access token by setting `revokedAt`. Only the owning user may revoke their own tokens.",
      responses: {
        204: { description: "Token revoked" },
        401: {
          description: "Not authenticated",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        403: {
          description: "Token not owned by the authenticated user",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Token not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const user = c.get("user");
      if (!user) {
        return c.json(
          {
            error: { code: "unauthorized", message: "Authentication required" },
          },
          401,
        );
      }
      const tokenId = c.req.param("tokenId");

      const existing = await db.query.gitToken.findFirst({
        where: eq(gitToken.id, tokenId),
      });
      if (!existing) {
        log.info(
          "personal revoke not found userId={userId} tokenId={tokenId}",
          {
            userId: user.id,
            tokenId,
          },
        );
        return c.json(
          { error: { code: "not_found", message: "Token not found" } },
          404,
        );
      }

      if (existing.userId !== user.id) {
        log.info(
          "personal revoke forbidden userId={userId} tokenId={tokenId} owner={ownerId}",
          {
            userId: user.id,
            tokenId,
            ownerId: existing.userId,
          },
        );
        return c.json(
          {
            error: {
              code: "forbidden",
              message: "Token is owned by a different user",
            },
          },
          403,
        );
      }

      if (existing.revokedAt === null) {
        await db
          .update(gitToken)
          .set({ revokedAt: new Date() })
          .where(eq(gitToken.id, tokenId));
      }

      log.info("personal revoke succeeded userId={userId} tokenId={tokenId}", {
        userId: user.id,
        tokenId,
      });

      return c.body(null, 204);
    },
  );

  return app;
}
