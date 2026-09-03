import type { Context, Env } from "hono";

import type { SessionInfo, SessionUser } from "./session";
import { errorResponse } from "./error-response";

export type TenantRow = {
  id: string;
  name: string;
  slug: string;
  domain: string;
  parentId: string | null;
  config: unknown;
  createdAt: Date;
  updatedAt: Date;
};

export type PrincipalRow = {
  id: string;
  tenantId: string;
  kind: "user" | "agent" | "workflow";
  refId: string;
  status: "active" | "suspended" | "invited" | "deactivated";
  createdAt: Date;
  updatedAt: Date;
};

export type AppEnv = Env & {
  Variables: {
    user: SessionUser | null;
    session: SessionInfo | null;
  };
};

export type TenantEnv = Env & {
  Variables: AppEnv["Variables"] & {
    tenant: TenantRow;
    principal: PrincipalRow;
  };
};

/**
 * The canonical 401 response for a request with no authenticated session
 * user. Routes that read `c.get("user")` inline -- rather than mounting the
 * `requireAuth` middleware -- return this so the "Authentication required"
 * body lives in exactly one place instead of being re-encoded at every
 * handler. The git smart-HTTP bearer middleware keeps its own private
 * variant because it additionally stamps a `WWW-Authenticate: Basic`
 * challenge header.
 */
export function unauthorizedResponse(c: Context): Response {
  return errorResponse(c, "unauthorized", "Authentication required");
}
