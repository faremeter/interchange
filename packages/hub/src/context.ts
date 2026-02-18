import type { DB } from "@interchange/db";
import type { Env } from "hono";

import type { Auth } from "./auth";

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
  kind: "user" | "agent";
  refId: string;
  status: "active" | "suspended" | "invited" | "deactivated";
  createdAt: Date;
  updatedAt: Date;
};

export type AppEnv = Env & {
  Variables: {
    db: DB["db"];
    user: Auth["$Infer"]["Session"]["user"] | null;
    session: Auth["$Infer"]["Session"]["session"] | null;
  };
};

export type TenantEnv = Env & {
  Variables: AppEnv["Variables"] & {
    tenant: TenantRow;
    principal: PrincipalRow;
  };
};
