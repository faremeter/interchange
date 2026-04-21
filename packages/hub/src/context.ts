import type { DB } from "@interchange/db";
import type { ConditionRegistry, GrantStore } from "@interchange/types/authz";
import type { Env } from "hono";

import type { Auth } from "./auth";
import type { EventCollectorRegistry } from "./event-collector-registry";
import type { SessionService } from "./session-service";
import type { SidecarRouter } from "./ws/sidecar-handler";

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
    grantStore: GrantStore;
    conditionRegistry: ConditionRegistry;
    sidecarRouter: SidecarRouter;
    sessionService: SessionService;
    eventCollectors: EventCollectorRegistry;
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
