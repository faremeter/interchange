import { sql } from "drizzle-orm";
import { pgTable, text, timestamp, uniqueIndex } from "drizzle-orm/pg-core";

import { user } from "./auth";
import { bytea } from "./column-types";
import { principal } from "./principals";
import { tenant } from "./tenants";

// Hub-issued bearer tokens for the smart-HTTP git endpoints. Opaque
// tokens of the form `itx_pat_<base64>` or `itx_svc_<base64>` whose
// SHA-256 digest is stored as `tokenHashSha256` (the raw secret is
// never persisted). Claims that bound the token's authority are
// modeled as typed columns rather than JSONB so the lookup path can
// rely on the database to enforce shape.
//
// `userId` identifies the owning user and is always set. `principalId`
// is set for tenant-bound tokens (the user acting in a specific
// tenant) and null for personal tokens that are not scoped to a
// principal. `tenantId` is always set for `kind: "svc"` tokens (they
// are inherently tenant-bound); for `kind: "pat"` it is set only when
// the user elected a tenant restriction at mint time. `kind`
// distinguishes interactive personal access tokens (`pat`) from
// service tokens (`svc`).
//
// `actions` stores the canonical RepoStore action vocabulary
// (`receivePack`, `createPack`, `resolveRef`, ...). The mint API
// translates user-facing aliases to canonical names before insert.
// `resource` is the single substrate authz resource string
// (e.g. `agent-state:ins_xxx`, `asset:def_yyy`) that the token grants
// access to. `refPattern` is a glob restricting which refs within the
// resource the token may read or write.
//
// Revocation is soft: setting `revokedAt` prevents future use while
// preserving the row for audit. The partial unique index on
// `(user_id, name)` filtered by `revoked_at is null` lets a user
// reuse a friendly name (e.g. "laptop") after revoking the old
// token bearing that name.
export const gitToken = pgTable(
  "git_token",
  {
    id: text("id").primaryKey(),
    tenantId: text("tenant_id").references(() => tenant.id),
    userId: text("user_id")
      .notNull()
      .references(() => user.id, { onDelete: "cascade" }),
    principalId: text("principal_id").references(() => principal.id, {
      onDelete: "cascade",
    }),
    name: text("name").notNull(),
    kind: text("kind", { enum: ["pat", "svc"] }).notNull(),
    tokenHashSha256: bytea("token_hash_sha256").notNull().unique(),
    resource: text("resource").notNull(),
    refPattern: text("ref_pattern").notNull(),
    actions: text("actions").array().notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    revokedAt: timestamp("revoked_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    uniqueIndex("git_token_user_id_name_active_idx")
      .on(t.userId, t.name)
      .where(sql`${t.revokedAt} is null`),
  ],
);
