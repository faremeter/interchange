import { sql } from "drizzle-orm";
import { check, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { bytea } from "./column-types";

export const sidecar = pgTable(
  "sidecar",
  {
    id: text("id").primaryKey(),
    // Allocated identities exist before their infrastructure connects, so they
    // do not have an endpoint until registration completes.
    url: text("url"),
    // SHA-256 digest of the sidecar's bearer token, presented on the
    // WebSocket handshake and matched against this column. Only the digest
    // is stored; the raw token is never persisted.
    tokenHashSha256: bytea("token_hash_sha256").notNull().unique(),
    credentialScope: text("credential_scope", {
      enum: ["shared", "allocated"],
    })
      .notNull()
      .default("shared"),
    status: text("status", {
      enum: ["online", "offline", "error"],
    })
      .notNull()
      .default("online"),
    lastHeartbeat: timestamp("last_heartbeat"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    check(
      "sidecar_credential_scope_check",
      sql`${t.credentialScope} in ('shared', 'allocated')`,
    ),
  ],
);
