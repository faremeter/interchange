import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { bytea } from "./column-types";

export const sidecar = pgTable("sidecar", {
  id: text("id").primaryKey(),
  url: text("url").notNull(),
  // SHA-256 digest of the sidecar's bearer token, presented on the
  // WebSocket handshake and matched against this column. Only the digest
  // is stored; the raw token is never persisted.
  tokenHashSha256: bytea("token_hash_sha256").notNull().unique(),
  status: text("status", {
    enum: ["online", "offline", "error"],
  })
    .notNull()
    .default("online"),
  lastHeartbeat: timestamp("last_heartbeat"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});
