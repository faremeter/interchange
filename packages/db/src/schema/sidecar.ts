import { integer, pgTable, text, timestamp } from "drizzle-orm/pg-core";

import { bytea } from "./column-types";

export const sidecar = pgTable("sidecar", {
  id: text("id").primaryKey(),
  // Provisioned probe/allocation identities exist before their infrastructure
  // connects, so they do not have an endpoint until registration completes.
  url: text("url"),
  // SHA-256 digest of the sidecar's bearer token, presented on the
  // WebSocket handshake and matched against this column. Only the digest
  // is stored; the raw token is never persisted.
  tokenHashSha256: bytea("token_hash_sha256").notNull().unique(),
  // The provisioner binding that minted this identity and the adapter
  // manifest its ensure carried. Reuse matches them, including on a sidecar
  // that hosts nothing.
  provisionerId: text("provisioner_id").notNull(),
  provisionerApiVersion: integer("provisioner_api_version")
    .$type<2>()
    .notNull(),
  provisionerBindingFingerprint: text(
    "provisioner_binding_fingerprint",
  ).notNull(),
  adapterManifestDigest: text("adapter_manifest_digest").notNull(),
  // Set once the Hub knows no sidecar runs with this identity. Migration 0110
  // also set it on every sidecar then hosting nothing current, some of which
  // may still have been running.
  retiredAt: timestamp("retired_at", { withTimezone: true }),
  // Set when its provisioner ended the reconnect window of this sidecar's
  // work, so that work gets none again, even after a Hub restart.
  reconnectWindowEndedAt: timestamp("reconnect_window_ended_at", {
    withTimezone: true,
  }),
  status: text("status", {
    enum: ["online", "offline", "error"],
  })
    .notNull()
    .default("online"),
  lastHeartbeat: timestamp("last_heartbeat", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});
