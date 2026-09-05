import { sql } from "drizzle-orm";
import { pgTable, text, timestamp, uniqueIndex } from "drizzle-orm/pg-core";

import { principal } from "./principals";

// A principal's Ed25519 signing key. `public_key` is the hex-encoded 32-byte
// public point; `private_key` holds the hex-encoded 32-byte seed sealed by the
// credential cipher (`enc:aead:...` ciphertext), so the seed is never at rest in
// plaintext under a configured key. The hub custodies the private key: a
// signature attributes an action to the principal, but is not non-repudiable
// against the hub operator. See docs/AUTH.md.
//
// The `status` axis expresses the one-active-key-per-principal invariant, which
// the partial unique index below enforces at the database. No path retires a
// key; every key a principal holds today is "active".
export const principalKey = pgTable(
  "principal_key",
  {
    id: text("id").primaryKey(),
    principalId: text("principal_id")
      .notNull()
      .references(() => principal.id, { onDelete: "cascade" }),
    publicKey: text("public_key").notNull().unique(),
    privateKey: text("private_key").notNull(),
    status: text("status", { enum: ["active", "retired"] }).notNull(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex("principal_key_one_active")
      .on(t.principalId)
      .where(sql`${t.status} = 'active'`),
  ],
);
