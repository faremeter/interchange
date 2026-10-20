import { pgTable, text, timestamp, uniqueIndex } from "drizzle-orm/pg-core";

import { bytea } from "./column-types";
import { principal } from "./principals";

// Bearer tokens a workflow run presents to the hub REST API as its run
// principal, one for each consumer (a tool package `tool:<package>` or an
// action handler `action:<handler>`). Opaque tokens of the form
// `itx_run_<base64url>`.
//
// `tokenHashSha256` is the lookup key for a presented bearer. Unlike a git
// token, the same token must be delivered to the run more than once, so
// `sealedToken` holds the plaintext sealed with the credential cipher, bound
// to this row by `runTokenAad`.
//
// A token has no expiry and no revocation column. Its authority is its run
// principal's, so the principal's status decides whether it is valid. The
// tenant is the principal's.
export const runToken = pgTable(
  "run_token",
  {
    id: text("id").primaryKey(),
    principalId: text("principal_id")
      .notNull()
      .references(() => principal.id, { onDelete: "cascade" }),
    consumer: text("consumer").notNull(),
    tokenHashSha256: bytea("token_hash_sha256").notNull().unique(),
    sealedToken: text("sealed_token").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    uniqueIndex("run_token_principal_consumer_idx").on(
      t.principalId,
      t.consumer,
    ),
  ],
);
