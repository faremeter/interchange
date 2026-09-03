import { sql } from "drizzle-orm";
import {
  check,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { executionHost } from "./execution-host";

export const executionHostSession = pgTable(
  "execution_host_session",
  {
    hostId: text("host_id")
      .primaryKey()
      .references(() => executionHost.id, { onDelete: "cascade" }),
    sessionId: text("session_id").notNull().unique(),
    generation: integer("generation").notNull(),
    hubInstanceId: text("hub_instance_id").notNull(),
    capabilities: jsonb("capabilities").notNull(),
    leaseExpiresAt: timestamp("lease_expires_at").notNull(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    check("execution_host_session_generation_check", sql`${t.generation} >= 1`),
  ],
);
