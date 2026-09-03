import { sql } from "drizzle-orm";
import {
  check,
  index,
  integer,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import { executionHost } from "./execution-host";
import { sidecar } from "./sidecar";
import { sidecarAllocation } from "./sidecar-allocation";

export const executionHostAssignment = pgTable(
  "execution_host_assignment",
  {
    sidecarId: text("sidecar_id")
      .primaryKey()
      .references(() => sidecar.id, { onDelete: "restrict" }),
    allocationId: text("allocation_id")
      .notNull()
      .references(() => sidecarAllocation.id, { onDelete: "cascade" }),
    generation: integer("generation").notNull(),
    hostId: text("host_id")
      .notNull()
      .references(() => executionHost.id, { onDelete: "restrict" }),
    hostSessionId: text("host_session_id").notNull(),
    hostSessionGeneration: integer("host_session_generation").notNull(),
    status: text("status", {
      enum: ["claiming", "assigned", "destroyed"],
    }).notNull(),
    destroyedGeneration: integer("destroyed_generation"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex("execution_host_assignment_active_host_idx")
      .on(t.hostId)
      .where(sql`${t.status} in ('claiming', 'assigned')`),
    uniqueIndex("execution_host_assignment_active_allocation_idx")
      .on(t.allocationId)
      .where(sql`${t.status} in ('claiming', 'assigned')`),
    index("execution_host_assignment_allocation_idx").on(t.allocationId),
    check(
      "execution_host_assignment_generation_check",
      sql`${t.generation} >= 0`,
    ),
    check(
      "execution_host_assignment_session_generation_check",
      sql`${t.hostSessionGeneration} >= 1`,
    ),
    check(
      "execution_host_assignment_status_check",
      sql`${t.status} in ('claiming', 'assigned', 'destroyed')`,
    ),
    check(
      "execution_host_assignment_destroyed_check",
      sql`(${t.status} = 'destroyed') = (${t.destroyedGeneration} is not null)`,
    ),
    check(
      "execution_host_assignment_destroyed_generation_check",
      sql`${t.destroyedGeneration} is null or ${t.destroyedGeneration} >= ${t.generation}`,
    ),
  ],
);
