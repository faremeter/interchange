import { sql } from "drizzle-orm";
import {
  check,
  index,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import type { SidecarCapabilityDeclaration } from "@intx/types";

import { executionHost } from "./execution-host";
import { sidecar } from "./sidecar";
import { sidecarOperation } from "./sidecar-operation";

export const executionHostAssignment = pgTable(
  "execution_host_assignment",
  {
    sidecarId: text("sidecar_id")
      .primaryKey()
      .references(() => sidecar.id, { onDelete: "restrict" }),
    operationId: text("operation_id")
      .notNull()
      .references(() => sidecarOperation.id, { onDelete: "cascade" }),
    generation: integer("generation").notNull(),
    hostId: text("host_id")
      .notNull()
      .references(() => executionHost.id, { onDelete: "restrict" }),
    hostSessionId: text("host_session_id").notNull(),
    hostSessionGeneration: integer("host_session_generation").notNull(),
    capabilities: jsonb("capabilities")
      .$type<SidecarCapabilityDeclaration[]>()
      .notNull(),
    status: text("status", {
      enum: ["claiming", "assigned", "releasing", "destroyed"],
    }).notNull(),
    destroyedGeneration: integer("destroyed_generation"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex("execution_host_assignment_active_host_idx")
      .on(t.hostId)
      .where(sql`${t.status} in ('claiming', 'assigned', 'releasing')`),
    uniqueIndex("execution_host_assignment_active_operation_idx")
      .on(t.operationId)
      .where(sql`${t.status} in ('claiming', 'assigned', 'releasing')`),
    index("execution_host_assignment_operation_idx").on(t.operationId),
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
      sql`${t.status} in ('claiming', 'assigned', 'releasing', 'destroyed')`,
    ),
    check(
      "execution_host_assignment_destroyed_check",
      sql`(${t.status} in ('releasing', 'destroyed')) = (${t.destroyedGeneration} is not null)`,
    ),
    check(
      "execution_host_assignment_destroyed_generation_check",
      sql`${t.destroyedGeneration} is null or ${t.destroyedGeneration} >= ${t.generation}`,
    ),
  ],
);
