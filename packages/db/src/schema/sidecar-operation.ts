import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

/** Stable provisioner lifecycle identity shared by probes and deployments. */
export const sidecarOperation = pgTable("sidecar_operation", {
  id: text("id").primaryKey(),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});
