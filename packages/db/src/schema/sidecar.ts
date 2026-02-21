import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

export const sidecar = pgTable("sidecar", {
  id: text("id").primaryKey(),
  url: text("url").notNull(),
  status: text("status", {
    enum: ["online", "offline", "error"],
  })
    .notNull()
    .default("online"),
  lastHeartbeat: timestamp("last_heartbeat"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  updatedAt: timestamp("updated_at").notNull().defaultNow(),
});
