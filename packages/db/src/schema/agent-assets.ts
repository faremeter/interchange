import { pgTable, text, timestamp, unique } from "drizzle-orm/pg-core";

import { agent } from "./agents";
import { asset } from "./assets";

export const agentAsset = pgTable(
  "agent_asset",
  {
    id: text("id").primaryKey(),
    agentId: text("agent_id")
      .notNull()
      .references(() => agent.id, { onDelete: "cascade" }),
    assetId: text("asset_id")
      .notNull()
      .references(() => asset.id, { onDelete: "cascade" }),
    ref: text("ref").notNull(),
    accessMode: text("access_mode").notNull().default("read-only"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => [unique("agent_asset_agent_asset").on(t.agentId, t.assetId)],
);
