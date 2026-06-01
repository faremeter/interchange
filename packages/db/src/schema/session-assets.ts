import {
  index,
  pgTable,
  primaryKey,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { agentAsset } from "./agent-assets";
import { agentInstance } from "./instances";

export const sessionAsset = pgTable(
  "session_asset",
  {
    instanceId: text("instance_id")
      .notNull()
      .references(() => agentInstance.id, { onDelete: "cascade" }),
    agentAssetId: text("agent_asset_id")
      .notNull()
      .references(() => agentAsset.id, { onDelete: "cascade" }),
    mountPath: text("mount_path").notNull(),
    assetPackSha: text("asset_pack_sha").notNull(),
    sourceCommitSha: text("source_commit_sha").notNull(),
    materializedAt: timestamp("materialized_at").notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.instanceId, t.agentAssetId] }),
    index("session_asset_pack_sha_idx").on(t.assetPackSha),
  ],
);
