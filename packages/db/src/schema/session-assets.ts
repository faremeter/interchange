import {
  index,
  pgTable,
  primaryKey,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { agentAsset } from "./agent-assets";
import { agentInstance } from "./instances";

// session_asset rows record per-(instance, agent_asset) pack
// acknowledgments. A row exists iff the sidecar acked the pack that
// materialized that asset for that instance. The launchSession flow
// inserts each row before the corresponding pack send and rolls it
// back if that single send fails, so each row reflects an ack the
// hub actually observed for that attachment.
//
// Caveat on multi-attachment partial-success: when a session attaches
// N assets and the fan-out succeeds for attachments 1..k-1 and fails
// on attachment k, only attachment k's row is rolled back. Rows
// 1..k-1 stay. The session as a whole never reached the running
// state — attemptCleanup runs sendAgentUndeploy — but the per-
// attachment ack invariant holds for the rows that remain.
// Forensics queries should treat row count as "packs the sidecar
// acked during this instance's launch," not "sessions that ran with
// assets." Pairing with agent_instance.status (or its successor
// session-lifecycle signal) is necessary to distinguish a row left
// behind by a partially-successful failed launch from one belonging
// to a fully-running session.
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
