import {
  index,
  pgTable,
  primaryKey,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

import { agentAsset } from "./agent-assets";
import { agentInstance } from "./instances";

// session_asset rows record per-(instance, materialization) pack
// acknowledgments. A row exists iff the sidecar acked the pack that
// materialized that asset for that instance. The launchSession flow
// inserts each row before the corresponding pack send and rolls it
// back if that single send fails, so each row reflects an ack the
// hub actually observed for that attachment.
//
// `source` distinguishes the two materialization paths:
//   - "direct"  : the asset reached the instance via an explicit
//                 agent_asset attachment row; `agentAssetId` is set.
//   - "resolved": the asset was picked by the tool-package resolver
//                 out of a tenant-visible package-registry; no
//                 agent_asset row exists for the pairing, so
//                 `agentAssetId` is null. The mount path and source
//                 commit are still recorded so forensic queries
//                 cover resolver-derived materializations the same
//                 way they cover direct ones.
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
export type SessionAssetSource = "direct" | "resolved";

export const sessionAsset = pgTable(
  "session_asset",
  {
    instanceId: text("instance_id")
      .notNull()
      .references(() => agentInstance.id, { onDelete: "cascade" }),
    // Nullable: resolver-derived materializations have no per-agent
    // attachment row. Direct attachments carry the agent_asset id.
    agentAssetId: text("agent_asset_id").references(() => agentAsset.id, {
      onDelete: "cascade",
    }),
    mountPath: text("mount_path").notNull(),
    assetPackSha: text("asset_pack_sha").notNull(),
    sourceCommitSha: text("source_commit_sha").notNull(),
    source: text("source").$type<SessionAssetSource>().notNull(),
    materializedAt: timestamp("materialized_at").notNull().defaultNow(),
  },
  (t) => [
    // (instanceId, mountPath) is the natural key: every materialized
    // asset lands at a distinct mount path inside one instance, so
    // this pair uniquely identifies a row regardless of whether the
    // pack came from a direct attachment or a resolver-derived
    // package-registry pick. Pre-split the PK was (instanceId,
    // agentAssetId), but that column is now nullable for the
    // resolved-source rows and Postgres rejects nullable PK columns.
    primaryKey({ columns: [t.instanceId, t.mountPath] }),
    index("session_asset_pack_sha_idx").on(t.assetPackSha),
  ],
);
