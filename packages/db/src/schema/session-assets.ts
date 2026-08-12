import {
  index,
  pgTable,
  primaryKey,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

// session_asset rows record per-(instance, materialization) pack
// acknowledgments. A row exists iff the sidecar acked the pack that
// materialized that asset for that instance. The launchSession flow
// inserts each row before the corresponding pack send and rolls it
// back if that single send fails, so each row reflects an ack the
// hub actually observed for that attachment.
//
// Assets reach an instance through the tool-package resolver: it picks
// package-registry assets out of the tenant-visible set, and the launch
// fan-out materializes each into the workspace. The mount path and
// source commit are recorded so forensic queries can trace every
// materialization.
//
// Caveat on multi-attachment partial-success: when a session attaches
// N assets and the fan-out succeeds for attachments 1..k-1 and fails
// on attachment k, only attachment k's row is rolled back. Rows
// 1..k-1 stay. The session as a whole never reached the running
// state -- attemptCleanup runs sendAgentUndeploy -- but the per-
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
    // The endpoint this materialization is for: a legacy agent_instance id or a
    // folded workflow_run id, from one shared id space. A polymorphic reference
    // carrying no foreign key (mirroring inference_turn.runId); the launch
    // layer owns the invariant. NOT NULL -- a materialization always names its
    // endpoint -- and part of the (instance_id, mount_path) key. A folded run
    // writes its run id here, which the dropped agent_instance FK would reject.
    runId: text("instance_id").notNull(),
    mountPath: text("mount_path").notNull(),
    assetPackSha: text("asset_pack_sha").notNull(),
    sourceCommitSha: text("source_commit_sha").notNull(),
    materializedAt: timestamp("materialized_at").notNull().defaultNow(),
  },
  (t) => [
    // (runId, mountPath) is the natural key: every materialized
    // asset lands at a distinct mount path inside one run, so this
    // pair uniquely identifies a row.
    primaryKey({ columns: [t.runId, t.mountPath] }),
    index("session_asset_pack_sha_idx").on(t.assetPackSha),
  ],
);
