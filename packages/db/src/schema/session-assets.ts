import {
  index,
  pgTable,
  primaryKey,
  text,
  timestamp,
} from "drizzle-orm/pg-core";

// session_asset rows record per-(instance, materialization) pack
// acknowledgments from past launches: a row exists iff the sidecar acked the
// pack that materialized that asset for that instance. Nothing writes this
// table now; it only holds those rows.
//
// The mount path and source commit trace each materialization. A launch that
// failed partway kept the rows of the attachments acked before the failure, so
// treat row count as "packs the sidecar acked during this instance's launch,"
// not "sessions that ran with assets."
export const sessionAsset = pgTable(
  "session_asset",
  {
    // The endpoint this materialization was for: a legacy agent_instance id or
    // a folded workflow_run id, from one shared id space. A polymorphic
    // reference carrying no foreign key (mirroring inference_turn.runId).
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
