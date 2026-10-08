// The sealed-form event log for a terminated run. Compaction folds a run's
// per-event `runs/<runId>/events/<seq>.json` blobs into one combined file,
// `runs/<runId>/events.jsonl`, once the run reaches a terminal event. Each
// line is the verbatim text of the per-event blob it replaced, in seq order,
// so the fold is a byte-for-byte transition the workflow-run kind handler can
// validate against the prior per-event tree. Readers handle both shapes:
// per-event files for in-flight runs, the combined file for sealed runs.

/** Filename of a run's combined event log, a sibling of its `events/` dir. */
export const WORKFLOW_RUN_EVENTS_FILE = "events.jsonl";

/**
 * Split a combined event-log file's content into its per-event JSON texts, in
 * file order: each non-empty line is the verbatim text of what was an
 * `events/<seq>.json` blob. Event JSON never contains a literal newline, so a
 * line split is a faithful inverse of the encode side.
 */
export function splitCombinedEventLog(content: string): string[] {
  const out: string[] = [];
  for (const line of content.split("\n")) {
    if (line.length === 0) continue;
    out.push(line);
  }
  return out;
}

/**
 * Join per-event blobs (in seq order) into a combined event-log file: each
 * blob's exact bytes followed by a newline. Operating on bytes -- not decoded
 * strings -- keeps the sealed file a verbatim concatenation of the per-event
 * blobs, which matters because each event is signed over its own bytes; a
 * decode/re-encode round-trip could alter them. This is the single source of
 * the combined-file byte layout shared by the compaction writer and the
 * validator's byte-equality bridge. An empty input yields an empty file.
 */
export function encodeCombinedEventLog(
  perEventBlobs: readonly Uint8Array[],
): Uint8Array {
  const NEWLINE = 0x0a;
  let total = 0;
  for (const blob of perEventBlobs) total += blob.byteLength + 1;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const blob of perEventBlobs) {
    out.set(blob, offset);
    offset += blob.byteLength;
    out[offset] = NEWLINE;
    offset += 1;
  }
  return out;
}
