// A workflow step's place in its deployment. The workflow runtime gives it to
// each step invocation and to the park hooks.

/**
 * Where a step sits in its deployment's run tree. `stepPath` holds base step
 * ids, never scoped `<id>[<index>]` ids, root first: the top-level step id,
 * then the id of each container step (loop, onTrigger, childWorkflow) down to
 * the step itself. A map item's path ends in the map's own id, and
 * `mapIndex` is the item's index. `mapIndex` is absent for a step that is not
 * a map item.
 */
export type StepPlace = { stepPath: readonly string[]; mapIndex?: number };

/**
 * The longest the URL-encoded form of a key may be. The repo stores a step's
 * state in a directory named `encodeURIComponent(key)`, and common
 * filesystems cap a directory name at 255 bytes. The limit applies to the
 * encoded form because that is the name on disk: a map item's `[` and `]`
 * each encode to three characters. The encoded form is ASCII, so its length
 * is its size in bytes.
 */
export const MAX_STEP_PLACE_SEGMENT_BYTES = 255;

/**
 * The largest index a map item can have. A map runs over a JavaScript array,
 * and an array index is at most 2^32 - 2. A deploy-time check of a map's key
 * uses this index, so every item the map can run has a key that fits.
 */
export const MAX_MAP_ITEM_INDEX = 2 ** 32 - 2;

/**
 * A step place's key and the size of its URL-encoded form, without the
 * length check `stepPlaceKey` applies. A deploy-time check uses it to report
 * every key that is too long, and `stepPlaceKey` uses it at run time, so both
 * apply one rule.
 *
 * Throws when the path is empty.
 */
export function measureStepPlaceKey(place: StepPlace): {
  key: string;
  segmentBytes: number;
} {
  if (place.stepPath.length === 0) {
    throw new Error("a step place must name at least one step");
  }
  const path = place.stepPath.join(".");
  const key =
    place.mapIndex === undefined ? path : `${path}[${String(place.mapIndex)}]`;
  return { key, segmentBytes: encodeURIComponent(key).length };
}

/**
 * The key of a step's place in its deployment: the step path joined with
 * `.`, plus `[<mapIndex>]` for a map item. A top-level step's key is its step
 * id, and a top-level map item's key is its scoped id, such as `m[2]`.
 *
 * Base step ids match `[a-zA-Z0-9_-]+`, so they hold no `.`, `[` or `]`, and
 * two different places never share a key. One key holds one step's state at
 * a time. That is safe because a container step never runs two of its
 * children at once: a map runs its items in sequence, loop iterations run one
 * after another, and an onTrigger section drives one body at a time.
 *
 * Throws when the path is empty or the URL-encoded key is longer than
 * `MAX_STEP_PLACE_SEGMENT_BYTES`. The hub refuses a deployment with a key
 * that is too long, so this throw is the last line of defense.
 */
export function stepPlaceKey(place: StepPlace): string {
  const { key, segmentBytes } = measureStepPlaceKey(place);
  if (segmentBytes > MAX_STEP_PLACE_SEGMENT_BYTES) {
    throw new Error(
      `the key of step place ${JSON.stringify(key)} encodes to ${String(segmentBytes)} bytes; a step's state directory name is limited to ${String(MAX_STEP_PLACE_SEGMENT_BYTES)} bytes, so nest fewer containers or use shorter step ids`,
    );
  }
  return key;
}

/**
 * Whether `key` names a step of the deployment's top-level run: a top-level
 * step, or a top-level map item such as `m[2]`. A nested step's key joins
 * its path with `.`, which no step id holds.
 */
export function isTopLevelStepPlaceKey(key: string): boolean {
  return !key.includes(".");
}
