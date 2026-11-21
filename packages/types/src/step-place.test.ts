import { describe, expect, test } from "bun:test";

import {
  isTopLevelStepPlaceKey,
  MAX_MAP_ITEM_INDEX,
  MAX_STEP_PLACE_SEGMENT_BYTES,
  measureStepPlaceKey,
  stepPlaceKey,
} from "./step-place";

describe("stepPlaceKey", () => {
  test("a top-level step's key is its step id", () => {
    expect(stepPlaceKey({ stepPath: ["reply"] })).toBe("reply");
  });

  test("a nested step's key joins its path with dots", () => {
    expect(stepPlaceKey({ stepPath: ["section", "work"] })).toBe(
      "section.work",
    );
    expect(stepPlaceKey({ stepPath: ["outer", "child", "reply"] })).toBe(
      "outer.child.reply",
    );
  });

  test("a top-level map item's key is its scoped id", () => {
    expect(stepPlaceKey({ stepPath: ["m"], mapIndex: 2 })).toBe("m[2]");
  });

  test("a map item in a loop body keys the loop, the map and the index", () => {
    expect(stepPlaceKey({ stepPath: ["loop", "summarize"], mapIndex: 0 })).toBe(
      "loop.summarize[0]",
    );
  });

  test("a top-level id that fills the encoded limit is accepted", () => {
    const longest = "a".repeat(MAX_STEP_PLACE_SEGMENT_BYTES);
    expect(stepPlaceKey({ stepPath: [longest] })).toBe(longest);
  });

  test("a key one byte past the encoded limit throws", () => {
    expect(() =>
      stepPlaceKey({
        stepPath: ["a".repeat(MAX_STEP_PLACE_SEGMENT_BYTES + 1)],
      }),
    ).toThrow(/limited to 255 bytes/);
  });

  test("a map item's encoded brackets count toward the limit", () => {
    // `[0]` is three characters but encodes to `%5B0%5D`, seven bytes.
    const id = "a".repeat(MAX_STEP_PLACE_SEGMENT_BYTES - 5);
    expect(`${id}[0]`.length).toBeLessThan(MAX_STEP_PLACE_SEGMENT_BYTES);
    expect(() => stepPlaceKey({ stepPath: [id], mapIndex: 0 })).toThrow(
      /encodes to 257 bytes/,
    );
    const fits = "a".repeat(MAX_STEP_PLACE_SEGMENT_BYTES - 7);
    expect(stepPlaceKey({ stepPath: [fits], mapIndex: 0 })).toBe(`${fits}[0]`);
  });

  test("an empty path throws", () => {
    expect(() => stepPlaceKey({ stepPath: [] })).toThrow(/at least one step/);
  });
});

describe("isTopLevelStepPlaceKey", () => {
  test("a top-level step and a top-level map item are top-level", () => {
    expect(isTopLevelStepPlaceKey("reply")).toBe(true);
    expect(isTopLevelStepPlaceKey("m[2]")).toBe(true);
  });

  test("a nested step is not top-level", () => {
    expect(isTopLevelStepPlaceKey("section.reply")).toBe(false);
    expect(isTopLevelStepPlaceKey("loop.summarize[2]")).toBe(false);
  });
});

describe("measureStepPlaceKey", () => {
  test("measures the encoded key without a length check", () => {
    const id = "a".repeat(MAX_STEP_PLACE_SEGMENT_BYTES);
    expect(measureStepPlaceKey({ stepPath: [id, "b"] })).toEqual({
      key: `${id}.b`,
      segmentBytes: MAX_STEP_PLACE_SEGMENT_BYTES + 2,
    });
    expect(
      measureStepPlaceKey({ stepPath: ["m"], mapIndex: MAX_MAP_ITEM_INDEX }),
    ).toEqual({ key: "m[4294967294]", segmentBytes: 17 });
  });
});
