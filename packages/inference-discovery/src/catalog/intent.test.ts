import { describe, test, expect } from "bun:test";
import { existsSync, statSync } from "node:fs";
import { type } from "arktype";
import { CAPABILITIES } from "./capability";
import {
  CapabilityIntent,
  INTENTS,
  MediaRef,
  ToolDecl,
  resolveMediaPath,
} from "./intent";

describe("INTENTS map", () => {
  test("has one entry per declared capability", () => {
    for (const name of CAPABILITIES) {
      expect(INTENTS[name]).toBeDefined();
    }
    expect(Object.keys(INTENTS).length).toBe(CAPABILITIES.length);
  });

  test("every entry parses as a CapabilityIntent", () => {
    for (const name of CAPABILITIES) {
      const result = CapabilityIntent(INTENTS[name]);
      expect(result instanceof type.errors).toBe(false);
    }
  });

  test("streaming variants share the non-streaming intent", () => {
    expect(INTENTS["plain-text-streaming"]).toBe(INTENTS["plain-text"]);
    expect(INTENTS["vision-input-streaming"]).toBe(INTENTS["vision-input"]);
    expect(INTENTS["reasoning-content-streaming"]).toBe(
      INTENTS["reasoning-content"],
    );
  });
});

describe("MediaRef paths", () => {
  test("every referenced media file exists on disk", () => {
    for (const name of CAPABILITIES) {
      const intent = INTENTS[name];
      const media = intent.media;
      if (media === undefined) continue;
      for (const ref of media) {
        const absolute = resolveMediaPath(ref);
        expect(existsSync(absolute)).toBe(true);
        expect(statSync(absolute).size).toBeGreaterThan(0);
      }
    }
  });

  test("resolveMediaPath rejects absolute paths", () => {
    expect(() =>
      resolveMediaPath({ kind: "image", path: "/etc/passwd" }),
    ).toThrow();
  });
});

describe("ToolDecl validator", () => {
  test("accepts a well-formed function declaration", () => {
    const result = ToolDecl({
      name: "get_weather",
      description: "Look up the weather.",
      parameters: {
        type: "object",
        properties: {
          location: { type: "string", description: "City name" },
        },
        required: ["location"],
      },
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects a parameters object with wrong type tag", () => {
    const result = ToolDecl({
      name: "x",
      description: "x",
      parameters: { type: "array", properties: {} },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("MediaRef validator", () => {
  test("accepts an image reference", () => {
    const result = MediaRef({ kind: "image", path: "media/sample.jpg" });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an unknown media kind", () => {
    const result = MediaRef({ kind: "spreadsheet", path: "x.csv" });
    expect(result instanceof type.errors).toBe(true);
  });
});
