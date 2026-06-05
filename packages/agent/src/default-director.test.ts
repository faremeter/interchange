import { describe, test, expect } from "bun:test";

import type { ToolDefinition } from "@intx/types/runtime";

import {
  buildDefaultDirectorRef,
  defaultDirectorFactory,
} from "./default-director";
import type { BaseEnv } from "./env";

const NO_TOOLS: readonly ToolDefinition[] = Object.freeze([]);

describe("defaultDirectorFactory", () => {
  test("carries the canonical @intx/agent/default id", () => {
    expect(defaultDirectorFactory.id).toBe("@intx/agent/default");
  });

  test("declares no required env keys", () => {
    expect(defaultDirectorFactory.requires).toEqual([]);
  });

  test("constructs a ReactorDirector given an empty config and agent context", () => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, the default director ignores env in its constructor
    const env = {} as BaseEnv;
    const director = defaultDirectorFactory({}, env, {
      systemPrompt: "you are a planner",
      toolDefinitions: NO_TOOLS,
      compactorNames: [],
    });
    expect(typeof director.decide).toBe("function");
  });

  test("accepts mode='reactive' through the config", () => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub, the default director ignores env in its constructor
    const env = {} as BaseEnv;
    const director = defaultDirectorFactory({ mode: "reactive" }, env, {
      systemPrompt: "ack only",
      toolDefinitions: NO_TOOLS,
      compactorNames: [],
    });
    expect(typeof director.decide).toBe("function");
  });
});

describe("buildDefaultDirectorRef", () => {
  test("constructs a ref pointing at the default factory", () => {
    const ref = buildDefaultDirectorRef({});
    expect(ref.id).toBe("@intx/agent/default");
    expect(ref.config).toEqual({});
  });

  test("accepts the known mode field and round-trips it as the ref config", () => {
    // The arktype schema permits unknown extra keys by default, so
    // rejection of unknowns is not tested here. If the schema is
    // tightened to reject unknown fields later, this test becomes the
    // contract pin for the known-field path.
    const ref = buildDefaultDirectorRef({ mode: "conversational" });
    expect(ref.config).toEqual({ mode: "conversational" });
  });
});
