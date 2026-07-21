import { type } from "arktype";
import { describe, test, expect } from "bun:test";
import {
  AnthropicQuirks,
  GoogleGenAIQuirks,
  OpenAIQuirks,
} from "@intx/inference/providers";

// These import the quirk validators from the package boundary rather than a
// relative path on purpose: the point is to prove the re-exports in the
// providers barrel are wired up. A dropped or renamed re-export would pass
// tsc and every other suite, because nothing else imports these symbols; only
// exercising them from `@intx/inference/providers` catches it. The catalog
// resolver builds typed quirk values against exactly these exports.
describe("provider quirk validators are exported and usable", () => {
  test("OpenAIQuirks validates a well-formed bag and returns the value", () => {
    const parsed = OpenAIQuirks({
      forceAssistantReasoningContent: false,
      reasoningFieldNames: ["reasoning"],
    });
    expect(parsed instanceof type.errors).toBe(false);
    if (!(parsed instanceof type.errors)) {
      expect(parsed.reasoningFieldNames).toEqual(["reasoning"]);
      expect(parsed.forceAssistantReasoningContent).toBe(false);
    }
  });

  test("OpenAIQuirks rejects a reasoning field name it cannot read", () => {
    expect(
      OpenAIQuirks({ reasoningFieldNames: ["thinking"] }) instanceof
        type.errors,
    ).toBe(true);
  });

  test("OpenAIQuirks rejects an unknown key", () => {
    expect(OpenAIQuirks({ nope: true }) instanceof type.errors).toBe(true);
  });

  test("AnthropicQuirks and GoogleGenAIQuirks accept an empty bag", () => {
    expect(AnthropicQuirks({}) instanceof type.errors).toBe(false);
    expect(GoogleGenAIQuirks({}) instanceof type.errors).toBe(false);
  });

  test("AnthropicQuirks and GoogleGenAIQuirks reject a populated bag", () => {
    expect(AnthropicQuirks({ anything: true }) instanceof type.errors).toBe(
      true,
    );
    expect(GoogleGenAIQuirks({ anything: true }) instanceof type.errors).toBe(
      true,
    );
  });
});
