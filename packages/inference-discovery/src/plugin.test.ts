import { describe, test, expect } from "bun:test";
import {
  resolveTurn1Response,
  type CapturedResponse,
  type Turn1Reconstructor,
} from "./plugin";

function capturedResponse(
  fields: Pick<CapturedResponse, "parsed" | "bytes">,
): CapturedResponse {
  return { status: 200, headers: {}, ...fields };
}

describe("resolveTurn1Response", () => {
  test("returns the parsed body without invoking reconstruct", () => {
    const parsed = { content: [{ type: "text", text: "hi" }] };
    let called = false;
    const reconstruct: Turn1Reconstructor = () => {
      called = true;
      return null;
    };

    const result = resolveTurn1Response(
      capturedResponse({ parsed, bytes: null }),
      reconstruct,
    );

    expect(result).toBe(parsed);
    expect(called).toBe(false);
  });

  test("reconstructs from SSE bytes when parsed is null", () => {
    const bytes = new Uint8Array([1, 2, 3]);
    const reconstructed = { candidates: [] };
    const received: Uint8Array[] = [];
    const reconstruct: Turn1Reconstructor = (input) => {
      received.push(input);
      return reconstructed;
    };

    const result = resolveTurn1Response(
      capturedResponse({ parsed: null, bytes }),
      reconstruct,
    );

    expect(result).toBe(reconstructed);
    expect(received).toEqual([bytes]);
  });

  test("throws when the response carries neither parsed body nor bytes", () => {
    expect(() =>
      resolveTurn1Response(
        capturedResponse({ parsed: null, bytes: null }),
        () => expect.unreachable("reconstruct must not run"),
      ),
    ).toThrow(/neither a parsed body nor SSE bytes/);
  });
});
