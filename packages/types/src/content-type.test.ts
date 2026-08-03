import { describe, test, expect } from "bun:test";
import { detectResponseKind } from "./content-type";

describe("detectResponseKind", () => {
  test("returns 'json' for application/json", () => {
    const headers = new Headers({ "content-type": "application/json" });
    expect(detectResponseKind(headers)).toBe("json");
  });

  test("returns 'json' for application/json with charset", () => {
    const headers = new Headers({
      "content-type": "application/json; charset=utf-8",
    });
    expect(detectResponseKind(headers)).toBe("json");
  });

  test("returns 'sse' for text/event-stream", () => {
    const headers = new Headers({ "content-type": "text/event-stream" });
    expect(detectResponseKind(headers)).toBe("sse");
  });

  test("returns 'sse' for text/event-stream with charset", () => {
    const headers = new Headers({
      "content-type": "text/event-stream; charset=utf-8",
    });
    expect(detectResponseKind(headers)).toBe("sse");
  });

  test("matches Content-Type case-insensitively", () => {
    const headers = new Headers({ "content-type": "Application/JSON" });
    expect(detectResponseKind(headers)).toBe("json");
  });

  test("throws when content-type is missing", () => {
    const headers = new Headers();
    expect(() => detectResponseKind(headers)).toThrow(
      /response has no Content-Type/,
    );
  });

  test("throws on unknown content-type", () => {
    const headers = new Headers({ "content-type": "text/plain" });
    expect(() => detectResponseKind(headers)).toThrow(/text\/plain/);
  });
});
