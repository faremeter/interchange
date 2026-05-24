// Credential-sentinel substitution. Adapters declare which credential
// shape they want by placing one of the exported sentinel strings as
// the header value; `injectCredentials` walks the header map and
// rewrites exact-match values with material derived from
// `InferenceSource.apiKey`. The harness uses this in place of the
// previous per-header hardcoded branches so adding a new provider
// requires no harness change.

import { describe, expect, test } from "bun:test";

import type { InferenceSource } from "@intx/types/runtime";

import {
  BEARER_CREDENTIAL_SENTINEL,
  CREDENTIAL_SENTINEL,
  injectCredentials,
} from "./auth";

const SOURCE: InferenceSource = {
  id: "test:model",
  provider: "test",
  baseURL: "https://test.invalid",
  apiKey: "sk-test-secret",
  model: "test-model",
};

describe("injectCredentials", () => {
  test("replaces CREDENTIAL_SENTINEL with apiKey verbatim", () => {
    const out = injectCredentials(
      {
        "x-api-key": CREDENTIAL_SENTINEL,
        "content-type": "application/json",
      },
      SOURCE,
    );
    expect(out["x-api-key"]).toBe("sk-test-secret");
    expect(out["content-type"]).toBe("application/json");
  });

  test("replaces BEARER_CREDENTIAL_SENTINEL with Bearer-prefixed apiKey", () => {
    const out = injectCredentials(
      {
        authorization: BEARER_CREDENTIAL_SENTINEL,
        "content-type": "application/json",
      },
      SOURCE,
    );
    expect(out["authorization"]).toBe("Bearer sk-test-secret");
    expect(out["content-type"]).toBe("application/json");
  });

  test("non-sentinel values pass through unchanged", () => {
    const out = injectCredentials(
      {
        "content-type": "application/json",
        "anthropic-version": "2023-06-01",
        "user-agent": "test",
      },
      SOURCE,
    );
    expect(out["content-type"]).toBe("application/json");
    expect(out["anthropic-version"]).toBe("2023-06-01");
    expect(out["user-agent"]).toBe("test");
  });

  test("replaces sentinels regardless of header name (new providers need no harness change)", () => {
    // The whole point of sentinel-based replacement: a brand-new
    // provider that uses, say, `x-goog-api-key` works without
    // touching this function. The exact header name is irrelevant
    // to the substitution logic.
    const out = injectCredentials(
      { "x-goog-api-key": CREDENTIAL_SENTINEL },
      SOURCE,
    );
    expect(out["x-goog-api-key"]).toBe("sk-test-secret");
  });

  test("substring matches are not replaced (exact match only)", () => {
    // A header value that *contains* the sentinel literal as a
    // substring -- but isn't exactly equal to it -- is left alone.
    // Partial replacement would be surprising and no legitimate
    // adapter constructs composite values around the sentinel.
    const wrapped = `prefix ${CREDENTIAL_SENTINEL} suffix`;
    const out = injectCredentials({ "x-weird-header": wrapped }, SOURCE);
    expect(out["x-weird-header"]).toBe(wrapped);
  });

  test("returns a new object and does not mutate the input", () => {
    const input: Record<string, string> = {
      "x-api-key": CREDENTIAL_SENTINEL,
    };
    const out = injectCredentials(input, SOURCE);
    expect(input["x-api-key"]).toBe(CREDENTIAL_SENTINEL);
    expect(out["x-api-key"]).toBe("sk-test-secret");
    expect(out).not.toBe(input);
  });

  test("handles multiple sentinels of mixed shapes in one request", () => {
    // Pathological but well-defined: an adapter that wants both
    // a verbatim credential header and a Bearer header (some
    // vendors do this for legacy + modern endpoints) gets both
    // replacements applied in one pass.
    const out = injectCredentials(
      {
        "x-api-key": CREDENTIAL_SENTINEL,
        authorization: BEARER_CREDENTIAL_SENTINEL,
      },
      SOURCE,
    );
    expect(out["x-api-key"]).toBe("sk-test-secret");
    expect(out["authorization"]).toBe("Bearer sk-test-secret");
  });

  test("empty headers in, empty headers out", () => {
    expect(injectCredentials({}, SOURCE)).toEqual({});
  });
});
