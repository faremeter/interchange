// Credential-sentinel substitution: adapters mark headers with a sentinel
// string, and injectCredentials rewrites exact matches to the secret the
// resolver returns for the source's credentialId. New providers need no
// harness change.

import { describe, expect, test } from "bun:test";

import type { CredentialMaterialResolver } from "@intx/types";
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
  credentialId: "sk-test-secret",
  model: "test-model",
};

// Resolves the source's `credentialId` to its secret. SOURCE.credentialId
// is the secret literal the assertions expect in the rewritten headers.
const readMaterial: CredentialMaterialResolver = (credentialId) => ({
  secret: credentialId,
});

describe("injectCredentials", () => {
  test("replaces CREDENTIAL_SENTINEL with apiKey verbatim", () => {
    const out = injectCredentials(
      {
        "x-api-key": CREDENTIAL_SENTINEL,
        "content-type": "application/json",
      },
      SOURCE,
      readMaterial,
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
      readMaterial,
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
      readMaterial,
    );
    expect(out["content-type"]).toBe("application/json");
    expect(out["anthropic-version"]).toBe("2023-06-01");
    expect(out["user-agent"]).toBe("test");
  });

  test("replaces sentinels regardless of header name (new providers need no harness change)", () => {
    // The exact header name is irrelevant to the substitution logic.
    const out = injectCredentials(
      { "x-goog-api-key": CREDENTIAL_SENTINEL },
      SOURCE,
      readMaterial,
    );
    expect(out["x-goog-api-key"]).toBe("sk-test-secret");
  });

  test("substring matches are not replaced (exact match only)", () => {
    // Values containing the sentinel as a substring are left alone; no
    // legitimate adapter composites values around it.
    const wrapped = `prefix ${CREDENTIAL_SENTINEL} suffix`;
    const out = injectCredentials(
      { "x-weird-header": wrapped },
      SOURCE,
      readMaterial,
    );
    expect(out["x-weird-header"]).toBe(wrapped);
  });

  test("returns a new object and does not mutate the input", () => {
    const input: Record<string, string> = {
      "x-api-key": CREDENTIAL_SENTINEL,
    };
    const out = injectCredentials(input, SOURCE, readMaterial);
    expect(input["x-api-key"]).toBe(CREDENTIAL_SENTINEL);
    expect(out["x-api-key"]).toBe("sk-test-secret");
    expect(out).not.toBe(input);
  });

  test("handles multiple sentinels of mixed shapes in one request", () => {
    // Both replacement shapes applied in one pass; some vendors want both.
    const out = injectCredentials(
      {
        "x-api-key": CREDENTIAL_SENTINEL,
        authorization: BEARER_CREDENTIAL_SENTINEL,
      },
      SOURCE,
      readMaterial,
    );
    expect(out["x-api-key"]).toBe("sk-test-secret");
    expect(out["authorization"]).toBe("Bearer sk-test-secret");
  });

  test("empty headers in, empty headers out", () => {
    expect(injectCredentials({}, SOURCE, readMaterial)).toEqual({});
  });
});
