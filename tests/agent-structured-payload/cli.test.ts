// agent-structured-payload end-to-end test. Drives the example's
// main() with a pinned offering payload and asserts both (a) that
// the example reports the correct delivery summary and (b) that the
// reactor's message.received event carries the typed payload
// verbatim.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  buildOfferingRequest,
  main,
} from "@intx/example-agent-structured-payload";
import { setupHarness, type Harness } from "@intx/inference-testing";
import type { InferenceSource } from "@intx/types/runtime";

const SOURCE: InferenceSource = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test-structured-payload",
  model: "claude-3-5-sonnet",
};

describe("agent-structured-payload CLI", () => {
  let workDir: string;
  let contextDir: string;
  let harness: Harness;
  let stdoutBuf: string;
  let stderrBuf: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "agent-structured-payload-"));
    contextDir = join(workDir, "ctx");
    harness = setupHarness();
    stdoutBuf = "";
    stderrBuf = "";
  });

  afterEach(() => {
    harness.dispose();
    rmSync(workDir, { recursive: true, force: true });
  });

  test("delivers an offering.request and reports the round-trip", async () => {
    const offering = {
      offeringId: "beta-seat-7",
      description: "Beta seat",
      priceCents: 499,
      currency: "USD",
    };

    const run = main(
      [],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        sourceOverride: SOURCE,
        deps: harness.deps,
        contextDir,
        offering,
      },
    );
    const code = await run;

    expect(stderrBuf).toBe("");
    expect(code).toBe(0);
    expect(stdoutBuf).toContain("delivering offering.request:");
    expect(stdoutBuf).toContain("offeringId:  beta-seat-7");
    expect(stdoutBuf).toContain("price:       USD 4.99");
    expect(stdoutBuf).toContain("type:        offering.request");
    expect(stdoutBuf).toContain("reactor received:");
    expect(stdoutBuf).toContain("payload.type:    offering.request");
    expect(stdoutBuf).toContain("payload.version: 1");
    expect(stdoutBuf).toContain('"offeringId":"beta-seat-7"');
    expect(stdoutBuf).toContain('"description":"Beta seat"');
    expect(stdoutBuf).toContain('"priceCents":499');
  });

  test("buildOfferingRequest produces a well-formed InboundMessage", () => {
    const msg = buildOfferingRequest({
      offeringId: "x-1",
      description: "thing",
      priceCents: 100,
      currency: "USD",
    });
    expect(msg.headers.interchangeType).toBe("offering.request");
    expect(msg.headers.interchangeOfferingId).toBe("x-1");
    expect(msg.payload?.type).toBe("offering.request");
    expect(msg.payload?.body).toEqual({
      offeringId: "x-1",
      description: "thing",
      priceCents: 100,
      currency: "USD",
    });
    expect(msg.payload?.version).toBe("1");
    expect(msg.signatureStatus).toBe("missing");
  });

  test("invalid --price-cents fails with exit code 1", async () => {
    const code = await main(
      ["--price-cents", "not-a-number"],
      { ANTHROPIC_API_KEY: "irrelevant" },
      {
        stdout: (s) => {
          stdoutBuf += s;
        },
        stderr: (s) => {
          stderrBuf += s;
        },
        sourceOverride: SOURCE,
        deps: harness.deps,
        contextDir,
      },
    );
    expect(code).toBe(1);
    expect(stderrBuf).toContain("--price-cents");
  });
});
