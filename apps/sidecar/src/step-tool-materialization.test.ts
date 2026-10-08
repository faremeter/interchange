import { describe, expect, test } from "bun:test";

import { stepDeployTreeDir } from "./step-tool-materialization";

describe("stepDeployTreeDir base-step resolution", () => {
  const dataDir = "/data";
  const mailboxAddress = "run_dep-map@example.com";

  test("a map iteration resolves the base step's deploy tree", () => {
    // Deploy stages one deploy tree per base step; every map iteration
    // `<base>[<index>]` must read that one tree, not a per-iteration
    // address that was never staged.
    const base = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "summarize",
      stepCount: 2,
    });
    const iter0 = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "summarize[0]",
      stepCount: 2,
    });
    const iter1 = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "summarize[1]",
      stepCount: 2,
    });
    expect(iter0).toBe(base);
    expect(iter1).toBe(base);
  });

  test("distinct base steps still resolve distinct deploy trees", () => {
    const a = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "alpha[0]",
      stepCount: 2,
    });
    const b = stepDeployTreeDir({
      dataDir,
      mailboxAddress,
      stepId: "beta[0]",
      stepCount: 2,
    });
    expect(a).not.toBe(b);
  });
});
