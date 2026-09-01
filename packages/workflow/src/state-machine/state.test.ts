import { describe, test, expect } from "bun:test";

import { decideTerminalRunFlip, type RunPhase } from "./index";

describe("decideTerminalRunFlip", () => {
  test("maps each terminal phase to its status", () => {
    expect(decideTerminalRunFlip("completed")).toBe("completed");
    expect(decideTerminalRunFlip("failed")).toBe("failed");
    expect(decideTerminalRunFlip("cancelled")).toBe("cancelled");
  });

  test("throws on a non-terminal phase", () => {
    const nonTerminal: RunPhase[] = ["pending", "running", "cancelling"];
    for (const phase of nonTerminal) {
      expect(() => decideTerminalRunFlip(phase)).toThrow(
        `decideTerminalRunFlip: non-terminal run phase ${phase}`,
      );
    }
  });
});
