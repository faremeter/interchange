import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { getLogger, resetSync, installDefaultConsoleSink } from "./index";

// Locks down two design contracts of the module-load default sink that
// default-sink.test.ts does not currently exercise:
//
//   1. Threshold is "warning" — info/debug must be suppressed; warning,
//      error, and fatal must be routed.
//   2. Formatter selection follows NODE_ENV — production yields JSON Lines,
//      anything else yields ANSI-colored text.
//
// Both behaviors are contracts a future refactor could accidentally break
// (e.g., bumping threshold to "info" or flipping the env heuristic).

/* eslint-disable no-console -- intentional spies on console methods */
describe("default console sink routing contract", () => {
  const warnCaptured: string[] = [];
  const errorCaptured: string[] = [];
  const infoCaptured: string[] = [];
  const debugCaptured: string[] = [];
  let originalWarn: typeof console.warn;
  let originalError: typeof console.error;
  let originalInfo: typeof console.info;
  let originalDebug: typeof console.debug;
  let originalNodeEnv: string | undefined;

  function joinArgs(args: unknown[]): string {
    return args.map((a) => String(a)).join(" ");
  }

  beforeEach(() => {
    warnCaptured.length = 0;
    errorCaptured.length = 0;
    infoCaptured.length = 0;
    debugCaptured.length = 0;
    originalWarn = console.warn;
    originalError = console.error;
    originalInfo = console.info;
    originalDebug = console.debug;
    console.warn = (...args: unknown[]) => {
      warnCaptured.push(joinArgs(args));
    };
    console.error = (...args: unknown[]) => {
      errorCaptured.push(joinArgs(args));
    };
    console.info = (...args: unknown[]) => {
      infoCaptured.push(joinArgs(args));
    };
    console.debug = (...args: unknown[]) => {
      debugCaptured.push(joinArgs(args));
    };
    originalNodeEnv = process.env["NODE_ENV"];
  });

  afterEach(() => {
    console.warn = originalWarn;
    console.error = originalError;
    console.info = originalInfo;
    console.debug = originalDebug;
    if (originalNodeEnv === undefined) delete process.env["NODE_ENV"];
    else process.env["NODE_ENV"] = originalNodeEnv;
  });

  test("info is suppressed (threshold is warning)", () => {
    resetSync();
    installDefaultConsoleSink();
    getLogger(["probe"]).info`info-msg`;
    expect(infoCaptured.some((w) => w.includes("info-msg"))).toBe(false);
    expect(warnCaptured.some((w) => w.includes("info-msg"))).toBe(false);
  });

  test("debug is suppressed (threshold is warning)", () => {
    resetSync();
    installDefaultConsoleSink();
    getLogger(["probe"]).debug`debug-msg`;
    expect(debugCaptured.some((w) => w.includes("debug-msg"))).toBe(false);
    expect(warnCaptured.some((w) => w.includes("debug-msg"))).toBe(false);
  });

  test("error is routed to console.error", () => {
    resetSync();
    installDefaultConsoleSink();
    getLogger(["probe"]).error`error-msg`;
    expect(errorCaptured.some((w) => w.includes("error-msg"))).toBe(true);
  });

  test("fatal is routed to console.error", () => {
    resetSync();
    installDefaultConsoleSink();
    getLogger(["probe"]).fatal`fatal-msg`;
    expect(errorCaptured.some((w) => w.includes("fatal-msg"))).toBe(true);
  });

  test("NODE_ENV=production yields JSON Lines output", () => {
    process.env["NODE_ENV"] = "production";
    resetSync();
    installDefaultConsoleSink();
    getLogger(["probe"]).warn`hello-prod`;
    const joined = warnCaptured.join("");
    expect(joined).toContain("hello-prod");
    expect(joined.includes("\x1b[")).toBe(false);
    expect(() => JSON.parse(joined.trim())).not.toThrow();
  });

  test("non-production NODE_ENV yields ANSI-colored text output", () => {
    process.env["NODE_ENV"] = "development";
    resetSync();
    installDefaultConsoleSink();
    getLogger(["probe"]).warn`hello-dev`;
    const joined = warnCaptured.join("");
    expect(joined).toContain("hello-dev");
    expect(joined.includes("\x1b[")).toBe(true);
  });
});
/* eslint-enable no-console */
