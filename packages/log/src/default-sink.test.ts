import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { getConfig, getLogger, resetSync, setup } from "./index";
import { installDefaultConsoleSink } from "./default-sink";

// Snapshot taken at test-file load, before any beforeEach hooks run. This is
// the only point at which we can observe the side effect at the bottom of
// `default-sink.ts` directly; the hooks below reset and reinstall manually
// for the per-test scenarios.
//
// Order-sensitive: any future test file in this directory that calls
// `resetSync()` at top level *before* this file imports will null this
// snapshot. The "importing the module installs a config" test below would
// then throw with a misleading error pointing at this file rather than the
// real cause.
const moduleLoadConfig = getConfig();

describe("default console sink", () => {
  const warnCaptured: string[] = [];
  /* eslint-disable no-console -- intentional spy on console.warn */
  let originalWarn: typeof console.warn;

  beforeEach(() => {
    warnCaptured.length = 0;
    originalWarn = console.warn;
    console.warn = (...args: unknown[]) => {
      warnCaptured.push(args.map((a) => String(a)).join(" "));
    };

    resetSync();
    installDefaultConsoleSink();
  });

  afterEach(() => {
    console.warn = originalWarn;
  });
  /* eslint-enable no-console */

  test("importing the module installs a config with the default sink", () => {
    if (!moduleLoadConfig) {
      throw new Error(
        "default-sink module-load side effect did not populate getConfig()",
      );
    }
    expect(Object.keys(moduleLoadConfig.sinks)).toEqual(["default"]);
  });

  test("warn lands on console.warn through the default sink", () => {
    getLogger(["test-default-sink-probe"]).warn`probe`;
    expect(warnCaptured.some((w) => w.includes("probe"))).toBe(true);
  });

  test("setup() replaces the default sink and routes warns through the new sink", async () => {
    const before = getConfig();
    if (!before) {
      throw new Error("default sink installer left getConfig() null");
    }
    expect(Object.keys(before.sinks)).toEqual(["default"]);

    await setup();

    const after = getConfig();
    if (!after) {
      throw new Error("setup() left getConfig() null");
    }
    expect(Object.keys(after.sinks)).toEqual(["console"]);

    warnCaptured.length = 0;
    getLogger(["test-default-sink-probe"]).warn`post-setup-probe`;
    expect(warnCaptured.some((w) => w.includes("post-setup-probe"))).toBe(true);
  });

  test("installDefaultConsoleSink is idempotent when a config is present", () => {
    const before = getConfig();
    installDefaultConsoleSink();
    expect(getConfig()).toBe(before);
  });
});
