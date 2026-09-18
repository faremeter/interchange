import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import path from "node:path";
import { getConfig, getLogger, resetSync, setup } from "./index";
import { installDefaultConsoleSink } from "./default-sink";

const REPO_ROOT = path.resolve(import.meta.dir, "../../..");
const PROBE = path.join(import.meta.dir, "default-sink-probe.ts");

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

  test("importing the package installs a config with the default sink", async () => {
    // Asserted in a child process. In-process this contract is unobservable:
    // the installer returns early when a config exists, and every package
    // imports this one, so by the time any test here runs the install has
    // already happened somewhere else and the reading is of that file's
    // leftovers. This assertion passed before the probe existed even though
    // it observed nothing -- the config it found happened to name its sink
    // "default" too.
    const proc = Bun.spawn(["bun", "run", "--conditions=intx-src", PROBE], {
      cwd: REPO_ROOT,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    const exitCode = await proc.exited;

    // Report both streams: the default sink writes through console, so a
    // failure to install can surface on either.
    expect(exitCode, `stdout: ${stdout}\nstderr: ${stderr}`).toBe(0);
    // Guards against a child that dies before reporting, which would
    // otherwise leave the assertions below trivially satisfied.
    expect(stdout).toContain("installed");
    expect(JSON.parse(stdout)).toEqual({
      installed: true,
      sinks: ["default"],
    });
  }, 30_000);

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
