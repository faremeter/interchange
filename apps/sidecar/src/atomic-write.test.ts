import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { writeFileAtomicDurable } from "./atomic-write";

async function makeDir(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "atomic-write-"));
}

async function listNames(dir: string): Promise<string[]> {
  return (await fs.readdir(dir)).sort();
}

describe("writeFileAtomicDurable", () => {
  test("round-trips contents to the target path", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");

    await writeFileAtomicDurable(file, '{"a":1}', { mode: 0o600 });
    expect(await fs.readFile(file, "utf8")).toBe('{"a":1}');

    await fs.rm(dir, { recursive: true, force: true });
  });

  test("applies mode on the created file", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");

    await writeFileAtomicDurable(file, "x", { mode: 0o600 });
    const stat = await fs.stat(file);
    expect(stat.mode & 0o777).toBe(0o600);

    await fs.rm(dir, { recursive: true, force: true });
  });

  test("re-applies mode when overwriting an existing file", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");

    // Seed a pre-existing file with a laxer mode. A plain in-place
    // overwrite keeps the original mode; the temp+rename path creates a
    // fresh file every write, so the requested mode actually lands.
    await fs.writeFile(file, "old", { mode: 0o644 });
    await writeFileAtomicDurable(file, "new", { mode: 0o600 });

    expect(await fs.readFile(file, "utf8")).toBe("new");
    const stat = await fs.stat(file);
    expect(stat.mode & 0o777).toBe(0o600);

    await fs.rm(dir, { recursive: true, force: true });
  });

  test("leaves no temp orphan after a successful write", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");

    await writeFileAtomicDurable(file, "x", { mode: 0o600 });
    expect(await listNames(dir)).toEqual(["record.json"]);

    await fs.rm(dir, { recursive: true, force: true });
  });

  test("a write that fails before staging leaves the prior file intact", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");
    await fs.writeFile(file, "old", { mode: 0o600 });

    // Deny writes to the directory so the temp-file creation fails
    // outright. The prior complete record must survive untouched -- an
    // interrupted rotation must never corrupt the sole restore source.
    await fs.chmod(dir, 0o500);
    try {
      await expect(
        writeFileAtomicDurable(file, "new", { mode: 0o600 }),
      ).rejects.toThrow();
      expect(await fs.readFile(file, "utf8")).toBe("old");
      expect(await listNames(dir)).toEqual(["record.json"]);
    } finally {
      await fs.chmod(dir, 0o700);
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("a write that fails at the rename unlinks its staged temp", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");

    // Occupy the target path with a directory so the rename fails after
    // the temp file has already been staged and fsynced. This exercises
    // the error-path unlink: the staged temp must be removed rather than
    // stranded as an orphan.
    await fs.mkdir(file);
    await expect(
      writeFileAtomicDurable(file, "new", { mode: 0o600 }),
    ).rejects.toThrow();
    expect(await listNames(dir)).toEqual(["record.json"]);

    await fs.rm(dir, { recursive: true, force: true });
  });
});
