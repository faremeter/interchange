import { describe, test, expect, spyOn } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import {
  removeFileAtomicDurable,
  writeFileAtomicDurable,
} from "./atomic-write";

async function makeDir(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "atomic-write-"));
}

async function listNames(dir: string): Promise<string[]> {
  return (await fs.readdir(dir)).sort();
}

describe("writeFileAtomicDurable", () => {
  for (const requireDirectorySync of [false, true]) {
    test(`directory sync failures reject only in strict mode (${String(requireDirectorySync)})`, async () => {
      const dir = await makeDir();
      const file = path.join(dir, "record.json");
      const failure = new Error("directory sync failed");
      const open = fs.open;
      const spy = spyOn(fs, "open").mockImplementation(
        async (target, flags, mode) => {
          const handle = await open(target, flags, mode);
          if (target === dir) {
            spyOn(handle, "sync").mockRejectedValue(failure);
          }
          return handle;
        },
      );
      try {
        const write = writeFileAtomicDurable(file, "new", {
          mode: 0o600,
          requireDirectorySync,
        });
        if (requireDirectorySync) await expect(write).rejects.toBe(failure);
        else await write;
        expect(await fs.readFile(file, "utf8")).toBe("new");
      } finally {
        spy.mockRestore();
        await fs.rm(dir, { recursive: true, force: true });
      }
    });
  }

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

describe("removeFileAtomicDurable", () => {
  test("retries an unlinked file's failed directory sync before confirming removal", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");
    await fs.writeFile(file, "record");
    const failure = new Error("directory sync failed");
    let syncAttempts = 0;
    const open = fs.open;
    const spy = spyOn(fs, "open").mockImplementation(
      async (target, flags, mode) => {
        const handle = await open(target, flags, mode);
        if (target === dir) {
          const sync = handle.sync.bind(handle);
          spyOn(handle, "sync").mockImplementation(async () => {
            syncAttempts++;
            if (syncAttempts === 1) throw failure;
            await sync();
          });
        }
        return handle;
      },
    );
    try {
      await expect(
        removeFileAtomicDurable(file, { requireDirectorySync: true }),
      ).rejects.toBe(failure);
      expect(await listNames(dir)).toEqual([]);
      await removeFileAtomicDurable(file, { requireDirectorySync: true });
      expect(syncAttempts).toBe(2);
    } finally {
      spy.mockRestore();
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("strict removal syncs the surviving ancestor of an already-removed directory", async () => {
    const dir = await makeDir();
    const parent = path.join(dir, "workflow-runs");
    const removed = path.join(parent, "gone");
    const file = path.join(removed, "record.json");
    const opened: string[] = [];
    const open = fs.open;
    const spy = spyOn(fs, "open").mockImplementation(
      async (target, flags, mode) => {
        opened.push(String(target));
        return open(target, flags, mode);
      },
    );
    try {
      await removeFileAtomicDurable(file, { requireDirectorySync: true });
      expect(opened).toEqual([removed, parent, dir]);
    } finally {
      spy.mockRestore();
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("strict removal does not bypass an unreadable parent directory", async () => {
    const dir = await makeDir();
    const failure = Object.assign(new Error("directory unavailable"), {
      code: "EACCES",
    });
    const open = fs.open;
    const spy = spyOn(fs, "open").mockImplementation(
      async (target, flags, mode) => {
        if (target === dir) throw failure;
        return open(target, flags, mode);
      },
    );
    try {
      await expect(
        removeFileAtomicDurable(path.join(dir, "record.json"), {
          requireDirectorySync: true,
        }),
      ).rejects.toBe(failure);
      expect(spy).toHaveBeenCalledTimes(1);
    } finally {
      spy.mockRestore();
      await fs.rm(dir, { recursive: true, force: true });
    }
  });

  test("removes an existing file", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "record.json");
    await fs.writeFile(file, "x");

    await removeFileAtomicDurable(file);
    expect(await listNames(dir)).toEqual([]);

    await fs.rm(dir, { recursive: true, force: true });
  });

  test("is a no-op for an already-absent file", async () => {
    const dir = await makeDir();
    const file = path.join(dir, "never-existed.json");

    // Idempotent: removing an absent file completes without throwing, so a
    // re-driven eviction of an already-evicted key is not a fault.
    await removeFileAtomicDurable(file);
    expect(await listNames(dir)).toEqual([]);

    await fs.rm(dir, { recursive: true, force: true });
  });

  test("leaves sibling files untouched", async () => {
    const dir = await makeDir();
    const target = path.join(dir, "target.json");
    const sibling = path.join(dir, "keep.json");
    await fs.writeFile(target, "gone");
    await fs.writeFile(sibling, "stay");

    await removeFileAtomicDurable(target);
    expect(await listNames(dir)).toEqual(["keep.json"]);
    expect(await fs.readFile(sibling, "utf8")).toBe("stay");

    await fs.rm(dir, { recursive: true, force: true });
  });
});
