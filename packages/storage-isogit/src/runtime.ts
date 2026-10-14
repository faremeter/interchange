import type { FsClient } from "isomorphic-git";
import { getLogger } from "@intx/log";
import { hasCode } from "@intx/types/has-code";

const logger = getLogger(["interchange", "storage-isogit", "git"]);

const textDecoder = new TextDecoder();

export function decodeUTF8(bytes: Uint8Array): string {
  return textDecoder.decode(bytes);
}

export interface IsogitPath {
  join(...parts: string[]): string;
  relative(from: string, to: string): string;
  resolve(filepath: string): string;
}

/** Minimal host capabilities required by the storage implementation. */
export interface IsogitRuntime {
  readonly fs: FsClient;
  readonly path: IsogitPath;
  /** Atomically move one namespace entry from `oldPath` to `newPath`. */
  rename(oldPath: string, newPath: string): Promise<void>;
  /** Flush pending backend writes at a completed mutation boundary. */
  flush?(): Promise<void>;
}

export interface FileSystemStat {
  readonly size: number;
  isDirectory(): boolean;
  isFile(): boolean;
}

type WriteFileOptions = {
  mode?: number;
};

type RemoveOptions = {
  force?: boolean;
  recursive?: boolean;
};

type FsMethod =
  | "lstat"
  | "mkdir"
  | "readFile"
  | "readdir"
  | "readlink"
  | "rmdir"
  | "stat"
  | "symlink"
  | "unlink"
  | "writeFile";

type UnknownOperation = (...args: unknown[]) => unknown;

function operation(
  value: unknown,
  method: FsMethod,
): UnknownOperation | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const candidate: unknown = Reflect.get(value, method);
  if (typeof candidate !== "function") {
    return undefined;
  }
  return (...args) => {
    const result: unknown = Reflect.apply(candidate, value, args);
    return result;
  };
}

/**
 * Invoke either shape accepted by isomorphic-git without changing errors.
 * Promise clients expose methods under `promises`; callback clients expose
 * the same methods on the client itself.
 */
function callFs(
  fs: FsClient,
  method: FsMethod,
  args: readonly unknown[],
): Promise<unknown> {
  const promises =
    typeof fs === "object" && fs !== null && "promises" in fs
      ? Reflect.get(fs, "promises")
      : undefined;
  const promiseOperation = operation(promises, method);
  if (promiseOperation !== undefined) {
    return Promise.resolve(promiseOperation(...args));
  }

  const callbackOperation = operation(fs, method);
  if (callbackOperation === undefined) {
    return Promise.reject(
      new TypeError(`Filesystem client does not implement ${method}`),
    );
  }
  return new Promise<unknown>((resolve, reject) => {
    callbackOperation(...args, (cause: unknown, result: unknown) => {
      if (cause !== null && cause !== undefined) {
        reject(cause);
      } else {
        resolve(result);
      }
    });
  });
}

type GitFsOperation = (...args: unknown[]) => Promise<unknown>;

function wrapGitFs(
  fs: FsClient,
  replace: Partial<Record<FsMethod, GitFsOperation>>,
): FsClient {
  const operation = (method: FsMethod): GitFsOperation =>
    replace[method] ?? (async (...args) => callFs(fs, method, args));
  return {
    promises: {
      readFile: operation("readFile"),
      writeFile: operation("writeFile"),
      mkdir: operation("mkdir"),
      rmdir: operation("rmdir"),
      unlink: operation("unlink"),
      stat: operation("stat"),
      lstat: operation("lstat"),
      readdir: operation("readdir"),
      readlink: operation("readlink"),
      symlink: operation("symlink"),
    },
  };
}

function isIndexPath(filepath: unknown): boolean {
  return (
    typeof filepath === "string" &&
    (filepath === ".git/index" || filepath.endsWith("/.git/index"))
  );
}

/**
 * isomorphic-git treats an index it fails to read, or one with no contents,
 * as empty, so one failed read could stage or commit a tree that drops every
 * other entry. Hand it empty bytes instead, which it rejects as a corrupt
 * index, and log the cause its error cannot carry.
 */
function guardIndexReads(fs: FsClient): FsClient {
  return wrapGitFs(fs, {
    readFile: async (...args) => {
      if (!isIndexPath(args[0])) return callFs(fs, "readFile", args);
      let contents: unknown;
      try {
        contents = await callFs(fs, "readFile", args);
      } catch (cause) {
        if (hasCode(cause) && cause.code === "ENOENT") throw cause;
        const detail = cause instanceof Error ? cause.message : String(cause);
        logger.error`Could not read the Git index ${String(args[0])}: ${detail}`;
        return new Uint8Array();
      }
      if (contents instanceof Uint8Array || contents instanceof ArrayBuffer) {
        return contents;
      }
      logger.error`The Git index ${String(args[0])} has no readable contents`;
      return new Uint8Array();
    },
  });
}

type ListingFailure = { path: unknown; cause: unknown };

function listingError(
  first: ListingFailure,
  errors: readonly unknown[],
): AggregateError {
  const detail =
    first.cause instanceof Error ? first.cause.message : String(first.cause);
  return new AggregateError(
    errors,
    `Could not list ${String(first.path)}: ${detail}`,
  );
}

/**
 * Run an isomorphic-git operation and fail it when a directory listing
 * fails. isomorphic-git turns every listing failure except ENOTDIR into an
 * empty listing, so staging a directory could otherwise skip its children
 * without an error.
 */
export async function withListingFailures<T>(
  fs: FsClient,
  run: (fs: FsClient) => Promise<T>,
): Promise<T> {
  const failures: ListingFailure[] = [];
  const client = wrapGitFs(fs, {
    readdir: async (...args) => {
      try {
        return await callFs(fs, "readdir", args);
      } catch (cause) {
        if (!hasCode(cause) || cause.code !== "ENOTDIR") {
          failures.push({ path: args[0], cause });
        }
        throw cause;
      }
    },
  });

  let result: T;
  try {
    result = await run(client);
  } catch (cause) {
    const first = failures[0];
    if (first === undefined) throw cause;
    throw listingError(first, [...failures.map((f) => f.cause), cause]);
  }
  const first = failures[0];
  if (first !== undefined) {
    throw listingError(
      first,
      failures.map((f) => f.cause),
    );
  }
  return result;
}

function fileStat(value: unknown, method: FsMethod): FileSystemStat {
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`Filesystem ${method} returned an invalid stat`);
  }
  const size: unknown = Reflect.get(value, "size");
  const isDirectory: unknown = Reflect.get(value, "isDirectory");
  const isFile: unknown = Reflect.get(value, "isFile");
  if (
    typeof size !== "number" ||
    typeof isDirectory !== "function" ||
    typeof isFile !== "function"
  ) {
    throw new TypeError(`Filesystem ${method} returned an invalid stat`);
  }
  return {
    size,
    isDirectory: () => Boolean(Reflect.apply(isDirectory, value, [])),
    isFile: () => Boolean(Reflect.apply(isFile, value, [])),
  };
}

function directoryEntries(value: unknown): string[] {
  if (!Array.isArray(value)) {
    throw new TypeError("Filesystem readdir returned a non-array value");
  }
  return value.map((entry) => {
    if (typeof entry !== "string") {
      throw new TypeError("Filesystem readdir returned a non-string entry");
    }
    return entry;
  });
}

export interface StorageFileSystem {
  readonly git: FsClient;
  access(filepath: string): Promise<void>;
  readFile(filepath: string): Promise<Uint8Array>;
  readTextFile(filepath: string): Promise<string>;
  writeFile(
    filepath: string,
    data: string | Uint8Array,
    options?: WriteFileOptions,
  ): Promise<void>;
  readdir(filepath: string): Promise<string[]>;
  mkdir(filepath: string, options?: { recursive?: boolean }): Promise<void>;
  stat(filepath: string): Promise<FileSystemStat>;
  lstat(filepath: string): Promise<FileSystemStat>;
  remove(filepath: string, options?: RemoveOptions): Promise<void>;
  rename(oldPath: string, newPath: string): Promise<void>;
}

/** Normalized internal view derived from the public runtime contract. */
export interface StorageRuntime {
  readonly fs: StorageFileSystem;
  readonly path: IsogitPath;
  flush?(): Promise<void>;
}

function createStorageFileSystem(runtime: IsogitRuntime): StorageFileSystem {
  const stat = async (filepath: string) =>
    fileStat(await callFs(runtime.fs, "stat", [filepath]), "stat");
  const lstat = async (filepath: string) =>
    fileStat(await callFs(runtime.fs, "lstat", [filepath]), "lstat");

  async function mkdir(
    filepath: string,
    options?: { recursive?: boolean },
  ): Promise<void> {
    if (options?.recursive !== true) {
      await callFs(runtime.fs, "mkdir", [filepath]);
      return;
    }

    try {
      await callFs(runtime.fs, "mkdir", [filepath]);
      return;
    } catch (cause) {
      if (hasCode(cause) && cause.code === "EEXIST") {
        if ((await stat(filepath)).isDirectory()) return;
        throw cause;
      }
      if (!hasCode(cause) || cause.code !== "ENOENT") throw cause;
    }

    const resolved = runtime.path.resolve(filepath);
    const parent = runtime.path.resolve(runtime.path.join(resolved, ".."));
    if (parent === resolved) {
      throw new Error(`Cannot create parent directory for ${filepath}`);
    }
    await mkdir(parent, { recursive: true });
    try {
      await callFs(runtime.fs, "mkdir", [filepath]);
    } catch (cause) {
      if (!hasCode(cause) || cause.code !== "EEXIST") throw cause;
      if (!(await stat(filepath)).isDirectory()) throw cause;
    }
  }

  async function remove(
    filepath: string,
    options: RemoveOptions = {},
  ): Promise<void> {
    let entry: FileSystemStat;
    try {
      entry = await lstat(filepath);
    } catch (cause) {
      if (options.force === true && hasCode(cause) && cause.code === "ENOENT") {
        return;
      }
      throw cause;
    }

    if (!entry.isDirectory()) {
      await callFs(runtime.fs, "unlink", [filepath]);
      return;
    }

    if (options.recursive === true) {
      const entries = directoryEntries(
        await callFs(runtime.fs, "readdir", [filepath]),
      );
      for (const name of entries) {
        await remove(runtime.path.join(filepath, name), {
          recursive: true,
          ...(options.force === undefined ? {} : { force: options.force }),
        });
      }
    }
    await callFs(runtime.fs, "rmdir", [filepath]);
  }

  const readFile = async (filepath: string): Promise<Uint8Array> => {
    const value = await callFs(runtime.fs, "readFile", [filepath]);
    if (value instanceof Uint8Array) {
      return value;
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    throw new TypeError("Filesystem readFile returned a non-binary value");
  };

  return {
    git: guardIndexReads(runtime.fs),
    access: async (filepath) => {
      await lstat(filepath);
    },
    readFile,
    readTextFile: async (filepath) => decodeUTF8(await readFile(filepath)),
    writeFile: async (filepath, data, options) => {
      const args =
        options === undefined ? [filepath, data] : [filepath, data, options];
      await callFs(runtime.fs, "writeFile", args);
    },
    readdir: async (filepath) =>
      directoryEntries(await callFs(runtime.fs, "readdir", [filepath])),
    mkdir,
    stat,
    lstat,
    remove,
    rename: (oldPath, newPath) => runtime.rename(oldPath, newPath),
  };
}

export function normalizeRuntime(runtime: IsogitRuntime): StorageRuntime {
  return {
    fs: createStorageFileSystem(runtime),
    path: runtime.path,
    ...(runtime.flush === undefined
      ? {}
      : { flush: () => runtime.flush?.() ?? Promise.resolve() }),
  };
}

export async function flushRuntime(runtime: StorageRuntime): Promise<void> {
  await runtime.flush?.();
}
