import { readFile } from "node:fs/promises";

import { hasCode } from "./errors";

export type ReadFileArgs = {
  path: string;
  offset?: number;
  limit?: number;
};

export async function runReadFile(
  args: ReadFileArgs,
  signal: AbortSignal,
): Promise<string> {
  signal.throwIfAborted();

  let content: string;
  try {
    const buf = await readFile(args.path, { signal });
    if (buf.includes(0)) {
      throw new Error(`refusing to read binary file: ${args.path}`);
    }
    content = buf.toString("utf8");
  } catch (err) {
    if (hasCode(err)) {
      if (err.code === "ENOENT") {
        throw new Error(`file not found: ${args.path}`, { cause: err });
      }
      if (err.code === "EACCES") {
        throw new Error(`permission denied: ${args.path}`, { cause: err });
      }
      if (err.code === "EISDIR") {
        throw new Error(`path is a directory: ${args.path}`, { cause: err });
      }
    }
    throw err;
  }

  const lines = content.split("\n");

  const offset = args.offset ?? 0;
  const limit = args.limit ?? lines.length;

  const slice = lines.slice(offset, offset + limit);

  const numbered = slice
    .map((line, i) => {
      const lineNum = offset + i + 1;
      return `${String(lineNum).padStart(6, " ")}\t${line}`;
    })
    .join("\n");

  return numbered;
}
