import { readFile } from "node:fs/promises";

import type { BlobReader } from "@intx/types/runtime";
import { hasCode } from "@intx/types";

export type ReadFileArgs = {
  path: string;
  offset?: number;
  limit?: number;
};

const TOOL_OUTPUT_URI_PREFIX = "tool-output:";

export async function runReadFile(
  args: ReadFileArgs,
  signal: AbortSignal,
  blobReader?: BlobReader,
): Promise<string> {
  signal.throwIfAborted();

  let content: string;

  if (args.path.startsWith(TOOL_OUTPUT_URI_PREFIX)) {
    if (blobReader === undefined) {
      throw new Error(
        `cannot read ${args.path}: no blob reader is configured for this tool runner`,
      );
    }
    const bytes = await blobReader.read(args.path);
    if (bytes.includes(0)) {
      throw new Error(`refusing to read binary file: ${args.path}`);
    }
    content = new TextDecoder("utf-8").decode(bytes);
  } else {
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
