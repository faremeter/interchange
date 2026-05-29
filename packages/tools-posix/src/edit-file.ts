import { readFile, writeFile } from "node:fs/promises";

import { hasCode } from "@intx/types";

export type EditFileArgs = {
  path: string;
  old_string: string;
  new_string: string;
  replace_all?: boolean;
};

export async function runEditFile(
  args: EditFileArgs,
  signal: AbortSignal,
): Promise<string> {
  signal.throwIfAborted();

  if (args.old_string === "") {
    throw new Error("old_string must not be empty");
  }

  let content: string;
  try {
    const buf = await readFile(args.path, { signal });
    if (buf.includes(0)) {
      throw new Error(`refusing to edit binary file: ${args.path}`);
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

  const count = content.split(args.old_string).length - 1;

  if (count === 0) {
    throw new Error(`old_string not found in ${args.path}`);
  }

  if (count > 1 && !args.replace_all) {
    throw new Error(
      `old_string is not unique (${count} occurrences) in ${args.path}; use replace_all to replace all`,
    );
  }

  // Use split/join for both paths. String.replace() interprets $-patterns
  // ($&, $', $`) in the replacement string, which corrupts content.
  const newContent = content.split(args.old_string).join(args.new_string);

  signal.throwIfAborted();

  try {
    await writeFile(args.path, newContent, { encoding: "utf8", signal });
  } catch (err) {
    if (hasCode(err)) {
      if (err.code === "EACCES") {
        throw new Error(`permission denied: ${args.path}`, { cause: err });
      }
    }
    throw err;
  }

  return `replaced ${count} occurrence(s) in ${args.path}`;
}
