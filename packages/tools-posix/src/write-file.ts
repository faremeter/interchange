import { writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

export type WriteFileArgs = {
  path: string;
  content: string;
};

export async function runWriteFile(
  args: WriteFileArgs,
  signal: AbortSignal,
): Promise<string> {
  signal.throwIfAborted();

  const dir = dirname(args.path);
  try {
    await mkdir(dir, { recursive: true });
  } catch (err) {
    if (err instanceof Error && "code" in err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === "EACCES") {
        throw new Error(`permission denied creating directory: ${dir}`, {
          cause: err,
        });
      }
    }
    throw err;
  }

  signal.throwIfAborted();

  const bytes = Buffer.byteLength(args.content, "utf8");

  try {
    await writeFile(args.path, args.content, { encoding: "utf8", signal });
  } catch (err) {
    if (err instanceof Error && "code" in err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === "EACCES") {
        throw new Error(`permission denied: ${args.path}`, { cause: err });
      }
      if (code === "EISDIR") {
        throw new Error(`path is a directory: ${args.path}`, { cause: err });
      }
    }
    throw err;
  }

  return `wrote ${bytes} bytes to ${args.path}`;
}
