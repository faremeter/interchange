// Pure POSIX path helpers.
//
// Portable code cannot import `node:path`: a browser bundler stubs it or
// replaces it with a polyfill whose `resolve` calls `process.cwd()`
// unguarded. These helpers work on
// `/`-separated strings only and never consult a working directory: a relative
// path stays relative, and `resolve` roots it at `/`.

/**
 * Collapse `//`, drop `.` segments and resolve `..` segments. A path that
 * starts with `/` stays absolute, and `..` above the root is dropped. A
 * relative path keeps its leading `..` segments. A trailing `/` is dropped.
 * An empty relative result is `.`.
 */
export function normalize(filepath: string): string {
  const absolute = filepath.startsWith("/");
  const segments: string[] = [];
  for (const segment of filepath.split("/")) {
    if (segment === "" || segment === ".") {
      continue;
    }
    if (segment === "..") {
      if (segments.length > 0 && segments.at(-1) !== "..") {
        segments.pop();
      } else if (!absolute) {
        segments.push(segment);
      }
      continue;
    }
    segments.push(segment);
  }

  if (absolute) {
    return segments.length === 0 ? "/" : `/${segments.join("/")}`;
  }
  return segments.length === 0 ? "." : segments.join("/");
}

/** `filepath` as an absolute path, rooting a relative path at `/`. */
export function resolve(filepath: string): string {
  return normalize(filepath.startsWith("/") ? filepath : `/${filepath}`);
}

/** Join `parts` with `/` and normalize the result. */
export function join(...parts: string[]): string {
  if (parts.length === 0) {
    return ".";
  }
  return normalize(parts.join("/"));
}

/** The relative path from `from` to `to`, both resolved first. */
export function relative(from: string, to: string): string {
  const fromSegments = resolve(from).split("/").filter(Boolean);
  const toSegments = resolve(to).split("/").filter(Boolean);
  let shared = 0;
  while (
    shared < fromSegments.length &&
    shared < toSegments.length &&
    fromSegments[shared] === toSegments[shared]
  ) {
    shared += 1;
  }
  return [
    ...fromSegments.slice(shared).map(() => ".."),
    ...toSegments.slice(shared),
  ].join("/");
}
