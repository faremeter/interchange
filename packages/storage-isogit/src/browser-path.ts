import type { IsogitPath } from "./runtime";

function normalize(filepath: string, absolute: boolean): string {
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

function resolve(filepath: string): string {
  return normalize(filepath.startsWith("/") ? filepath : `/${filepath}`, true);
}

function join(...parts: string[]): string {
  if (parts.length === 0) {
    return ".";
  }
  const combined = parts.join("/");
  return normalize(combined, combined.startsWith("/"));
}

function relative(from: string, to: string): string {
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

export const browserPath: IsogitPath = { join, relative, resolve };
