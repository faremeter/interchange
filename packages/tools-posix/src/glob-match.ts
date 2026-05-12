const SKIP_SEGMENTS = new Set(["node_modules", ".git"]);

/** Check if a relative path contains a directory segment that should be skipped. */
export function shouldSkip(relativePath: string): boolean {
  const segments = relativePath.split("/");
  return segments.some((s) => SKIP_SEGMENTS.has(s));
}

/**
 * Convert a glob pattern to a RegExp that matches against relative paths.
 *
 * Supports:
 *   `**`  -- zero or more directory segments
 *   `*`   -- zero or more characters within a single segment (not `/`)
 *   `?`   -- exactly one character (not `/`)
 *
 * Does not support brace expansion (`{a,b}`).
 */
export function globToRegex(pattern: string): RegExp {
  if (/\{[^}]+\}/.test(pattern)) {
    throw new Error(
      `brace expansion is not supported: "${pattern}". Use separate searches or a ** pattern instead.`,
    );
  }

  let regex = "";
  let i = 0;

  while (i < pattern.length) {
    const c = pattern.charAt(i);

    if (c === "*" && pattern[i + 1] === "*") {
      // `**` -- match zero or more path segments
      i += 2;
      if (pattern[i] === "/") {
        i++; // consume trailing slash after **
        regex += "(?:.+/)?";
      } else {
        // `**` at end of pattern -- match everything remaining
        regex += ".*";
      }
    } else if (c === "*") {
      // `*` -- match within a single segment
      regex += "[^/]*";
      i++;
    } else if (c === "?") {
      regex += "[^/]";
      i++;
    } else if (".+^${}()|[]\\".includes(c)) {
      regex += "\\" + c;
      i++;
    } else {
      regex += c;
      i++;
    }
  }

  return new RegExp("^" + regex + "$");
}

/** Test whether a relative path matches a glob pattern. */
export function matchGlob(pattern: string, filePath: string): boolean {
  return globToRegex(pattern).test(filePath);
}
