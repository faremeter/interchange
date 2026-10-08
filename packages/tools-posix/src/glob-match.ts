const SKIP_SEGMENTS = new Set(["node_modules", ".git"]);

/** True if the relative path passes through a skipped directory segment. */
export function shouldSkip(relativePath: string): boolean {
  const segments = relativePath.split("/");
  return segments.some((s) => SKIP_SEGMENTS.has(s));
}

/**
 * Convert a glob to a RegExp over relative paths. `**` crosses segments,
 * `*`/`?` stay within one; braces are rejected.
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
      i += 2;
      if (pattern[i] === "/") {
        i++; // consume trailing slash after **
        regex += "(?:.+/)?";
      } else {
        // trailing `**`: match the rest
        regex += ".*";
      }
    } else if (c === "*") {
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

/** Whether a relative path matches a glob pattern. */
export function matchGlob(pattern: string, filePath: string): boolean {
  return globToRegex(pattern).test(filePath);
}
