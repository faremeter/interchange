/**
 * Simple-glob matcher for `refPattern` validation at the mint endpoint
 * and ref-advertisement filtering. Grammar:
 *
 * - literals match themselves
 * - `*` matches any run of characters within one `/`-delimited segment
 * - `**` matches any run of characters including `/` (may match zero)
 */

type Token =
  | { kind: "literal"; text: string }
  | { kind: "star" }
  | { kind: "doublestar" };

function tokenize(pattern: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  let literal = "";
  while (i < pattern.length) {
    if (pattern[i] === "*") {
      if (literal.length > 0) {
        out.push({ kind: "literal", text: literal });
        literal = "";
      }
      if (i + 1 < pattern.length && pattern[i + 1] === "*") {
        out.push({ kind: "doublestar" });
        i += 2;
      } else {
        out.push({ kind: "star" });
        i += 1;
      }
    } else {
      literal += pattern[i];
      i += 1;
    }
  }
  if (literal.length > 0) {
    out.push({ kind: "literal", text: literal });
  }
  return out;
}

function matchTokens(
  tokens: Token[],
  ti: number,
  input: string,
  ii: number,
): boolean {
  if (ti === tokens.length) {
    return ii === input.length;
  }
  const tok = tokens[ti];
  if (tok === undefined) {
    throw new Error(`glob: token index ${ti} out of range`);
  }
  if (tok.kind === "literal") {
    const text = tok.text;
    if (ii + text.length > input.length) return false;
    for (let k = 0; k < text.length; k++) {
      if (input[ii + k] !== text[k]) return false;
    }
    return matchTokens(tokens, ti + 1, input, ii + text.length);
  }
  if (tok.kind === "star") {
    for (let j = ii; j <= input.length; j++) {
      if (j > ii && input[j - 1] === "/") break;
      if (matchTokens(tokens, ti + 1, input, j)) return true;
      if (j === input.length) break;
    }
    return matchTokens(tokens, ti + 1, input, ii);
  }
  // doublestar
  for (let j = ii; j <= input.length; j++) {
    if (matchTokens(tokens, ti + 1, input, j)) return true;
  }
  return false;
}

export const glob = {
  /** Match `input` against `pattern` under the grammar above. */
  match(pattern: string, input: string): boolean {
    const tokens = tokenize(pattern);
    return matchTokens(tokens, 0, input, 0);
  },
};
