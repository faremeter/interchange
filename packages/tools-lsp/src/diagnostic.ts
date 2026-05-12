import type { Diagnostic } from "vscode-languageserver-types";

const MAX_PER_FILE = 20;

const SEVERITY = ["", "ERROR", "WARN", "INFO", "HINT"] as const;

export function pretty(d: Diagnostic): string {
  const severity = SEVERITY[d.severity ?? 1];
  const line = d.range.start.line + 1;
  const col = d.range.start.character + 1;
  return `${severity} [${line}:${col}] ${d.message}`;
}

// Output is XML-tagged for structured consumption by LLM tool responses.
export function report(
  file: string,
  issues: Diagnostic[],
  minSeverity = 1,
): string {
  const filtered = issues.filter((i) => (i.severity ?? 1) <= minSeverity);
  if (filtered.length === 0) return "";
  const limited = filtered.slice(0, MAX_PER_FILE);
  const more = filtered.length - MAX_PER_FILE;
  const suffix = more > 0 ? `\n... and ${more} more` : "";
  return `<diagnostics file="${file}">\n${limited.map(pretty).join("\n")}${suffix}\n</diagnostics>`;
}
