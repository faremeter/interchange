export type AvailableSkillEntry = {
  /** Qualified skill identifier in the form `<asset.name>/<skill-name>`. */
  qualifiedName: string;
  /** SKILL.md frontmatter description, verbatim. */
  description: string;
  /** Workspace-relative path the agent's `read_file` should target,
   * shaped like `workspace/<mountPath>/<skill-name>/`. */
  workspacePath: string;
};

/**
 * Render the `<available_skills>` stanza appended to the agent's
 * system prompt. Returns the empty string when `entries` is empty —
 * an empty `<available_skills></available_skills>` wrapper would be
 * misleading noise for agents with no skills attached.
 *
 * Values are XML-escaped at the boundary. The skill kind handler
 * already rejects descriptions containing literal `<` or `>` so the
 * `&` escape is the practical case in production; the others are
 * defensive.
 */
export function buildAvailableSkillsStanza(
  entries: AvailableSkillEntry[],
): string {
  if (entries.length === 0) {
    return "";
  }
  const lines = ["<available_skills>"];
  for (const entry of entries) {
    lines.push("  <skill>");
    lines.push(`    <name>${escapeXml(entry.qualifiedName)}</name>`);
    lines.push(
      `    <description>${escapeXml(entry.description)}</description>`,
    );
    lines.push(`    <path>${escapeXml(entry.workspacePath)}</path>`);
    lines.push("  </skill>");
  }
  lines.push("</available_skills>");
  return lines.join("\n");
}

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}
