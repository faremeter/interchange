import { describe, test, expect } from "bun:test";
import {
  buildAvailableSkillsStanza,
  type AvailableSkillEntry,
} from "./available-skills-stanza";

describe("buildAvailableSkillsStanza", () => {
  test("returns empty string when no entries are provided", () => {
    expect(buildAvailableSkillsStanza([])).toBe("");
  });

  test("renders a single skill in the expected shape", () => {
    const entries: AvailableSkillEntry[] = [
      {
        qualifiedName: "greeter/wave",
        description: "Waves at the user.",
        workspacePath: "workspace/skills/greeter/wave/",
      },
    ];
    expect(buildAvailableSkillsStanza(entries)).toBe(
      [
        "<available_skills>",
        "  <skill>",
        "    <name>greeter/wave</name>",
        "    <description>Waves at the user.</description>",
        "    <path>workspace/skills/greeter/wave/</path>",
        "  </skill>",
        "</available_skills>",
      ].join("\n"),
    );
  });

  test("preserves declared order across multiple skills in one asset", () => {
    const entries: AvailableSkillEntry[] = [
      {
        qualifiedName: "tools/alpha",
        description: "First.",
        workspacePath: "workspace/skills/tools/alpha/",
      },
      {
        qualifiedName: "tools/beta",
        description: "Second.",
        workspacePath: "workspace/skills/tools/beta/",
      },
    ];
    const out = buildAvailableSkillsStanza(entries);
    const alphaIdx = out.indexOf("tools/alpha");
    const betaIdx = out.indexOf("tools/beta");
    expect(alphaIdx).toBeGreaterThan(-1);
    expect(betaIdx).toBeGreaterThan(-1);
    expect(alphaIdx).toBeLessThan(betaIdx);
  });

  test("qualifies same-named skills across distinct assets", () => {
    const entries: AvailableSkillEntry[] = [
      {
        qualifiedName: "ops/deploy",
        description: "Ops deploy.",
        workspacePath: "workspace/skills/ops/deploy/",
      },
      {
        qualifiedName: "web/deploy",
        description: "Web deploy.",
        workspacePath: "workspace/skills/web/deploy/",
      },
    ];
    const out = buildAvailableSkillsStanza(entries);
    expect(out).toContain("<name>ops/deploy</name>");
    expect(out).toContain("<name>web/deploy</name>");
  });

  test("XML-escapes ampersands and angle brackets in field values", () => {
    const entries: AvailableSkillEntry[] = [
      {
        qualifiedName: "tools/a&b",
        description: "Handles A & B (with <stuff> too).",
        workspacePath: "workspace/skills/tools/a&b/",
      },
    ];
    const out = buildAvailableSkillsStanza(entries);
    expect(out).toContain("<name>tools/a&amp;b</name>");
    expect(out).toContain(
      "<description>Handles A &amp; B (with &lt;stuff&gt; too).</description>",
    );
    expect(out).toContain("<path>workspace/skills/tools/a&amp;b/</path>");
  });
});
