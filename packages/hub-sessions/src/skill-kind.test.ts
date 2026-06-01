import { describe, test, expect } from "bun:test";
import {
  skillKindHandler,
  skillFrontmatterSchema,
  getSkillIndex,
  type SkillIndexEntry,
} from "./skill-kind";
import type { RepoId } from "./repo-store";
import { type } from "arktype";

const REF = "refs/heads/main";

function makeReadBlob(
  files: Record<string, string>,
): (path: string) => Promise<Uint8Array> {
  return async (path) => {
    const body = files[path];
    if (body === undefined) {
      throw new Error(`readBlob: ${path} not found`);
    }
    return new TextEncoder().encode(body);
  };
}

function skillMd(frontmatter: Record<string, unknown>, body = ""): string {
  const lines = ["---"];
  for (const [k, v] of Object.entries(frontmatter)) {
    if (typeof v === "string") {
      lines.push(`${k}: ${JSON.stringify(v)}`);
    } else {
      lines.push(`${k}: ${JSON.stringify(v)}`);
    }
  }
  lines.push("---");
  if (body.length > 0) {
    lines.push(body);
  }
  return lines.join("\n");
}

function uniqueRepoId(prefix: string): RepoId {
  const id = `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
  return { kind: "skill", id };
}

describe("skillFrontmatterSchema", () => {
  test("accepts valid frontmatter", () => {
    const result = skillFrontmatterSchema({
      name: "good-skill",
      description: "Describes what this skill does.",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects non-kebab name", () => {
    const result = skillFrontmatterSchema({
      name: "BadName",
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects name longer than 64 characters", () => {
    const result = skillFrontmatterSchema({
      name: "a".repeat(65),
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects forbidden name 'anthropic'", () => {
    const result = skillFrontmatterSchema({
      name: "anthropic",
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects forbidden name 'claude'", () => {
    const result = skillFrontmatterSchema({
      name: "claude",
      description: "ok",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects empty description", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects description longer than 1024 characters", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "a".repeat(1025),
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects description containing XML tags", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "this has <tag>content</tag>",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts and preserves optional Claude Code superset fields", () => {
    const result = skillFrontmatterSchema({
      name: "ok",
      description: "ok",
      when_to_use: "when greeting",
      "allowed-tools": ["Read", "Edit"],
      paths: ["src/"],
      model: "opus",
    });
    expect(result instanceof type.errors).toBe(false);
  });
});

describe("skillKindHandler.validatePush", () => {
  test("accepts a valid single-skill asset and populates the index after onRefUpdated", async () => {
    const repoId = uniqueRepoId("single");
    const files = {
      "greet/SKILL.md": skillMd({
        name: "greet",
        description: "Greets the user.",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "deadbeef",
    });

    const entries: SkillIndexEntry[] = getSkillIndex(repoId.id, REF);
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    if (entry === undefined) throw new Error("unreachable");
    expect(entry.name).toBe("greet");
    expect(entry.description).toBe("Greets the user.");
    expect(entry.workspaceSubpath).toBe("greet/");
    expect(entry.frontmatter.name).toBe("greet");
  });

  test("accepts a multi-skill asset and populates one index entry per skill", async () => {
    const repoId = uniqueRepoId("multi");
    const files = {
      "greet/SKILL.md": skillMd({
        name: "greet",
        description: "Greets the user.",
      }),
      "farewell/SKILL.md": skillMd({
        name: "farewell",
        description: "Says goodbye.",
        when_to_use: "at end of session",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet", "farewell"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "cafebabe",
    });

    const entries = getSkillIndex(repoId.id, REF);
    expect(entries).toHaveLength(2);
    const names = entries.map((e) => e.name).sort();
    expect(names).toEqual(["farewell", "greet"]);

    const farewell = entries.find((e) => e.name === "farewell");
    if (farewell === undefined) throw new Error("missing entry");
    expect(farewell.frontmatter.when_to_use).toBe("at end of session");
  });

  test("accepts an empty asset (no skill subdirectories)", async () => {
    const repoId = uniqueRepoId("empty");
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [],
      readBlob: makeReadBlob({}),
    });
    expect(result.ok).toBe(true);

    await skillKindHandler.onRefUpdated({
      repoId,
      ref: REF,
      oldSha: null,
      newSha: "0000",
    });

    expect(getSkillIndex(repoId.id, REF)).toEqual([]);
  });

  test("rejects when SKILL.md is missing from a skill subdirectory", async () => {
    const repoId = uniqueRepoId("missing");
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet"],
      readBlob: makeReadBlob({}),
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/missing SKILL\.md/);
  });

  test("rejects when frontmatter.name does not match the directory name", async () => {
    const repoId = uniqueRepoId("mismatch");
    const files = {
      "greet/SKILL.md": skillMd({
        name: "different",
        description: "Mismatch.",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["greet"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/does not match directory name/);
  });

  test("rejects when the frontmatter name regex fails", async () => {
    const repoId = uniqueRepoId("badname");
    const files = {
      "BadName/SKILL.md": skillMd({
        name: "BadName",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["BadName"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("unreachable");
    expect(result.reason).toMatch(/frontmatter is invalid/);
  });

  test("rejects when the frontmatter name exceeds 64 characters", async () => {
    const longName = "a".repeat(65);
    const repoId = uniqueRepoId("longname");
    const files = {
      [`${longName}/SKILL.md`]: skillMd({
        name: longName,
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: [longName],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when frontmatter name is 'anthropic'", async () => {
    const repoId = uniqueRepoId("anthropic");
    const files = {
      "anthropic/SKILL.md": skillMd({
        name: "anthropic",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["anthropic"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when frontmatter name is 'claude'", async () => {
    const repoId = uniqueRepoId("claude");
    const files = {
      "claude/SKILL.md": skillMd({
        name: "claude",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["claude"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when description is empty", async () => {
    const repoId = uniqueRepoId("emptydesc");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "ok",
        description: "",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when description exceeds 1024 characters", async () => {
    const repoId = uniqueRepoId("longdesc");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "ok",
        description: "a".repeat(1025),
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
  });

  test("rejects when description contains an XML tag", async () => {
    const repoId = uniqueRepoId("xmldesc");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "ok",
        description: "has a <tag>",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);
  });

  test("validatePush rejection does not populate the skill index after a subsequent onRefUpdated would be skipped", async () => {
    const repoId = uniqueRepoId("rejected");
    const files = {
      "ok/SKILL.md": skillMd({
        name: "wrongname",
        description: "ok",
      }),
    };
    const result = await skillKindHandler.validatePush({
      repoId,
      ref: REF,
      topLevelTreePaths: ["ok"],
      readBlob: makeReadBlob(files),
    });
    expect(result.ok).toBe(false);

    // The substrate does not call onRefUpdated when validatePush
    // rejects, so the live index stays empty for this (assetId, ref).
    expect(getSkillIndex(repoId.id, REF)).toEqual([]);
  });
});

describe("skillKindHandler metadata", () => {
  test("declares the skill kind and assets/skill directory prefix", () => {
    expect(skillKindHandler.kind).toBe("skill");
    expect(skillKindHandler.directoryPrefix).toBe("assets/skill");
  });
});
