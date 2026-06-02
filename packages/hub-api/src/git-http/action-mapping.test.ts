import { describe, test, expect } from "bun:test";
import {
  httpToRepoAction,
  repoActionToGrantVerb,
  RepoActionAliases,
  expandRepoActionAlias,
} from "./action-mapping";
import type { RepoAction } from "@intx/hub-sessions";

describe("httpToRepoAction", () => {
  test("GET /info/refs?service=git-upload-pack maps to resolveRef", () => {
    expect(
      httpToRepoAction({
        method: "GET",
        path: "/info/refs",
        query: { service: "git-upload-pack" },
      }),
    ).toBe("resolveRef");
  });

  test("GET /info/refs?service=git-receive-pack maps to resolveRef", () => {
    expect(
      httpToRepoAction({
        method: "GET",
        path: "/info/refs",
        query: { service: "git-receive-pack" },
      }),
    ).toBe("resolveRef");
  });

  test("POST /git-upload-pack maps to createPack", () => {
    expect(
      httpToRepoAction({
        method: "POST",
        path: "/git-upload-pack",
        query: {},
      }),
    ).toBe("createPack");
  });

  test("POST /git-receive-pack maps to receivePack", () => {
    expect(
      httpToRepoAction({
        method: "POST",
        path: "/git-receive-pack",
        query: {},
      }),
    ).toBe("receivePack");
  });

  test("matches against trailing smart-HTTP suffix regardless of mount prefix", () => {
    expect(
      httpToRepoAction({
        method: "POST",
        path: "/tenants/t1/assets/repo-id/git-upload-pack",
        query: {},
      }),
    ).toBe("createPack");
    expect(
      httpToRepoAction({
        method: "POST",
        path: "/tenants/t1/agents/agt_xyz/state/git-receive-pack",
        query: {},
      }),
    ).toBe("receivePack");
    expect(
      httpToRepoAction({
        method: "GET",
        path: "/tenants/t1/assets/repo-id/info/refs",
        query: { service: "git-upload-pack" },
      }),
    ).toBe("resolveRef");
  });

  test("unrecognised shape throws", () => {
    expect(() =>
      httpToRepoAction({
        method: "POST",
        path: "/something/unrelated",
        query: {},
      }),
    ).toThrow(/unrecognised git smart-HTTP request/i);
  });

  test("missing service query on /info/refs throws", () => {
    expect(() =>
      httpToRepoAction({
        method: "GET",
        path: "/info/refs",
        query: {},
      }),
    ).toThrow(/service/i);
  });

  test("unknown service value throws", () => {
    expect(() =>
      httpToRepoAction({
        method: "GET",
        path: "/info/refs",
        query: { service: "git-upload-archive" },
      }),
    ).toThrow(/service/i);
  });
});

describe("repoActionToGrantVerb", () => {
  const expected: [RepoAction, string][] = [
    ["init", "create"],
    ["writeTree", "write"],
    ["receivePack", "write"],
    ["createPack", "read"],
    ["resolveRef", "read"],
  ];

  for (const [action, verb] of expected) {
    test(`${action} -> ${verb}`, () => {
      expect(repoActionToGrantVerb(action)).toBe(verb);
    });
  }

  test("every RepoAction has a defined grant verb (no ambiguity, no undefined)", () => {
    const allActions: RepoAction[] = [
      "init",
      "writeTree",
      "receivePack",
      "createPack",
      "resolveRef",
    ];
    for (const a of allActions) {
      const v = repoActionToGrantVerb(a);
      expect(typeof v).toBe("string");
      expect(v.length).toBeGreaterThan(0);
    }
  });
});

describe("RepoActionAliases", () => {
  test("can_read expands to createPack + resolveRef", () => {
    expect(RepoActionAliases.can_read).toEqual(["createPack", "resolveRef"]);
  });

  test("can_push expands to receivePack", () => {
    expect(RepoActionAliases.can_push).toEqual(["receivePack"]);
  });

  test("expandRepoActionAlias resolves known aliases", () => {
    expect(expandRepoActionAlias("can_read")).toEqual([
      "createPack",
      "resolveRef",
    ]);
    expect(expandRepoActionAlias("can_push")).toEqual(["receivePack"]);
  });

  test("expandRepoActionAlias passes through bare RepoAction strings", () => {
    expect(expandRepoActionAlias("createPack")).toEqual(["createPack"]);
    expect(expandRepoActionAlias("receivePack")).toEqual(["receivePack"]);
    expect(expandRepoActionAlias("resolveRef")).toEqual(["resolveRef"]);
    expect(expandRepoActionAlias("writeTree")).toEqual(["writeTree"]);
    expect(expandRepoActionAlias("init")).toEqual(["init"]);
  });

  test("expandRepoActionAlias rejects unknown strings", () => {
    expect(() => expandRepoActionAlias("nope")).toThrow(/unknown/i);
  });
});
