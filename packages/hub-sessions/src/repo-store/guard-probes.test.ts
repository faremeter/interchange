// Recursion coverage for the delete_type_mismatch guards in assembleTree.
// delta-adversarial.test.ts exercises the two mismatch shapes only at the
// tree root; these two cover paths it misses:
//
//   1. A NESTED no-slash delete of a directory -- proves the guard is
//      carried through assembleTree's recursion and the thrown path
//      string reflects the full nested prefix, not just the leaf name.
//   2. A single-trailing-slash delete naming a base BLOB directly
//      ("foo/"), as opposed to the two-level descent into a blob
//      ("foo/bar/"). This is the minimal trailing-slash-over-blob shape
//      and hits the child-extraction branch at the root.

import { test, expect, beforeAll, afterAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore } from "./store";
import type { AuthorizeFn, Principal, RepoId } from "./types";

const tempDirs: string[] = [];
let signingKey: KeyPair;
const allowAll: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };
const REF = "refs/heads/events";
const repoId: RepoId = { kind: "agent-state", id: "subject" };

beforeAll(async () => {
  signingKey = await generateKeyPair();
});
afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises
      .rm(d, { recursive: true, force: true })
      .catch(() => undefined);
  }
});

function makeStore(dataDir: string) {
  return createRepoStore({
    dataDir,
    signingKey,
    handlers: {
      "agent-state": {
        kind: "agent-state",
        directoryPrefix: "repos-under-test",
        validatePush: () => ({ ok: true }),
        onRefUpdated: () => undefined,
      },
    },
    authorize: allowAll,
  });
}
async function freshStore(label: string) {
  const dataDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), label));
  tempDirs.push(dataDir);
  return makeStore(dataDir);
}
async function seed(
  store: ReturnType<typeof makeStore>,
  base: Record<string, string>,
) {
  await store.initRepo(repoId);
  await store.writeTree(principal, repoId, REF, {
    files: base,
    message: "seed",
  });
}

test("nested no-slash delete of a directory is rejected with the full path", async () => {
  const base = {
    "top/sub/a.json": `{"a":1}`,
    "top/keep.json": `{"k":1}`,
  };
  const store = await freshStore("guard-nested-deldir-");
  await seed(store, base);
  let caught: unknown;
  await store
    .writeTreeDelta(principal, repoId, REF, {
      changedPathPrefixes: new Set(["top/"]),
      message: "delete-nested-dir-as-file",
      computeDelta: async () => ({ puts: {}, deletes: ["top/sub"] }),
    })
    .catch((e: unknown) => (caught = e));
  expect(String(caught)).toContain("delete_type_mismatch");
  expect(String(caught)).toContain("top/sub");
});

test("single trailing-slash delete naming a base blob directly is rejected", async () => {
  const base = { foo: `{"is":"blob"}`, "keep.json": `{"keep":true}` };
  const store = await freshStore("guard-trailblob-");
  await seed(store, base);
  let caught: unknown;
  await store
    .writeTreeDelta(principal, repoId, REF, {
      changedPathPrefixes: new Set(["foo/"]),
      message: "trailing-del-blob",
      computeDelta: async () => ({ puts: {}, deletes: ["foo/"] }),
    })
    .catch((e: unknown) => (caught = e));
  expect(String(caught)).toContain("delete_type_mismatch");
});
