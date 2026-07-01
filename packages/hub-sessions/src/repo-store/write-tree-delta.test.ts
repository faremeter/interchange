// OID-equivalence guard for the delta write path (writeTreeDelta) added
// alongside the claim-check narrowing. A delta ({puts, deletes} carried
// against the parent tree, reusing untouched entries by oid) MUST commit
// the byte-identical tree that a full-replace writeTree of the same
// logical final state produces. SHA-1 tree hashing is deterministic
// given identical content and structure, so equal commit-tree oids prove
// the delta path never diverges from canonical git for the claim-check
// move shapes: enqueue-add, dequeue-move, markConsumed-move,
// empty->first-entry, and prune-to-empty.

import { test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import { createRepoStore } from "./store";
import type { AuthorizeFn, Principal, RepoId } from "./types";

const tempDirs: string[] = [];
let signingKey: KeyPair;

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

const allowAll: AuthorizeFn = () => ({ allowed: true });
const principal: Principal = { kind: "test" };
const REF = "refs/heads/events";
const SCOPE = "addresses/a/";

// Permissive store: the delta path is what is under test, so the handler
// accepts every prospective tree.
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

const repoId: RepoId = { kind: "agent-state", id: "subject" };

async function commitTreeOid(
  store: ReturnType<typeof makeStore>,
): Promise<string> {
  const dir = store.getRepoDir(repoId);
  const sha = await git.resolveRef({ fs, dir, ref: REF });
  const { commit } = await git.readCommit({ fs, dir, oid: sha });
  return commit.tree;
}

// Apply a delta to a flat path->content base, mirroring assembleTree's
// delete semantics (trailing-slash prefix delete vs exact file delete).
function applyDelta(
  base: Record<string, string>,
  puts: Record<string, string>,
  deletes: readonly string[],
): Record<string, string> {
  const exactDeletes = new Set(deletes.filter((d) => !d.endsWith("/")));
  const prefixDeletes = deletes.filter((d) => d.endsWith("/"));
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(base)) {
    if (exactDeletes.has(k)) continue;
    if (prefixDeletes.some((d) => k.startsWith(d))) continue;
    out[k] = v;
  }
  for (const [k, v] of Object.entries(puts)) out[k] = v;
  return out;
}

// Seed a repo's SCOPE subtree from `base`, then produce the final tree
// two ways and assert byte-identical commit trees:
//   delta       — writeTreeDelta({ puts, deletes })
//   full-replace — writeTree clearing SCOPE and re-supplying the whole
//                  post-delta SCOPE content (the pre-narrowing shape)
async function assertDeltaMatchesFullReplace(
  label: string,
  base: Record<string, string>,
  puts: Record<string, string>,
  deletes: readonly string[],
) {
  const finalState = applyDelta(base, puts, deletes);
  const finalScopeFiles: Record<string, string> = {};
  for (const [k, v] of Object.entries(finalState)) {
    if (k.startsWith(SCOPE)) finalScopeFiles[k] = v;
  }

  const deltaStore = await freshStore(`wtd-delta-${label}-`);
  await deltaStore.initRepo(repoId);
  if (Object.keys(base).length > 0) {
    await deltaStore.writeTree(principal, repoId, REF, {
      files: base,
      message: "seed",
    });
  }
  await deltaStore.writeTreeDelta(principal, repoId, REF, {
    changedPathPrefixes: new Set([SCOPE]),
    message: "delta",
    computeDelta: async () => ({ puts, deletes }),
  });
  const deltaTree = await commitTreeOid(deltaStore);

  const fullStore = await freshStore(`wtd-full-${label}-`);
  await fullStore.initRepo(repoId);
  if (Object.keys(base).length > 0) {
    await fullStore.writeTree(principal, repoId, REF, {
      files: base,
      message: "seed",
    });
  }
  await fullStore.writeTree(principal, repoId, REF, {
    files: finalScopeFiles,
    clearPrefix: SCOPE,
    message: "full-replace",
  });
  const fullTree = await commitTreeOid(fullStore);

  expect(deltaTree).toBe(fullTree);
}

const consumedBase = {
  [`${SCOPE}consumed/m1.json`]: `{"receivedAt":1}`,
  [`${SCOPE}consumed/m2.json`]: `{"receivedAt":2}`,
  [`${SCOPE}watermark.json`]: `{"watermark":0}`,
};

test("delta enqueue-add matches full-replace", async () => {
  await assertDeltaMatchesFullReplace(
    "enqueue",
    consumedBase,
    { [`${SCOPE}inbox/10-m3.json`]: `{"messageId":"m3"}` },
    [],
  );
});

test("delta dequeue-move matches full-replace", async () => {
  await assertDeltaMatchesFullReplace(
    "dequeue",
    { ...consumedBase, [`${SCOPE}inbox/10-m3.json`]: `{"messageId":"m3"}` },
    { [`${SCOPE}processing/10-m3.json`]: `{"messageId":"m3"}` },
    [`${SCOPE}inbox/10-m3.json`],
  );
});

test("delta markConsumed-move matches full-replace", async () => {
  await assertDeltaMatchesFullReplace(
    "consume",
    {
      ...consumedBase,
      [`${SCOPE}processing/10-m3.json`]: `{"messageId":"m3"}`,
    },
    {
      [`${SCOPE}consumed/m3.json`]: `{"receivedAt":10}`,
      [`${SCOPE}watermark.json`]: `{"watermark":5}`,
    },
    [`${SCOPE}processing/10-m3.json`],
  );
});

test("delta empty->first-entry matches full-replace", async () => {
  await assertDeltaMatchesFullReplace(
    "first",
    {},
    { [`${SCOPE}inbox/10-m1.json`]: `{"messageId":"m1"}` },
    [],
  );
});

test("delta prune-to-empty matches full-replace", async () => {
  await assertDeltaMatchesFullReplace(
    "prune",
    consumedBase,
    { [`${SCOPE}watermark.json`]: `{"watermark":9}` },
    [`${SCOPE}consumed/m1.json`, `${SCOPE}consumed/m2.json`],
  );
});
