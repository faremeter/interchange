import { describe, test, expect, afterAll, beforeAll, spyOn } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto";
import { collectReachableObjects, runGC } from "@intx/storage-isogit/node";
import type { KeyPair } from "@intx/types/runtime";
import { waitUntil } from "@intx/types/testing";
import { createRepoStore } from "./store";
import { workflowAuthorize, workflowKindHandler } from "../workflow-kind";
import { RepoRemovedDuringReceiveError } from "./types";
import type {
  AuthorizeFn,
  KindHandler,
  NewlyTerminalRun,
  PackCommitResult,
  Principal,
  RepoAction,
  RepoId,
  ValidatePushResult,
} from "./types";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch((_e) => {
      /* best effort cleanup */
    });
  }
});

type RefUpdateRecord = {
  repoId: RepoId;
  ref: string;
  oldSha: string | null;
  newSha: string;
};

type TestHandler = KindHandler & {
  onRefUpdatedCalls: RefUpdateRecord[];
};

function createTestHandler(opts?: {
  allowTopLevelPaths?: (topLevelTreePaths: string[]) => boolean;
}): TestHandler {
  const onRefUpdatedCalls: RefUpdateRecord[] = [];
  const allowFn = opts?.allowTopLevelPaths;
  return {
    kind: "agent-state",
    directoryPrefix: "repos-under-test",
    validatePush({ topLevelTreePaths }): ValidatePushResult {
      // Ignore the readBlob argument: this fixture only ever needs path-level
      // checks. Real handlers that need blob contents (e.g. skillKindHandler)
      // exercise readBlob in their own dedicated tests.
      if (allowFn === undefined) {
        return { ok: true };
      }
      if (allowFn(topLevelTreePaths)) {
        return { ok: true };
      }
      return { ok: false, reason: "stub rejected push" };
    },
    onRefUpdated(args) {
      onRefUpdatedCalls.push(args);
    },
    onRefUpdatedCalls,
  };
}

const allowAll: AuthorizeFn = () => ({ allowed: true });

const principal: Principal = { kind: "test" };

const repoId: RepoId = { kind: "agent-state", id: "subject" };

const REF = "refs/heads/test";

async function readTreePaths(dir: string, treeOid: string): Promise<string[]> {
  const { tree } = await git.readTree({ fs, dir, oid: treeOid });
  return tree.map((e) => e.path);
}

type RefUpdatedEvent = {
  type: "ref.updated";
  ref: string;
  oldSha: string | null;
  newSha: string;
};

function isRefUpdatedEvent(value: unknown): value is RefUpdatedEvent {
  if (value === null || typeof value !== "object") return false;
  if (!("type" in value) || value.type !== "ref.updated") return false;
  if (!("ref" in value) || typeof value.ref !== "string") return false;
  if (!("newSha" in value) || typeof value.newSha !== "string") return false;
  if (!("oldSha" in value)) return false;
  if (value.oldSha !== null && typeof value.oldSha !== "string") return false;
  return true;
}

function asRefUpdated(event: unknown): RefUpdatedEvent {
  if (!isRefUpdatedEvent(event)) {
    throw new Error(`event is not RefUpdatedEvent: ${JSON.stringify(event)}`);
  }
  return event;
}

// The real `readCommit`, bound before any test spies on it, so a spy
// left behind by a test that timed out cannot make a later mock call
// itself. Each gated mock also matches only its own test's repository,
// so such a spy gates no later test.
const gitReadCommit = git.readCommit.bind(git);

// Runs every queued microtask, so code that continues with no I/O after
// an awaited step has run up to its next I/O.
function drainMicrotasks(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

// A one-shot gate: `reached` resolves when the gated step arrives, and
// the step waits until `release()`.
function gate(): {
  reached: Promise<void>;
  arrive: () => Promise<void>;
  release: () => void;
} {
  let markReached: () => void = () => undefined;
  const reached = new Promise<void>((resolve) => {
    markReached = resolve;
  });
  let release: () => void = () => undefined;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  return {
    reached,
    arrive: () => {
      markReached();
      return released;
    },
    release,
  };
}

describe("RepoStore", () => {
  describe("retained workflow ref namespace", () => {
    const definitionRepo: RepoId = { kind: "workflow", id: "definition" };
    const hub: Principal = { kind: "hub" };
    const content = {
      files: {
        "package.json": JSON.stringify({
          name: "definition",
          version: "1.0.0",
          interchange: { workflow: "./workflow.ts" },
        }),
        "workflow.ts": "export const workflow = {};",
      },
      message: "workflow source",
    };

    async function fixture() {
      const dataDir = await makeTempDir("repo-store-definition-namespace-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: { workflow: workflowKindHandler },
        authorize: workflowAuthorize,
      });
      const contributor = {
        kind: "user",
        principalId: "user-definition",
        tenantId: "tenant-definition",
        authz: {
          effect: "allow",
          resource: `asset:${definitionRepo.id}`,
          grantVerb: "write",
        },
        tokenClaims: {
          refPattern: "refs/**",
          actions: ["writeTree", "receivePack"],
          expiresAt: Number.MAX_SAFE_INTEGER,
        },
      };
      await store.writeTree(hub, definitionRepo, REF, content);
      const pack = await store.createPack(hub, definitionRepo, REF);
      return { store, contributor, pack };
    }

    for (const ref of [
      "refs/interchange",
      "refs/interchange/workflow-definitions",
      "refs/interchange/workflow-definitions/contributor",
      "refs/INTERCHANGE",
      "refs/Interchange/workflow-definitions/contributor",
    ]) {
      test.each(["writeTree", "receivePack"] as const)(
        `rejects contributor %s to ${ref} before it can block retention`,
        async (action) => {
          const { store, contributor, pack } = await fixture();
          expect(
            workflowAuthorize(contributor, definitionRepo, ref, action),
          ).toEqual({ allowed: true });
          const writing =
            action === "writeTree"
              ? store.writeTree(contributor, definitionRepo, ref, content)
              : store.receivePack(
                  contributor,
                  definitionRepo,
                  ref,
                  pack.pack,
                  pack.commitSha,
                  null,
                );
          await expect(writing).rejects.toThrow(
            "authorize_denied: refs/interchange refs are Hub-owned",
          );
          expect(await store.resolveRef(hub, definitionRepo, ref)).toBeNull();
          const retained = await store.createPack(hub, definitionRepo, REF, {
            commitSha: pack.commitSha,
            retain: true,
          });
          expect(retained.commitSha).toBe(pack.commitSha);
        },
      );
    }

    // APFS folds `ſ` to `s`, so `refſ/interchange` is the namespace on disk.
    for (const ref of [
      "refſ/interchange",
      "refſ/interchange/workflow-definitions/contributor",
      "refs/heads/ſ",
    ]) {
      test.each(["writeTree", "receivePack"] as const)(
        `rejects contributor %s to non-ASCII ${ref}`,
        async (action) => {
          const { store, contributor: scoped, pack } = await fixture();
          const contributor = {
            ...scoped,
            tokenClaims: { ...scoped.tokenClaims, refPattern: "**" },
          };
          expect(
            workflowAuthorize(contributor, definitionRepo, ref, action),
          ).toEqual({ allowed: true });
          const writing =
            action === "writeTree"
              ? store.writeTree(contributor, definitionRepo, ref, content)
              : store.receivePack(
                  contributor,
                  definitionRepo,
                  ref,
                  pack.pack,
                  pack.commitSha,
                  null,
                );
          await expect(writing).rejects.toThrow(
            "authorize_denied: only the Hub may write non-ASCII ref names",
          );
          expect(await store.resolveRef(hub, definitionRepo, ref)).toBeNull();
          const retained = await store.createPack(hub, definitionRepo, REF, {
            commitSha: pack.commitSha,
            retain: true,
          });
          expect(retained.commitSha).toBe(pack.commitSha);
        },
      );
    }

    test("allows a similarly named sibling ref", async () => {
      const { store, contributor, pack } = await fixture();
      const sibling = "refs/interchange-backup";
      await store.receivePack(
        contributor,
        definitionRepo,
        sibling,
        pack.pack,
        pack.commitSha,
        null,
      );
      expect(await store.resolveRef(hub, definitionRepo, sibling)).toBe(
        pack.commitSha,
      );
      const retained = await store.createPack(hub, definitionRepo, REF, {
        commitSha: pack.commitSha,
        retain: true,
      });
      expect(retained.commitSha).toBe(pack.commitSha);
    });
  });

  describe("pinned definition packs", () => {
    const hub: Principal = { kind: "hub" };
    const definitionRepo: RepoId = { kind: "workflow", id: "definition" };
    const retainedRef = (sha: string) =>
      `refs/interchange/workflow-definitions/${sha}`;

    async function fixture() {
      const dataDir = await makeTempDir("repo-store-pinned-guards-");
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: {
          workflow: { ...createTestHandler(), kind: "workflow" },
          "agent-state": createTestHandler(),
        },
        authorize: allowAll,
      });
      const { commitSha } = await store.writeTree(hub, definitionRepo, REF, {
        files: { "workflow.ts": "export const workflow = {};" },
        message: "workflow source",
      });
      return { store, commitSha };
    }

    test("rejects a non-hub principal and writes no retained ref", async () => {
      const { store, commitSha } = await fixture();
      await expect(
        store.createPack(principal, definitionRepo, REF, {
          commitSha,
          retain: true,
        }),
      ).rejects.toThrow("Pinned definition packs require Hub access");
      expect(
        await store.resolveRef(hub, definitionRepo, retainedRef(commitSha)),
      ).toBeNull();
    });

    test("rejects a repo that is not a definition asset", async () => {
      const { store, commitSha } = await fixture();
      await store.initRepo(repoId);
      await expect(
        store.createPack(hub, repoId, REF, { commitSha, retain: true }),
      ).rejects.toThrow("Pinned definition packs require Hub access");
    });

    test.each(["../../heads/main", "HEAD", "A".repeat(40), "a".repeat(39)])(
      "rejects the malformed commit %s",
      async (commitSha) => {
        const { store } = await fixture();
        await expect(
          store.createPack(hub, definitionRepo, REF, {
            commitSha,
            retain: true,
          }),
        ).rejects.toThrow("Invalid pinned definition commit");
      },
    );

    test("refuses an unknown commit and writes no retained ref", async () => {
      const { store } = await fixture();
      const missing = "0123456789abcdef0123456789abcdef01234567";
      await expect(
        store.createPack(hub, definitionRepo, REF, {
          commitSha: missing,
          retain: true,
        }),
      ).rejects.toThrow();
      expect(
        await store.resolveRef(hub, definitionRepo, retainedRef(missing)),
      ).toBeNull();
    });

    test("refuses a tree id and writes no retained ref", async () => {
      const { store, commitSha } = await fixture();
      const { commit } = await git.readCommit({
        fs,
        dir: store.getRepoDir(definitionRepo),
        oid: commitSha,
      });
      await expect(
        store.createPack(hub, definitionRepo, REF, {
          commitSha: commit.tree,
          retain: true,
        }),
      ).rejects.toThrow();
      expect(
        await store.resolveRef(hub, definitionRepo, retainedRef(commit.tree)),
      ).toBeNull();
    });
  });

  test("packs and retains the approved commit after the source branch advances", async () => {
    const dataDir = await makeTempDir("repo-store-pinned-definition-");
    const handler = { ...createTestHandler(), kind: "workflow" as const };
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { workflow: handler },
      authorize: allowAll,
    });
    const definitionRepo: RepoId = { kind: "workflow", id: "definition" };
    const hub: Principal = { kind: "hub" };
    const first = await store.writeTree(hub, definitionRepo, REF, {
      files: { "workflow.ts": "first" },
      message: "first",
    });
    const second = await store.writeTree(hub, definitionRepo, REF, {
      files: { "workflow.ts": "second" },
      message: "second",
    });
    const pinned = await store.createPack(hub, definitionRepo, REF, {
      commitSha: first.commitSha,
      retain: true,
    });
    expect(pinned.commitSha).toBe(first.commitSha);
    expect(pinned.ref).toBe(REF);
    expect(await store.resolveRef(hub, definitionRepo, REF)).toBe(
      second.commitSha,
    );
    expect(
      await store.resolveRef(
        hub,
        definitionRepo,
        `refs/interchange/workflow-definitions/${first.commitSha}`,
      ),
    ).toBe(first.commitSha);
    await expect(
      store.writeTree(
        principal,
        definitionRepo,
        `refs/interchange/workflow-definitions/${first.commitSha}`,
        {
          files: { "workflow.ts": "replacement" },
          message: "replace retained definition",
        },
      ),
    ).rejects.toThrow("refs/interchange refs are Hub-owned");
    expect(
      (await store.listRefs(hub, definitionRepo)).map((entry) => entry.name),
    ).toEqual(["refs/heads/main", REF]);
    await runGC(store.getRepoDir(definitionRepo), { retention: "tip-only" });
    const afterGC = await store.createPack(hub, definitionRepo, REF, {
      commitSha: first.commitSha,
      retain: true,
    });
    expect(afterGC.commitSha).toBe(first.commitSha);
    const receiverDir = await makeTempDir("repo-store-pinned-receiver-");
    const receiver = createRepoStore({
      dataDir: receiverDir,
      signingKey,
      handlers: { workflow: handler },
      authorize: allowAll,
    });
    await receiver.receivePack(
      hub,
      definitionRepo,
      REF,
      pinned.pack,
      pinned.commitSha,
      null,
    );
    const reads = await receiver.openCommittedReads(hub, definitionRepo, REF);
    if (reads === null) throw new Error("Pinned commit missing from receiver");
    const [entry] = await reads.listDir("");
    if (entry === undefined) throw new Error("Pinned tree is empty");
    expect(new TextDecoder().decode(await reads.readBlobByOid(entry.oid))).toBe(
      "first",
    );
  });

  test("initRepo is idempotent", async () => {
    const dataDir = await makeTempDir("repo-store-init-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.initRepo(repoId);
    await store.initRepo(repoId);

    const gitDir = path.join(
      dataDir,
      handler.directoryPrefix,
      repoId.id,
      ".git",
    );
    const stat = await fs.promises.stat(gitDir);
    expect(stat.isDirectory()).toBe(true);
  });

  test("writeTree writes files, signs the commit, advances the ref, calls onRefUpdated", async () => {
    const dataDir = await makeTempDir("repo-store-write-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const { commitSha } = await store.writeTree(principal, repoId, REF, {
      files: {
        "deploy/prompt.md": "hello",
        "workspace/example.txt": "example",
      },
      message: "initial",
    });

    expect(commitSha).toMatch(/^[0-9a-f]{40}$/);

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const resolved = await git.resolveRef({ fs, dir, ref: REF });
    expect(resolved).toBe(commitSha);

    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    expect(commit.gpgsig).toBeDefined();
    expect(commit.gpgsig).toContain("-----BEGIN SSH SIGNATURE-----");

    expect(handler.onRefUpdatedCalls).toHaveLength(1);
    const call = handler.onRefUpdatedCalls[0];
    if (!call) throw new Error("unreachable");
    expect(call.ref).toBe(REF);
    expect(call.oldSha).toBeNull();
    expect(call.newSha).toBe(commitSha);
    expect(call.repoId).toEqual(repoId);
  });

  test("removeRepo deletes a repository so one created again starts from nothing", async () => {
    const dataDir = await makeTempDir("repo-store-remove-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    const first = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/prompt.md": "first" },
      message: "first",
    });
    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);

    await store.removeRepo(repoId);
    await store.removeRepo(repoId);

    expect(fs.existsSync(dir)).toBe(false);
    const { commitSha } = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/prompt.md": "second" },
      message: "second",
    });
    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    expect(commit.parent).not.toContain(first.commitSha);
    expect(handler.onRefUpdatedCalls.at(-1)?.oldSha).toBeNull();
    await expect(
      git.readCommit({ fs, dir, oid: first.commitSha }),
    ).rejects.toThrow();
  });

  test("removeRepo removes the entry named last after every other one", async () => {
    const dataDir = await makeTempDir("repo-store-remove-last-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    await store.writeTree(principal, repoId, REF, {
      files: { "deploy/prompt.md": "first" },
      message: "first",
    });
    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    fs.writeFileSync(path.join(dir, "record.json"), "{}");
    const entries = fs.readdirSync(dir);
    expect(entries.length).toBeGreaterThan(1);

    const removed: string[] = [];
    const rm = fs.promises.rm.bind(fs.promises);
    const spy = spyOn(fs.promises, "rm").mockImplementation(
      async (target, options) => {
        removed.push(String(target));
        return rm(target, options);
      },
    );
    try {
      await store.removeRepo(repoId, { last: "record.json" });
    } finally {
      spy.mockRestore();
    }

    expect(fs.existsSync(dir)).toBe(false);
    const record = removed.indexOf(path.join(dir, "record.json"));
    expect(record).toBe(removed.length - 2);
    expect(removed.at(-1)).toBe(dir);
    for (const entry of entries.filter((name) => name !== "record.json")) {
      expect(removed.indexOf(path.join(dir, entry))).toBeLessThan(record);
    }
  });

  test.each(["before record removal", "after tree removal"] as const)(
    "removeRepo retries a sync failure %s",
    async (stage) => {
      const dataDir = await makeTempDir("repo-store-remove-sync-");
      const handler = createTestHandler();
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: { "agent-state": handler },
        authorize: allowAll,
      });
      await store.writeTree(principal, repoId, REF, {
        files: { "deploy/prompt.md": "first" },
        message: "first",
      });
      const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
      const record = path.join(dir, "record.json");
      await fs.promises.writeFile(record, "{}");
      const syncDir =
        stage === "before record removal" ? dir : path.dirname(dir);
      const open = fs.promises.open;
      let syncAttempts = 0;
      const spy = spyOn(fs.promises, "open").mockImplementation(
        async (target, flags, mode) => {
          const handle = await open(target, flags, mode);
          if (target === syncDir) {
            const sync = handle.sync.bind(handle);
            spyOn(handle, "sync").mockImplementation(async () => {
              syncAttempts++;
              if (syncAttempts === 1) throw new Error("repository sync failed");
              await sync();
            });
          }
          return handle;
        },
      );
      try {
        const options = { last: "record.json" };
        await expect(store.removeRepo(repoId, options)).rejects.toThrow(
          "repository sync failed",
        );
        expect(fs.existsSync(record)).toBe(stage === "before record removal");
        await store.removeRepo(repoId, options);
        expect(syncAttempts).toBe(2);
        expect(fs.existsSync(dir)).toBe(false);
      } finally {
        spy.mockRestore();
      }
    },
  );

  test.each(["before record removal", "after tree removal"] as const)(
    "removeRepo invalidates cached history when sync fails %s",
    async (stage) => {
      const dataDir = await makeTempDir("repo-store-remove-cache-");
      const handler = createTestHandler();
      const store = createRepoStore({
        dataDir,
        signingKey,
        handlers: { "agent-state": handler },
        authorize: allowAll,
      });
      for (const message of ["first", "second"])
        await store.writeTree(principal, repoId, REF, {
          files: { "deploy/prompt.md": message },
          message,
        });
      const dir = store.getRepoDir(repoId);
      const record = path.join(dir, "record.json");
      await fs.promises.writeFile(record, "{}");
      const syncDir =
        stage === "before record removal" ? dir : path.dirname(dir);
      const failure = new Error("repository sync failed");
      const open = fs.promises.open;
      const spy = spyOn(fs.promises, "open").mockImplementation(
        async (target, flags, mode) => {
          const handle = await open(target, flags, mode);
          if (target === syncDir)
            spyOn(handle, "sync").mockRejectedValue(failure);
          return handle;
        },
      );
      try {
        await expect(
          store.removeRepo(repoId, { last: "record.json" }),
        ).rejects.toBe(failure);
      } finally {
        spy.mockRestore();
      }
      expect(fs.existsSync(record)).toBe(stage === "before record removal");
      await store.initRepo(repoId);
      const controller = new AbortController();
      const stream = store.subscribe(principal, repoId, REF, {
        from: "head",
        signal: controller.signal,
      });
      const next = stream.next();
      try {
        await store.writeTree(principal, repoId, REF, {
          files: { "deploy/prompt.md": "fresh" },
          message: "fresh",
        });
        const entry = await next;
        if (entry.done)
          throw new Error("Missing update for the recreated repository");
        const history = await git.log({ fs, dir, ref: REF });
        expect(entry.value.seq).toBe(history.length - 1);
        expect(entry.value.seq).toBe(1);
      } finally {
        controller.abort();
        await next;
        await stream.return?.();
      }
    },
  );

  test("a subscriber seeding during an update's ref write gives the update its own seq", async () => {
    const dataDir = await makeTempDir("repo-store-seed-window-");
    // The lock map is shared by every store in the process, so this
    // test's own repo id keeps a gate it leaves held from blocking
    // other tests.
    const raceRepoId: RepoId = { kind: "agent-state", id: "seed-window" };
    const before = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    for (const message of ["v1", "v2"])
      await before.writeTree(principal, raceRepoId, REF, {
        files: { "a.md": message },
        message,
      });
    // A new store over the same data, as after a restart, has no seq
    // cached, so the subscriber below seeds one.
    const hold = gate();
    let holding = false;
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: {
        "agent-state": {
          ...createTestHandler(),
          async onRefUpdated() {
            if (holding) await hold.arrive();
          },
        },
      },
      authorize: allowAll,
    });
    const dir = store.getRepoDir(raceRepoId);
    const root = (await git.log({ fs, dir, ref: REF })).at(-1)?.oid;
    if (root === undefined) throw new Error("Missing root commit");

    // The update moves the ref, then waits in onRefUpdated, before it
    // computes its seq.
    holding = true;
    const write = store.writeTree(principal, raceRepoId, REF, {
      files: { "a.md": "v3" },
      message: "v3",
    });
    await hold.reached;
    holding = false;

    // The subscriber's seed walks the moved ref to its root meanwhile.
    let markWalked: () => void = () => undefined;
    const walked = new Promise<void>((resolve) => {
      markWalked = resolve;
    });
    const spy = spyOn(git, "readCommit").mockImplementation(async (args) => {
      const result = await gitReadCommit(args);
      if (args.dir === dir && args.oid === root) markWalked();
      return result;
    });
    const ac = new AbortController();
    const stream = store.subscribe(principal, raceRepoId, REF, {
      from: "head",
      signal: ac.signal,
    });
    try {
      const first = stream.next();
      await walked;
      spy.mockRestore();
      await drainMicrotasks();
      hold.release();
      const { commitSha } = await write;
      const a = await first;
      const second = stream.next();
      await store.writeTree(principal, raceRepoId, REF, {
        files: { "a.md": "v4" },
        message: "v4",
      });
      const b = await second;
      if (a.done || b.done) throw new Error("Missing update");
      expect(asRefUpdated(a.value.event).newSha).toBe(commitSha);
      expect(a.value.seq).toBe(3);
      expect(b.value.seq).toBe(4);
    } finally {
      spy.mockRestore();
      hold.release();
      ac.abort();
    }
  });

  test("a subscriber seeding its seq while an update lands keeps later seqs in step", async () => {
    const dataDir = await makeTempDir("repo-store-seed-race-");
    // The lock map is shared by every store in the process, so this
    // test's own repo id keeps a gate it leaves held from blocking
    // other tests.
    const raceRepoId: RepoId = { kind: "agent-state", id: "seed-race" };
    const before = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    for (const message of ["v1", "v2"])
      await before.writeTree(principal, raceRepoId, REF, {
        files: { "a.md": message },
        message,
      });
    // A new store over the same data, as after a restart, has no seq
    // cached, so the subscriber below seeds one.
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    const dir = store.getRepoDir(raceRepoId);
    const root = (await git.log({ fs, dir, ref: REF })).at(-1)?.oid;
    if (root === undefined) throw new Error("Missing root commit");

    // The seed reads the history up to its root, then waits until a
    // concurrent update has computed and cached its own seq.
    const hold = gate();
    let held = false;
    const spy = spyOn(git, "readCommit").mockImplementation(async (args) => {
      const result = await gitReadCommit(args);
      if (args.dir === dir && args.oid === root && !held) {
        held = true;
        await hold.arrive();
      }
      return result;
    });
    const ac = new AbortController();
    const stream = store.subscribe(principal, raceRepoId, REF, {
      from: "head",
      signal: ac.signal,
    });
    try {
      const first = stream.next();
      await hold.reached;
      await store.writeTree(principal, raceRepoId, REF, {
        files: { "a.md": "v3" },
        message: "v3",
      });
      hold.release();
      const a = await first;
      const second = stream.next();
      await store.writeTree(principal, raceRepoId, REF, {
        files: { "a.md": "v4" },
        message: "v4",
      });
      const b = await second;
      if (a.done || b.done) throw new Error("Missing update");
      expect(a.value.seq).toBe(3);
      expect(b.value.seq).toBe(4);
    } finally {
      spy.mockRestore();
      hold.release();
      ac.abort();
    }
  });

  test("writeTree on an existing ref advances from that ref", async () => {
    const dataDir = await makeTempDir("repo-store-advance-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const first = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v1" },
      message: "first",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v2" },
      message: "second",
    });

    expect(second.commitSha).not.toBe(first.commitSha);

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: second.commitSha,
    });
    expect(commit.parent).toContain(first.commitSha);

    expect(handler.onRefUpdatedCalls).toHaveLength(2);
    const secondCall = handler.onRefUpdatedCalls[1];
    if (!secondCall) throw new Error("unreachable");
    expect(secondCall.oldSha).toBe(first.commitSha);
    expect(secondCall.newSha).toBe(second.commitSha);
  });

  test("writeTree with clearPrefix clears stale tracked files under that prefix", async () => {
    const dataDir = await makeTempDir("repo-store-clear-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "A", "deploy/b.md": "B" },
      clearPrefix: "deploy/",
      message: "first",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "A2" },
      clearPrefix: "deploy/",
      message: "second",
    });

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: second.commitSha,
    });
    const rootEntries = await readTreePaths(dir, commit.tree);
    expect(rootEntries).toContain("deploy");

    const { tree: rootTree } = await git.readTree({
      fs,
      dir,
      oid: commit.tree,
    });
    const deployEntry = rootTree.find((e) => e.path === "deploy");
    if (!deployEntry) throw new Error("deploy subtree missing");
    const deployPaths = await readTreePaths(dir, deployEntry.oid);
    expect(deployPaths).toContain("a.md");
    expect(deployPaths).not.toContain("b.md");
  });

  test("writeTree with clearPrefix on a first write clears nothing and does not error", async () => {
    // clearIndexPrefix narrows git.statusMatrix to the cleared prefix.
    // A first write to a fresh repo has no tracked entries under the
    // prefix, so the narrowed statusMatrix must return an empty matrix
    // (clear nothing) rather than erroring on a non-existent subtree.
    const dataDir = await makeTempDir("repo-store-clear-first-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const first = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "A" },
      clearPrefix: "deploy/",
      message: "first write with clearPrefix",
    });
    expect(first.commitSha).toMatch(/^[0-9a-f]{40}$/);

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: first.commitSha,
    });
    const { tree: rootTree } = await git.readTree({
      fs,
      dir,
      oid: commit.tree,
    });
    const deployEntry = rootTree.find((e) => e.path === "deploy");
    if (!deployEntry) throw new Error("deploy subtree missing");
    const deployPaths = await readTreePaths(dir, deployEntry.oid);
    expect(deployPaths).toEqual(["a.md"]);
  });

  test("writeTree clearPrefix targeting a never-written prefix is a no-op clear", async () => {
    // The narrowed statusMatrix is asked for a prefix that exists in
    // neither the index nor the tree (a sibling subtree was written, but
    // not this one). It must return empty and leave the sibling intact.
    const dataDir = await makeTempDir("repo-store-clear-absent-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "keep/a.md": "A" },
      message: "seed sibling subtree",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "fresh/b.md": "B" },
      clearPrefix: "fresh/",
      message: "clear a never-written prefix",
    });

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: second.commitSha,
    });
    const rootEntries = await readTreePaths(dir, commit.tree);
    expect(rootEntries).toContain("keep");
    expect(rootEntries).toContain("fresh");
  });

  test("writeTree without clearPrefix is purely additive", async () => {
    const dataDir = await makeTempDir("repo-store-additive-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "x/a": "1" },
      message: "first",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "x/b": "2" },
      message: "second",
    });

    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    const { commit } = await git.readCommit({
      fs,
      dir,
      oid: second.commitSha,
    });
    const { tree: rootTree } = await git.readTree({
      fs,
      dir,
      oid: commit.tree,
    });
    const xEntry = rootTree.find((e) => e.path === "x");
    if (!xEntry) throw new Error("x subtree missing");
    const xPaths = await readTreePaths(dir, xEntry.oid);
    expect(xPaths).toContain("a");
    expect(xPaths).toContain("b");
  });

  test("interleaved writes to two refs of one repo never contaminate either ref's tree", async () => {
    const dataDir = await makeTempDir("repo-store-ref-switch-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);

    const REF_A = "refs/heads/alpha";
    const REF_B = "refs/heads/beta";
    const ROUNDS = 3;

    // Each ref owns a disjoint nested top-level prefix and replaces its
    // whole subtree every round, so a ref switch reconciles a non-trivial
    // subtree each time. Both the interleaved run and the serial baseline
    // below stage identical bytes through this, so equal content must
    // yield equal tree oids unless a ref switch contaminates the tree.
    const roundWrite = (topPrefix: string, round: number) => ({
      files: {
        [`${topPrefix}/state/${round}.json`]: `${topPrefix}-${round}`,
        [`${topPrefix}/head`]: String(round),
      },
      clearPrefix: `${topPrefix}/`,
      message: `${topPrefix} ${round}`,
    });

    const treeOf = async (commitSha: string): Promise<string> =>
      (await git.readCommit({ fs, dir, oid: commitSha })).commit.tree;

    const aTrees: string[] = [];
    const bTrees: string[] = [];
    for (let round = 0; round < ROUNDS; round++) {
      const a = await store.writeTree(
        principal,
        repoId,
        REF_A,
        roundWrite("alpha", round),
      );
      const b = await store.writeTree(
        principal,
        repoId,
        REF_B,
        roundWrite("beta", round),
      );
      aTrees.push(await treeOf(a.commitSha));
      bTrees.push(await treeOf(b.commitSha));
    }

    // Direct contamination check: each ref's tip tree carries ONLY its own
    // top-level prefix. A switch that left stale cross-ref index entries
    // would union the other ref's subtree into this commit's tree.
    const aTip = await git.resolveRef({ fs, dir, ref: REF_A });
    const bTip = await git.resolveRef({ fs, dir, ref: REF_B });
    expect(await readTreePaths(dir, await treeOf(aTip))).toEqual(["alpha"]);
    expect(await readTreePaths(dir, await treeOf(bTip))).toEqual(["beta"]);

    // Equivalence to a serial single-ref baseline: writing only one ref's
    // sequence into a fresh repo never switches refs, so its tree is
    // contamination-free by construction. Equal tree oids prove the
    // interleave reconstructed each ref's tree exactly. Tree oids are
    // asserted rather than commit oids because the signer injects
    // wall-clock and timezone into the commit, so commit oids are not
    // reproducible across writes.
    const serialTrees = async (topPrefix: string): Promise<string[]> => {
      const baseDataDir = await makeTempDir("repo-store-ref-switch-base-");
      const baseHandler = createTestHandler();
      const baseStore = createRepoStore({
        dataDir: baseDataDir,
        signingKey,
        handlers: { "agent-state": baseHandler },
        authorize: allowAll,
      });
      const baseDir = path.join(
        baseDataDir,
        baseHandler.directoryPrefix,
        repoId.id,
      );
      const ref = `refs/heads/${topPrefix}`;
      const trees: string[] = [];
      for (let round = 0; round < ROUNDS; round++) {
        const r = await baseStore.writeTree(
          principal,
          repoId,
          ref,
          roundWrite(topPrefix, round),
        );
        trees.push(
          (await git.readCommit({ fs, dir: baseDir, oid: r.commitSha })).commit
            .tree,
        );
      }
      return trees;
    };

    expect(aTrees).toEqual(await serialTrees("alpha"));
    expect(bTrees).toEqual(await serialTrees("beta"));
  });

  test("receivePack accepts a valid pack and rejects one failing validatePush", async () => {
    const sourceDataDir = await makeTempDir("repo-store-pack-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDataDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "from-source" },
      message: "source content",
    });
    const { pack, commitSha } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const acceptDir = await makeTempDir("repo-store-pack-accept-");
    const acceptHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const acceptStore = createRepoStore({
      dataDir: acceptDir,
      signingKey,
      handlers: { "agent-state": acceptHandler },
      authorize: allowAll,
    });
    await acceptStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );
    expect(await acceptStore.resolveRef(principal, repoId, REF)).toBe(
      commitSha,
    );
    expect(acceptHandler.onRefUpdatedCalls).toHaveLength(1);

    const rejectDir = await makeTempDir("repo-store-pack-reject-");
    const rejectHandler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const rejectStore = createRepoStore({
      dataDir: rejectDir,
      signingKey,
      handlers: { "agent-state": rejectHandler },
      authorize: allowAll,
    });
    await expect(
      rejectStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);
    expect(await rejectStore.resolveRef(principal, repoId, REF)).toBeNull();
  });

  // Build a pack carrying one commit whose root tree holds a single symlink or
  // submodule entry, and receivePack it into a fresh store. Returns the store
  // and the receivePack promise so the caller can assert rejection + a
  // still-absent ref. The handler accepts everything, so a rejection can only
  // come from the substrate's admission gate, not the kind handler.
  async function pushSingleEntryTree(
    prefix: string,
    spec:
      | { kind: "symlink"; path: string; target: string }
      | { kind: "submodule"; path: string; gitlinkOid: string },
  ): Promise<{
    store: ReturnType<typeof createRepoStore>;
    receive: Promise<unknown>;
  }> {
    const buildDir = await makeTempDir(`${prefix}-build-`);
    await git.init({ fs, dir: buildDir, defaultBranch: "main" });

    let entry: {
      mode: string;
      path: string;
      oid: string;
      type: "blob" | "commit";
    };
    const extraOids: string[] = [];
    if (spec.kind === "symlink") {
      const blobOid = await git.writeBlob({
        fs,
        dir: buildDir,
        blob: new TextEncoder().encode(spec.target),
      });
      entry = { mode: "120000", path: spec.path, oid: blobOid, type: "blob" };
      extraOids.push(blobOid);
    } else {
      // A gitlink references a commit that is NOT in the repo, so it is not
      // packed; the tree entry references it by oid only.
      entry = {
        mode: "160000",
        path: spec.path,
        oid: spec.gitlinkOid,
        type: "commit",
      };
    }

    const treeOid = await git.writeTree({ fs, dir: buildDir, tree: [entry] });
    const commitSha = await git.writeCommit({
      fs,
      dir: buildDir,
      commit: {
        tree: treeOid,
        parent: [],
        message: "single-entry",
        author: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
        committer: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
      },
    });
    const { packfile } = await git.packObjects({
      fs,
      dir: buildDir,
      oids: [commitSha, treeOid, ...extraOids],
    });
    if (packfile === undefined) {
      throw new Error("pushSingleEntryTree: packObjects returned no packfile");
    }

    const storeDir = await makeTempDir(`${prefix}-store-`);
    const store = createRepoStore({
      dataDir: storeDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    return {
      store,
      receive: store.receivePack(
        principal,
        repoId,
        REF,
        packfile,
        commitSha,
        null,
      ),
    };
  }

  test("receivePack rejects a pushed tree carrying a symlink and does not advance the ref", async () => {
    const { store, receive } = await pushSingleEntryTree("symlink", {
      kind: "symlink",
      path: "link",
      target: "../escape",
    });
    // Anchored so a doubled `path_violation:` prefix would fail the assertion.
    await expect(receive).rejects.toThrow(
      /^path_violation: symlink at link is not allowed/,
    );
    expect(await store.resolveRef(principal, repoId, REF)).toBeNull();
  });

  test("receivePack rejects a pushed tree carrying a submodule and does not advance the ref", async () => {
    const { store, receive } = await pushSingleEntryTree("submodule", {
      kind: "submodule",
      path: "sub",
      gitlinkOid: "0".repeat(40),
    });
    await expect(receive).rejects.toThrow(
      /^path_violation: submodule at sub is not allowed/,
    );
    expect(await store.resolveRef(principal, repoId, REF)).toBeNull();
  });

  test("receivePack rejects a nested symlink with its full path and does not advance the ref", async () => {
    const buildDir = await makeTempDir("nested-symlink-build-");
    await git.init({ fs, dir: buildDir, defaultBranch: "main" });
    const blobOid = await git.writeBlob({
      fs,
      dir: buildDir,
      blob: new TextEncoder().encode("../escape"),
    });
    const subTreeOid = await git.writeTree({
      fs,
      dir: buildDir,
      tree: [{ mode: "120000", path: "link", oid: blobOid, type: "blob" }],
    });
    const rootTreeOid = await git.writeTree({
      fs,
      dir: buildDir,
      tree: [{ mode: "040000", path: "deep", oid: subTreeOid, type: "tree" }],
    });
    const commitSha = await git.writeCommit({
      fs,
      dir: buildDir,
      commit: {
        tree: rootTreeOid,
        parent: [],
        message: "nested",
        author: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
        committer: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
      },
    });
    const { packfile } = await git.packObjects({
      fs,
      dir: buildDir,
      oids: [commitSha, rootTreeOid, subTreeOid, blobOid],
    });
    if (packfile === undefined) {
      throw new Error("nested symlink: packObjects returned no packfile");
    }

    const storeDir = await makeTempDir("nested-symlink-store-");
    const store = createRepoStore({
      dataDir: storeDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    // The recursion must build the full "deep/link" path, not just "link".
    await expect(
      store.receivePack(principal, repoId, REF, packfile, commitSha, null),
    ).rejects.toThrow(/^path_violation: symlink at deep\/link is not allowed/);
    expect(await store.resolveRef(principal, repoId, REF)).toBeNull();
  });

  test("receivePack rejects a pushed tree that repeats an entry name and does not advance the ref", async () => {
    // A repeated name lets a name lookup and a scan resolve different
    // entries, so validation could check one while a reader serves the
    // other. The handler accepts everything, so only the admission gate
    // can reject.
    const buildDir = await makeTempDir("repeated-name-build-");
    await git.init({ fs, dir: buildDir, defaultBranch: "main" });
    const firstOid = await git.writeBlob({
      fs,
      dir: buildDir,
      blob: new TextEncoder().encode("first\n"),
    });
    const secondOid = await git.writeBlob({
      fs,
      dir: buildDir,
      blob: new TextEncoder().encode("second\n"),
    });
    const subTreeOid = await git.writeTree({
      fs,
      dir: buildDir,
      tree: [
        { mode: "100644", path: "a.md", oid: firstOid, type: "blob" },
        { mode: "100644", path: "a.md", oid: secondOid, type: "blob" },
      ],
    });
    const rootTreeOid = await git.writeTree({
      fs,
      dir: buildDir,
      tree: [{ mode: "040000", path: "deploy", oid: subTreeOid, type: "tree" }],
    });
    const commitSha = await git.writeCommit({
      fs,
      dir: buildDir,
      commit: {
        tree: rootTreeOid,
        parent: [],
        message: "repeated name",
        author: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
        committer: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
      },
    });
    const { packfile } = await git.packObjects({
      fs,
      dir: buildDir,
      oids: [commitSha, rootTreeOid, subTreeOid, firstOid, secondOid],
    });
    if (packfile === undefined) {
      throw new Error("repeated name: packObjects returned no packfile");
    }

    const storeDir = await makeTempDir("repeated-name-store-");
    const store = createRepoStore({
      dataDir: storeDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    await expect(
      store.receivePack(principal, repoId, REF, packfile, commitSha, null),
    ).rejects.toThrow(
      /^path_violation: repeated name at deploy\/a\.md is not allowed/,
    );
    expect(await store.resolveRef(principal, repoId, REF)).toBeNull();
  });

  test("receivePack admits a legitimate tree with an exec bit and a subtree", async () => {
    const buildDir = await makeTempDir("legit-push-build-");
    await git.init({ fs, dir: buildDir, defaultBranch: "main" });
    const fileOid = await git.writeBlob({
      fs,
      dir: buildDir,
      blob: new TextEncoder().encode("hello\n"),
    });
    const execOid = await git.writeBlob({
      fs,
      dir: buildDir,
      blob: new TextEncoder().encode("#!/bin/sh\necho hi\n"),
    });
    const nestedOid = await git.writeBlob({
      fs,
      dir: buildDir,
      blob: new TextEncoder().encode("nested\n"),
    });
    const subTreeOid = await git.writeTree({
      fs,
      dir: buildDir,
      tree: [{ mode: "100644", path: "n.txt", oid: nestedOid, type: "blob" }],
    });
    const rootTreeOid = await git.writeTree({
      fs,
      dir: buildDir,
      tree: [
        { mode: "100644", path: "a.txt", oid: fileOid, type: "blob" },
        { mode: "100755", path: "run.sh", oid: execOid, type: "blob" },
        { mode: "040000", path: "sub", oid: subTreeOid, type: "tree" },
      ],
    });
    const commitSha = await git.writeCommit({
      fs,
      dir: buildDir,
      commit: {
        tree: rootTreeOid,
        parent: [],
        message: "legit",
        author: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
        committer: {
          name: "t",
          email: "t@t.dev",
          timestamp: 0,
          timezoneOffset: 0,
        },
      },
    });
    const { packfile } = await git.packObjects({
      fs,
      dir: buildDir,
      oids: [commitSha, rootTreeOid, subTreeOid, fileOid, execOid, nestedOid],
    });
    if (packfile === undefined) {
      throw new Error("legit push: packObjects returned no packfile");
    }

    const storeDir = await makeTempDir("legit-push-store-");
    const store = createRepoStore({
      dataDir: storeDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    // A regular file, an executable (100755), and a subtree must all pass.
    await store.receivePack(principal, repoId, REF, packfile, commitSha, null);
    expect(await store.resolveRef(principal, repoId, REF)).toBe(commitSha);
  });

  test("createPack and receivePack round-trip", async () => {
    const sourceDir = await makeTempDir("repo-store-rt-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/payload.txt": "round-trip body" },
      message: "rt",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-rt-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );
    const resolved = await targetStore.resolveRef(principal, repoId, REF);
    expect(resolved).toBe(commitSha);
  });

  test("resolveRef returns null for a missing ref", async () => {
    const dataDir = await makeTempDir("repo-store-resolve-missing-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    await store.initRepo(repoId);
    const resolved = await store.resolveRef(
      principal,
      repoId,
      "refs/heads/nonexistent",
    );
    expect(resolved).toBeNull();
  });

  test("authorize gates each authorize-gated operation", async () => {
    const sourceDir = await makeTempDir("repo-store-authz-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: existingSha, ...rest } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      {
        files: { "deploy/a.md": "pack-source" },
        message: "source",
      },
    );
    void rest;
    const { pack: validPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const denyDir = await makeTempDir("repo-store-authz-deny-");
    let denyCallCount = 0;
    const denyAuthorize: AuthorizeFn = () => {
      denyCallCount += 1;
      return { allowed: false, reason: "denied" };
    };
    const denyHandler = createTestHandler({ allowTopLevelPaths: () => true });
    const denyStore = createRepoStore({
      dataDir: denyDir,
      signingKey,
      handlers: { "agent-state": denyHandler },
      authorize: denyAuthorize,
    });

    await denyStore.initRepo(repoId);
    expect(denyCallCount).toBe(0);

    await expect(
      denyStore.writeTree(principal, repoId, REF, {
        files: { a: "1" },
        message: "x",
      }),
    ).rejects.toThrow(/^authorize_denied:.*denied/);
    await expect(
      denyStore.receivePack(
        principal,
        repoId,
        REF,
        validPack,
        existingSha,
        null,
      ),
    ).rejects.toThrow(/^authorize_denied:.*denied/);
    await expect(denyStore.createPack(principal, repoId, REF)).rejects.toThrow(
      /^authorize_denied:.*denied/,
    );
    await expect(denyStore.resolveRef(principal, repoId, REF)).rejects.toThrow(
      /^authorize_denied:.*denied/,
    );

    const partialDir = await makeTempDir("repo-store-authz-partial-");
    let partialCallCount = 0;
    const allowOnlyResolve: AuthorizeFn = (
      _p,
      _r,
      _ref,
      action: RepoAction,
    ) => {
      partialCallCount += 1;
      if (action === "resolveRef") return { allowed: true };
      return { allowed: false, reason: "denied" };
    };
    const partialHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const partialStore = createRepoStore({
      dataDir: partialDir,
      signingKey,
      handlers: { "agent-state": partialHandler },
      authorize: allowOnlyResolve,
    });

    await partialStore.initRepo(repoId);
    expect(partialCallCount).toBe(0);

    const resolvedMissing = await partialStore.resolveRef(
      principal,
      repoId,
      REF,
    );
    expect(resolvedMissing).toBeNull();
    expect(partialCallCount).toBe(1);

    await expect(
      partialStore.writeTree(principal, repoId, REF, {
        files: { a: "1" },
        message: "x",
      }),
    ).rejects.toThrow(/^authorize_denied/);
    await expect(
      partialStore.receivePack(
        principal,
        repoId,
        REF,
        validPack,
        existingSha,
        null,
      ),
    ).rejects.toThrow(/^authorize_denied/);
    await expect(
      partialStore.createPack(principal, repoId, REF),
    ).rejects.toThrow(/^authorize_denied/);
  });

  test("onRefUpdated is not called on a failed receivePack", async () => {
    const sourceDir = await makeTempDir("repo-store-fail-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "pack-source" },
      message: "source",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-fail-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await expect(
      targetStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);

    expect(targetHandler.onRefUpdatedCalls).toHaveLength(0);
    const resolved = await targetStore.resolveRef(principal, repoId, REF);
    expect(resolved).toBeNull();
  });

  test("writeTree rejects when the handler's validatePush rejects", async () => {
    const dataDir = await makeTempDir("repo-store-writetree-reject-");
    const handler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await expect(
      store.writeTree(principal, repoId, REF, {
        files: { "deploy/a.md": "rejected" },
        message: "should be rejected",
      }),
    ).rejects.toThrow(/^path_violation/);

    expect(handler.onRefUpdatedCalls).toHaveLength(0);
    const resolved = await store.resolveRef(principal, repoId, REF);
    expect(resolved).toBeNull();
  });

  test("writeTree rolls the staging area back when validatePush rejects so subsequent writes land cleanly", async () => {
    const dataDir = await makeTempDir("repo-store-writetree-rollback-");
    let allowNext = false;
    const handler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      validatePush(): ValidatePushResult {
        if (allowNext) return { ok: true };
        return { ok: false, reason: "stub rejected push" };
      },
      onRefUpdated(args) {
        this.onRefUpdatedCalls.push(args);
      },
      onRefUpdatedCalls: [],
    };
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await expect(
      store.writeTree(principal, repoId, REF, {
        files: { "deploy/rejected.md": "first attempt" },
        message: "first attempt — should be rejected",
      }),
    ).rejects.toThrow(/^path_violation/);

    // The previous attempt's writeFileEntry staged
    // deploy/rejected.md before validation refused. A working
    // rollback drops the staged file from disk and the index so the
    // next legitimate writeTree commits exactly the files it
    // declares — no leftover content carried over.
    allowNext = true;
    const commit = await store.writeTree(principal, repoId, REF, {
      files: { "deploy/accepted.md": "second attempt" },
      message: "second attempt — accepted",
    });

    const gitDir = path.join(
      dataDir,
      handler.directoryPrefix,
      repoId.id,
      ".git",
    );
    const repoDir = path.dirname(gitDir);
    const { tree: commitTree } = await git.readTree({
      fs,
      dir: repoDir,
      oid: commit.commitSha,
    });
    const treeEntries = await readTreePaths(
      repoDir,
      commitTree.find((e) => e.path === "deploy")?.oid ?? "",
    );
    expect(treeEntries.sort()).toEqual(["accepted.md"]);
    expect(
      await fs.promises
        .access(path.join(repoDir, "deploy", "rejected.md"))
        .then(() => true)
        .catch(() => false),
    ).toBe(false);
  });

  test("writeTree rollback restores ref-existing files that the rejected push overwrote", async () => {
    // A rejected push must not destroy content the target ref already
    // held. The earlier rollback implementation unconditionally
    // unlinked every staged path on validation failure; for paths that
    // already lived at the ref (e.g. a top-level file the new push
    // overwrote) that turned a validation refusal into silent data
    // loss. This pins the restore-on-rollback behaviour: an accepted
    // first push seeds a file at the ref, a rejected second push
    // attempts to overwrite that same file, and after the validation
    // failure the original ref content must still be on disk and in
    // the index.
    const dataDir = await makeTempDir("repo-store-writetree-restore-ref-");
    let allowNext = true;
    const handler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      validatePush(): ValidatePushResult {
        if (allowNext) return { ok: true };
        return { ok: false, reason: "stub rejected push" };
      },
      onRefUpdated(args) {
        this.onRefUpdatedCalls.push(args);
      },
      onRefUpdatedCalls: [],
    };
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "top-level.txt": "original content at ref" },
      message: "seed ref-existing file",
    });

    allowNext = false;
    await expect(
      store.writeTree(principal, repoId, REF, {
        files: { "top-level.txt": "rejected overwrite" },
        message: "second attempt — should be rejected without losing ref",
      }),
    ).rejects.toThrow(/^path_violation/);

    const gitDir = path.join(
      dataDir,
      handler.directoryPrefix,
      repoId.id,
      ".git",
    );
    const repoDir = path.dirname(gitDir);
    const onDisk = await fs.promises.readFile(
      path.join(repoDir, "top-level.txt"),
      "utf-8",
    );
    expect(onDisk).toBe("original content at ref");

    // A subsequent legitimate writeTree should land cleanly and the
    // resulting commit's tree should hold the original content at
    // top-level.txt — confirming the rejected push neither destroyed
    // the file on disk nor left the index in a torn state that a
    // follow-up commit would propagate.
    allowNext = true;
    const commit = await store.writeTree(principal, repoId, REF, {
      files: { "other.txt": "unrelated next push" },
      message: "third attempt — accepted",
    });
    const { tree } = await git.readTree({
      fs,
      dir: repoDir,
      oid: commit.commitSha,
    });
    const topLevelEntry = tree.find((e) => e.path === "top-level.txt");
    expect(topLevelEntry).toBeDefined();
    if (topLevelEntry === undefined) throw new Error("unreachable");
    const blob = await git.readBlob({
      fs,
      dir: repoDir,
      oid: topLevelEntry.oid,
    });
    expect(new TextDecoder().decode(blob.blob)).toBe("original content at ref");
  });

  test("writeTree passes the readBlob callback that resolves declared file contents", async () => {
    const dataDir = await makeTempDir("repo-store-writetree-readblob-");
    const captured: { paths: string[] | null; blob: Uint8Array | null } = {
      paths: null,
      blob: null,
    };
    const handler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      async validatePush({ topLevelTreePaths, readBlob }) {
        captured.paths = topLevelTreePaths;
        captured.blob = await readBlob("deploy/a.md");
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
      onRefUpdatedCalls: [],
    };
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "blob-body", "top.txt": "top" },
      message: "with readBlob",
    });

    expect(captured.paths?.sort()).toEqual(["deploy", "top.txt"]);
    expect(captured.blob).not.toBeNull();
    expect(new TextDecoder().decode(captured.blob ?? new Uint8Array())).toBe(
      "blob-body",
    );
  });

  test("validatePush runs on receivePack regardless of authorize verdict", async () => {
    const sourceDir = await makeTempDir("repo-store-bypass-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "pack-source" },
      message: "source",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-bypass-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => false,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await expect(
      targetStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);
  });

  test("receivePack accepts a fresh ref when expectedOldSha is null", async () => {
    const sourceDir = await makeTempDir("repo-store-cas-fresh-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v1" },
      message: "v1",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-cas-fresh-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });

    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );

    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(
      commitSha,
    );
    expect(targetHandler.onRefUpdatedCalls).toHaveLength(1);
    const call = targetHandler.onRefUpdatedCalls[0];
    if (!call) throw new Error("unreachable");
    expect(call.oldSha).toBeNull();
    expect(call.newSha).toBe(commitSha);
  });

  test("receivePack rejects when expectedOldSha is stale", async () => {
    const sourceDir = await makeTempDir("repo-store-cas-stale-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: firstSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v1" }, message: "v1" },
    );
    const { pack: firstPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v2" }, message: "v2" },
    );
    const { pack: secondPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const targetDir = await makeTempDir("repo-store-cas-stale-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      firstPack,
      firstSha,
      null,
    );

    await expect(
      targetStore.receivePack(
        principal,
        repoId,
        REF,
        secondPack,
        secondSha,
        null,
      ),
    ).rejects.toThrow(/^non_fast_forward:/);

    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(firstSha);
    expect(targetHandler.onRefUpdatedCalls).toHaveLength(1);
  });

  test("receivePack feeds the substrate-returned oldSha into onRefUpdated", async () => {
    const sourceDir = await makeTempDir("repo-store-oldsha-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: firstSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v1" }, message: "v1" },
    );
    const { pack: firstPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v2" }, message: "v2" },
    );
    const { pack: secondPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const targetDir = await makeTempDir("repo-store-oldsha-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      firstPack,
      firstSha,
      null,
    );
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      secondPack,
      secondSha,
      firstSha,
    );

    expect(targetHandler.onRefUpdatedCalls).toHaveLength(2);
    const second = targetHandler.onRefUpdatedCalls[1];
    if (!second) throw new Error("unreachable");
    expect(second.oldSha).toBe(firstSha);
    expect(second.newSha).toBe(secondSha);
  });

  test("concurrent receivePack against the same repo serializes", async () => {
    const sourceDir = await makeTempDir("repo-store-serial-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: firstSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v1" }, message: "v1" },
    );
    const { pack: firstPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      repoId,
      REF,
      { files: { "deploy/a.md": "v2" }, message: "v2" },
    );
    const { pack: secondPack } = await sourceStore.createPack(
      principal,
      repoId,
      REF,
    );

    const targetDir = await makeTempDir("repo-store-serial-target-");
    const events: string[] = [];
    // Each push parks inside validatePush until the test releases it, so the
    // first push holds the repo for as long as the test wants rather than for
    // a fixed delay the second push has to arrive inside of.
    const parked: (() => void)[] = [];
    const slowHandler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      async validatePush() {
        events.push("validate");
        await new Promise<void>((resolve) => {
          parked.push(resolve);
        });
        return { ok: true };
      },
      onRefUpdated({ newSha }) {
        events.push(`update:${newSha.substring(0, 7)}`);
      },
      onRefUpdatedCalls: [],
    };
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": slowHandler },
      authorize: allowAll,
    });

    const firstP = targetStore.receivePack(
      principal,
      repoId,
      REF,
      firstPack,
      firstSha,
      null,
    );
    const secondP = targetStore.receivePack(
      principal,
      repoId,
      REF,
      secondPack,
      secondSha,
      firstSha,
    );

    // Release each push only once it has parked. A store that ran the two in
    // parallel would park both before either release, which the event order
    // below reports as two adjacent "validate" entries.
    await waitUntil(() => parked.length >= 1);
    parked[0]?.();
    await waitUntil(() => parked.length >= 2);
    parked[1]?.();

    await Promise.all([firstP, secondP]);

    expect(events).toEqual([
      "validate",
      `update:${firstSha.substring(0, 7)}`,
      "validate",
      `update:${secondSha.substring(0, 7)}`,
    ]);
    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(
      secondSha,
    );
  });

  test("concurrent receivePack against distinct repos runs in parallel", async () => {
    const sourceDir = await makeTempDir("repo-store-parallel-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const repoA: RepoId = { kind: "agent-state", id: "alpha" };
    const repoB: RepoId = { kind: "agent-state", id: "beta" };
    const { commitSha: shaA } = await sourceStore.writeTree(
      principal,
      repoA,
      REF,
      { files: { "deploy/a.md": "a" }, message: "a" },
    );
    const { pack: packA } = await sourceStore.createPack(principal, repoA, REF);
    const { commitSha: shaB } = await sourceStore.writeTree(
      principal,
      repoB,
      REF,
      { files: { "deploy/b.md": "b" }, message: "b" },
    );
    const { pack: packB } = await sourceStore.createPack(principal, repoB, REF);

    const targetDir = await makeTempDir("repo-store-parallel-target-");
    let activeConcurrent = 0;
    let observedMaxConcurrent = 0;
    let reportBothInFlight!: () => void;
    const bothInFlight = new Promise<void>((resolve) => {
      reportBothInFlight = resolve;
    });
    const trackingHandler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      async validatePush() {
        activeConcurrent += 1;
        observedMaxConcurrent = Math.max(
          observedMaxConcurrent,
          activeConcurrent,
        );
        // A barrier, not a delay: each handler holds the push until BOTH have
        // entered, so the second one arrives however long it takes. The delay
        // this replaces closed the window after 100ms, and a push that
        // reached the handler later than that left the counter at 1 and
        // failed a store that was in fact parallel.
        if (activeConcurrent === 2) reportBothInFlight();
        await bothInFlight;
        activeConcurrent -= 1;
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
      onRefUpdatedCalls: [],
    };
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": trackingHandler },
      authorize: allowAll,
    });

    await Promise.all([
      targetStore.receivePack(principal, repoA, REF, packA, shaA, null),
      targetStore.receivePack(principal, repoB, REF, packB, shaB, null),
    ]);

    // Both handlers were in flight at once, which is the property: the
    // counter rises to 2 only if the second push entered before the first
    // released. Exact equality also rules out serialization, which would
    // leave it at 1, so no elapsed-time bound is needed to tell the two
    // apart -- and a bound would only add a dependency on how fast the
    // machine happens to be. A store that serialized the two repos never
    // releases the barrier at all, which the lane timeout reports as a hang.
    expect(observedMaxConcurrent).toBe(2);
  });

  test("getRepoDir returns dataDir/<directoryPrefix>/<id>", async () => {
    const dataDir = await makeTempDir("repo-store-dir-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const expected = path.join(dataDir, handler.directoryPrefix, repoId.id);
    expect(store.getRepoDir(repoId)).toBe(expected);
  });

  test("getRepoDir rejects an unsafe repo id without touching the filesystem", () => {
    const dataDir = path.join(os.tmpdir(), "repo-store-dir-unsafe-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    expect(() =>
      store.getRepoDir({ kind: "agent-state", id: "../escape" }),
    ).toThrow(/^repo_id_invalid/);
  });

  test("listRefs returns the genesis branch on a freshly-initialised repo", async () => {
    const dataDir = await makeTempDir("repo-store-list-genesis-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.initRepo(repoId);
    const refs = await store.listRefs(principal, repoId);
    expect(refs.length).toBeGreaterThanOrEqual(1);

    const main = refs.find((r) => r.name === "refs/heads/main");
    expect(main).toBeDefined();
    expect(main?.sha).toMatch(/^[0-9a-f]{40}$/);

    const dir = store.getRepoDir(repoId);
    const tipFromGit = await git.resolveRef({
      fs,
      dir,
      ref: "refs/heads/main",
    });
    expect(main?.sha).toBe(tipFromGit);
  });

  test("listRefs returns names sorted lexicographically including tags", async () => {
    const dataDir = await makeTempDir("repo-store-list-sort-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, "refs/heads/main", {
      files: { "a.md": "one" },
      message: "first",
    });
    await store.writeTree(principal, repoId, "refs/heads/zzz-branch", {
      files: { "b.md": "two" },
      message: "second",
    });

    const dir = store.getRepoDir(repoId);
    const mainSha = await git.resolveRef({
      fs,
      dir,
      ref: "refs/heads/main",
    });
    await git.writeRef({
      fs,
      dir,
      ref: "refs/tags/v1",
      value: mainSha,
      force: true,
    });

    const refs = await store.listRefs(principal, repoId);
    const names = refs.map((r) => r.name);
    const sorted = [...names].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    expect(names).toEqual(sorted);
    expect(names).toContain("refs/heads/main");
    expect(names).toContain("refs/heads/zzz-branch");
    expect(names).toContain("refs/tags/v1");
  });

  test("listRefs returns the empty list before initRepo is called", async () => {
    const dataDir = await makeTempDir("repo-store-list-empty-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const refs = await store.listRefs(principal, repoId);
    expect(refs).toEqual([]);
  });

  test("listRefs is gated under the resolveRef authorize action", async () => {
    const dataDir = await makeTempDir("repo-store-list-deny-");
    const seenActions: RepoAction[] = [];
    const denyAuthorize: AuthorizeFn = (_p, _r, _ref, action) => {
      seenActions.push(action);
      return { allowed: false, reason: "denied" };
    };
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: denyAuthorize,
    });

    await expect(store.listRefs(principal, repoId)).rejects.toThrow(
      /^authorize_denied/,
    );
    expect(seenActions).toEqual(["resolveRef"]);
  });

  test("initRepo forwards a per-call gitignore override into the genesis tree", async () => {
    const dataDir = await makeTempDir("repo-store-init-gitignore-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const customBody = ".DS_Store\nnode_modules/\nkeys/\n";
    await store.initRepo(repoId, { gitignore: customBody });

    const dir = store.getRepoDir(repoId);
    const onDisk = await fs.promises.readFile(
      path.join(dir, ".gitignore"),
      "utf-8",
    );
    expect(onDisk).toBe(customBody);
  });

  test("withRepoLock releases on substrate exception and the map drains", async () => {
    const sourceDir = await makeTempDir("repo-store-release-source-");
    const sourceHandler = createTestHandler();
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/a.md": "v1" },
      message: "v1",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const targetDir = await makeTempDir("repo-store-release-target-");
    let rejectOnce = true;
    const flakyHandler: TestHandler = {
      kind: "agent-state",
      directoryPrefix: "repos-under-test",
      validatePush() {
        if (rejectOnce) {
          rejectOnce = false;
          return { ok: false, reason: "first attempt rejected" };
        }
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
      onRefUpdatedCalls: [],
    };
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": flakyHandler },
      authorize: allowAll,
    });

    await expect(
      targetStore.receivePack(principal, repoId, REF, pack, commitSha, null),
    ).rejects.toThrow(/^path_violation/);

    // The second call would deadlock if the lock were never released.
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );
    expect(await targetStore.resolveRef(principal, repoId, REF)).toBe(
      commitSha,
    );
  });

  test("subscribe replays the full history from seq 0", async () => {
    const dataDir = await makeTempDir("repo-store-sub-replay-0-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const first = await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "1" },
      message: "one",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "2" },
      message: "two",
    });

    const ac = new AbortController();
    const iter = store.subscribe(principal, repoId, REF, {
      signal: ac.signal,
      from: { seq: 0 },
    });

    const collected: { seq: number; event: unknown }[] = [];
    for (let i = 0; i < 3; i++) {
      const next = await iter.next();
      if (next.done) break;
      collected.push(next.value);
    }
    ac.abort();

    expect(collected.length).toBeGreaterThanOrEqual(2);
    const seqs = collected.map((e) => e.seq);
    expect(seqs).toEqual(Array.from({ length: seqs.length }, (_, i) => i));

    const newShaList = collected.map((e) => {
      const ev = asRefUpdated(e.event);
      expect(ev.type).toBe("ref.updated");
      expect(ev.ref).toBe(REF);
      return ev.newSha;
    });
    expect(newShaList).toContain(first.commitSha);
    expect(newShaList).toContain(second.commitSha);
  });

  test("subscribe replays from a non-zero seq", async () => {
    const dataDir = await makeTempDir("repo-store-sub-replay-n-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "1" },
      message: "one",
    });
    const second = await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "2" },
      message: "two",
    });
    const third = await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "3" },
      message: "three",
    });

    const ac = new AbortController();
    const iter = store.subscribe(principal, repoId, REF, {
      signal: ac.signal,
      from: { seq: 2 },
    });

    const collected: { seq: number; event: unknown }[] = [];
    for (let i = 0; i < 2; i++) {
      const next = await iter.next();
      if (next.done) break;
      collected.push(next.value);
      if (collected.length === 2) break;
    }
    ac.abort();

    const seqs = collected.map((e) => e.seq);
    expect(seqs.every((s) => s >= 2)).toBe(true);

    const newShaList = collected.map((e) => asRefUpdated(e.event).newSha);
    expect(newShaList).toContain(second.commitSha);
    expect(newShaList).toContain(third.commitSha);
  });

  test("subscribe from head emits only commits that land after subscribe", async () => {
    const dataDir = await makeTempDir("repo-store-sub-head-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "before" },
      message: "before subscribe",
    });

    const ac = new AbortController();
    const iter = store.subscribe(principal, repoId, REF, {
      signal: ac.signal,
      from: "head",
    });

    const newWrite = store.writeTree(principal, repoId, REF, {
      files: { "a.md": "after" },
      message: "after subscribe",
    });
    const [next, newCommit] = await Promise.all([iter.next(), newWrite]);
    ac.abort();

    expect(next.done).toBe(false);
    if (next.done) throw new Error("unreachable");
    const ev = asRefUpdated(next.value.event);
    expect(ev.newSha).toBe(newCommit.commitSha);
    expect(ev.oldSha).not.toBeNull();
  });

  test("subscribe ends cleanly when the abort signal fires", async () => {
    const dataDir = await makeTempDir("repo-store-sub-abort-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    await store.initRepo(repoId);

    const ac = new AbortController();
    const iter = store.subscribe(principal, repoId, REF, {
      signal: ac.signal,
      from: "head",
    });

    // Schedule the abort on the next tick, then await next(). The
    // pending waiter should resolve to {done: true} cleanly — no
    // throw, no hang.
    setTimeout(() => ac.abort(), 10);
    const done = await iter.next();
    expect(done.done).toBe(true);

    // A second next() after abort is also done; the iterator stays
    // closed without rethrowing.
    const again = await iter.next();
    expect(again.done).toBe(true);
  });

  test("subscribe throws on buffer overrun", async () => {
    const dataDir = await makeTempDir("repo-store-sub-overrun-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    await store.initRepo(repoId);

    const ac = new AbortController();
    const iter = store.subscribe(principal, repoId, REF, {
      signal: ac.signal,
      from: "head",
      bufferLimit: 2,
    });

    // Prime the iterator so the replay phase runs (and the seq
    // cache is seeded) before we start filling the buffer.
    const drainPromise = iter.next();

    // The first commit's event satisfies the pending waiter set up
    // by drainPromise — it does not occupy a buffer slot. The next
    // three commits fill the buffer to its limit and then overrun.
    const first = await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "1" },
      message: "one",
    });
    await drainPromise.then((r) => {
      if (r.done) throw new Error("unreachable");
      expect(asRefUpdated(r.value.event).newSha).toBe(first.commitSha);
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "2" },
      message: "two",
    });
    await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "3" },
      message: "three",
    });
    await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "4" },
      message: "four",
    });

    // Drain the first two buffered events normally.
    const a = await iter.next();
    const b = await iter.next();
    expect(a.done).toBe(false);
    expect(b.done).toBe(false);

    // The third pull surfaces the captured overrun error.
    await expect(iter.next()).rejects.toThrow(/subscribe_buffer_overrun/);

    ac.abort();
  });

  test("subscribe isolates multiple concurrent subscribers", async () => {
    const dataDir = await makeTempDir("repo-store-sub-multi-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });
    await store.initRepo(repoId);

    const acA = new AbortController();
    const acB = new AbortController();
    const iterA = store.subscribe(principal, repoId, REF, {
      signal: acA.signal,
      from: "head",
    });
    const iterB = store.subscribe(principal, repoId, REF, {
      signal: acB.signal,
      from: "head",
    });

    const pendingA = iterA.next();
    const pendingB = iterB.next();
    const write = await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "1" },
      message: "one",
    });

    const [a, b] = await Promise.all([pendingA, pendingB]);
    if (a.done || b.done) throw new Error("unreachable");

    expect(asRefUpdated(a.value.event).newSha).toBe(write.commitSha);
    expect(asRefUpdated(b.value.event).newSha).toBe(write.commitSha);
    expect(a.value.seq).toBe(b.value.seq);

    // Cancelling A does not affect B.
    acA.abort();
    const closedA = await iterA.next();
    expect(closedA.done).toBe(true);

    const pendingB2 = iterB.next();
    const write2 = await store.writeTree(principal, repoId, REF, {
      files: { "a.md": "2" },
      message: "two",
    });
    const b2 = await pendingB2;
    if (b2.done) throw new Error("unreachable");
    expect(asRefUpdated(b2.value.event).newSha).toBe(write2.commitSha);
    acB.abort();
  });

  test("live seqs count from the oldest commit of a history that ends in a missing parent", async () => {
    const source = createRepoStore({
      dataDir: await makeTempDir("repo-store-dangling-src-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    await source.writeTree(principal, repoId, REF, {
      files: { "a.md": "v1" },
      message: "v1",
    });
    const { commitSha } = await source.writeTree(principal, repoId, REF, {
      files: { "a.md": "v2" },
      message: "v2",
    });
    // The pack carries the tip only, so the target cannot read its
    // parent.
    const { pack } = await source.createPack(principal, repoId, REF);
    const store = createRepoStore({
      dataDir: await makeTempDir("repo-store-dangling-tgt-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    await store.receivePack(principal, repoId, REF, pack, commitSha, null);

    const ac = new AbortController();
    const stream = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: ac.signal,
    });
    const readCommit = spyOn(git, "readCommit");
    const seqs: number[] = [];
    const reads: number[] = [];
    try {
      for (const message of ["v3", "v4", "v5", "v6"]) {
        const next = stream.next();
        const before = readCommit.mock.calls.length;
        await store.writeTree(principal, repoId, REF, {
          files: { "a.md": message },
          message,
        });
        reads.push(readCommit.mock.calls.length - before);
        const entry = await next;
        if (entry.done) throw new Error("Missing update");
        seqs.push(entry.value.seq);
      }
      expect(seqs).toEqual([1, 2, 3, 4]);
      // After the first update caches a seq, each update reads the same
      // number of commits however long the history grows.
      expect(new Set(reads.slice(1)).size).toBe(1);
    } finally {
      ac.abort();
      readCommit.mockRestore();
    }
  });

  test("live seqs advance across received tips whose parents are missing", async () => {
    const source = createRepoStore({
      dataDir: await makeTempDir("repo-store-gap-src-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    const write = (message: string) =>
      source.writeTree(principal, repoId, REF, {
        files: { "a.md": message },
        message,
      });
    await write("v1");
    const { commitSha: first } = await write("v2");
    const store = createRepoStore({
      dataDir: await makeTempDir("repo-store-gap-tgt-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    // Each pack carries its tip only, and the source writes two commits
    // between packs, so every received tip's parent is missing.
    const firstPack = await source.createPack(principal, repoId, REF);
    await store.receivePack(
      principal,
      repoId,
      REF,
      firstPack.pack,
      first,
      null,
    );

    const ac = new AbortController();
    const stream = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: ac.signal,
    });
    try {
      const seqs: number[] = [];
      let old = first;
      for (const [skipped, message] of [
        ["v3", "v4"],
        ["v5", "v6"],
        ["v7", "v8"],
      ] as const) {
        await write(skipped);
        const { commitSha } = await write(message);
        const { pack } = await source.createPack(principal, repoId, REF);
        const next = stream.next();
        await store.receivePack(principal, repoId, REF, pack, commitSha, old);
        old = commitSha;
        const entry = await next;
        if (entry.done) throw new Error("Missing update");
        seqs.push(entry.value.seq);
      }
      const [base] = seqs;
      if (base === undefined) throw new Error("Missing seq");
      expect(seqs).toEqual([base, base + 1, base + 2]);
    } finally {
      ac.abort();
    }
  });

  test("a replay subscriber on a history that ends in a missing parent seeds its seq outside the lock", async () => {
    const source = createRepoStore({
      dataDir: await makeTempDir("repo-store-dangling-replay-src-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    const { commitSha: missing } = await source.writeTree(
      principal,
      repoId,
      REF,
      { files: { "a.md": "v1" }, message: "v1" },
    );
    const { commitSha } = await source.writeTree(principal, repoId, REF, {
      files: { "a.md": "v2" },
      message: "v2",
    });
    const { pack } = await source.createPack(principal, repoId, REF);
    const dataDir = await makeTempDir("repo-store-dangling-replay-tgt-");
    const target = () =>
      createRepoStore({
        dataDir,
        signingKey,
        handlers: { "agent-state": createTestHandler() },
        authorize: allowAll,
      });
    await target().receivePack(principal, repoId, REF, pack, commitSha, null);
    // A new store over the same data, as after a restart, has no seq
    // cached, so the subscriber below seeds one.
    const store = target();
    const dir = store.getRepoDir(repoId);

    // The replay has no entries, so the seed walks the readable history
    // and stops at the missing parent.
    let markWalked: () => void = () => undefined;
    const walked = new Promise<void>((resolve) => {
      markWalked = resolve;
    });
    const readCommit = spyOn(git, "readCommit").mockImplementation(
      async (args) => {
        try {
          return await gitReadCommit(args);
        } finally {
          if (args.dir === dir && args.oid === missing) markWalked();
        }
      },
    );
    const ac = new AbortController();
    try {
      const stream = store.subscribe(principal, repoId, REF, {
        from: { seq: 0 },
        signal: ac.signal,
      });
      const first = stream.next();
      await walked;
      await drainMicrotasks();
      const seqs: number[] = [];
      const reads: number[] = [];
      for (const [message, next] of [
        ["v3", first],
        ["v4", null],
      ] as const) {
        const pending = next ?? stream.next();
        const before = readCommit.mock.calls.length;
        await store.writeTree(principal, repoId, REF, {
          files: { "a.md": message },
          message,
        });
        reads.push(readCommit.mock.calls.length - before);
        const entry = await pending;
        if (entry.done) throw new Error("Missing update");
        seqs.push(entry.value.seq);
      }
      expect(seqs).toEqual([1, 2]);
      expect(reads[0]).toBe(reads[1]);
    } finally {
      ac.abort();
      readCommit.mockRestore();
    }
  });

  test("a subscriber on a kind that GC reclaims does not seed its seq with no lock", async () => {
    const dataDir = await makeTempDir("repo-store-gc-seed-");
    const gcStore = () =>
      createRepoStore({
        dataDir,
        signingKey,
        handlers: { "agent-state": createTestHandler() },
        authorize: allowAll,
        gc: {
          kinds: ["agent-state"],
          retention: "keep-history",
          packThreshold: 1000,
          looseThreshold: 1e6,
          warnBytes: 1e12,
        },
      });
    const before = gcStore();
    for (const message of ["v1", "v2", "v3"])
      await before.writeTree(principal, repoId, REF, {
        files: { "a.md": message },
        message,
      });
    // A new store over the same data, as after a restart, has no seq
    // cached, so the subscriber below would seed one.
    const store = gcStore();
    const dir = store.getRepoDir(repoId);

    // A seed would first resolve the ref, and that call starts within
    // the microtasks after the first `next()`.
    const resolveRef = spyOn(git, "resolveRef");
    const ac = new AbortController();
    try {
      const stream = store.subscribe(principal, repoId, REF, {
        from: "head",
        signal: ac.signal,
      });
      const next = stream.next();
      await drainMicrotasks();
      expect(
        resolveRef.mock.calls.filter(
          ([args]) => args.dir === dir && args.ref === REF,
        ),
      ).toHaveLength(0);

      await store.writeTree(principal, repoId, REF, {
        files: { "a.md": "v4" },
        message: "v4",
      });
      const entry = await next;
      if (entry.done) throw new Error("Missing update");
      const history = await git.log({ fs, dir, ref: REF });
      expect(entry.value.seq).toBe(history.length - 1);
    } finally {
      ac.abort();
      resolveRef.mockRestore();
    }
  });

  test("a subscriber after updates with no subscriber seeds its seq outside the lock", async () => {
    const dataDir = await makeTempDir("repo-store-forget-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    const write = (message: string) =>
      store.writeTree(principal, repoId, REF, {
        files: { "a.md": message },
        message,
      });
    await write("v1");

    // A subscriber that sees one update leaves a seq cached for the ref.
    const earlier = new AbortController();
    const earlierStream = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: earlier.signal,
    });
    try {
      const earlierNext = earlierStream.next();
      await write("v2");
      expect((await earlierNext).done).toBe(false);
    } finally {
      earlier.abort();
    }
    for (let i = 3; i <= 20; i++) await write(`v${String(i)}`);

    // The next subscriber's seed walks the history to its root with no
    // lock. Had an entry for v2 stayed cached, the seed would skip, and
    // the first update below would walk back to v2 under the lock.
    const dir = store.getRepoDir(repoId);
    const root = (await git.log({ fs, dir, ref: REF })).at(-1)?.oid;
    if (root === undefined) throw new Error("Missing root commit");
    let markWalked: () => void = () => undefined;
    const walked = new Promise<void>((resolve) => {
      markWalked = resolve;
    });
    const readCommit = spyOn(git, "readCommit").mockImplementation(
      async (args) => {
        const result = await gitReadCommit(args);
        if (args.dir === dir && args.oid === root) markWalked();
        return result;
      },
    );
    const ac = new AbortController();
    try {
      const stream = store.subscribe(principal, repoId, REF, {
        from: "head",
        signal: ac.signal,
      });
      const first = stream.next();
      await walked;
      await drainMicrotasks();
      const seqs: number[] = [];
      const reads: number[] = [];
      for (const [message, next] of [
        ["v21", first],
        ["v22", null],
      ] as const) {
        const pending = next ?? stream.next();
        const before = readCommit.mock.calls.length;
        await write(message);
        reads.push(readCommit.mock.calls.length - before);
        const entry = await pending;
        if (entry.done) throw new Error("Missing update");
        seqs.push(entry.value.seq);
      }
      expect(seqs).toEqual([21, 22]);
      expect(reads[0]).toBe(reads[1]);
    } finally {
      ac.abort();
      readCommit.mockRestore();
    }
  });

  test("subscribe authorize denial throws immediately", async () => {
    const dataDir = await makeTempDir("repo-store-sub-deny-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: () => ({ allowed: false, reason: "denied" }),
    });

    const ac = new AbortController();
    expect(() =>
      store.subscribe(principal, repoId, REF, {
        signal: ac.signal,
        from: "head",
      }),
    ).toThrow(/^authorize_denied/);
  });

  // The per-commit walk's parent traversal asserts every commit has at
  // most one parent so an intermediate-state validation always reads
  // against the single ancestor's tree. No kind handler today produces
  // merge commits; the assert exists to catch any future writer that
  // accidentally does. This test pins the assertion shape so a future
  // change to receivePack cannot quietly start accepting multi-parent
  // commits — under the new behaviour the pack-walk would have no
  // canonical "the predecessor" to consult, and every kind handler that
  // depended on a single-parent chain would silently drift.
  test("receivePack rejects a pack carrying a merge commit with pack_walk_multi_parent", async () => {
    const sourceDir = await makeTempDir("repo-store-merge-source-");
    const sourceHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "agent-state": sourceHandler },
      authorize: allowAll,
    });
    const { commitSha: parentA } = await sourceStore.writeTree(
      principal,
      repoId,
      "refs/heads/parent-a",
      { files: { "deploy/a.md": "branch-a content" }, message: "branch a" },
    );
    const { commitSha: parentB } = await sourceStore.writeTree(
      principal,
      repoId,
      "refs/heads/parent-b",
      { files: { "deploy/b.md": "branch-b content" }, message: "branch b" },
    );

    const sourceRepoDir = sourceStore.getRepoDir(repoId);
    const { commit: parentACommit } = await git.readCommit({
      fs,
      dir: sourceRepoDir,
      oid: parentA,
    });
    // Author the merge directly through isomorphic-git so the substrate
    // never sees a multi-parent commit on the source side. The merge
    // reuses one parent's tree wholesale because the per-commit walk
    // only inspects the parent chain — the tree content is incidental
    // to the assertion being pinned here.
    const mergeSha = await git.commit({
      fs,
      dir: sourceRepoDir,
      ref: "refs/heads/merge",
      message: "synthetic merge of parent-a and parent-b",
      author: { name: "test", email: "test@example.com" },
      parent: [parentA, parentB],
      tree: parentACommit.tree,
    });

    const reachableFromMerge = await collectReachableObjects(
      sourceRepoDir,
      mergeSha,
    );
    const reachableFromB = await collectReachableObjects(
      sourceRepoDir,
      parentB,
    );
    const oids = Array.from(
      new Set([...reachableFromMerge, ...reachableFromB]),
    );
    const packResult = await git.packObjects({
      fs,
      dir: sourceRepoDir,
      oids,
      write: false,
    });
    if (packResult.packfile === undefined) {
      throw new Error("git.packObjects returned no packfile");
    }
    const pack = packResult.packfile;

    const targetDir = await makeTempDir("repo-store-merge-target-");
    const targetHandler = createTestHandler({
      allowTopLevelPaths: () => true,
    });
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "agent-state": targetHandler },
      authorize: allowAll,
    });
    await targetStore.initRepo(repoId);

    await expect(
      targetStore.receivePack(
        principal,
        repoId,
        "refs/heads/merge",
        pack,
        mergeSha,
        null,
      ),
    ).rejects.toThrow(
      new RegExp(
        `pack_walk_multi_parent: commit ${mergeSha} has 2 parents; merge commits are not supported in repo-store packs`,
      ),
    );

    // The rejected pack must leave the ref unset so a retry sees a
    // pristine target.
    const resolved = await targetStore.resolveRef(
      principal,
      repoId,
      "refs/heads/merge",
    );
    expect(resolved).toBeNull();
    expect(targetHandler.onRefUpdatedCalls).toHaveLength(0);
  });

  // A workflow-run pack whose oldest new commit declares a parent
  // SHA the receiver does not have in its object store — and that
  // the pack itself does not carry — leaves the substrate with no
  // way to reconstruct the prior tree the kind handler validates
  // append-only invariants against. Silently collapsing to "no
  // prior" is unsafe: a handler enforcing append-only against a
  // missing prior entry accepts only a genuinely new path; a path
  // that contradicts a prior-tree entry the handler cannot read
  // would slip through unchecked. The substrate refuses workflow-run
  // packs with a dangling parent outright; the production
  // workflow-run `createPack` ships the full parent chain, so the
  // branch is unreachable on the production path.
  //
  // The substrate keeps the silent-degrade behavior for other kinds
  // (e.g. agent-state) whose deploy-shape packs intentionally omit
  // the parent chain and whose handlers do not read prior bytes;
  // gating the rejection on `repoId.kind === "workflow-run"` is what
  // keeps the agent-state state-push flow working through the same
  // substrate primitive.
  test("receivePack rejects a workflow-run pack with a dangling parent", async () => {
    const wfRepoId: RepoId = { kind: "workflow-run", id: "subject" };
    // Permissive test handler stamped as the workflow-run kind so
    // the substrate's kind-aware dangling-parent check fires without
    // pulling in the real workflow-run handler.
    const permissiveWorkflowRun = (): TestHandler => {
      const onRefUpdatedCalls: RefUpdateRecord[] = [];
      return {
        kind: "workflow-run",
        directoryPrefix: "workflow-runs-under-test",
        validatePush(): ValidatePushResult {
          return { ok: true };
        },
        onRefUpdated(args) {
          onRefUpdatedCalls.push(args);
        },
        onRefUpdatedCalls,
      };
    };

    const sourceDir = await makeTempDir("repo-store-dangling-source-");
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "workflow-run": permissiveWorkflowRun() },
      authorize: allowAll,
    });

    // Build three commits on the source so the third commit's parent
    // is the second commit. The synthesised pack below carries the
    // third commit's reachable objects with the second commit object
    // deliberately excluded, so the third commit's `parent` field
    // references a SHA the receiver cannot resolve.
    await sourceStore.writeTree(principal, wfRepoId, REF, {
      files: { "runs/r1/events/0.json": "v1" },
      message: "v1",
    });
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      wfRepoId,
      REF,
      { files: { "runs/r1/events/1.json": "v2" }, message: "v2" },
    );
    const { commitSha: thirdSha } = await sourceStore.writeTree(
      principal,
      wfRepoId,
      REF,
      { files: { "runs/r1/events/2.json": "v3" }, message: "v3" },
    );

    const sourceRepoDir = sourceStore.getRepoDir(wfRepoId);
    const reachableFromThird = await collectReachableObjects(
      sourceRepoDir,
      thirdSha,
    );
    const oids = reachableFromThird.filter((oid) => oid !== secondSha);
    const packResult = await git.packObjects({
      fs,
      dir: sourceRepoDir,
      oids,
      write: false,
    });
    if (packResult.packfile === undefined) {
      throw new Error("git.packObjects returned no packfile");
    }
    const pack = packResult.packfile;

    const targetDir = await makeTempDir("repo-store-dangling-target-");
    const targetHandler = permissiveWorkflowRun();
    const targetStore = createRepoStore({
      dataDir: targetDir,
      signingKey,
      handlers: { "workflow-run": targetHandler },
      authorize: allowAll,
    });
    await targetStore.initRepo(wfRepoId);

    await expect(
      targetStore.receivePack(principal, wfRepoId, REF, pack, thirdSha, null),
    ).rejects.toThrow(
      new RegExp(
        `pack_walk_dangling_parent: commit ${thirdSha} declares parent ${secondSha} which is neither in the receiver's store nor in the pack`,
      ),
    );

    // The rejected pack must leave the ref unset so a retry sees a
    // pristine target.
    const resolved = await targetStore.resolveRef(principal, wfRepoId, REF);
    expect(resolved).toBeNull();
    expect(targetHandler.onRefUpdatedCalls).toHaveLength(0);
  });

  // A workflow-run pack transfer cancelled by a reconnect never lands
  // an ack. The send-side "last shipped tip" must therefore advance on
  // the ack, not when the pack is built: an un-acked build has to leave
  // the tip where it was so the next rebuild re-ships the un-acked
  // commit. If the tip advanced at build time, the rebuild would walk
  // its incremental chain back only as far as the un-acked commit and
  // omit that commit's own object from the pack, stranding the receiver
  // with a dangling parent it can never resolve.
  test("createPack advances the packed tip on commitPackedTip, not at build time, so a cancelled transfer re-ships the un-acked commit", async () => {
    const makePermissiveWorkflowRunHandler = (): TestHandler => {
      const onRefUpdatedCalls: RefUpdateRecord[] = [];
      return {
        kind: "workflow-run",
        directoryPrefix: "workflow-runs-under-test",
        validatePush(): ValidatePushResult {
          return { ok: true };
        },
        onRefUpdated(args) {
          onRefUpdatedCalls.push(args);
        },
        onRefUpdatedCalls,
      };
    };

    const wfRepoId: RepoId = { kind: "workflow-run", id: "subject" };

    const sourceDir = await makeTempDir("repo-store-ack-tip-source-");
    const sourceStore = createRepoStore({
      dataDir: sourceDir,
      signingKey,
      handlers: { "workflow-run": makePermissiveWorkflowRunHandler() },
      authorize: allowAll,
    });

    // First commit and its pack. This transfer is the one a reconnect
    // cancels: the build succeeds but no ack ever lands, so the tip is
    // never committed for it.
    const { commitSha: firstSha } = await sourceStore.writeTree(
      principal,
      wfRepoId,
      REF,
      { files: { "runs/r1/events/0.json": "v1" }, message: "v1" },
    );
    const firstBuild = await sourceStore.createPack(principal, wfRepoId, REF);
    expect(firstBuild.commitSha).toBe(firstSha);

    // The reconnect cancels the transfer without an ack, then the
    // coalescing retry loop commits a second event and rebuilds. The
    // rebuild must re-include the un-acked first commit because the tip
    // was never committed.
    const { commitSha: secondSha } = await sourceStore.writeTree(
      principal,
      wfRepoId,
      REF,
      { files: { "runs/r1/events/1.json": "v2" }, message: "v2" },
    );
    const rebuild = await sourceStore.createPack(principal, wfRepoId, REF);
    expect(rebuild.commitSha).toBe(secondSha);

    // A fresh receiver that has never seen the first commit must accept
    // the rebuilt pack: it carries the full chain from genesis, so no
    // commit references a parent the receiver cannot resolve. Under a
    // build-time tip advance the rebuild's chain would stop at
    // `firstSha` and omit it, and this receive would reject with
    // `pack_walk_dangling_parent` on the second commit's parent.
    const rebuildTargetDir = await makeTempDir("repo-store-ack-tip-rebuild-");
    const rebuildTargetHandler = makePermissiveWorkflowRunHandler();
    const rebuildTargetStore = createRepoStore({
      dataDir: rebuildTargetDir,
      signingKey,
      handlers: { "workflow-run": rebuildTargetHandler },
      authorize: allowAll,
    });
    await rebuildTargetStore.initRepo(wfRepoId);
    await rebuildTargetStore.receivePack(
      principal,
      wfRepoId,
      REF,
      rebuild.pack,
      secondSha,
      null,
    );
    expect(await rebuildTargetStore.resolveRef(principal, wfRepoId, REF)).toBe(
      secondSha,
    );

    // Committing the tip on the ack lets the next build ship only the
    // commits added since: after `commitPackedTip(secondSha)`, a third
    // commit's pack walks back to `secondSha` and stops, so the pack
    // no longer carries the already-acked history. A receiver that
    // already holds the chain up to the second commit accepts this
    // incremental pack.
    sourceStore.commitPackedTip(wfRepoId, REF, secondSha);
    const { commitSha: thirdSha } = await sourceStore.writeTree(
      principal,
      wfRepoId,
      REF,
      { files: { "runs/r1/events/2.json": "v3" }, message: "v3" },
    );
    const incremental = await sourceStore.createPack(principal, wfRepoId, REF);
    expect(incremental.commitSha).toBe(thirdSha);
    await rebuildTargetStore.receivePack(
      principal,
      wfRepoId,
      REF,
      incremental.pack,
      thirdSha,
      secondSha,
    );
    expect(await rebuildTargetStore.resolveRef(principal, wfRepoId, REF)).toBe(
      thirdSha,
    );
  });

  test("openCommittedReads lists committed entries and reads blobs by oid", async () => {
    const dataDir = await makeTempDir("repo-store-committed-reads-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: {
        "runs/r1/events/0.json": "zero",
        "runs/r1/events/1.json": "one",
      },
      message: "seed events",
    });

    const reads = await store.openCommittedReads(principal, repoId, REF);
    if (reads === null) throw new Error("expected committed reads");

    const runs = await reads.listDir("runs");
    expect(runs.map((e) => e.name)).toEqual(["r1"]);
    expect(runs[0]?.type).toBe("tree");

    const events = await reads.listDir("runs/r1/events");
    expect(events.map((e) => e.name).sort()).toEqual(["0.json", "1.json"]);
    for (const e of events) expect(e.type).toBe("blob");

    const zero = events.find((e) => e.name === "0.json");
    if (zero === undefined) throw new Error("unreachable");
    const bytes = await reads.readBlobByOid(zero.oid);
    expect(new TextDecoder().decode(bytes)).toBe("zero");
  });

  test("openCommittedReads reads committed state after the working tree is wiped", async () => {
    const dataDir = await makeTempDir("repo-store-committed-vs-worktree-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": "committed" },
      message: "seed",
    });

    // Diverge the working tree from the object store: delete the
    // materialized checkout while leaving `.git` intact. A working-tree
    // read would now miss the event; a committed read must not.
    const dir = path.join(dataDir, handler.directoryPrefix, repoId.id);
    await fs.promises.rm(path.join(dir, "runs"), {
      recursive: true,
      force: true,
    });

    const reads = await store.openCommittedReads(principal, repoId, REF);
    if (reads === null) throw new Error("expected committed reads");
    const events = await reads.listDir("runs/r1/events");
    expect(events.map((e) => e.name)).toEqual(["0.json"]);
    const only = events[0];
    if (only === undefined) throw new Error("unreachable");
    const bytes = await reads.readBlobByOid(only.oid);
    expect(new TextDecoder().decode(bytes)).toBe("committed");
  });

  test("openCommittedReads returns null for a missing repo or unresolved ref", async () => {
    const dataDir = await makeTempDir("repo-store-committed-null-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    expect(await store.openCommittedReads(principal, repoId, REF)).toBeNull();

    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": "zero" },
      message: "seed",
    });
    expect(
      await store.openCommittedReads(principal, repoId, "refs/heads/absent"),
    ).toBeNull();
  });

  test("openCommittedReads listing an absent directory returns the empty array", async () => {
    const dataDir = await makeTempDir("repo-store-committed-empty-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": "zero" },
      message: "seed",
    });

    const reads = await store.openCommittedReads(principal, repoId, REF);
    if (reads === null) throw new Error("expected committed reads");
    expect(await reads.listDir("runs/absent/events")).toEqual([]);
  });

  test("openCommittedReads lists the repository root with the empty path", async () => {
    const dataDir = await makeTempDir("repo-store-committed-root-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "a.json": "x", "runs/r1/events/0.json": "y" },
      message: "seed",
    });

    const reads = await store.openCommittedReads(principal, repoId, REF);
    if (reads === null) throw new Error("expected committed reads");
    const root = await reads.listDir("");
    expect(root.map((e) => e.name).sort()).toEqual(["a.json", "runs"]);
  });

  test("openCommittedReads is gated on the resolveRef authorize action", async () => {
    const dataDir = await makeTempDir("repo-store-committed-gate-");
    const handler = createTestHandler();
    const denyResolveRef: AuthorizeFn = (_p, _r, _ref, action) =>
      action === "resolveRef"
        ? { allowed: false, reason: "no committed reads" }
        : { allowed: true };
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: denyResolveRef,
    });

    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": "zero" },
      message: "seed",
    });

    await expect(
      store.openCommittedReads(principal, repoId, REF),
    ).rejects.toThrow("authorize_denied: no committed reads");
  });

  test("openCommittedReadsAtCommit reads the pinned commit, not the ref tip", async () => {
    const dataDir = await makeTempDir("repo-store-committed-at-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const first = await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": "zero" },
      message: "c1",
    });
    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/1.json": "one" },
      message: "c2",
    });

    // Pinned to the first commit, the reads see only that commit's tree
    // even though the ref tip has advanced to include 1.json.
    const reads = await store.openCommittedReadsAtCommit(
      principal,
      repoId,
      first.commitSha,
    );
    if (reads === null) throw new Error("expected committed reads");
    const events = await reads.listDir("runs/r1/events");
    expect(events.map((e) => e.name)).toEqual(["0.json"]);
  });

  test("openCommittedReadsAtCommit returns null for a missing repo or unknown commit", async () => {
    const dataDir = await makeTempDir("repo-store-committed-at-null-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    const absent = "0".repeat(40);
    expect(
      await store.openCommittedReadsAtCommit(principal, repoId, absent),
    ).toBeNull();

    await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": "zero" },
      message: "seed",
    });
    // Well-formed SHA that names no commit in the object store.
    expect(
      await store.openCommittedReadsAtCommit(principal, repoId, absent),
    ).toBeNull();
  });

  test("openCommittedReadsAtCommit rejects a malformed commit SHA", async () => {
    const dataDir = await makeTempDir("repo-store-committed-at-bad-");
    const handler = createTestHandler();
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: allowAll,
    });

    await expect(
      store.openCommittedReadsAtCommit(principal, repoId, "not-a-sha"),
    ).rejects.toThrow("commit_sha_invalid");
  });

  test("openCommittedReadsAtCommit is gated on the resolveRef authorize action", async () => {
    const dataDir = await makeTempDir("repo-store-committed-at-gate-");
    const handler = createTestHandler();
    const denyResolveRef: AuthorizeFn = (_p, _r, _ref, action) =>
      action === "resolveRef"
        ? { allowed: false, reason: "no committed reads" }
        : { allowed: true };
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": handler },
      authorize: denyResolveRef,
    });

    const { commitSha } = await store.writeTree(principal, repoId, REF, {
      files: { "runs/r1/events/0.json": "zero" },
      message: "seed",
    });

    await expect(
      store.openCommittedReadsAtCommit(principal, repoId, commitSha),
    ).rejects.toThrow("authorize_denied: no committed reads");
  });
});

describe("RepoStore pack receive reads", () => {
  // More git calls than the store's per-repo cache budget (8192 calls
  // between rebuilds), so one receive of this many reads would cross a
  // rebuild if the receive took the cache per call.
  const READS_OVER_BUDGET = 8300;

  // A handler that reads one blob `count` times in each validatePush, so
  // a test controls how many git calls a receive makes.
  function readingHandler(
    kind: RepoId["kind"],
    count: number,
    onRefUpdated: () => void = () => undefined,
  ): KindHandler {
    return {
      kind,
      directoryPrefix: "repos-under-test",
      async validatePush({ readBlob }): Promise<ValidatePushResult> {
        for (let i = 0; i < count; i++) {
          await readBlob("deploy/payload.txt");
        }
        return { ok: true };
      },
      onRefUpdated,
    };
  }

  async function sourcePack(id: RepoId): Promise<{
    pack: Uint8Array;
    commitSha: string;
  }> {
    const sourceStore = createRepoStore({
      dataDir: await makeTempDir("repo-store-reads-source-"),
      signingKey,
      handlers: { [id.kind]: { ...createTestHandler(), kind: id.kind } },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, id, REF, {
      files: { "deploy/payload.txt": "payload" },
      message: "payload",
    });
    const { pack } = await sourceStore.createPack(principal, id, REF);
    return { pack, commitSha };
  }

  async function receivingStore(
    prefix: string,
    id: RepoId,
    count: number,
    onRefUpdated?: () => void,
  ): Promise<{ store: ReturnType<typeof createRepoStore>; repoDir: string }> {
    const dataDir = await makeTempDir(prefix);
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { [id.kind]: readingHandler(id.kind, count, onRefUpdated) },
      authorize: allowAll,
    });
    await store.initRepo(id);
    return {
      store,
      repoDir: path.join(dataDir, "repos-under-test", id.id),
    };
  }

  // The pack file a read in `repoDir` loads, by the name it has in
  // objects/pack, or null for any other file. A receive reads a pack
  // from its staging path until it publishes it, so a staged path maps
  // to the published name: one pack counts under one name, staged or
  // published.
  function packFileOf(repoDir: string, target: string): string | null {
    const objectsDir = path.join(repoDir, ".git", "objects");
    const packDir = path.join(objectsDir, "pack") + path.sep;
    if (
      target.startsWith(packDir) &&
      (target.endsWith(".idx") || target.endsWith(".pack"))
    ) {
      return target;
    }
    const stagingDir = path.join(objectsDir, "pack-staging") + path.sep;
    if (!target.startsWith(stagingDir)) return null;
    const [transferId, file] = target
      .substring(stagingDir.length)
      .split(path.sep);
    if (
      transferId === undefined ||
      (file !== "pack.idx" && file !== "pack.pack")
    )
      return null;
    return `${packDir}pack-recv-${transferId}${path.extname(file)}`;
  }

  // Record, in order, the pack file each pack read in `repoDir` loads
  // while `fn` runs. isomorphic-git reads a pack file only when its cache
  // has no entry for that pack. The receive's own index of a staged pack
  // also reads the pack once, which is not a cache load, so reads during
  // `git.indexPack` are left out. `fn` gets the loads so far.
  async function recordPackLoads(
    repoDir: string,
    fn: (loadsSoFar: () => string[]) => Promise<unknown>,
  ): Promise<string[]> {
    // Ranges of readFile calls made while an index runs.
    const indexingCalls: [number, number][] = [];
    const readFileSpy = spyOn(fs.promises, "readFile");
    const originalIndexPack = git.indexPack.bind(git);
    const indexPackSpy = spyOn(git, "indexPack").mockImplementation(
      async (args) => {
        const start = readFileSpy.mock.calls.length;
        try {
          return await originalIndexPack(args);
        } finally {
          indexingCalls.push([start, readFileSpy.mock.calls.length]);
        }
      },
    );
    const loadsSoFar = (): string[] =>
      readFileSpy.mock.calls.flatMap(([target], i) => {
        const name = packFileOf(repoDir, String(target));
        const indexing = indexingCalls.some(
          ([start, end]) => i >= start && i < end,
        );
        return name === null || indexing ? [] : [name];
      });
    try {
      await fn(loadsSoFar);
      return loadsSoFar();
    } finally {
      readFileSpy.mockRestore();
      indexPackSpy.mockRestore();
    }
  }

  function countLoads(loads: readonly string[]): Map<string, number> {
    const counts = new Map<string, number>();
    for (const name of loads) counts.set(name, (counts.get(name) ?? 0) + 1);
    return counts;
  }

  // Count the loads of each .idx and .pack file in `repoDir` while `fn`
  // runs.
  async function countPackLoads(
    repoDir: string,
    fn: () => Promise<unknown>,
  ): Promise<Map<string, number>> {
    return countLoads(await recordPackLoads(repoDir, fn));
  }

  async function readPayload(
    store: ReturnType<typeof createRepoStore>,
    id: RepoId,
  ): Promise<string> {
    const committed = await store.openCommittedReads(principal, id, REF);
    if (committed === null) throw new Error("expected committed reads");
    const payload = (await committed.listDir("deploy")).find(
      (e) => e.name === "payload.txt",
    );
    if (payload === undefined) throw new Error("expected deploy/payload.txt");
    return new TextDecoder().decode(await committed.readBlobByOid(payload.oid));
  }

  test("one receive's validation loads each pack once although its reads pass the cache budget", async () => {
    const { pack, commitSha } = await sourcePack(repoId);
    // The ref update comes after the validation, which ends with the
    // charge of its reads to the budget.
    let loadsSoFar: () => string[] = () => [];
    let loadsAtRefUpdate: string[] = [];
    const { store, repoDir } = await receivingStore(
      "repo-store-reads-budget-",
      repoId,
      READS_OVER_BUDGET,
      () => {
        loadsAtRefUpdate = loadsSoFar();
      },
    );

    await recordPackLoads(repoDir, async (soFar) => {
      loadsSoFar = soFar;
      await store.receivePack(principal, repoId, REF, pack, commitSha, null);
    });

    const loads = countLoads(loadsAtRefUpdate);
    const names = [...loads.keys()];
    expect(names.some((n) => n.endsWith(".idx"))).toBe(true);
    expect(names.some((n) => n.endsWith(".pack"))).toBe(true);
    for (const [name, count] of loads) {
      expect({ name, count }).toEqual({ name, count: 1 });
    }
  });

  test("a receive keeps the cache, so a read after it loads no pack", async () => {
    const { pack, commitSha } = await sourcePack(repoId);
    const { store, repoDir } = await receivingStore(
      "repo-store-reads-kept-",
      repoId,
      1,
    );
    await store.receivePack(principal, repoId, REF, pack, commitSha, null);

    const loads = await countPackLoads(repoDir, () =>
      readPayload(store, repoId),
    );
    expect([...loads.entries()]).toEqual([]);
  });

  test("a receive charges its validation reads to the cache budget, so a later read rebuilds the cache", async () => {
    const { pack, commitSha } = await sourcePack(repoId);
    const { store, repoDir } = await receivingStore(
      "repo-store-reads-charged-",
      repoId,
      READS_OVER_BUDGET,
    );

    // The validation loads the pack once and spends more calls than the
    // budget, so the first read after it rebuilds the cache and loads
    // the pack index again, whether that read is the receive's own ref
    // update or the read below.
    const loads = await countPackLoads(repoDir, async () => {
      await store.receivePack(principal, repoId, REF, pack, commitSha, null);
      await readPayload(store, repoId);
    });
    const idxLoads = [...loads.entries()].filter(([n]) => n.endsWith(".idx"));
    expect(idxLoads.length).toBeGreaterThan(0);
    for (const [name, count] of idxLoads) {
      expect({ name, count }).toEqual({ name, count: 2 });
    }
  });

  test("a prepared receive keeps the cache, so a read after its commit loads no pack", async () => {
    // prepareReceivePack accepts only the workflow-run kind.
    const runRepoId: RepoId = { kind: "workflow-run", id: "subject" };
    const { pack, commitSha } = await sourcePack(runRepoId);
    const { store, repoDir } = await receivingStore(
      "repo-store-reads-prepared-kept-",
      runRepoId,
      1,
    );
    const prepared = await store.prepareReceivePack(
      principal,
      runRepoId,
      REF,
      pack,
      commitSha,
    );
    if (prepared.kind !== "prepared") throw new Error("expected prepared");
    expect((await prepared.commit()).kind).toBe("applied");

    const loads = await countPackLoads(repoDir, () =>
      readPayload(store, runRepoId),
    );
    expect([...loads.entries()]).toEqual([]);
  });

  test("a rejected receive still charges its reads to the budget", async () => {
    const sourceStore = createRepoStore({
      dataDir: await makeTempDir("repo-store-reads-rejected-source-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    const first = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/payload.txt": "payload 0" },
      message: "payload 0",
    });
    const firstPack = await sourceStore.createPack(principal, repoId, REF);
    const second = await sourceStore.writeTree(principal, repoId, REF, {
      files: { "deploy/payload.txt": "payload 1" },
      message: "payload 1",
    });
    const secondPack = await sourceStore.createPack(principal, repoId, REF);

    let reads = 1;
    let accept = true;
    const dataDir = await makeTempDir("repo-store-reads-rejected-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: {
        "agent-state": {
          kind: "agent-state",
          directoryPrefix: "repos-under-test",
          async validatePush({ readBlob }): Promise<ValidatePushResult> {
            for (let i = 0; i < reads; i++) {
              await readBlob("deploy/payload.txt");
            }
            return accept ? { ok: true } : { ok: false, reason: "rejected" };
          },
          onRefUpdated() {
            return;
          },
        },
      },
      authorize: allowAll,
    });
    await store.initRepo(repoId);
    await store.receivePack(
      principal,
      repoId,
      REF,
      firstPack.pack,
      first.commitSha,
      null,
    );

    reads = READS_OVER_BUDGET;
    accept = false;
    await expect(
      store.receivePack(
        principal,
        repoId,
        REF,
        secondPack.pack,
        second.commitSha,
        first.commitSha,
      ),
    ).rejects.toThrow("rejected");

    // The rejected receive spent more calls than the budget, so the
    // next read rebuilds the cache and loads the pack index again.
    let payload: string | undefined;
    const loads = await countPackLoads(
      path.join(dataDir, "repos-under-test", repoId.id),
      async () => {
        payload = await readPayload(store, repoId);
      },
    );
    expect(payload).toBe("payload 0");
    expect([...loads.keys()].some((n) => n.endsWith(".idx"))).toBe(true);
  });

  test("reads stay correct across GC repacks between receives", async () => {
    const RECEIVES = 6;
    const sourceStore = createRepoStore({
      dataDir: await makeTempDir("repo-store-reads-gc-source-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });

    const priors: (string | null)[] = [];
    const dataDir = await makeTempDir("repo-store-reads-gc-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: {
        "agent-state": {
          kind: "agent-state",
          directoryPrefix: "repos-under-test",
          async validatePush({
            readBlob,
            priorReadBlob,
          }): Promise<ValidatePushResult> {
            await readBlob("deploy/payload.txt");
            const prior = await priorReadBlob("deploy/payload.txt");
            priors.push(
              prior === null ? null : new TextDecoder().decode(prior),
            );
            return { ok: true };
          },
          onRefUpdated() {
            return;
          },
        },
      },
      authorize: allowAll,
      // A low pack threshold makes the receives repack several times,
      // so later receives read objects from a GC pack.
      gc: {
        kinds: ["agent-state"],
        packThreshold: 2,
        looseThreshold: 1e6,
        warnBytes: 1e12,
        retention: "keep-history",
      },
    });
    await store.initRepo(repoId);

    let tip: string | null = null;
    for (let i = 0; i < RECEIVES; i++) {
      const { commitSha } = await sourceStore.writeTree(
        principal,
        repoId,
        REF,
        {
          files: { "deploy/payload.txt": `payload ${String(i)}` },
          message: `payload ${String(i)}`,
        },
      );
      const { pack } = await sourceStore.createPack(principal, repoId, REF);
      await store.receivePack(principal, repoId, REF, pack, commitSha, tip);
      tip = commitSha;
      expect(await readPayload(store, repoId)).toBe(`payload ${String(i)}`);
    }

    expect(priors).toEqual([
      null,
      ...Array.from({ length: RECEIVES - 1 }, (_, i) => `payload ${String(i)}`),
    ]);
    const packNames = await fs.promises.readdir(
      path.join(
        dataDir,
        "repos-under-test",
        repoId.id,
        ".git",
        "objects",
        "pack",
      ),
    );
    // A GC repack publishes its pack under the transfer id `gc-<uuid>`.
    expect(packNames.some((n) => n.startsWith("pack-recv-gc-"))).toBe(true);
  });

  test("a multi-commit receive parses each commit and tree a bounded number of times", async () => {
    // Each commit adds one file to `deploy/`, and validation reads every
    // file on both sides. Without a memo, each path read parses the root
    // and `deploy/` trees again, so the parses grow as the square of the
    // commit count.
    const COMMITS = 24;
    const srcDir = await makeTempDir("repo-store-reads-memo-src-");
    await git.init({ fs, dir: srcDir, defaultBranch: "test" });
    const shas: string[] = [];
    for (let i = 0; i < COMMITS; i++) {
      const rel = `deploy/f${String(i)}.txt`;
      await fs.promises.mkdir(path.join(srcDir, "deploy"), {
        recursive: true,
      });
      await fs.promises.writeFile(path.join(srcDir, rel), `body ${String(i)}`);
      await git.add({ fs, dir: srcDir, filepath: rel });
      shas.push(
        await git.commit({
          fs,
          dir: srcDir,
          message: `commit ${String(i)}`,
          author: { name: "t", email: "t@t.dev" },
          ref: REF,
        }),
      );
    }
    const tip = shas.at(-1);
    if (tip === undefined) throw new Error("unreachable");
    const oids = new Set<string>();
    for (const sha of shas) {
      for (const oid of await collectReachableObjects(srcDir, sha)) {
        oids.add(oid);
      }
    }
    const { packfile } = await git.packObjects({
      fs,
      dir: srcDir,
      oids: [...oids],
    });
    if (packfile === undefined) throw new Error("no packfile");

    let validated = 0;
    const store = createRepoStore({
      dataDir: await makeTempDir("repo-store-reads-memo-tgt-"),
      signingKey,
      handlers: {
        "agent-state": {
          kind: "agent-state",
          directoryPrefix: "repos-under-test",
          async validatePush({
            listDir,
            readBlob,
            priorReadBlob,
          }): Promise<ValidatePushResult> {
            validated += 1;
            for (const name of await listDir("deploy")) {
              await readBlob(`deploy/${name}`);
              await priorReadBlob(`deploy/${name}`);
            }
            return { ok: true };
          },
          onRefUpdated() {
            return;
          },
        },
      },
      authorize: allowAll,
    });

    const readTree = spyOn(git, "readTree");
    const readCommit = spyOn(git, "readCommit");
    try {
      await store.receivePack(principal, repoId, REF, packfile, tip, null);
      expect(validated).toBe(COMMITS);
      // Per commit: its root and `deploy/` trees, and its commit read by
      // the walk and by validation. A few more reads come from the tip
      // checks in the pack receive.
      expect(readTree.mock.calls.length).toBeLessThanOrEqual(2 * COMMITS + 4);
      expect(readCommit.mock.calls.length).toBeLessThanOrEqual(2 * COMMITS + 4);
    } finally {
      readTree.mockRestore();
      readCommit.mockRestore();
    }
  });

  test("a receive resolves each path in a wide directory with one lookup, not a scan", async () => {
    // Validation reads every blob of one wide directory by path. A scan
    // of the directory per path reads about WIDE^2 / 2 tree entries; a
    // lookup by name reads each entry a fixed number of times.
    const WIDE = 2000;
    const files: Record<string, string> = {};
    for (let i = 0; i < WIDE; i++) {
      files[`wide/f${String(i)}.txt`] = `body ${String(i)}`;
    }
    const sourceStore = createRepoStore({
      dataDir: await makeTempDir("repo-store-reads-wide-source-"),
      signingKey,
      handlers: { "agent-state": createTestHandler() },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files,
      message: "wide",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    let read = 0;
    const store = createRepoStore({
      dataDir: await makeTempDir("repo-store-reads-wide-target-"),
      signingKey,
      handlers: {
        "agent-state": {
          kind: "agent-state",
          directoryPrefix: "repos-under-test",
          async validatePush({
            listDir,
            readBlob,
          }): Promise<ValidatePushResult> {
            for (const name of await listDir("wide")) {
              await readBlob(`wide/${name}`);
              read += 1;
            }
            return { ok: true };
          },
          onRefUpdated() {
            return;
          },
        },
      },
      authorize: allowAll,
    });
    await store.initRepo(repoId);

    // Count each access to an entry of a tree that git.readTree returns.
    let entryReads = 0;
    const readTree = git.readTree.bind(git);
    const spy = spyOn(git, "readTree").mockImplementation(async (args) => {
      const result = await readTree(args);
      const counted = new Proxy(result.tree, {
        get(target, key, receiver) {
          if (typeof key === "string" && /^[0-9]+$/.test(key)) entryReads += 1;
          return Reflect.get(target, key, receiver) as unknown;
        },
      });
      return { ...result, tree: counted };
    });
    try {
      await store.receivePack(principal, repoId, REF, pack, commitSha, null);
    } finally {
      spy.mockRestore();
    }
    expect(read).toBe(WIDE);
    // The wide tree is read once, then indexed by name and listed a few
    // times; a scan per path would read about 2,000,000 entries.
    expect(entryReads).toBeLessThanOrEqual(6 * WIDE);
  });

  test("a received pack's validatePush gets the same listDirOids as a writeTree of the same tree", async () => {
    const listedPaths = ["", "deploy", "deploy/sub", "absent"];
    type Listing = { name: string; oid: string; type: string }[];

    function listingHandler(out: Map<string, Listing>): KindHandler {
      return {
        kind: "agent-state",
        directoryPrefix: "repos-under-test",
        async validatePush({ listDirOids }): Promise<ValidatePushResult> {
          for (const p of listedPaths) out.set(p, await listDirOids(p));
          return { ok: true };
        },
        onRefUpdated() {
          return;
        },
      };
    }

    const written = new Map<string, Listing>();
    const sourceStore = createRepoStore({
      dataDir: await makeTempDir("repo-store-listing-source-"),
      signingKey,
      handlers: { "agent-state": listingHandler(written) },
      authorize: allowAll,
    });
    const { commitSha } = await sourceStore.writeTree(principal, repoId, REF, {
      files: {
        "top.txt": "top",
        "deploy/a.txt": "a",
        "deploy/sub/b.txt": "b",
      },
      message: "listing",
    });
    const { pack } = await sourceStore.createPack(principal, repoId, REF);

    const received = new Map<string, Listing>();
    const targetStore = createRepoStore({
      dataDir: await makeTempDir("repo-store-listing-target-"),
      signingKey,
      handlers: { "agent-state": listingHandler(received) },
      authorize: allowAll,
    });
    await targetStore.receivePack(
      principal,
      repoId,
      REF,
      pack,
      commitSha,
      null,
    );

    expect(received).toEqual(written);
    expect(
      received.get("deploy")?.map((e) => ({ name: e.name, type: e.type })),
    ).toEqual([
      { name: "a.txt", type: "blob" },
      { name: "sub", type: "tree" },
    ]);
    expect(received.get("deploy/sub")?.map((e) => e.name)).toEqual(["b.txt"]);
    expect(received.get("absent")).toEqual([]);
  });
});

describe("RepoStore prepared pack receive", () => {
  // prepareReceivePack accepts only the workflow-run kind.
  const repoId: RepoId = { kind: "workflow-run", id: "subject" };
  const decoder = new TextDecoder();

  function runOf(content: string): NewlyTerminalRun {
    return { runId: content, status: "completed", terminalEventJson: "{}" };
  }

  // A pack that carries every commit, tree and blob from the root up to
  // `tip`, so a receive validates each commit against its own parent.
  async function historyPack(dir: string, tip: string): Promise<Uint8Array> {
    const oids = new Set<string>();
    for (const entry of await git.log({ fs, dir, ref: tip })) {
      for (const oid of await collectReachableObjects(dir, entry.oid)) {
        oids.add(oid);
      }
    }
    const { packfile } = await git.packObjects({ fs, dir, oids: [...oids] });
    if (packfile === undefined) throw new Error("packObjects");
    return packfile;
  }

  // Commits on one line of `REF`, each setting `deploy/a.md` to one of
  // `contents`.
  async function sourceLine(contents: readonly string[]): Promise<{
    commits: string[];
    packAt: (commitSha: string) => Promise<Uint8Array>;
  }> {
    const dataDir = await makeTempDir("repo-store-prepared-source-");
    const source = createRepoStore({
      dataDir,
      signingKey,
      handlers: {
        "workflow-run": { ...createTestHandler(), kind: "workflow-run" },
      },
      authorize: allowAll,
    });
    const commits: string[] = [];
    for (const content of contents) {
      const { commitSha } = await source.writeTree(principal, repoId, REF, {
        files: { "deploy/a.md": content },
        message: content,
      });
      commits.push(commitSha);
    }
    const dir = path.join(dataDir, "repos-under-test", repoId.id);
    return { commits, packAt: (commitSha) => historyPack(dir, commitSha) };
  }

  // Holds the validation of the commit whose `deploy/a.md` is `content`
  // until the test releases it, once. `release(false)` makes that
  // validation reject.
  function holdAt(content: string): {
    reached: Promise<void>;
    release: (accept: boolean) => void;
    check: (text: string) => Promise<boolean>;
  } {
    let markReached: () => void = () => undefined;
    const reached = new Promise<void>((resolve) => {
      markReached = resolve;
    });
    let release: (accept: boolean) => void = () => undefined;
    const released = new Promise<boolean>((resolve) => {
      release = resolve;
    });
    let armed = true;
    return {
      reached,
      release,
      async check(text) {
        if (!armed || text !== content) return true;
        armed = false;
        markReached();
        return released;
      },
    };
  }

  // A handler that reports one run per commit, named by the commit's
  // `deploy/a.md`. A commit without `deploy/a.md` carries no run. The
  // source line's genesis is such a commit, and a pack carries it when
  // it differs from the target's genesis: each store writes its own,
  // and the two match only when written in the same second.
  function runsHandler(
    check: (text: string) => Promise<boolean> = async () => true,
  ): TestHandler {
    const onRefUpdatedCalls: RefUpdateRecord[] = [];
    return {
      kind: "workflow-run",
      directoryPrefix: "repos-under-test",
      async validatePush({ listDir, readBlob }): Promise<ValidatePushResult> {
        if (!(await listDir("deploy")).includes("a.md")) {
          return { ok: true, newlyTerminalRuns: [] };
        }
        const text = decoder.decode(await readBlob("deploy/a.md"));
        if (!(await check(text))) {
          return { ok: false, reason: `rejected ${text}` };
        }
        return { ok: true, newlyTerminalRuns: [runOf(text)] };
      },
      onRefUpdated(args) {
        onRefUpdatedCalls.push(args);
      },
      onRefUpdatedCalls,
    };
  }

  async function targetStore(handler: KindHandler): Promise<{
    store: ReturnType<typeof createRepoStore>;
    dataDir: string;
    repoDirOf: (id: RepoId) => string;
  }> {
    const dataDir = await makeTempDir("repo-store-prepared-target-");
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": handler },
      authorize: allowAll,
    });
    return {
      store,
      dataDir,
      repoDirOf: (id) => path.join(dataDir, "repos-under-test", id.id),
    };
  }

  async function prepare(
    store: ReturnType<typeof createRepoStore>,
    id: RepoId,
    pack: Uint8Array,
    commitSha: string,
  ): Promise<{
    commit: () => Promise<PackCommitResult>;
    discard: () => Promise<void>;
  }> {
    const prepared = await store.prepareReceivePack(
      principal,
      id,
      REF,
      pack,
      commitSha,
    );
    if (prepared.kind !== "prepared") throw new Error("expected prepared");
    return prepared;
  }

  async function receivedPacks(repoDir: string): Promise<string[]> {
    const entries = await fs.promises
      .readdir(path.join(repoDir, ".git", "objects", "pack"))
      .catch(() => []);
    return entries
      .filter((e) => e.startsWith("pack-recv-") && e.endsWith(".pack"))
      .sort();
  }

  async function stagedPacks(repoDir: string): Promise<string[]> {
    return fs.promises
      .readdir(path.join(repoDir, ".git", "objects", "pack-staging"))
      .catch(() => []);
  }

  test("prepare then commit moves the ref and returns each commit's runs", async () => {
    const line = await sourceLine(["v1", "v2"]);
    const [, tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const handler = runsHandler();
    const { store, repoDirOf } = await targetStore(handler);

    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);
    expect(await store.resolveRef(principal, repoId, REF)).toBeNull();
    expect(await receivedPacks(repoDirOf(repoId))).toEqual([]);

    expect(await prepared.commit()).toEqual({
      kind: "applied",
      newlyTerminalRuns: [runOf("v1"), runOf("v2")],
    });
    expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
    expect(handler.onRefUpdatedCalls).toEqual([
      { repoId, ref: REF, oldSha: null, newSha: tip },
    ]);
    expect(await receivedPacks(repoDirOf(repoId))).toHaveLength(1);
    expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);
    await expect(prepared.commit()).rejects.toThrow(/already committed/);
  });

  test("a pack whose objects exist only in staging validates", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    let repoDir = "";
    let unpublishedRead: unknown = null;
    const handler = runsHandler(async () => {
      // The validation reads the pack, yet a reader of the repo's own
      // files cannot find its commit.
      unpublishedRead = await git
        .readCommit({ fs, dir: repoDir, oid: tip })
        .then(
          () => "found",
          (err: unknown) => err,
        );
      return true;
    });
    const target = await targetStore(handler);
    repoDir = target.repoDirOf(repoId);

    const prepared = await prepare(
      target.store,
      repoId,
      await line.packAt(tip),
      tip,
    );

    expect(unpublishedRead).toBeInstanceOf(Error);
    expect(await receivedPacks(repoDir)).toEqual([]);
    expect(await stagedPacks(repoDir)).toHaveLength(1);
    expect((await prepared.commit()).kind).toBe("applied");
    expect((await git.readCommit({ fs, dir: repoDir, oid: tip })).oid).toBe(
      tip,
    );
  });

  test("a prepare whose validation rejects the pack discards the staged pack", async () => {
    const line = await sourceLine(["v1", "v2"]);
    const [first, tip] = line.commits;
    if (first === undefined || tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(
      runsHandler(async (text) => text !== "v2"),
    );
    await (
      await prepare(store, repoId, await line.packAt(first), first)
    ).commit();
    const repoDir = repoDirOf(repoId);
    const published = await receivedPacks(repoDir);

    await expect(
      store.prepareReceivePack(
        principal,
        repoId,
        REF,
        await line.packAt(tip),
        tip,
      ),
    ).rejects.toThrow(/^path_violation: .*rejected v2/);

    expect(await stagedPacks(repoDir)).toEqual([]);
    expect(await receivedPacks(repoDir)).toEqual(published);
    expect(await store.resolveRef(principal, repoId, REF)).toBe(first);
  });

  test("while one prepare validates, another repo's commit and the same repo's one-call receive proceed", async () => {
    const line = await sourceLine(["v1", "v2"]);
    const [first, tip] = line.commits;
    if (first === undefined || tip === undefined) throw new Error("line");
    const hold = holdAt("v2");
    const { store } = await targetStore(runsHandler(hold.check));
    const other: RepoId = { kind: "workflow-run", id: "other" };

    const preparing = store.prepareReceivePack(
      principal,
      repoId,
      REF,
      await line.packAt(tip),
      tip,
    );
    await hold.reached;

    // Neither of these can finish while the held validation keeps a
    // lock they need; the lane timeout reports a hang.
    const otherPrepared = await prepare(
      store,
      other,
      await line.packAt(tip),
      tip,
    );
    expect((await otherPrepared.commit()).kind).toBe("applied");
    await store.receivePack(
      principal,
      repoId,
      REF,
      await line.packAt(first),
      first,
      null,
    );
    expect(await store.resolveRef(principal, repoId, REF)).toBe(first);

    hold.release(true);
    const prepared = await preparing;
    if (prepared.kind !== "prepared") throw new Error("expected prepared");
    expect(await prepared.commit()).toEqual({
      kind: "applied",
      newlyTerminalRuns: [runOf("v2")],
    });
    expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
    expect(await store.resolveRef(principal, other, REF)).toBe(tip);
  });

  test("a ref moved to a commit in the prepared chain gets the rest and only the later runs", async () => {
    const line = await sourceLine(["v1", "v2", "v3"]);
    const [, middle, tip] = line.commits;
    if (middle === undefined || tip === undefined) throw new Error("line");
    const handler = runsHandler();
    const { store } = await targetStore(handler);

    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);
    await store.receivePack(
      principal,
      repoId,
      REF,
      await line.packAt(middle),
      middle,
      null,
    );

    expect(await prepared.commit()).toEqual({
      kind: "applied",
      newlyTerminalRuns: [runOf("v3")],
    });
    expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
    expect(handler.onRefUpdatedCalls.at(-1)).toEqual({
      repoId,
      ref: REF,
      oldSha: middle,
      newSha: tip,
    });
  });

  test("a ref moved to a descendant of the pack tip answers already_applied", async () => {
    const line = await sourceLine(["v1", "v2", "v3"]);
    const [, middle, tip] = line.commits;
    if (middle === undefined || tip === undefined) throw new Error("line");
    const handler = runsHandler();
    const { store, repoDirOf } = await targetStore(handler);

    const prepared = await prepare(
      store,
      repoId,
      await line.packAt(middle),
      middle,
    );
    await store.receivePack(
      principal,
      repoId,
      REF,
      await line.packAt(tip),
      tip,
      null,
    );
    const packsBefore = await receivedPacks(repoDirOf(repoId));
    const updatesBefore = handler.onRefUpdatedCalls.length;

    expect(await prepared.commit()).toEqual({ kind: "already_applied" });
    expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
    expect(await receivedPacks(repoDirOf(repoId))).toEqual(packsBefore);
    expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);
    expect(handler.onRefUpdatedCalls).toHaveLength(updatesBefore);
  });

  test("a ref moved to an unrelated commit gets the pack tip and every run", async () => {
    const line = await sourceLine(["v1", "v2"]);
    const unrelated = await sourceLine(["u1"]);
    const [, tip] = line.commits;
    const [otherTip] = unrelated.commits;
    if (tip === undefined || otherTip === undefined) throw new Error("line");
    const { store } = await targetStore(runsHandler());

    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);
    await store.receivePack(
      principal,
      repoId,
      REF,
      await unrelated.packAt(otherTip),
      otherTip,
      null,
    );

    expect(await prepared.commit()).toEqual({
      kind: "applied",
      newlyTerminalRuns: [runOf("v1"), runOf("v2")],
    });
    expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
  });

  test("discard leaves no staged pack and nothing in objects/pack", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(runsHandler());

    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);
    expect(await stagedPacks(repoDirOf(repoId))).toHaveLength(1);
    await prepared.discard();
    await prepared.discard();

    expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);
    expect(await receivedPacks(repoDirOf(repoId))).toEqual([]);
    expect(await store.resolveRef(principal, repoId, REF)).toBeNull();
    await expect(prepared.commit()).rejects.toThrow(/already discarded/);
  });

  test("a discard called in the same tick as commit does nothing", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(runsHandler());

    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);
    const committing = prepared.commit();
    await prepared.discard();

    expect(await committing).toEqual({
      kind: "applied",
      newlyTerminalRuns: [runOf("v1")],
    });
    expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
    expect(await receivedPacks(repoDirOf(repoId))).toHaveLength(1);
    expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);
  });

  test("two prepares of one pack commit once and then answer already_applied", async () => {
    const line = await sourceLine(["v1", "v2"]);
    const [, tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const handler = runsHandler();
    const { store, repoDirOf } = await targetStore(handler);
    const pack = await line.packAt(tip);

    const a = await prepare(store, repoId, pack, tip);
    const b = await prepare(store, repoId, pack, tip);

    expect(await a.commit()).toEqual({
      kind: "applied",
      newlyTerminalRuns: [runOf("v1"), runOf("v2")],
    });
    expect(await b.commit()).toEqual({ kind: "already_applied" });
    expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
    expect(handler.onRefUpdatedCalls).toEqual([
      { repoId, ref: REF, oldSha: null, newSha: tip },
    ]);
    expect(await receivedPacks(repoDirOf(repoId))).toHaveLength(1);
    expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);
  });

  test("a commit after removeRepo and initRepo reports the removed repo", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(runsHandler());

    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);
    await store.removeRepo(repoId);
    await store.initRepo(repoId);

    await expect(prepared.commit()).rejects.toBeInstanceOf(
      RepoRemovedDuringReceiveError,
    );
    expect(await store.resolveRef(principal, repoId, REF)).toBeNull();
    expect(await receivedPacks(repoDirOf(repoId))).toEqual([]);
    expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);
  });

  test("a validation that fails after removeRepo reports the removed repo and recreates nothing", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const hold = holdAt("v1");
    const { store, repoDirOf } = await targetStore(runsHandler(hold.check));

    const preparing = store.prepareReceivePack(
      principal,
      repoId,
      REF,
      await line.packAt(tip),
      tip,
    );
    await hold.reached;
    // removeRepo takes the repo lock, so it finishing proves the
    // validation holds none.
    await store.removeRepo(repoId);
    hold.release(false);

    await expect(preparing).rejects.toBeInstanceOf(
      RepoRemovedDuringReceiveError,
    );
    await expect(fs.promises.access(repoDirOf(repoId))).rejects.toThrow();
  });

  test("a validation that passes after removeRepo fails its commit and recreates nothing", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const hold = holdAt("v1");
    const { store, repoDirOf } = await targetStore(runsHandler(hold.check));

    const preparing = store.prepareReceivePack(
      principal,
      repoId,
      REF,
      await line.packAt(tip),
      tip,
    );
    await hold.reached;
    await store.removeRepo(repoId);
    hold.release(true);

    // The pack may still read from the cache after its files are gone, so
    // the validation can pass; the commit then finds the repo removed.
    const outcome = await preparing
      .then(async (prepared) =>
        prepared.kind === "prepared" ? prepared.commit() : prepared,
      )
      .catch((err: unknown) => err);
    expect(outcome).toBeInstanceOf(RepoRemovedDuringReceiveError);
    await expect(fs.promises.access(repoDirOf(repoId))).rejects.toThrow();
  });

  test("removeRepo during a stage waits for it and succeeds", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(runsHandler());
    const pack = await line.packAt(tip);

    const originalIndexPack = git.indexPack.bind(git);
    let markIndexing: () => void = () => undefined;
    const indexing = new Promise<void>((resolve) => {
      markIndexing = resolve;
    });
    let releaseIndexing: () => void = () => undefined;
    const indexingReleased = new Promise<void>((resolve) => {
      releaseIndexing = resolve;
    });
    const indexPackSpy = spyOn(git, "indexPack").mockImplementation(
      async (args) => {
        markIndexing();
        await indexingReleased;
        return originalIndexPack(args);
      },
    );
    try {
      const preparing = store.prepareReceivePack(
        principal,
        repoId,
        REF,
        pack,
        tip,
      );
      await indexing;
      const removing = store.removeRepo(repoId);
      releaseIndexing();

      await removing;
      const outcome = await preparing
        .then(async (prepared) =>
          prepared.kind === "prepared" ? prepared.commit() : prepared,
        )
        .catch((err: unknown) => err);
      expect(outcome).toBeInstanceOf(RepoRemovedDuringReceiveError);
      await expect(fs.promises.access(repoDirOf(repoId))).rejects.toThrow();
    } finally {
      releaseIndexing();
      indexPackSpy.mockRestore();
    }
  });

  // Holds removeRepo's removal of `repoDir` until the test releases it.
  // `touchedStaging` tells if a staged pack under `repoDir` was touched
  // while the removal was held. A staged pack removal starts with an
  // lstat of its dir.
  function holdRepoRemoval(repoDir: string): {
    reached: Promise<void>;
    release: () => void;
    touchedStaging: () => boolean;
    restore: () => void;
  } {
    const stagingRoot = path.join(repoDir, ".git", "objects", "pack-staging");
    let markReached: () => void = () => undefined;
    const reached = new Promise<void>((resolve) => {
      markReached = resolve;
    });
    let releaseRemoval: () => void = () => undefined;
    const released = new Promise<void>((resolve) => {
      releaseRemoval = resolve;
    });
    const lstatSpy = spyOn(fs.promises, "lstat");
    let heldFrom: number | null = null;
    let heldTo: number | null = null;
    const rm = fs.promises.rm.bind(fs.promises);
    const rmSpy = spyOn(fs.promises, "rm").mockImplementation(
      async (target, options) => {
        if (target === repoDir) {
          heldFrom = lstatSpy.mock.calls.length;
          markReached();
          await released;
          heldTo = lstatSpy.mock.calls.length;
        }
        return rm(target, options);
      },
    );
    return {
      reached,
      release: releaseRemoval,
      touchedStaging: () =>
        heldFrom !== null &&
        lstatSpy.mock.calls
          .slice(heldFrom, heldTo ?? undefined)
          .some(([target]) => String(target).startsWith(stagingRoot)),
      restore: () => {
        releaseRemoval();
        rmSpy.mockRestore();
        lstatSpy.mockRestore();
      },
    };
  }

  test("the discard of a failed validation waits for removeRepo", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const hold = holdAt("v1");
    const { store, repoDirOf } = await targetStore(runsHandler(hold.check));
    const pack = await line.packAt(tip);

    const removal = holdRepoRemoval(repoDirOf(repoId));
    try {
      const preparing = store.prepareReceivePack(
        principal,
        repoId,
        REF,
        pack,
        tip,
      );
      await hold.reached;
      const removing = store.removeRepo(repoId);
      await removal.reached;
      hold.release(false);
      // A staged pack removal that does not wait for the repo lock
      // starts within the drained microtasks: no I/O comes before it.
      await drainMicrotasks();
      removal.release();

      await removing;
      await expect(preparing).rejects.toBeInstanceOf(
        RepoRemovedDuringReceiveError,
      );
      expect(removal.touchedStaging()).toBe(false);
      await expect(fs.promises.access(repoDirOf(repoId))).rejects.toThrow();
    } finally {
      removal.restore();
    }
  });

  test("discard waits for removeRepo", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(runsHandler());
    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);

    const removal = holdRepoRemoval(repoDirOf(repoId));
    try {
      const removing = store.removeRepo(repoId);
      await removal.reached;
      const discarding = prepared.discard();
      await drainMicrotasks();
      removal.release();

      await removing;
      await discarding;
      expect(removal.touchedStaging()).toBe(false);
      await expect(fs.promises.access(repoDirOf(repoId))).rejects.toThrow();
    } finally {
      removal.restore();
    }
  });

  test("the first receive for a repo in a process removes leftover staged packs", async () => {
    const line = await sourceLine(["v1", "v2"]);
    const [first, tip] = line.commits;
    if (first === undefined || tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(runsHandler());
    const stagingRoot = path.join(
      repoDirOf(repoId),
      ".git",
      "objects",
      "pack-staging",
    );
    await store.initRepo(repoId);
    await fs.promises.mkdir(path.join(stagingRoot, "left-by-crash"), {
      recursive: true,
    });
    await fs.promises.writeFile(
      path.join(stagingRoot, "left-by-crash", "pack.pack"),
      "partial",
    );

    await store.receivePack(
      principal,
      repoId,
      REF,
      await line.packAt(first),
      first,
      null,
    );
    expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);

    // A later receive leaves staged packs alone: in this process they
    // can belong to a receive in flight.
    await fs.promises.mkdir(path.join(stagingRoot, "in-flight"));
    await store.receivePack(
      principal,
      repoId,
      REF,
      await line.packAt(tip),
      tip,
      first,
    );
    expect(await stagedPacks(repoDirOf(repoId))).toEqual(["in-flight"]);
  });

  test.each([
    ["non_fast_forward", "stale"],
    ["sha_mismatch", "bogus"],
    ["path_violation", "rejected"],
  ] as const)(
    "a %s rejection on the one-call path never puts the pack in objects/pack",
    async (prefix, variant) => {
      const line = await sourceLine(["v1", "v2"]);
      const [first, tip] = line.commits;
      if (first === undefined || tip === undefined) throw new Error("line");
      const { store, repoDirOf } = await targetStore(
        runsHandler(async (text) => !(variant === "rejected" && text === "v2")),
      );
      await store.receivePack(
        principal,
        repoId,
        REF,
        await line.packAt(first),
        first,
        null,
      );
      const packDir = path.join(repoDirOf(repoId), ".git", "objects", "pack");
      const packsBefore = await receivedPacks(repoDirOf(repoId));

      const renameSpy = spyOn(fs.promises, "rename");
      try {
        await expect(
          store.receivePack(
            principal,
            repoId,
            REF,
            await line.packAt(tip),
            variant === "bogus" ? "0".repeat(40) : tip,
            variant === "stale" ? null : first,
          ),
        ).rejects.toThrow(new RegExp(`^${prefix}:`));
        const intoPackDir = renameSpy.mock.calls.filter(([, to]) =>
          String(to).startsWith(packDir),
        );
        expect(intoPackDir).toEqual([]);
      } finally {
        renameSpy.mockRestore();
      }
      expect(await receivedPacks(repoDirOf(repoId))).toEqual(packsBefore);
      expect(await stagedPacks(repoDirOf(repoId))).toEqual([]);
      expect(await store.resolveRef(principal, repoId, REF)).toBe(first);
    },
  );

  test("two first receives for a fresh repo leave the genesis branch at the pack tip", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    // The pack's history carries the source's genesis, which has no
    // `deploy/a.md`, so the handler accepts every commit.
    const { store, repoDirOf } = await targetStore({
      ...createTestHandler(),
      kind: "workflow-run",
    });
    const dir = path.resolve(repoDirOf(repoId));
    const main = "refs/heads/main";
    const pack = await line.packAt(tip);

    // A receive as the Hub runs it: init the repo, then prepare and
    // commit the pack outside that init.
    async function receive(
      commitSha: string,
    ): Promise<PackCommitResult["kind"]> {
      await store.initRepo(repoId);
      const prepared = await store.prepareReceivePack(
        principal,
        repoId,
        main,
        pack,
        commitSha,
      );
      if (prepared.kind !== "prepared") return prepared.kind;
      return (await prepared.commit()).kind;
    }

    // Holds the first genesis commit of the target repo, after its
    // `git.init` and `git.add`, until the test releases it.
    const originalCommit = git.commit.bind(git);
    let markGenesisHeld: () => void = () => undefined;
    const genesisHeld = new Promise<void>((resolve) => {
      markGenesisHeld = resolve;
    });
    let releaseGenesis: () => void = () => undefined;
    const genesisReleased = new Promise<void>((resolve) => {
      releaseGenesis = resolve;
    });
    let armed = true;
    const commitSpy = spyOn(git, "commit").mockImplementation(async (args) => {
      if (armed && args.dir === dir) {
        armed = false;
        markGenesisHeld();
        await genesisReleased;
      }
      return originalCommit(args);
    });
    const mkdirSpy = spyOn(fs.promises, "mkdir");
    const repoDirMkdirs = () =>
      mkdirSpy.mock.calls.filter(([p]) => p === dir).length;

    let first: Promise<PackCommitResult["kind"]> | undefined;
    let second: Promise<PackCommitResult["kind"]> | undefined;
    try {
      first = receive(tip);
      await genesisHeld;
      const mkdirsAtHold = repoDirMkdirs();
      second = receive(tip);
      // No await separates this check from the call above. An init
      // that takes the repo lock queues on it synchronously and
      // touches no file. An init without the lock calls its first
      // `mkdir` of the repo dir synchronously, before its first
      // pending await, so the count would already be higher.
      expect(repoDirMkdirs()).toBe(mkdirsAtHold);
      releaseGenesis();

      expect((await Promise.all([first, second])).sort()).toEqual([
        "already_applied",
        "applied",
      ]);
      expect(await store.resolveRef(principal, repoId, main)).toBe(tip);
    } finally {
      releaseGenesis();
      await Promise.allSettled([first, second]);
      commitSpy.mockRestore();
      mkdirSpy.mockRestore();
    }
  });

  test("prepareReceivePack refuses a kind that GC reclaims", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const store = createRepoStore({
      dataDir: await makeTempDir("repo-store-prepared-gc-"),
      signingKey,
      handlers: { "workflow-run": runsHandler() },
      authorize: allowAll,
      gc: {
        kinds: ["workflow-run"],
        retention: "keep-history",
        packThreshold: 1000,
        looseThreshold: 1000,
        warnBytes: Number.MAX_SAFE_INTEGER,
      },
    });

    await expect(
      store.prepareReceivePack(
        principal,
        repoId,
        REF,
        await line.packAt(tip),
        tip,
      ),
    ).rejects.toThrow(/reclaimed by GC/);
  });

  test("a receive with no subscriber reads no ref history under the lock", async () => {
    const line = await sourceLine(
      Array.from({ length: 12 }, (_, i) => `v${String(i + 1)}`),
    );
    const middle = line.commits[9];
    const tip = line.commits.at(-1);
    if (middle === undefined || tip === undefined) throw new Error("line");
    const { store: before, dataDir } = await targetStore(runsHandler());
    await (
      await prepare(before, repoId, await line.packAt(middle), middle)
    ).commit();

    // A new store over the same data, as after a restart, has no seq
    // cached. The commit step of a prepared receive holds the lock and
    // moves the ref; with no subscriber it reads no commit.
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": runsHandler() },
      authorize: allowAll,
    });
    const prepared = await prepare(store, repoId, await line.packAt(tip), tip);
    const readCommit = spyOn(git, "readCommit");
    const ac = new AbortController();
    try {
      await prepared.commit();
      expect(readCommit).toHaveBeenCalledTimes(0);

      // A subscriber seeds its seq by walking the history through the
      // same spy, so the zero count above is not a spy that sees nothing.
      const stream = store.subscribe(principal, repoId, REF, {
        from: "head",
        signal: ac.signal,
      });
      const next = stream.next();
      await store.writeTree(principal, repoId, REF, {
        files: { "deploy/a.md": "v13" },
        message: "v13",
      });
      expect((await next).done).toBe(false);
      expect(readCommit.mock.calls.length).toBeGreaterThanOrEqual(12);
    } finally {
      ac.abort();
      readCommit.mockRestore();
    }
  });

  test("a subscriber after updates with no subscriber gets the seq of the ref's history", async () => {
    const line = await sourceLine(["v1", "v2", "v3", "v4", "v5", "v6", "v7"]);
    const [first, second] = line.commits;
    const tip = line.commits.at(-1);
    if (first === undefined || second === undefined || tip === undefined) {
      throw new Error("line");
    }
    const { store, repoDirOf } = await targetStore(runsHandler());
    await (
      await prepare(store, repoId, await line.packAt(first), first)
    ).commit();

    // A subscriber that sees one update leaves a seq cached for the ref.
    const earlier = new AbortController();
    const earlierStream = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: earlier.signal,
    });
    try {
      const earlierNext = earlierStream.next();
      await (
        await prepare(store, repoId, await line.packAt(second), second)
      ).commit();
      expect((await earlierNext).done).toBe(false);
    } finally {
      earlier.abort();
    }

    // One receive of five commits with no subscriber.
    await (await prepare(store, repoId, await line.packAt(tip), tip)).commit();

    const ac = new AbortController();
    const stream = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: ac.signal,
    });
    try {
      const next = stream.next();
      await store.writeTree(principal, repoId, REF, {
        files: { "deploy/a.md": "v8" },
        message: "v8",
      });
      const entry = await next;
      if (entry.done) throw new Error("Missing update");
      const history = await git.log({ fs, dir: repoDirOf(repoId), ref: REF });
      expect(entry.value.seq).toBe(history.length - 1);
    } finally {
      ac.abort();
    }
  });

  // The seq a `from: { seq: 0 }` replay of `REF` gives `commitSha`.
  async function replayedSeq(
    store: ReturnType<typeof createRepoStore>,
    commitSha: string,
  ): Promise<number> {
    const ac = new AbortController();
    const replay = store.subscribe(principal, repoId, REF, {
      from: { seq: 0 },
      signal: ac.signal,
    });
    try {
      for (;;) {
        const entry = await replay.next();
        if (entry.done) throw new Error(`replay ended before ${commitSha}`);
        if (asRefUpdated(entry.value.event).newSha === commitSha) {
          return entry.value.seq;
        }
      }
    } finally {
      ac.abort();
    }
  }

  test("a live subscriber's seq after a receive of many commits matches the replay", async () => {
    const line = await sourceLine(
      Array.from({ length: 11 }, (_, i) => `v${String(i + 1)}`),
    );
    const [first] = line.commits;
    const middle = line.commits[5];
    const tip = line.commits.at(-1);
    if (first === undefined || middle === undefined || tip === undefined) {
      throw new Error("line");
    }
    const { store } = await targetStore(runsHandler());
    await (
      await prepare(store, repoId, await line.packAt(first), first)
    ).commit();
    const ac = new AbortController();
    const live = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: ac.signal,
    });
    try {
      const firstNext = live.next();
      await (
        await prepare(store, repoId, await line.packAt(middle), middle)
      ).commit();
      const a = await firstNext;
      if (a.done) throw new Error("Missing update");
      expect(a.value.seq).toBe(await replayedSeq(store, middle));

      // The cached seq of the previous tip bounds the next walk to the
      // commits this receive adds.
      const pack = await line.packAt(tip);
      const secondNext = live.next();
      const log = spyOn(git, "log");
      try {
        await (await prepare(store, repoId, pack, tip)).commit();
        expect(log).toHaveBeenCalledTimes(0);
      } finally {
        log.mockRestore();
      }
      const b = await secondNext;
      if (b.done) throw new Error("Missing update");
      expect(b.value.seq).toBe(await replayedSeq(store, tip));
      expect(b.value.seq - a.value.seq).toBe(5);
    } finally {
      ac.abort();
    }
  });

  test("a live subscriber's seq after a receive that replaces the ref's line matches the replay", async () => {
    const lineA = await sourceLine(["a1", "a2", "a3"]);
    const lineB = await sourceLine(
      Array.from({ length: 8 }, (_, i) => `b${String(i + 1)}`),
    );
    const tipA = lineA.commits.at(-1);
    const tipB = lineB.commits.at(-1);
    if (tipA === undefined || tipB === undefined) throw new Error("line");
    const { store } = await targetStore(runsHandler());
    await (
      await prepare(store, repoId, await lineA.packAt(tipA), tipA)
    ).commit();
    const ac = new AbortController();
    const live = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: ac.signal,
    });
    try {
      const next = live.next();
      await (
        await prepare(store, repoId, await lineB.packAt(tipB), tipB)
      ).commit();
      const entry = await next;
      if (entry.done) throw new Error("Missing update");
      expect(asRefUpdated(entry.value.event).newSha).toBe(tipB);
      expect(entry.value.seq).toBe(await replayedSeq(store, tipB));
    } finally {
      ac.abort();
    }
  });

  test("a replay subscriber continues the replay's seqs after a receive of many commits", async () => {
    const line = await sourceLine(["v1", "v2", "v3", "v4", "v5"]);
    const [first] = line.commits;
    const tip = line.commits.at(-1);
    if (first === undefined || tip === undefined) throw new Error("line");
    const { store, repoDirOf } = await targetStore(runsHandler());
    await (
      await prepare(store, repoId, await line.packAt(first), first)
    ).commit();
    const earlier = new AbortController();
    const earlierStream = store.subscribe(principal, repoId, REF, {
      from: "head",
      signal: earlier.signal,
    });
    const ac = new AbortController();
    try {
      const earlierNext = earlierStream.next();
      await (
        await prepare(store, repoId, await line.packAt(tip), tip)
      ).commit();
      const earlierEntry = await earlierNext;
      if (earlierEntry.done) throw new Error("Missing update");

      const history = await git.log({ fs, dir: repoDirOf(repoId), ref: REF });
      expect(earlierEntry.value.seq).toBe(history.length - 1);
      const replay = store.subscribe(principal, repoId, REF, {
        from: { seq: 0 },
        signal: ac.signal,
      });
      const seqs: number[] = [];
      for (const _entry of history) {
        const entry = await replay.next();
        if (entry.done) throw new Error("Missing replay entry");
        seqs.push(entry.value.seq);
      }
      const liveNext = replay.next();
      await store.writeTree(principal, repoId, REF, {
        files: { "deploy/a.md": "v6" },
        message: "v6",
      });
      const live = await liveNext;
      if (live.done) throw new Error("Missing update");
      seqs.push(live.value.seq);
      expect(seqs).toEqual(
        Array.from({ length: history.length + 1 }, (_, i) => i),
      );
    } finally {
      earlier.abort();
      ac.abort();
    }
  });

  // A store over `dataDir` whose workflow-run handler counts the
  // commits it validates, by their `deploy/a.md`, and can hold a ref
  // update in onRefUpdated while it holds the repo lock.
  function countingStore(
    dataDir: string,
    gc?: Parameters<typeof createRepoStore>[0]["gc"],
  ): {
    store: ReturnType<typeof createRepoStore>;
    validated: string[];
    holdNextUpdate: () => ReturnType<typeof gate>;
  } {
    const validated: string[] = [];
    let hold: ReturnType<typeof gate> | null = null;
    const store = createRepoStore({
      dataDir,
      signingKey,
      handlers: {
        "workflow-run": {
          ...runsHandler(async (text) => {
            validated.push(text);
            return true;
          }),
          async onRefUpdated() {
            const held = hold;
            hold = null;
            if (held !== null) await held.arrive();
          },
        },
      },
      authorize: allowAll,
      ...(gc === undefined ? {} : { gc }),
    });
    return {
      store,
      validated,
      holdNextUpdate: () => {
        hold = gate();
        return hold;
      },
    };
  }

  // A line of `contents` whose first `receivedCount` commits a new
  // target receives, and the target's data dir, for a new store over it
  // as after a restart, with no existing-commit set.
  async function restartedTarget(
    contents: readonly string[],
    receivedCount: number,
    id: RepoId = repoId,
  ): Promise<{
    line: Awaited<ReturnType<typeof sourceLine>>;
    dataDir: string;
    repoDir: string;
  }> {
    const line = await sourceLine(contents);
    const received = line.commits[receivedCount - 1];
    if (received === undefined) throw new Error("line");
    const { store, dataDir, repoDirOf } = await targetStore(runsHandler());
    await (
      await prepare(store, id, await line.packAt(received), received)
    ).commit();
    return { line, dataDir, repoDir: repoDirOf(id) };
  }

  test("a receive into a repo with no existing-commit set scans the history before it takes the lock", async () => {
    const contents = Array.from({ length: 32 }, (_, i) => `v${String(i + 1)}`);
    // The test holds the repo lock, and the lock map is shared by every
    // store in the process, so the test's own repo id keeps a lock it
    // leaves held from blocking other tests.
    const scanRepoId: RepoId = { kind: "workflow-run", id: "scan-before-lock" };
    const { line, dataDir, repoDir } = await restartedTarget(
      contents,
      30,
      scanRepoId,
    );
    const history = await git.log({ fs, dir: repoDir, ref: REF });
    const received = line.commits[29];
    const tip = line.commits.at(-1);
    if (received === undefined || tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store, validated, holdNextUpdate } = countingStore(dataDir);
    const OTHER = "refs/heads/other";

    // One write holds the repo lock; a second waits for it.
    const held = holdNextUpdate();
    const first = store.writeTree(principal, scanRepoId, OTHER, {
      files: { "deploy/a.md": "w1" },
      message: "w1",
    });
    await held.reached;
    const second = store.writeTree(principal, scanRepoId, OTHER, {
      files: { "deploy/a.md": "w2" },
      message: "w2",
    });

    // The scan reads the tip of the ref after it has resolved every
    // branch tip (the branches scan in name order, and `test` sorts
    // last). It arrives there while the write holds the lock, so the
    // scan runs with no lock, and it saw the other ref before the
    // second write moved it.
    const scan = gate();
    let gated = false;
    const readCommit = spyOn(git, "readCommit").mockImplementation(
      async (args) => {
        const result = await gitReadCommit(args);
        if (args.dir === repoDir && args.oid === received && !gated) {
          gated = true;
          await scan.arrive();
        }
        return result;
      },
    );
    let preparing: ReturnType<typeof prepare> | null = null;
    try {
      preparing = prepare(store, scanRepoId, pack, tip);
      await scan.reached;
      held.release();
      await first;
      const { commitSha: w2 } = await second;
      // The writes validate their own commits.
      expect(validated).toEqual(["w1", "w2"]);
      validated.length = 0;
      scan.release();
      const prepared = await preparing;
      await prepared.commit();
      expect(validated).toEqual(["v31", "v32"]);
      // The scan reads each commit once; a second, locked scan would
      // read the whole history again.
      expect(readCommit.mock.calls.length).toBeLessThan(2 * history.length);

      // The scan did not see the second write's commit. The locked
      // step adds it from the other ref's moved tip, so a receive of
      // that commit validates nothing.
      validated.length = 0;
      const w2Pack = await historyPack(repoDir, w2);
      await (await prepare(store, scanRepoId, w2Pack, w2)).commit();
      expect(validated).toEqual([]);
    } finally {
      readCommit.mockRestore();
      scan.release();
      held.release();
      await Promise.allSettled([first, second]);
      // A discard after a commit does nothing.
      await (await preparing?.catch(() => null))?.discard();
    }
  });

  test("a scan of a repo removed during it is not used for the repo created again", async () => {
    const contents = Array.from({ length: 12 }, (_, i) => `v${String(i + 1)}`);
    const { line, dataDir, repoDir } = await restartedTarget(contents, 10);
    const [oldest] = line.commits;
    const tip = line.commits.at(-1);
    if (oldest === undefined || tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store, validated } = countingStore(dataDir);

    // The scan has read the ref's history when it reads the line's
    // oldest commit. The repository is removed and created again
    // before the scan returns, so the scan ends on the new repository.
    const scan = gate();
    let gated = false;
    const readCommit = spyOn(git, "readCommit").mockImplementation(
      async (args) => {
        const result = await gitReadCommit(args);
        if (args.dir === repoDir && args.oid === oldest && !gated) {
          gated = true;
          await scan.arrive();
        }
        return result;
      },
    );
    let preparing: ReturnType<typeof prepare> | null = null;
    try {
      preparing = prepare(store, repoId, pack, tip);
      await scan.reached;
      await store.removeRepo(repoId);
      await store.initRepo(repoId);
      scan.release();
      await (await preparing).commit();
      expect(validated).toEqual(contents);
    } finally {
      readCommit.mockRestore();
      scan.release();
      // A discard after a commit does nothing.
      await (await preparing?.catch(() => null))?.discard();
    }
  });

  test("receives that start together on a repo with no existing-commit set share one scan", async () => {
    const contents = Array.from({ length: 62 }, (_, i) => `v${String(i + 1)}`);
    const { line, dataDir, repoDir } = await restartedTarget(contents, 60);
    const history = await git.log({ fs, dir: repoDir, ref: REF });
    const tip = line.commits.at(-1);
    if (tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store } = countingStore(dataDir);

    const readCommit = spyOn(git, "readCommit");
    const preparing = Array.from({ length: 4 }, () =>
      prepare(store, repoId, pack, tip),
    );
    try {
      await Promise.all(preparing);
      const reads = readCommit.mock.calls.filter(
        ([args]) => args.dir === repoDir,
      ).length;
      // One scan reads the history once; each receive adds a few reads
      // for its own two commits.
      expect(reads).toBeLessThan(2 * history.length);
    } finally {
      readCommit.mockRestore();
      for (const result of await Promise.allSettled(preparing)) {
        if (result.status === "fulfilled") await result.value.discard();
      }
    }
  });

  test("a receive that starts after the scan ends, while the lock is held, uses that scan", async () => {
    const contents = Array.from({ length: 62 }, (_, i) => `v${String(i + 1)}`);
    // The test holds the repo lock, so it uses its own repo id.
    const lateRepoId: RepoId = { kind: "workflow-run", id: "late-scan" };
    const { line, dataDir, repoDir } = await restartedTarget(
      contents,
      60,
      lateRepoId,
    );
    const history = await git.log({ fs, dir: repoDir, ref: REF });
    const tip = line.commits.at(-1);
    if (tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store, holdNextUpdate } = countingStore(dataDir);

    const held = holdNextUpdate();
    const write = store.writeTree(principal, lateRepoId, "refs/heads/other", {
      files: { "deploy/a.md": "w1" },
      message: "w1",
    });
    await held.reached;

    // The scan lists tags last; after that it returns with no more I/O.
    const listTags = git.listTags.bind(git);
    let markListed: () => void = () => undefined;
    const listed = new Promise<void>((resolve) => {
      markListed = resolve;
    });
    const tagsSpy = spyOn(git, "listTags").mockImplementation(async (args) => {
      const result = await listTags(args);
      if (args.dir === repoDir) markListed();
      return result;
    });
    const readCommit = spyOn(git, "readCommit");
    const preparing: ReturnType<typeof prepare>[] = [];
    try {
      preparing.push(prepare(store, lateRepoId, pack, tip));
      await listed;
      tagsSpy.mockRestore();
      await drainMicrotasks();
      // The first receive's scan has ended, and the receive waits for
      // the lock. A second receive starts now.
      preparing.push(prepare(store, lateRepoId, pack, tip));
      await drainMicrotasks();
      held.release();
      await write;
      await Promise.all(preparing);
      const reads = readCommit.mock.calls.filter(
        ([args]) => args.dir === repoDir,
      ).length;
      expect(reads).toBeLessThan(2 * history.length);
    } finally {
      tagsSpy.mockRestore();
      readCommit.mockRestore();
      held.release();
      await Promise.allSettled([write]);
      for (const result of await Promise.allSettled(preparing)) {
        if (result.status === "fulfilled") await result.value.discard();
      }
    }
  });

  test("a receive whose locked step fails leaves a shared scan whole for the next receive", async () => {
    const contents = Array.from({ length: 12 }, (_, i) => `v${String(i + 1)}`);
    // The test holds the repo lock, so it uses its own repo id.
    const failRepoId: RepoId = { kind: "workflow-run", id: "shared-scan" };
    const { line, dataDir, repoDir } = await restartedTarget(
      contents,
      10,
      failRepoId,
    );
    const received = line.commits[9];
    const tip = line.commits.at(-1);
    if (received === undefined || tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store, validated, holdNextUpdate } = countingStore(dataDir);
    const OTHER = "refs/heads/other";

    // A write holds the lock while the scan runs, and two more writes
    // move the other ref past what the scan saw.
    const held = holdNextUpdate();
    const writes = [
      store.writeTree(principal, failRepoId, OTHER, {
        files: { "deploy/a.md": "w1" },
        message: "w1",
      }),
    ];
    await held.reached;
    for (const message of ["w2", "w3"]) {
      writes.push(
        store.writeTree(principal, failRepoId, OTHER, {
          files: { "deploy/a.md": message },
          message,
        }),
      );
    }

    // Both receives wait for one scan, held at the ref's tip. Once the
    // writes land, the first receive's locked step fails to read w2.
    const scan = gate();
    let gated = false;
    let failOid: string | null = null;
    const readCommit = spyOn(git, "readCommit").mockImplementation(
      async (args) => {
        if (args.dir === repoDir && args.oid === failOid) {
          failOid = null;
          throw new Error("transient read failure");
        }
        const result = await gitReadCommit(args);
        if (args.dir === repoDir && args.oid === received && !gated) {
          gated = true;
          await scan.arrive();
        }
        return result;
      },
    );
    const preparing = [
      prepare(store, failRepoId, pack, tip),
      prepare(store, failRepoId, pack, tip),
    ];
    try {
      await scan.reached;
      held.release();
      const [, w2] = await Promise.all(writes);
      if (w2 === undefined) throw new Error("writes");
      failOid = w2.commitSha;
      scan.release();
      const [first, second] = await Promise.allSettled(preparing);
      expect(first?.status).toBe("rejected");
      if (second?.status !== "fulfilled") throw new Error("second receive");
      await second.value.commit();

      // The second receive's set holds w2, so a receive of w2 validates
      // nothing.
      validated.length = 0;
      const w2Pack = await historyPack(repoDir, w2.commitSha);
      await (await prepare(store, failRepoId, w2Pack, w2.commitSha)).commit();
      expect(validated).toEqual([]);
    } finally {
      readCommit.mockRestore();
      scan.release();
      held.release();
      await Promise.allSettled(writes);
      for (const result of await Promise.allSettled(preparing)) {
        // A discard after a commit does nothing.
        if (result.status === "fulfilled") await result.value.discard();
      }
    }
  });

  test("a scan before the lock leaves the locked step its git cache", async () => {
    const contents = Array.from({ length: 32 }, (_, i) => `v${String(i + 1)}`);
    const { line, dataDir, repoDir } = await restartedTarget(contents, 30);
    const received = line.commits[29];
    const tip = line.commits.at(-1);
    if (received === undefined || tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store } = countingStore(dataDir);

    // The store rebuilds a dir's git cache once the cache has served
    // 8192 calls, and each read below charges one. The scan's reads
    // take the count past that, so a scan charged before the lock
    // would make the locked step start from an empty cache.
    for (let i = 0; i < 8192 - 10; i++) {
      await store.openCommittedReadsAtCommit(principal, repoId, received);
    }
    const readCommit = spyOn(git, "readCommit");
    try {
      await (await prepare(store, repoId, pack, tip)).discard();
      const reads = readCommit.mock.calls
        .filter(([args]) => args.dir === repoDir)
        .map(([args]) => args);
      const scanCache = reads[0]?.cache;
      // The first read of the pack tip is the locked step's walk.
      const lockedCache = reads.find((args) => args.oid === tip)?.cache;
      expect(scanCache).toBeDefined();
      expect(lockedCache).toBe(scanCache);
    } finally {
      readCommit.mockRestore();
    }
  });

  test("a one-call receive into a repo with no existing-commit set scans the history before it takes the lock", async () => {
    const contents = Array.from({ length: 12 }, (_, i) => `v${String(i + 1)}`);
    // The test holds the repo lock, so it uses its own repo id.
    const oneCallRepoId: RepoId = { kind: "workflow-run", id: "one-call" };
    const { line, dataDir, repoDir } = await restartedTarget(
      contents,
      10,
      oneCallRepoId,
    );
    const received = line.commits[9];
    const tip = line.commits.at(-1);
    if (received === undefined || tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store, validated, holdNextUpdate } = countingStore(dataDir);

    const held = holdNextUpdate();
    const write = store.writeTree(
      principal,
      oneCallRepoId,
      "refs/heads/other",
      {
        files: { "deploy/a.md": "w1" },
        message: "w1",
      },
    );
    await held.reached;

    // The scan reads the received tip while the write holds the lock.
    const scan = gate();
    let gated = false;
    const readCommit = spyOn(git, "readCommit").mockImplementation(
      async (args) => {
        const result = await gitReadCommit(args);
        if (args.dir === repoDir && args.oid === received && !gated) {
          gated = true;
          await scan.arrive();
        }
        return result;
      },
    );
    let receiving: Promise<unknown> | null = null;
    try {
      receiving = store.receivePack(
        principal,
        oneCallRepoId,
        REF,
        pack,
        tip,
        received,
      );
      await scan.reached;
      scan.release();
      held.release();
      await write;
      validated.length = 0;
      await receiving;
      expect(validated).toEqual(["v11", "v12"]);
      expect(await store.resolveRef(principal, oneCallRepoId, REF)).toBe(tip);
    } finally {
      readCommit.mockRestore();
      scan.release();
      held.release();
      await Promise.allSettled([write, receiving]);
    }
  });

  test("a receive into a kind that GC reclaims scans its existing commits only under the lock", async () => {
    const contents = Array.from({ length: 6 }, (_, i) => `v${String(i + 1)}`);
    // The test holds the repo lock, so it uses its own repo id.
    const gcRepoId: RepoId = { kind: "workflow-run", id: "gc-kind" };
    const { line, dataDir, repoDir } = await restartedTarget(
      contents,
      4,
      gcRepoId,
    );
    const received = line.commits[3];
    const tip = line.commits.at(-1);
    if (received === undefined || tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store, validated, holdNextUpdate } = countingStore(dataDir, {
      kinds: ["workflow-run"],
      retention: "keep-history",
      packThreshold: 1000,
      looseThreshold: 1000,
      warnBytes: Number.MAX_SAFE_INTEGER,
    });

    const held = holdNextUpdate();
    const write = store.writeTree(principal, gcRepoId, "refs/heads/other", {
      files: { "deploy/a.md": "w1" },
      message: "w1",
    });
    await held.reached;

    // A scan with no lock first checks that the repository exists, and
    // that check starts within the microtasks after the receive starts.
    const gitDir = path.join(repoDir, ".git");
    const stat = spyOn(fs.promises, "stat");
    let receiving: Promise<unknown> | null = null;
    try {
      receiving = store.receivePack(
        principal,
        gcRepoId,
        REF,
        pack,
        tip,
        received,
      );
      await drainMicrotasks();
      expect(
        stat.mock.calls.filter(([target]) => target === gitDir),
      ).toHaveLength(0);
      held.release();
      await write;
      validated.length = 0;
      await receiving;
      expect(validated).toEqual(["v5", "v6"]);
    } finally {
      stat.mockRestore();
      held.release();
      await Promise.allSettled([write, receiving]);
    }
  });

  test("a receive whose scan before the lock fails scans under the lock", async () => {
    const contents = Array.from({ length: 6 }, (_, i) => `v${String(i + 1)}`);
    const { line, dataDir, repoDir } = await restartedTarget(contents, 4);
    const tip = line.commits.at(-1);
    if (tip === undefined) throw new Error("line");
    const pack = await line.packAt(tip);
    const { store, validated } = countingStore(dataDir);

    let failed = false;
    const readCommit = spyOn(git, "readCommit").mockImplementation(
      async (args) => {
        if (args.dir === repoDir && !failed) {
          failed = true;
          throw new Error("scan read failed");
        }
        return gitReadCommit(args);
      },
    );
    try {
      await (await prepare(store, repoId, pack, tip)).commit();
      expect(failed).toBe(true);
      expect(validated).toEqual(["v5", "v6"]);
      expect(await store.resolveRef(principal, repoId, REF)).toBe(tip);
    } finally {
      readCommit.mockRestore();
    }
  });

  test("prepareReceivePack refuses a kind other than workflow-run", async () => {
    const line = await sourceLine(["v1"]);
    const [tip] = line.commits;
    if (tip === undefined) throw new Error("line");
    const skillRepoId: RepoId = { kind: "skill", id: "subject" };
    const store = createRepoStore({
      dataDir: await makeTempDir("repo-store-prepared-kind-"),
      signingKey,
      handlers: { skill: { ...runsHandler(), kind: "skill" } },
      authorize: allowAll,
    });

    await expect(
      store.prepareReceivePack(
        principal,
        skillRepoId,
        REF,
        await line.packAt(tip),
        tip,
      ),
    ).rejects.toThrow(/kind skill is not workflow-run/);
  });
});
