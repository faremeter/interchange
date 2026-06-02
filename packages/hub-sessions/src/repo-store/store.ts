import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { createSSHSignature } from "@intx/crypto-node";
import {
  initRepo as storageInitRepo,
  createDeployPack,
  receivePackObjects,
  type CommitSigner,
} from "@intx/storage-isogit";
import { hasCode } from "@intx/types";
import { getLogger } from "@intx/log";
import type {
  AuthorizeFn,
  InitRepoOpts,
  KindHandler,
  Principal,
  RefEntry,
  RepoAction,
  RepoId,
  RepoKind,
  RepoStore,
  TreeContent,
} from "./types";
import { SAFE_REPO_ID } from "./types";

const AUTHOR = {
  name: "interchange-hub",
  email: "hub@interchange.local",
};

const logger = getLogger(["hub", "repo-store"]);

type SigningKey = { privateKey: Uint8Array; publicKey: Uint8Array };

/**
 * In-process push-serialization lock. Keyed by `${kind}/${id}`. Each
 * entry holds the tail of the chain of in-flight critical sections for
 * that repo; the next acquirer awaits the current tail and replaces it
 * with its own pending completion. On release the tail-check
 * (`if (locks.get(key) === myTail) locks.delete(key)`) prevents the map
 * from leaking entries once the chain drains.
 *
 * Single-process assumption: this protects against concurrent operations
 * inside a single hub instance. Cross-process serialization (e.g. a
 * second hub replica or an external git client touching the same
 * on-disk repo) would need a filesystem-backed lock; the migration path
 * is to swap the body of `withRepoLock` for an FS-lock acquire/release
 * around the same critical section.
 */
const locks = new Map<string, Promise<void>>();

async function withRepoLock<T>(
  repoId: RepoId,
  fn: () => Promise<T>,
): Promise<T> {
  const key = `${repoId.kind}/${repoId.id}`;
  const previous = locks.get(key) ?? Promise.resolve();
  let releaseFn: () => void = () => undefined;
  const tail = new Promise<void>((resolve) => {
    releaseFn = resolve;
  });
  locks.set(key, tail);
  try {
    await previous;
    return await fn();
  } finally {
    if (locks.get(key) === tail) {
      locks.delete(key);
    }
    releaseFn();
  }
}

export type CreateRepoStoreConfig = {
  dataDir: string;
  signingKey: SigningKey;
  /**
   * Handler map keyed by repo kind. The substrate throws at request
   * time when a kind has no registered handler, so callers may omit
   * kinds they do not service (e.g. a per-asset-kind store that only
   * registers `skill`).
   */
  handlers: Partial<Record<RepoKind, KindHandler>>;
  authorize: AuthorizeFn;
  /**
   * Optional per-repo signing callback. When supplied and the callback
   * returns a `CommitSigner` for the given `repoId`, the substrate
   * passes that signer to `initRepo` so the genesis commit is authored
   * as `interchange-hub` and signed. When the callback returns
   * `undefined`, or the field is omitted entirely, the substrate falls
   * back to the unsigned harness-authored genesis.
   */
  signingCallback?: (repoId: RepoId) => CommitSigner | undefined;
};

export function createRepoStore(config: CreateRepoStoreConfig): RepoStore {
  const { dataDir, signingKey, handlers, authorize, signingCallback } = config;

  function handlerFor(repoId: RepoId): KindHandler {
    const handler = handlers[repoId.kind];
    if (handler === undefined) {
      throw new Error(`no handler registered for kind: ${repoId.kind}`);
    }
    return handler;
  }

  function signerFor(repoId: RepoId): CommitSigner | undefined {
    return signingCallback === undefined ? undefined : signingCallback(repoId);
  }

  function repoDir(repoId: RepoId): string {
    if (!SAFE_REPO_ID.test(repoId.id)) {
      throw new Error(`repo_id_invalid: ${repoId.id}`);
    }
    const handler = handlerFor(repoId);
    return path.join(dataDir, handler.directoryPrefix, repoId.id);
  }

  function gateAccess(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    action: RepoAction,
  ): void {
    const verdict = authorize(principal, repoId, ref, action);
    if (!verdict.allowed) {
      throw new Error(`authorize_denied: ${verdict.reason}`);
    }
  }

  function validateClearPrefix(clearPrefix: string): void {
    const malformed =
      clearPrefix.length === 0 ||
      !clearPrefix.endsWith("/") ||
      clearPrefix.startsWith("/") ||
      clearPrefix.split("/").includes("..");
    if (malformed) {
      throw new Error(`clear_prefix_invalid: ${clearPrefix}`);
    }
  }

  function storageOptsFor(
    repoId: RepoId,
    opts: InitRepoOpts | undefined,
  ): { signer?: CommitSigner; gitignore?: string } {
    const out: { signer?: CommitSigner; gitignore?: string } = {};
    const signer = signerFor(repoId);
    if (signer !== undefined) out.signer = signer;
    if (opts?.gitignore !== undefined) out.gitignore = opts.gitignore;
    return out;
  }

  async function initRepo(repoId: RepoId, opts?: InitRepoOpts): Promise<void> {
    await storageInitRepo(repoDir(repoId), storageOptsFor(repoId, opts));
  }

  function getRepoDir(repoId: RepoId): string {
    return repoDir(repoId);
  }

  async function listRefs(
    principal: Principal,
    repoId: RepoId,
  ): Promise<RefEntry[]> {
    gateAccess(principal, repoId, "*", "resolveRef");
    const dir = repoDir(repoId);
    const repoExists = await fs.promises
      .stat(path.join(dir, ".git"))
      .then(() => true)
      .catch(() => false);
    if (!repoExists) return [];

    const [branches, tags] = await Promise.all([
      git.listBranches({ fs, dir }),
      git.listTags({ fs, dir }),
    ]);

    const names: string[] = [];
    for (const b of branches) names.push(`refs/heads/${b}`);
    for (const t of tags) names.push(`refs/tags/${t}`);
    names.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

    const entries: RefEntry[] = [];
    for (const name of names) {
      try {
        const sha = await git.resolveRef({ fs, dir, ref: name });
        entries.push({ name, sha });
      } catch (err: unknown) {
        if (hasCode(err) && err.code === "NotFoundError") continue;
        throw err;
      }
    }
    return entries;
  }

  async function resolveHead(
    principal: Principal,
    repoId: RepoId,
  ): Promise<{ symbolicTarget: string; sha: string } | null> {
    gateAccess(principal, repoId, "*", "resolveRef");
    const dir = repoDir(repoId);
    const repoExists = await fs.promises
      .stat(path.join(dir, ".git"))
      .then(() => true)
      .catch(() => false);
    if (!repoExists) return null;

    const symbolicTarget = await git.currentBranch({ fs, dir, fullname: true });
    if (symbolicTarget === undefined) return null;

    const sha = await resolveRefSha(dir, symbolicTarget);
    if (sha === null) return null;

    return { symbolicTarget, sha };
  }

  async function resolveRefSha(
    dir: string,
    ref: string,
  ): Promise<string | null> {
    try {
      return await git.resolveRef({ fs, dir, ref });
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "NotFoundError") {
        return null;
      }
      throw err;
    }
  }

  async function clearIndexPrefix(dir: string, prefix: string): Promise<void> {
    const matrix = await git.statusMatrix({ fs, dir });
    for (const row of matrix) {
      const filepath = row[0];
      if (filepath.startsWith(prefix)) {
        await git.remove({ fs, dir, filepath });
      }
    }
  }

  async function writeFileEntry(
    dir: string,
    relPath: string,
    contents: string | Uint8Array,
  ): Promise<void> {
    const fullPath = path.join(dir, relPath);
    await fs.promises.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.promises.writeFile(fullPath, contents);
    await git.add({ fs, dir, filepath: relPath });
  }

  async function writeTree(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    content: TreeContent,
  ): Promise<{ commitSha: string }> {
    gateAccess(principal, repoId, ref, "writeTree");

    // The lock spans the entire substrate body of writeTree: index
    // mutation, validatePush, commit, and the onRefUpdated hook. Holding
    // the lock through onRefUpdated keeps post-update consumers
    // serialized against the same ref's next writer.
    return withRepoLock(repoId, async () => {
      const dir = repoDir(repoId);
      await storageInitRepo(dir, storageOptsFor(repoId, undefined));

      const handler = handlerFor(repoId);

      if (content.clearPrefix !== undefined) {
        validateClearPrefix(content.clearPrefix);
        await clearIndexPrefix(dir, content.clearPrefix);
        await fs.promises.rm(path.join(dir, content.clearPrefix), {
          recursive: true,
          force: true,
        });
      }

      for (const [relPath, contents] of Object.entries(content.files)) {
        await writeFileEntry(dir, relPath, contents);
      }

      const topLevelTreePaths = Array.from(
        new Set(
          Object.keys(content.files).map((p) => {
            const slash = p.indexOf("/");
            return slash === -1 ? p : p.substring(0, slash);
          }),
        ),
      );
      const readBlob = async (relPath: string): Promise<Uint8Array> => {
        const entry = content.files[relPath];
        if (entry === undefined) {
          throw new Error(
            `readBlob: path ${relPath} not present in tree content`,
          );
        }
        if (typeof entry === "string") {
          return new TextEncoder().encode(entry);
        }
        return entry;
      };
      const validation = await handler.validatePush({
        repoId,
        ref,
        topLevelTreePaths,
        readBlob,
      });
      if (!validation.ok) {
        throw new Error(`path_violation: ${validation.reason}`);
      }

      // Resolve the previous ref value for the post-update hook (precise:
      // null only when the ref truly doesn't exist) and the parent SHA for
      // the new commit (best-effort: any error falls back to HEAD, so a
      // first write on a never-touched ref produces a commit parented on
      // the repo's initial commit instead of failing).
      const oldSha = await resolveRefSha(dir, ref);
      let parentSha: string;
      try {
        parentSha = await git.resolveRef({ fs, dir, ref });
      } catch {
        parentSha = await git.resolveRef({ fs, dir, ref: "HEAD" });
      }

      const commitSha = await git.commit({
        fs,
        dir,
        message: content.message,
        author: AUTHOR,
        parent: [parentSha],
        ref,
        signingKey: "sshsig",
        onSign: async ({ payload }) => ({
          signature: createSSHSignature(
            payload,
            signingKey.privateKey,
            signingKey.publicKey,
          ),
        }),
      });

      await handler.onRefUpdated({ repoId, ref, oldSha, newSha: commitSha });

      return { commitSha };
    });
  }

  async function receivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
    expectedOldSha: string | null,
  ): Promise<void> {
    gateAccess(principal, repoId, ref, "receivePack");

    // The lock spans the entire substrate body of receivePack: the
    // packfile index, the CAS check against `expectedOldSha`, the
    // validateTree hook, the ref write, and the onRefUpdated hook.
    // `oldSha` for onRefUpdated is taken from the receivePackObjects
    // return value so the post-update hook sees the same pre-image the
    // CAS read observed, without a second resolveRef.
    return withRepoLock(repoId, async () => {
      const dir = repoDir(repoId);
      await storageInitRepo(dir, storageOptsFor(repoId, undefined));

      const handler = handlerFor(repoId);
      const transferId = crypto.randomUUID().replace(/-/g, "");

      const oldSha = await receivePackObjects(
        dir,
        pack,
        ref,
        commitSha,
        transferId,
        expectedOldSha,
        async (paths, readBlob) => {
          const result = await handler.validatePush({
            repoId,
            ref,
            topLevelTreePaths: paths,
            readBlob,
          });
          if (!result.ok) {
            logger.debug`validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${result.reason}`;
            return { ok: false, reason: result.reason };
          }
          return true;
        },
      );

      await handler.onRefUpdated({ repoId, ref, oldSha, newSha: commitSha });
    });
  }

  async function createPack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    gateAccess(principal, repoId, ref, "createPack");
    const { pack, commitSha } = await createDeployPack(repoDir(repoId), ref);
    return { pack, commitSha, ref };
  }

  async function resolveRef(
    principal: Principal,
    repoId: RepoId,
    ref: string,
  ): Promise<string | null> {
    gateAccess(principal, repoId, ref, "resolveRef");
    return resolveRefSha(repoDir(repoId), ref);
  }

  return {
    initRepo,
    writeTree,
    receivePack,
    createPack,
    resolveRef,
    listRefs,
    resolveHead,
    getRepoDir,
  };
}
