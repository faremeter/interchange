import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { createSSHSignature } from "@intx/crypto-node";
import {
  initRepo as storageInitRepo,
  createDeployPack,
  receivePackObjects,
} from "@intx/storage-isogit";
import { hasCode } from "@intx/types";
import { getLogger } from "@intx/log";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
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
};

export function createRepoStore(config: CreateRepoStoreConfig): RepoStore {
  const { dataDir, signingKey, handlers, authorize } = config;

  function handlerFor(repoId: RepoId): KindHandler {
    const handler = handlers[repoId.kind];
    if (handler === undefined) {
      throw new Error(`no handler registered for kind: ${repoId.kind}`);
    }
    return handler;
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

  async function initRepo(repoId: RepoId): Promise<void> {
    await storageInitRepo(repoDir(repoId));
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

    const dir = repoDir(repoId);
    await storageInitRepo(dir);

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
  }

  async function receivePack(
    principal: Principal,
    repoId: RepoId,
    ref: string,
    pack: Uint8Array,
    commitSha: string,
  ): Promise<void> {
    gateAccess(principal, repoId, ref, "receivePack");

    const dir = repoDir(repoId);
    await storageInitRepo(dir);

    const handler = handlerFor(repoId);
    const oldSha = await resolveRefSha(dir, ref);
    const transferId = crypto.randomUUID().replace(/-/g, "");

    await receivePackObjects(
      dir,
      pack,
      ref,
      commitSha,
      transferId,
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
  };
}
