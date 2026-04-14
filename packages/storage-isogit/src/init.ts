import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";

const AUTHOR = {
  name: "interchange-harness",
  email: "harness@interchange.local",
};

/**
 * Ensure the agent data directory exists as an initialized git repository
 * with the expected subdirectories. Idempotent: safe to call on a directory
 * that already contains a git repo.
 *
 * On first call the repo gets an initial empty commit so that HEAD resolves
 * to a real ref immediately — isomorphic-git requires at least one commit
 * before branching operations work.
 */
export async function initAgentRepo(dir: string): Promise<void> {
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.mkdir(path.join(dir, "workspace"), { recursive: true });

  const isAlreadyInit = await fs.promises
    .stat(path.join(dir, ".git"))
    .then(() => true)
    .catch(() => false);

  if (!isAlreadyInit) {
    await git.init({ fs, dir, defaultBranch: "main" });

    // Write an empty context so the initial commit has a real tree.
    const contextPath = path.join(dir, "context.json");
    await fs.promises.writeFile(
      contextPath,
      JSON.stringify(
        {
          messages: [],
          pendingOperations: [],
          tokenUsage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            thinking: 0,
          },
        },
        null,
        2,
      ),
    );

    await git.add({ fs, dir, filepath: "context.json" });
    await git.commit({
      fs,
      dir,
      message: "Initialize agent repository",
      author: AUTHOR,
    });
  }
}

export { AUTHOR };
