import {
  spawn as nodeSpawn,
  type ChildProcessWithoutNullStreams,
} from "node:child_process";

export interface SpawnOptions {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
}

export function spawn(
  cmd: string,
  args: string[],
  opts?: SpawnOptions,
): ChildProcessWithoutNullStreams {
  const proc = nodeSpawn(cmd, args, {
    cwd: opts?.cwd,
    env: opts?.env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  if (!proc.stdin || !proc.stdout || !proc.stderr) {
    throw new Error(`failed to acquire stdio pipes for ${cmd}`);
  }
  return proc;
}
