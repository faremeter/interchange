// A synchronous "run this command or throw with its output" helper shared by
// the packaging scripts. The captured stdout and stderr are surfaced on a
// non-zero exit so a failing subprocess (a 404 from npm, a pack error) is
// diagnosable rather than a bare exit code.

/** Build a `run(cmd, cwd)` that throws — with the command's captured output —
 *  on a non-zero exit, tagging the error with `prefix`. */
export function makeRun(prefix: string): (cmd: string[], cwd: string) => void {
  return (cmd, cwd) => {
    const proc = Bun.spawnSync(cmd, { cwd, stdout: "pipe", stderr: "pipe" });
    if (proc.exitCode !== 0) {
      const detail = [
        proc.stdout.toString().trim(),
        proc.stderr.toString().trim(),
      ]
        .filter(Boolean)
        .join("\n");
      throw new Error(
        `${prefix}: \`${cmd.join(" ")}\` failed in ${cwd}:\n${detail}`,
      );
    }
  };
}
