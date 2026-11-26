// Test-only entry for the deploy-flow fixture's sidecar subprocess. It ends
// the process when the parent that spawned it dies, then runs the real
// sidecar entry.
//
// The sidecar's hub link redials forever, so a sidecar orphaned by a test
// worker that crashed or was killed keeps dialing a port the OS may since
// have given to another test's server. The fixture spawns this entry with a
// stdin pipe it never writes to. The read below ends only when no process
// holds the pipe's write end. The test worker is the only holder, because
// Bun closes the write end in the other processes the worker spawns, so the
// read ends when the kernel closes the worker's copy as the worker dies.
//
// This lives in the fixture rather than in the sidecar because stdin
// carries no such meaning under a production supervisor, where it can be
// /dev/null and would end at boot.

import path from "node:path";

void (async () => {
  const reader = Bun.stdin.stream().getReader();
  while (!(await reader.read()).done) {
    // The fixture writes nothing; any bytes are discarded.
  }
  process.exit(1);
})();

// Held in a variable so `tsc -b` does not try to resolve a `.ts` specifier
// outside this project. The spawn's `--conditions=intx-src` applies to this
// import too.
const sidecarEntry = path.resolve(
  import.meta.dir,
  "../../../apps/sidecar/src/index.ts",
);
await import(sidecarEntry);
