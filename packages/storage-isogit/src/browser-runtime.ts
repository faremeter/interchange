import FS from "@isomorphic-git/lightning-fs";
import { Buffer as BrowserBuffer } from "buffer";

import { browserPath } from "./browser-path";
import type { IsogitRuntime } from "./runtime";

export type BrowserIsogitRuntime = {
  runtime: IsogitRuntime & { flush(): Promise<void> };
  fs: FS;
};

function ensureBuffer(): void {
  if (globalThis.Buffer === undefined) {
    globalThis.Buffer = BrowserBuffer;
  }
}

/**
 * Claim a fresh, single-owner IndexedDB filesystem for this page.
 *
 * Construction clears any prior contents for `name`. Create one instance per
 * name and pass it to every consumer; a second owner for the same name may
 * clear the first owner's active data.
 */
export function createBrowserIsogitRuntime(
  name = "interchange",
): BrowserIsogitRuntime {
  ensureBuffer();
  const fs = new FS(name, { wipe: true });
  return {
    runtime: {
      fs,
      path: browserPath,
      rename: (oldPath, newPath) => fs.promises.rename(oldPath, newPath),
      flush: () => fs.promises.flush(),
    },
    fs,
  };
}
