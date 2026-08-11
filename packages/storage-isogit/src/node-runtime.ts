import fs from "node:fs";
import path from "node:path";

import type { IsogitRuntime } from "./runtime";

export function createNodeIsogitRuntime(): IsogitRuntime {
  return {
    fs,
    path,
    rename: (oldPath, newPath) => fs.promises.rename(oldPath, newPath),
  };
}
