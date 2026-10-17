import { join, relative, resolve } from "@intx/types/posix-path";

import type { IsogitPath } from "./runtime";

export const browserPath: IsogitPath = { join, relative, resolve };
