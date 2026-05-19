// agent-rewind ships a second working tree (the rewound clone)
// alongside the primary contextDir. The primary contextDir comes
// from agent-common's shared helper; the rewound sibling lives one
// directory over and is unique to this example, so it stays here.

import { resolve } from "node:path";

import { defaultContextDir } from "@intx/example-agent-common";

export const EXAMPLE_NAME = "agent-rewind";

export function defaultRewindDir(): string {
  const ctx = defaultContextDir(EXAMPLE_NAME);
  return resolve(ctx, "..", "context-rewound");
}
