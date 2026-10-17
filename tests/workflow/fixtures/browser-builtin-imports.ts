// A positive control for the browser-bundle test: static Node builtin
// imports, one of which Bun polyfills, and one literal dynamic import, which
// the builtin recorder must all see.
import { readFileSync } from "node:fs";
import { posix } from "node:path";

export const read = readFileSync;
export const joinPosix = posix.join;
export const loadPromises = () => import("node:fs/promises");
