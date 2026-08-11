import git from "isomorphic-git";
import { type } from "arktype";
import type { StorageRuntime } from "./runtime";

const CommitObject = type({ tree: "string" });
const TreeEntry = type({ oid: "string", type: "string" });
const RawObject = type({ object: type.instanceOf(Uint8Array) });

export async function readCommitObject(
  runtime: StorageRuntime,
  dir: string,
  oid: string,
) {
  const { object } = await git.readObject({
    fs: runtime.fs.git,
    dir,
    oid,
    format: "parsed",
  });
  return CommitObject.assert(object);
}

export async function readTreeEntries(
  runtime: StorageRuntime,
  dir: string,
  oid: string,
) {
  const { object } = await git.readObject({
    fs: runtime.fs.git,
    dir,
    oid,
    format: "parsed",
  });
  return TreeEntry.array().assert(object);
}

export async function readRawObject(
  runtime: StorageRuntime,
  dir: string,
  oid: string,
) {
  const { object } = await git.readObject({
    fs: runtime.fs.git,
    dir,
    oid,
    format: "content",
  });
  return RawObject.assert({ object });
}
