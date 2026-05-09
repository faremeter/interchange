import fs from "node:fs";
import git from "isomorphic-git";
import { type } from "arktype";

const CommitObject = type({ tree: "string" });
const TreeEntry = type({ oid: "string", type: "string" });
const RawObject = type({ object: type.instanceOf(Uint8Array) });

export async function readCommitObject(dir: string, oid: string) {
  const { object } = await git.readObject({ fs, dir, oid, format: "parsed" });
  return CommitObject.assert(object);
}

export async function readTreeEntries(dir: string, oid: string) {
  const { object } = await git.readObject({ fs, dir, oid, format: "parsed" });
  return TreeEntry.array().assert(object);
}

export async function readRawObject(dir: string, oid: string) {
  const { object } = await git.readObject({ fs, dir, oid, format: "content" });
  return RawObject.assert({ object });
}
