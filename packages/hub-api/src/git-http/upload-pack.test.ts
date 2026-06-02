import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import type { RepoId } from "@intx/types/sidecar";
import {
  handleUploadPack,
  UPLOAD_PACK_RESULT_CONTENT_TYPE,
  type UploadPackPrincipal,
  type UploadPackRepoStore,
} from "./upload-pack";
import type { RefEntry } from "./advertise-refs";

const REPO_ID: RepoId = { kind: "agent-state", id: "test" };

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "interchange-upload-pack-test-"),
  );
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

const AUTHOR = { name: "Test", email: "test@interchange.dev" };

async function writeAndCommit(
  dir: string,
  files: { filepath: string; content: string }[],
  message: string,
  parent?: string[],
): Promise<string> {
  for (const { filepath, content } of files) {
    const full = path.join(dir, filepath);
    await fs.promises.mkdir(path.dirname(full), { recursive: true });
    await fs.promises.writeFile(full, content);
    await git.add({ fs, dir, filepath });
  }
  return git.commit({
    fs,
    dir,
    message,
    author: AUTHOR,
    ...(parent ? { parent } : {}),
  });
}

async function makeLinearRepo(): Promise<{
  dir: string;
  c1: string;
  c2: string;
  c3: string;
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  const c1 = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "v1" }],
    "c1",
  );
  const c2 = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "v2" }],
    "c2",
  );
  const c3 = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "v3" }],
    "c3",
  );
  await git.writeRef({
    fs,
    dir,
    ref: "refs/heads/main",
    value: c3,
    force: true,
  });
  return { dir, c1, c2, c3 };
}

async function makeTwoBranchRepo(): Promise<{
  dir: string;
  base: string;
  branchA: string;
  branchB: string;
}> {
  const dir = await tempDir();
  await git.init({ fs, dir, defaultBranch: "main" });
  const base = await writeAndCommit(
    dir,
    [{ filepath: "base.txt", content: "base" }],
    "base",
  );
  const branchA = await writeAndCommit(
    dir,
    [{ filepath: "a.txt", content: "alpha" }],
    "branch A",
  );
  await git.writeRef({
    fs,
    dir,
    ref: "refs/heads/main",
    value: base,
    force: true,
  });
  await fs.promises.rm(path.join(dir, "a.txt"), { force: true });
  await git.remove({ fs, dir, filepath: "a.txt" }).catch(() => undefined);
  const branchB = await writeAndCommit(
    dir,
    [{ filepath: "b.txt", content: "bravo" }],
    "branch B",
  );
  await git.writeRef({
    fs,
    dir,
    ref: "refs/heads/branch-a",
    value: branchA,
    force: true,
  });
  await git.writeRef({
    fs,
    dir,
    ref: "refs/heads/branch-b",
    value: branchB,
    force: true,
  });
  return { dir, base, branchA, branchB };
}

function principalWith(refPattern: string): UploadPackPrincipal {
  return { kind: "user", tokenClaims: { refPattern } };
}

function repoStoreFor(dir: string, refs: RefEntry[]): UploadPackRepoStore {
  return {
    listRefs: async () => refs.slice(),
    getRepoDir: async () => dir,
  };
}

function hex4(n: number): string {
  return n.toString(16).padStart(4, "0");
}

function pkt(payload: string): Uint8Array {
  const body = new TextEncoder().encode(payload);
  const header = new TextEncoder().encode(hex4(body.length + 4));
  const out = new Uint8Array(header.length + body.length);
  out.set(header, 0);
  out.set(body, header.length);
  return out;
}

function flush(): Uint8Array {
  return new TextEncoder().encode("0000");
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function uploadPackRequest(body: Uint8Array): Request {
  return new Request("https://hub.example/upload-pack", {
    method: "POST",
    headers: { "content-type": "application/x-git-upload-pack-request" },
    body,
  });
}

async function readAll(response: Response): Promise<Uint8Array> {
  if (!response.body) {
    throw new Error("response has no body");
  }
  const reader = response.body.getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const r = await reader.read();
    if (r.done) break;
    if (r.value) parts.push(r.value);
  }
  return concat(...parts);
}

type ParsedPkt = { kind: "flush" } | { kind: "data"; payload: Uint8Array };

function parsePktStream(buf: Uint8Array): ParsedPkt[] {
  const out: ParsedPkt[] = [];
  let off = 0;
  const dec = new TextDecoder();
  while (off < buf.length) {
    if (off + 4 > buf.length) {
      throw new Error(`truncated pkt-line at ${off.toString()}`);
    }
    const lenHex = dec.decode(buf.slice(off, off + 4));
    const len = parseInt(lenHex, 16);
    if (Number.isNaN(len)) {
      throw new Error(`bad pkt-line length: ${lenHex}`);
    }
    off += 4;
    if (len === 0) {
      out.push({ kind: "flush" });
      continue;
    }
    if (len < 4) {
      throw new Error(`reserved pkt-line length: ${len.toString()}`);
    }
    const bodyLen = len - 4;
    if (off + bodyLen > buf.length) {
      throw new Error(`truncated pkt-line body at ${off.toString()}`);
    }
    out.push({ kind: "data", payload: buf.slice(off, off + bodyLen) });
    off += bodyLen;
  }
  return out;
}

type Frame = { channel: number; payload: Uint8Array };

function splitNakAndFrames(buf: Uint8Array): {
  nak: Uint8Array;
  frames: Frame[];
} {
  const pkts = parsePktStream(buf);
  if (pkts.length === 0) {
    throw new Error("expected at least one pkt-line");
  }
  const first = pkts[0];
  if (!first || first.kind !== "data") {
    throw new Error("expected NAK pkt-line first");
  }
  const nak = first.payload;
  const frames: Frame[] = [];
  for (let i = 1; i < pkts.length; i++) {
    const p = pkts[i];
    if (!p) continue;
    if (p.kind === "flush") continue;
    const channel = p.payload[0];
    if (channel === undefined) {
      throw new Error("side-band frame missing channel byte");
    }
    frames.push({ channel, payload: p.payload.slice(1) });
  }
  return { nak, frames };
}

function assembleChannel1(frames: Frame[]): Uint8Array {
  const ch1: Uint8Array[] = [];
  for (const f of frames) {
    if (f.channel === 1) ch1.push(f.payload);
  }
  return concat(...ch1);
}

describe("handleUploadPack request parsing", () => {
  test("parses canonical want/have/done into a NAK + pack response", async () => {
    const { dir, c1, c3 } = await makeLinearRepo();
    const body = concat(
      pkt(`want ${c3}\n`),
      flush(),
      pkt(`have ${c1}\n`),
      pkt("done\n"),
    );
    const store = repoStoreFor(dir, [{ name: "refs/heads/main", sha: c3 }]);
    const response = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(body),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe(
      UPLOAD_PACK_RESULT_CONTENT_TYPE,
    );
    const buf = await readAll(response);
    const { nak, frames } = splitNakAndFrames(buf);
    expect(new TextDecoder().decode(nak)).toBe("NAK\n");
    expect(frames.length).toBeGreaterThan(0);
    const channels = new Set(frames.map((f) => f.channel));
    expect(channels.has(1)).toBe(true);
    expect(channels.has(2)).toBe(false);
  });
});

describe("handleUploadPack pack contents", () => {
  test("single want / no haves yields a full pack", async () => {
    const { dir, c3 } = await makeLinearRepo();
    const body = concat(pkt(`want ${c3}\n`), flush(), pkt("done\n"));
    const store = repoStoreFor(dir, [{ name: "refs/heads/main", sha: c3 }]);
    const response = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(body),
    );
    const buf = await readAll(response);
    const { frames } = splitNakAndFrames(buf);
    const pack = assembleChannel1(frames);
    expect(pack.length).toBeGreaterThan(0);
    expect(new TextDecoder().decode(pack.slice(0, 4))).toBe("PACK");
  });

  test("single want / overlapping have yields a smaller delta pack", async () => {
    const { dir, c1, c3 } = await makeLinearRepo();
    const store = repoStoreFor(dir, [{ name: "refs/heads/main", sha: c3 }]);

    const fullResponse = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(concat(pkt(`want ${c3}\n`), flush(), pkt("done\n"))),
    );
    const full = await readAll(fullResponse);
    const deltaResponse = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(
        concat(
          pkt(`want ${c3}\n`),
          flush(),
          pkt(`have ${c1}\n`),
          pkt("done\n"),
        ),
      ),
    );
    const delta = await readAll(deltaResponse);

    const fullPack = assembleChannel1(splitNakAndFrames(full).frames);
    const deltaPack = assembleChannel1(splitNakAndFrames(delta).frames);
    expect(deltaPack.length).toBeLessThan(fullPack.length);
  });

  test("multi-want union pack covers both branch tips", async () => {
    const { dir, branchA, branchB } = await makeTwoBranchRepo();
    const store = repoStoreFor(dir, [
      { name: "refs/heads/branch-a", sha: branchA },
      { name: "refs/heads/branch-b", sha: branchB },
    ]);
    const body = concat(
      pkt(`want ${branchA}\n`),
      pkt(`want ${branchB}\n`),
      flush(),
      pkt("done\n"),
    );
    const response = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(body),
    );
    const buf = await readAll(response);
    const { frames } = splitNakAndFrames(buf);
    const pack = assembleChannel1(frames);
    expect(new TextDecoder().decode(pack.slice(0, 4))).toBe("PACK");
    expect(pack.length).toBeGreaterThan(0);
  });
});

describe("handleUploadPack refPattern filter", () => {
  test("want of a SHA only reachable from a forbidden ref produces ERR forbidden ref", async () => {
    const { dir, branchA, branchB } = await makeTwoBranchRepo();
    const store = repoStoreFor(dir, [
      { name: "refs/heads/branch-a", sha: branchA },
      { name: "refs/heads/branch-b", sha: branchB },
    ]);
    const body = concat(pkt(`want ${branchB}\n`), flush(), pkt("done\n"));
    const response = await handleUploadPack(
      store,
      principalWith("refs/heads/branch-a"),
      REPO_ID,
      uploadPackRequest(body),
    );
    expect(response.status).toBe(200);
    const buf = await readAll(response);
    const pkts = parsePktStream(buf);
    const data = pkts.filter(
      (p): p is { kind: "data"; payload: Uint8Array } => p.kind === "data",
    );
    expect(data.length).toBe(1);
    const first = data[0];
    if (!first) throw new Error("expected one data pkt-line");
    expect(new TextDecoder().decode(first.payload)).toBe("ERR forbidden ref\n");
  });

  test("want of an unknown SHA produces ERR upload-pack: not our ref", async () => {
    const { dir, c3 } = await makeLinearRepo();
    const store = repoStoreFor(dir, [{ name: "refs/heads/main", sha: c3 }]);
    const bogus = "0".repeat(40);
    const body = concat(pkt(`want ${bogus}\n`), flush(), pkt("done\n"));
    const response = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(body),
    );
    expect(response.status).toBe(200);
    const buf = await readAll(response);
    const pkts = parsePktStream(buf);
    const data = pkts.filter(
      (p): p is { kind: "data"; payload: Uint8Array } => p.kind === "data",
    );
    expect(data.length).toBe(1);
    const first = data[0];
    if (!first) throw new Error("expected one data pkt-line");
    expect(new TextDecoder().decode(first.payload)).toBe(
      "ERR upload-pack: not our ref\n",
    );
  });

  test("want of a non-commit OID reachable from an allowed ref produces ERR upload-pack: not our ref", async () => {
    // A hand-crafted client could want a blob or tree OID that
    // appears in some allowed ref's tree. Such an OID passes the
    // bare `allowedObjects.has(want)` membership check but is not a
    // commit, so it cannot be a valid `want` per the smart-HTTP
    // protocol. The classifier must reject it explicitly rather than
    // letting it fall through to an empty NAK response.
    const { dir, c3 } = await makeLinearRepo();
    const commit = await git.readCommit({ fs, dir, oid: c3 });
    const treeOid = commit.commit.tree;
    const store = repoStoreFor(dir, [{ name: "refs/heads/main", sha: c3 }]);
    const body = concat(pkt(`want ${treeOid}\n`), flush(), pkt("done\n"));
    const response = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(body),
    );
    expect(response.status).toBe(200);
    const buf = await readAll(response);
    const pkts = parsePktStream(buf);
    const data = pkts.filter(
      (p): p is { kind: "data"; payload: Uint8Array } => p.kind === "data",
    );
    expect(data.length).toBe(1);
    const first = data[0];
    if (!first) throw new Error("expected one data pkt-line");
    expect(new TextDecoder().decode(first.payload)).toBe(
      "ERR upload-pack: not our ref\n",
    );
  });

  test("want of an allowed ref tip succeeds when refPattern includes it", async () => {
    const { dir, branchA, branchB } = await makeTwoBranchRepo();
    const store = repoStoreFor(dir, [
      { name: "refs/heads/branch-a", sha: branchA },
      { name: "refs/heads/branch-b", sha: branchB },
    ]);
    const body = concat(pkt(`want ${branchA}\n`), flush(), pkt("done\n"));
    const response = await handleUploadPack(
      store,
      principalWith("refs/heads/branch-a"),
      REPO_ID,
      uploadPackRequest(body),
    );
    expect(response.status).toBe(200);
    const buf = await readAll(response);
    const { nak, frames } = splitNakAndFrames(buf);
    expect(new TextDecoder().decode(nak)).toBe("NAK\n");
    const pack = assembleChannel1(frames);
    expect(new TextDecoder().decode(pack.slice(0, 4))).toBe("PACK");
  });
});

describe("handleUploadPack substrate error translation", () => {
  test("authorize_denied thrown by listRefs surfaces as ERR forbidden ref", async () => {
    const { dir, c3 } = await makeLinearRepo();
    const store: UploadPackRepoStore = {
      listRefs: async () => {
        throw new Error("authorize_denied: token expired");
      },
      getRepoDir: async () => dir,
    };
    const body = concat(pkt(`want ${c3}\n`), flush(), pkt("done\n"));
    const response = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(body),
    );
    expect(response.status).toBe(200);
    const buf = await readAll(response);
    const pkts = parsePktStream(buf);
    const data = pkts.filter(
      (p): p is { kind: "data"; payload: Uint8Array } => p.kind === "data",
    );
    expect(data.length).toBe(1);
    const first = data[0];
    if (!first) throw new Error("expected one data pkt-line");
    expect(new TextDecoder().decode(first.payload)).toBe("ERR forbidden ref\n");
  });

  test("non-substrate errors propagate so the HTTP layer sees a 500", async () => {
    const { dir, c3 } = await makeLinearRepo();
    const store: UploadPackRepoStore = {
      listRefs: async () => {
        throw new Error("disk on fire");
      },
      getRepoDir: async () => dir,
    };
    const body = concat(pkt(`want ${c3}\n`), flush(), pkt("done\n"));
    await expect(
      handleUploadPack(
        store,
        principalWith("**"),
        REPO_ID,
        uploadPackRequest(body),
      ),
    ).rejects.toThrow("disk on fire");
  });
});

describe("handleUploadPack channel routing", () => {
  test("emits no progress frames on channel 2 by default", async () => {
    const { dir, c3 } = await makeLinearRepo();
    const store = repoStoreFor(dir, [{ name: "refs/heads/main", sha: c3 }]);
    const body = concat(pkt(`want ${c3}\n`), flush(), pkt("done\n"));
    const response = await handleUploadPack(
      store,
      principalWith("**"),
      REPO_ID,
      uploadPackRequest(body),
    );
    const buf = await readAll(response);
    const { frames } = splitNakAndFrames(buf);
    for (const f of frames) {
      expect(f.channel).toBe(1);
    }
  });
});
