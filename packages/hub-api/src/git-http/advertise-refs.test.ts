import { describe, test, expect } from "bun:test";
import type { RepoId } from "@intx/types/sidecar";
import {
  advertiseUploadPack,
  advertiseReceivePack,
  UPLOAD_PACK_CAPABILITIES,
  RECEIVE_PACK_CAPABILITIES,
  EMPTY_REPO_OID,
  type AdvertisePrincipal,
  type RefEntry,
  type RefSource,
} from "./advertise-refs";

const REPO_ID: RepoId = { kind: "agent-state", id: "test" };

function principalWith(refPattern: string): AdvertisePrincipal {
  return {
    kind: "user",
    tokenClaims: { refPattern },
  };
}

function refSourceOf(refs: RefEntry[]): RefSource {
  return {
    listRefs: async () => refs.slice(),
  };
}

async function collect(
  stream: ReadableStream<Uint8Array>,
): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  for (;;) {
    const r = await reader.read();
    if (r.done) break;
    if (r.value) chunks.push(r.value);
  }
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

function hex4(n: number): string {
  return n.toString(16).padStart(4, "0");
}

function pkt(payload: string): string {
  const enc = new TextEncoder().encode(payload);
  return hex4(enc.length + 4) + payload;
}

const FLUSH = "0000";

describe("advertiseUploadPack capability set", () => {
  test("contains side-band-64k, ofs-delta, object-format=sha1, agent", () => {
    expect(UPLOAD_PACK_CAPABILITIES).toContain("side-band-64k");
    expect(UPLOAD_PACK_CAPABILITIES).toContain("ofs-delta");
    expect(UPLOAD_PACK_CAPABILITIES).toContain("object-format=sha1");
    expect(UPLOAD_PACK_CAPABILITIES).toMatch(/\bagent=interchange-hub\/[^\s]+/);
  });

  test("does not include receive-pack-only capabilities", () => {
    expect(UPLOAD_PACK_CAPABILITIES).not.toContain("report-status");
    expect(UPLOAD_PACK_CAPABILITIES).not.toContain("multi_ack");
    expect(UPLOAD_PACK_CAPABILITIES).not.toContain("thin-pack");
    expect(UPLOAD_PACK_CAPABILITIES).not.toContain("shallow");
  });
});

describe("advertiseReceivePack capability set", () => {
  test("advertises report-status alongside the shared baseline", () => {
    expect(RECEIVE_PACK_CAPABILITIES).toContain("report-status");
    expect(RECEIVE_PACK_CAPABILITIES).toContain("ofs-delta");
    expect(RECEIVE_PACK_CAPABILITIES).toContain("object-format=sha1");
    expect(RECEIVE_PACK_CAPABILITIES).toMatch(
      /\bagent=interchange-hub\/[^\s]+/,
    );
  });

  test("does not advertise side-band-64k on receive-pack", () => {
    // The receive-pack handler returns the report-status payload as
    // raw pkt-lines. Advertising side-band-64k would make stock git
    // expect a channel-framed response and abort with
    // `protocol error: bad band`.
    expect(RECEIVE_PACK_CAPABILITIES).not.toContain("side-band-64k");
  });

  test("does not include thin-pack on receive-pack", () => {
    expect(RECEIVE_PACK_CAPABILITIES).not.toContain("thin-pack");
  });
});

describe("advertiseUploadPack stream shape", () => {
  test("service-prefix, flush, single ref carries caps NUL-separated, trailing flush", async () => {
    const sha = "1111111111111111111111111111111111111111";
    const refs: RefEntry[] = [{ name: "refs/heads/main", sha }];
    const out = await collect(
      await advertiseUploadPack(
        refSourceOf(refs),
        principalWith("**"),
        REPO_ID,
      ),
    );
    const expected =
      pkt("# service=git-upload-pack\n") +
      FLUSH +
      pkt(`${sha} refs/heads/main\0${UPLOAD_PACK_CAPABILITIES}\n`) +
      FLUSH;
    expect(new TextDecoder().decode(out)).toBe(expected);
  });

  test("first ref carries caps NUL-separated; subsequent refs have no NUL", async () => {
    const shaA = "a".repeat(40);
    const shaB = "b".repeat(40);
    const refs: RefEntry[] = [
      { name: "refs/heads/main", sha: shaA },
      { name: "refs/tags/v1", sha: shaB },
    ];
    const out = await collect(
      await advertiseUploadPack(
        refSourceOf(refs),
        principalWith("**"),
        REPO_ID,
      ),
    );
    const decoded = new TextDecoder().decode(out);
    const expected =
      pkt("# service=git-upload-pack\n") +
      FLUSH +
      pkt(`${shaA} refs/heads/main\0${UPLOAD_PACK_CAPABILITIES}\n`) +
      pkt(`${shaB} refs/tags/v1\n`) +
      FLUSH;
    expect(decoded).toBe(expected);
    expect(decoded.includes(`refs/tags/v1\0`)).toBe(false);
  });

  test("refs are listed deterministically by lexicographic name", async () => {
    const sha = "c".repeat(40);
    const refs: RefEntry[] = [
      { name: "refs/heads/zeta", sha },
      { name: "refs/heads/alpha", sha },
      { name: "refs/heads/main", sha },
    ];
    const out = await collect(
      await advertiseUploadPack(
        refSourceOf(refs),
        principalWith("**"),
        REPO_ID,
      ),
    );
    const decoded = new TextDecoder().decode(out);
    const expected =
      pkt("# service=git-upload-pack\n") +
      FLUSH +
      pkt(`${sha} refs/heads/alpha\0${UPLOAD_PACK_CAPABILITIES}\n`) +
      pkt(`${sha} refs/heads/main\n`) +
      pkt(`${sha} refs/heads/zeta\n`) +
      FLUSH;
    expect(decoded).toBe(expected);
  });
});

describe("advertiseUploadPack empty-repo special case", () => {
  test("emits zero-oid capabilities^{} record so git clone succeeds", async () => {
    const out = await collect(
      await advertiseUploadPack(refSourceOf([]), principalWith("**"), REPO_ID),
    );
    const expected =
      pkt("# service=git-upload-pack\n") +
      FLUSH +
      pkt(`${EMPTY_REPO_OID} capabilities^{}\0${UPLOAD_PACK_CAPABILITIES}\n`) +
      FLUSH;
    expect(new TextDecoder().decode(out)).toBe(expected);
    expect(EMPTY_REPO_OID).toBe("0".repeat(40));
  });

  test("empty-repo record emitted when refPattern filters all refs out", async () => {
    const sha = "d".repeat(40);
    const refs: RefEntry[] = [
      { name: "refs/heads/main", sha },
      { name: "refs/tags/v1", sha },
    ];
    const out = await collect(
      await advertiseUploadPack(
        refSourceOf(refs),
        principalWith("refs/heads/release/*"),
        REPO_ID,
      ),
    );
    const expected =
      pkt("# service=git-upload-pack\n") +
      FLUSH +
      pkt(`${EMPTY_REPO_OID} capabilities^{}\0${UPLOAD_PACK_CAPABILITIES}\n`) +
      FLUSH;
    expect(new TextDecoder().decode(out)).toBe(expected);
  });
});

describe("advertiseUploadPack ref filtering by refPattern", () => {
  test("hides refs that do not match the principal's refPattern", async () => {
    const sha = "e".repeat(40);
    const refs: RefEntry[] = [
      { name: "refs/heads/main", sha },
      { name: "refs/heads/feature/x", sha },
      { name: "refs/tags/v1", sha },
    ];
    const out = await collect(
      await advertiseUploadPack(
        refSourceOf(refs),
        principalWith("refs/heads/*"),
        REPO_ID,
      ),
    );
    const decoded = new TextDecoder().decode(out);
    const expected =
      pkt("# service=git-upload-pack\n") +
      FLUSH +
      pkt(`${sha} refs/heads/main\0${UPLOAD_PACK_CAPABILITIES}\n`) +
      FLUSH;
    expect(decoded).toBe(expected);
    expect(decoded.includes("refs/heads/feature/x")).toBe(false);
    expect(decoded.includes("refs/tags/v1")).toBe(false);
  });

  test("doublestar pattern allows nested refs through", async () => {
    const sha = "f".repeat(40);
    const refs: RefEntry[] = [
      { name: "refs/heads/main", sha },
      { name: "refs/heads/feature/x", sha },
    ];
    const out = await collect(
      await advertiseUploadPack(
        refSourceOf(refs),
        principalWith("refs/heads/**"),
        REPO_ID,
      ),
    );
    const decoded = new TextDecoder().decode(out);
    const expected =
      pkt("# service=git-upload-pack\n") +
      FLUSH +
      pkt(`${sha} refs/heads/feature/x\0${UPLOAD_PACK_CAPABILITIES}\n`) +
      pkt(`${sha} refs/heads/main\n`) +
      FLUSH;
    expect(decoded).toBe(expected);
  });
});

describe("advertiseReceivePack stream shape", () => {
  test("service-prefix is git-receive-pack and caps include report-status", async () => {
    const sha = "1".repeat(40);
    const refs: RefEntry[] = [{ name: "refs/heads/main", sha }];
    const out = await collect(
      await advertiseReceivePack(
        refSourceOf(refs),
        principalWith("**"),
        REPO_ID,
      ),
    );
    const expected =
      pkt("# service=git-receive-pack\n") +
      FLUSH +
      pkt(`${sha} refs/heads/main\0${RECEIVE_PACK_CAPABILITIES}\n`) +
      FLUSH;
    expect(new TextDecoder().decode(out)).toBe(expected);
  });

  test("empty repo on receive-pack emits zero-oid capabilities^{} record", async () => {
    const out = await collect(
      await advertiseReceivePack(refSourceOf([]), principalWith("**"), REPO_ID),
    );
    const expected =
      pkt("# service=git-receive-pack\n") +
      FLUSH +
      pkt(`${EMPTY_REPO_OID} capabilities^{}\0${RECEIVE_PACK_CAPABILITIES}\n`) +
      FLUSH;
    expect(new TextDecoder().decode(out)).toBe(expected);
  });
});

describe("advertise functions pass principal and repoId through to RefSource", () => {
  test("listRefs receives principal and repoId untouched", async () => {
    const sha = "2".repeat(40);
    const seen: { principal: AdvertisePrincipal; repoId: RepoId }[] = [];
    const refSource: RefSource = {
      listRefs: async (principal, repoId) => {
        seen.push({ principal, repoId });
        return [{ name: "refs/heads/main", sha }];
      },
    };
    const principal = principalWith("**");
    await collect(await advertiseUploadPack(refSource, principal, REPO_ID));
    expect(seen).toEqual([{ principal, repoId: REPO_ID }]);
  });
});
