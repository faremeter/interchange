import { describe, test, expect } from "bun:test";
import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";

import { handleReceivePack } from "./receive-pack";

const REPO_ID: RepoId = { kind: "agent-state", id: "test" };
const ZERO_OID = "0".repeat(40);

function hex4(n: number): string {
  return n.toString(16).padStart(4, "0");
}

function pkt(payload: string): Uint8Array {
  const enc = new TextEncoder().encode(payload);
  const header = new TextEncoder().encode(hex4(enc.length + 4));
  const out = new Uint8Array(header.length + enc.length);
  out.set(header, 0);
  out.set(enc, header.length);
  return out;
}

const FLUSH = new TextEncoder().encode("0000");

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

function buildBody(
  commands: { oldSha: string; newSha: string; ref: string }[],
  pack: Uint8Array,
  capabilities?: string,
): Uint8Array {
  if (commands.length === 0) {
    throw new Error("buildBody: need at least one command");
  }
  const parts: Uint8Array[] = [];
  for (let i = 0; i < commands.length; i++) {
    const cmd = commands[i];
    if (cmd === undefined) {
      throw new Error("unreachable");
    }
    let line = `${cmd.oldSha} ${cmd.newSha} ${cmd.ref}`;
    if (i === 0 && capabilities !== undefined) {
      line += `\0${capabilities}`;
    }
    line += "\n";
    parts.push(pkt(line));
  }
  parts.push(FLUSH);
  parts.push(pack);
  return concat(...parts);
}

function buildRequest(body: Uint8Array): Request {
  return new Request("http://hub.test/repo/git-receive-pack", {
    method: "POST",
    headers: { "content-type": "application/x-git-receive-pack-request" },
    body,
  });
}

async function readBody(response: Response): Promise<string> {
  const buf = new Uint8Array(await response.arrayBuffer());
  return new TextDecoder().decode(buf);
}

const PRINCIPAL: Principal = { kind: "user" };

type ReceivePackCall = {
  ref: string;
  newSha: string;
  expectedOldSha: string | null;
  packLength: number;
};

type ReceivePackStub = (call: ReceivePackCall) => Promise<void> | void;

function createStubStore(stub: ReceivePackStub): {
  store: RepoStore;
  calls: ReceivePackCall[];
} {
  const calls: ReceivePackCall[] = [];
  const store: RepoStore = {
    initRepo: async () => {
      throw new Error("initRepo: not used in this test");
    },
    writeTree: async () => {
      throw new Error("writeTree: not used in this test");
    },
    createPack: async () => {
      throw new Error("createPack: not used in this test");
    },
    resolveRef: async () => {
      throw new Error("resolveRef: not used in this test");
    },
    listRefs: async () => {
      throw new Error("listRefs: not used in this test");
    },
    resolveHead: async () => {
      throw new Error("resolveHead: not used in this test");
    },
    getRepoDir: () => {
      throw new Error("getRepoDir: not used in this test");
    },
    receivePack: async (
      _principal: Principal,
      _repoId: RepoId,
      ref: string,
      pack: Uint8Array,
      commitSha: string,
      expectedOldSha: string | null,
    ) => {
      const call: ReceivePackCall = {
        ref,
        newSha: commitSha,
        expectedOldSha,
        packLength: pack.length,
      };
      calls.push(call);
      await stub(call);
    },
  };
  return { store, calls };
}

const PACK_BYTES = new Uint8Array([
  0x50, 0x41, 0x43, 0x4b, 0x00, 0x00, 0x00, 0x02,
]);

describe("handleReceivePack response framing", () => {
  test("Content-Type is application/x-git-receive-pack-result", async () => {
    const { store } = createStubStore(() => undefined);
    const body = buildBody(
      [
        {
          oldSha: ZERO_OID,
          newSha: "a".repeat(40),
          ref: "refs/heads/main",
        },
      ],
      PACK_BYTES,
      "report-status side-band-64k",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    expect(response.headers.get("content-type")).toBe(
      "application/x-git-receive-pack-result",
    );
  });
});

describe("handleReceivePack single-ref happy path", () => {
  test("emits unpack ok and ok <ref>", async () => {
    const { store, calls } = createStubStore(() => undefined);
    const newSha = "1".repeat(40);
    const body = buildBody(
      [{ oldSha: ZERO_OID, newSha, ref: "refs/heads/main" }],
      PACK_BYTES,
      "report-status",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    const text = await readBody(response);
    const expected =
      pktText("unpack ok\n") + pktText("ok refs/heads/main\n") + "0000";
    expect(text).toBe(expected);
    expect(calls).toEqual([
      {
        ref: "refs/heads/main",
        newSha,
        expectedOldSha: null,
        packLength: PACK_BYTES.length,
      },
    ]);
  });

  test("non-zero oldSha is forwarded as expectedOldSha", async () => {
    const { store, calls } = createStubStore(() => undefined);
    const oldSha = "a".repeat(40);
    const newSha = "b".repeat(40);
    const body = buildBody(
      [{ oldSha, newSha, ref: "refs/heads/main" }],
      PACK_BYTES,
      "report-status",
    );
    await handleReceivePack(store, PRINCIPAL, REPO_ID, buildRequest(body));
    expect(calls[0]?.expectedOldSha).toBe(oldSha);
  });
});

describe("handleReceivePack multi-ref sequence", () => {
  test("reports per-ref status in the order received", async () => {
    const { store } = createStubStore(() => undefined);
    const newShaA = "a".repeat(40);
    const newShaB = "b".repeat(40);
    const body = buildBody(
      [
        { oldSha: ZERO_OID, newSha: newShaA, ref: "refs/heads/main" },
        { oldSha: ZERO_OID, newSha: newShaB, ref: "refs/heads/dev" },
      ],
      PACK_BYTES,
      "report-status",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    const text = await readBody(response);
    const expected =
      pktText("unpack ok\n") +
      pktText("ok refs/heads/main\n") +
      pktText("ok refs/heads/dev\n") +
      "0000";
    expect(text).toBe(expected);
  });

  test("partial success produces mixed ok/ng status", async () => {
    const { store } = createStubStore((call) => {
      if (call.ref === "refs/heads/dev") {
        throw new Error("path_violation: dev tree not allowed");
      }
    });
    const newShaA = "a".repeat(40);
    const newShaB = "b".repeat(40);
    const body = buildBody(
      [
        { oldSha: ZERO_OID, newSha: newShaA, ref: "refs/heads/main" },
        { oldSha: ZERO_OID, newSha: newShaB, ref: "refs/heads/dev" },
      ],
      PACK_BYTES,
      "report-status",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    const text = await readBody(response);
    const expected =
      pktText("unpack ok\n") +
      pktText("ok refs/heads/main\n") +
      pktText("ng refs/heads/dev path-violation: dev tree not allowed\n") +
      "0000";
    expect(text).toBe(expected);
  });
});

describe("handleReceivePack substrate error translations", () => {
  test("non_fast_forward translates to ng <ref> non-fast-forward", async () => {
    const { store } = createStubStore(() => {
      throw new Error("non_fast_forward: stale oldSha");
    });
    const oldSha = "a".repeat(40);
    const newSha = "b".repeat(40);
    const body = buildBody(
      [{ oldSha, newSha, ref: "refs/heads/main" }],
      PACK_BYTES,
      "report-status",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    const text = await readBody(response);
    const expected =
      pktText("unpack ok\n") +
      pktText("ng refs/heads/main non-fast-forward\n") +
      "0000";
    expect(text).toBe(expected);
  });

  test("path_violation translates byte-correct with reason", async () => {
    const { store } = createStubStore(() => {
      throw new Error("path_violation: foo not under skill/");
    });
    const body = buildBody(
      [
        {
          oldSha: ZERO_OID,
          newSha: "a".repeat(40),
          ref: "refs/heads/main",
        },
      ],
      PACK_BYTES,
      "report-status",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    const text = await readBody(response);
    const expected =
      pktText("unpack ok\n") +
      pktText("ng refs/heads/main path-violation: foo not under skill/\n") +
      "0000";
    expect(text).toBe(expected);
  });

  test("authorize_denied translates byte-correct to forbidden", async () => {
    const { store } = createStubStore(() => {
      throw new Error("authorize_denied: refPattern mismatch");
    });
    const body = buildBody(
      [
        {
          oldSha: ZERO_OID,
          newSha: "a".repeat(40),
          ref: "refs/heads/main",
        },
      ],
      PACK_BYTES,
      "report-status",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    const text = await readBody(response);
    const expected =
      pktText("unpack ok\n") +
      pktText("ng refs/heads/main forbidden\n") +
      "0000";
    expect(text).toBe(expected);
  });

  test("sha_mismatch translates to ng <ref> sha-mismatch", async () => {
    const { store } = createStubStore(() => {
      throw new Error("sha_mismatch: pack tip != claimed newSha");
    });
    const body = buildBody(
      [
        {
          oldSha: ZERO_OID,
          newSha: "a".repeat(40),
          ref: "refs/heads/main",
        },
      ],
      PACK_BYTES,
      "report-status",
    );
    const response = await handleReceivePack(
      store,
      PRINCIPAL,
      REPO_ID,
      buildRequest(body),
    );
    const text = await readBody(response);
    const expected =
      pktText("unpack ok\n") +
      pktText("ng refs/heads/main sha-mismatch\n") +
      "0000";
    expect(text).toBe(expected);
  });
});

describe("handleReceivePack capability parsing", () => {
  test("strips NUL-separated capabilities from the first command line", async () => {
    const { store, calls } = createStubStore(() => undefined);
    const newSha = "a".repeat(40);
    const body = buildBody(
      [{ oldSha: ZERO_OID, newSha, ref: "refs/heads/main" }],
      PACK_BYTES,
      "report-status side-band-64k agent=git/2.40.0",
    );
    await handleReceivePack(store, PRINCIPAL, REPO_ID, buildRequest(body));
    expect(calls[0]?.ref).toBe("refs/heads/main");
  });
});

function pktText(payload: string): string {
  const enc = new TextEncoder().encode(payload);
  return hex4(enc.length + 4) + payload;
}
