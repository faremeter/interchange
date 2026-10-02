// Drift guard: the static `mail.definitions` declaration must match the
// tool names the factory's bundle actually emits when instantiated. The
// deploy-time capability walk reads the static declaration WITHOUT
// invoking the factory, so the two must not diverge.
//
// The factory is instantiated with a minimal real env: a host-assembled
// capabilities bag carrying a mock `mail.transport` (the factory's
// `requires: ["capabilities", "address"]`) plus the BaseEnv contract
// fields. Plugin/env-injected tools are intentionally out of scope for
// the static declaration — the walk never sees them.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createDefaultDirectorRegistry } from "@intx/agent";
import { noopAuditStore, permissiveAuthorize } from "@intx/agent/testing";
import { createIsogitStore } from "@intx/storage-isogit/node";
import type {
  InboundMessage,
  InferenceSource,
  MailboxEvent,
  MessageHeaders,
  MessagePart,
  MessageRef,
  MessageTransport,
  OutboundMessage,
  SearchQuery,
  SendReceipt,
  Unsubscribe,
} from "@intx/types/runtime";
import { createRuntimeCapabilities } from "@intx/types/runtime-capabilities";

import type { MailToolEnv } from "./sidecar-bundle";
import { mail } from "./sidecar-bundle";

const SOURCE: InferenceSource = {
  id: "anthropic:mock-model",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  credentialId: "sk-test",
  model: "mock-model",
};

// Minimal MessageTransport: the factory only holds the transport in the
// capability registry at construction time; no method is invoked while
// building the bundle, so the stubs return inert values.
function makeMockTransport(): MessageTransport {
  return {
    async send(_message: OutboundMessage): Promise<SendReceipt> {
      return { messageId: "<stub@test>", status: "delivered" };
    },
    async search(_mailbox: string, _query: SearchQuery): Promise<MessageRef[]> {
      return [];
    },
    async fetchHeaders(ref: MessageRef): Promise<MessageHeaders> {
      return {
        from: "sender@test",
        to: ["agent@test"],
        date: new Date().toISOString(),
        messageId: `<${String(ref.uid)}@test>`,
      };
    },
    async fetchPart(): Promise<MessagePart> {
      return { contentType: "text/plain", content: new Uint8Array() };
    },
    async fetchFull(ref: MessageRef): Promise<InboundMessage> {
      return {
        ref,
        headers: {
          from: "sender@test",
          to: ["agent@test"],
          date: new Date().toISOString(),
          messageId: `<${String(ref.uid)}@test>`,
        },
        flags: [],
        content: "",
        signatureStatus: "missing",
      };
    },

    readRaw(): Promise<Uint8Array> {
      throw new Error(
        "this mock does not serve raw bytes; no case here reads them",
      );
    },
    async setFlags(): Promise<void> {
      throw new Error("mock: setFlags not called in this test");
    },
    async clearFlags(): Promise<void> {
      throw new Error("mock: clearFlags not called in this test");
    },
    async expunge(): Promise<{ expungedUids: number[] }> {
      throw new Error("mock: expunge not called in this test");
    },
    watch(
      _mailbox: string,
      _callback: (event: MailboxEvent) => void,
    ): Unsubscribe {
      throw new Error("mock: watch not called in this test");
    },
  };
}

let tmpDir: string;
let env: MailToolEnv;

beforeAll(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "tools-mail-sidecar-bundle-test-"));
  const storage = await createIsogitStore(tmpDir);
  env = {
    sources: [SOURCE],
    defaultSource: SOURCE.id,
    storage,
    workdir: tmpDir,
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
    capabilities: createRuntimeCapabilities({
      "mail.transport": makeMockTransport(),
    }),
    address: "agent@local.interchange",
  };
});

afterAll(async () => {
  if (tmpDir !== undefined) {
    await rm(tmpDir, { recursive: true, force: true });
  }
});

describe("mail sidecar-bundle static declaration", () => {
  test("declared definition names match the instantiated bundle's names", () => {
    const bundle = mail(env);
    const declared = new Set(mail.definitions.map((d) => d.name));
    const emitted = new Set(bundle.definitions.map((d) => d.name));
    expect(emitted).toEqual(declared);
  });

  test("no mail tool is gated behind per-invocation approval", () => {
    // Mail is ungated: every declaration must omit the approval marker so
    // the capability walk emits no approval gate for these tools. Guard
    // against a vacuous pass on an empty declaration set.
    expect(mail.definitions.length).toBeGreaterThan(0);
    for (const def of mail.definitions) {
      expect(def.approval).toBeUndefined();
    }
  });
});
