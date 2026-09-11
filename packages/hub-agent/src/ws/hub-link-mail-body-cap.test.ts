// The sidecar refuses to emit an over-cap `mail.outbound` frame. The two send
// paths handle it differently and this pins both: the remote-send handler
// throws (it is awaited through the transport, so the producing agent gets a
// real error), while the post-delivery audit handler logs and skips (a throw
// there is swallowed by the transport's Promise.allSettled, so throwing would
// be an invisible dead check). The hub re-enforces the cap on receive -- that
// is the authoritative DoS backstop, covered separately in the hub-sessions
// suite; these send-side checks are the producer-facing complement.
//
// The two handlers are captured through a fake transport and invoked directly,
// so the test needs no live socket: `createHubLink` only touches the transport
// to register these handlers.

import {
  describe,
  test,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
} from "bun:test";
import { MAX_MAIL_OUTBOUND_BODY_BYTES } from "@intx/types/sidecar";
import type {
  HubTransport,
  RemoteSendHandler,
  MessageSentHandler,
} from "@intx/mail-memory";
import { configureSync, getConfig, resetSync } from "@intx/log";

import { createHubLink, type DeployRouter } from "./hub-link";
import type { AgentKeyStore } from "../agent-key-store";
import type { SessionManager } from "../session-manager";

// A transport that captures the two send handlers `createHubLink` registers and
// nothing else -- construction never calls the remaining methods, so they guard
// against accidental use rather than model real behavior.
function createCapturingTransport(): {
  transport: HubTransport;
  remoteSend: () => RemoteSendHandler;
  messageSent: () => MessageSentHandler;
} {
  let remoteSend: RemoteSendHandler | undefined;
  let messageSent: MessageSentHandler | undefined;
  const transport: HubTransport = {
    register: () => undefined,
    unregister: () => undefined,
    getTransportFor: () => {
      throw new Error("getTransportFor is not used in this test");
    },
    deliver: () => undefined,
    setRemoteSendHandler: (handler) => {
      remoteSend = handler;
    },
    addMessageSentHandler: (handler) => {
      messageSent = handler;
    },
  };
  return {
    transport,
    remoteSend: () => {
      if (remoteSend === undefined) throw new Error("remote handler not set");
      return remoteSend;
    },
    messageSent: () => {
      if (messageSent === undefined)
        throw new Error("messageSent handler not set");
      return messageSent;
    },
  };
}

// The deploy/session/key deps are required by the config but never reached by
// the send-cap paths; they throw if misused rather than model behavior.
const unusedSessions: SessionManager = {
  initRepo: () => {
    throw new Error("sessions not used");
  },
  applyDeployPack: () => {
    throw new Error("sessions not used");
  },
  applyAssetPack: () => {
    throw new Error("sessions not used");
  },
  createStatePack: () => {
    throw new Error("sessions not used");
  },
  deleteAgentDir: () => {
    throw new Error("sessions not used");
  },
  getAddresses: () => [],
  // Reached only on the within-cap audit path; returns no session id.
  getSessionId: () => undefined,
};

const unusedKeyStore: AgentKeyStore = {
  loadOrGenerateKey: () => {
    throw new Error("keyStore not used");
  },
  recordHubKey: () => {
    throw new Error("keyStore not used");
  },
  verifyDeployCommit: () => {
    throw new Error("keyStore not used");
  },
  forgetAgent: () => {
    throw new Error("keyStore not used");
  },
};

const unusedDeployRouter: DeployRouter = {
  deploy: () => {
    throw new Error("deployRouter not used");
  },
};

function makeLink() {
  const capture = createCapturingTransport();
  const link = createHubLink({
    hubURL: "ws://localhost:0/ws",
    sidecarId: "sc-body-cap",
    token: "test-token",
    transport: capture.transport,
    sessions: unusedSessions,
    keyStore: unusedKeyStore,
    resolveSenderCrypto: () => undefined,
    cacheSenderKey: async () => undefined,
    evictSenderKey: async () => undefined,
    deployRouter: unusedDeployRouter,
  });
  return { capture, link };
}

// A rawMessage whose base64 encoding exceeds the body cap. base64 inflates by
// ~4/3, so 44MB+1 raw bytes is comfortably over the 44MB base64 ceiling.
const overCapRaw = new Uint8Array(MAX_MAIL_OUTBOUND_BODY_BYTES + 1);
const smallRaw = new TextEncoder().encode("a legit little message");

type CapturedLog = {
  category: readonly string[];
  level: string;
  message: readonly unknown[];
};

const capturedLogs: CapturedLog[] = [];
const savedLogConfig = getConfig();

beforeAll(() => {
  configureSync({
    reset: true,
    sinks: {
      capture: (record) => {
        capturedLogs.push({
          category: record.category,
          level: record.level,
          message: record.message,
        });
      },
    },
    loggers: [
      { category: [], lowestLevel: "debug", sinks: ["capture"] },
      {
        category: ["logtape", "meta"],
        lowestLevel: "warning",
        sinks: ["capture"],
      },
    ],
  });
});

afterAll(() => {
  if (savedLogConfig) {
    configureSync({ reset: true, ...savedLogConfig });
  } else {
    resetSync();
  }
});

function capErrors(): string[] {
  return capturedLogs
    .filter((r) => r.level === "error")
    .map((r) => r.message.join(""));
}

describe("hub-link mail.outbound body cap", () => {
  beforeEach(() => {
    capturedLogs.length = 0;
  });

  test("the remote-send handler throws on an over-cap mail", async () => {
    const { capture, link } = makeLink();
    try {
      await expect(
        capture.remoteSend()(overCapRaw, ["r@example.test"], "s@example.test"),
      ).rejects.toThrow(/exceeds the .* cap/);
    } finally {
      link.close();
    }
  });

  test("the remote-send handler passes a within-cap mail", async () => {
    const { capture, link } = makeLink();
    try {
      await capture.remoteSend()(
        smallRaw,
        ["r@example.test"],
        "s@example.test",
      );
      // A legit-sized send does not trip the cap: no throw, no error log.
      expect(capErrors()).toEqual([]);
    } finally {
      link.close();
    }
  });

  test("the audit handler logs and skips an over-cap mail without throwing", async () => {
    const { capture, link } = makeLink();
    try {
      // A throw here is swallowed by the transport, so the check must LOG.
      await capture.messageSent()({
        senderAddress: "s@example.test",
        rawMessage: overCapRaw,
        messageId: "mid-over",
        recipients: ["r@example.test"],
        to: ["r@example.test"],
        cc: [],
        localOnly: true,
      });
      const errors = capErrors();
      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatch(/exceeds the .* cap/);
    } finally {
      link.close();
    }
  });

  test("the audit handler passes a within-cap mail", async () => {
    const { capture, link } = makeLink();
    try {
      await capture.messageSent()({
        senderAddress: "s@example.test",
        rawMessage: smallRaw,
        messageId: "mid-ok",
        recipients: ["r@example.test"],
        to: ["r@example.test"],
        cc: [],
        localOnly: true,
      });
      expect(capErrors()).toEqual([]);
    } finally {
      link.close();
    }
  });
});
