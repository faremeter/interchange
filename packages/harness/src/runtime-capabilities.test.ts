import { describe, test, expect } from "bun:test";
import type { MessageTransport } from "@intx/types/runtime";
import type { MailSigner } from "@intx/types/mail-signer";

import { createHarnessRuntimeCapabilities } from "./runtime-capabilities";

// Minimal stand-in for MessageTransport. The factory passes the handle
// through; it does not invoke any methods on it.
// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test-only stand-in; factory never calls these methods
const stubTransport = {} as unknown as MessageTransport;

const stubSigner: MailSigner = {
  compose() {
    return Promise.reject(new Error("not called"));
  },
};

describe("createHarnessRuntimeCapabilities", () => {
  test("resolve('mail.transport') returns the supplied transport reference", () => {
    const capabilities = createHarnessRuntimeCapabilities({
      transport: stubTransport,
      signer: stubSigner,
    });

    expect(capabilities.resolve("mail.transport")).toBe(stubTransport);
  });

  test("resolve('mail.signer') returns the supplied signer reference", () => {
    const capabilities = createHarnessRuntimeCapabilities({
      transport: stubTransport,
      signer: stubSigner,
    });

    expect(capabilities.resolve("mail.signer")).toBe(stubSigner);
  });
});
