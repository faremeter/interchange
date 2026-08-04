import { describe, test, expect } from "bun:test";
import type { CredentialProvider } from "@intx/types";

import {
  createCredentialProviderRegistry,
  createHttpCredentialProvider,
  builtinCredentialProviders,
  type FetchLike,
} from "./credential-providers";

// A fetch stub that records the URL and Authorization header it was handed,
// so origin-pinning and auth injection can be checked without a network.
function recordingFetch(): {
  fetch: FetchLike;
  last: () => { url: string; auth: string | null } | undefined;
} {
  let captured: { url: string; auth: string | null } | undefined;
  const fetch: FetchLike = async (input, init) => {
    const req =
      input instanceof Request ? input : new Request(String(input), init);
    captured = { url: req.url, auth: req.headers.get("authorization") };
    return new Response("ok", { status: 200 });
  };
  return { fetch, last: () => captured };
}

function makeHandle(net: { fetch: FetchLike }, secret = "sk-1") {
  const provider = createHttpCredentialProvider({ fetch: net.fetch });
  return provider.shape({
    origin: "https://api.example.com",
    readCurrentMaterial: () => ({ secret }),
  });
}

describe("createHttpCredentialProvider", () => {
  test("injects the current secret as a bearer token, pinned to the origin", async () => {
    const net = recordingFetch();
    const provider = createHttpCredentialProvider({ fetch: net.fetch });
    const handle = provider.shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret: "sk-1" }),
    });

    const res = await handle.fetch("/repos");
    expect(res.status).toBe(200);
    expect(net.last()).toEqual({
      url: "https://api.example.com/repos",
      auth: "Bearer sk-1",
    });
  });

  test("reads material fresh on every call so a rotation is picked up", async () => {
    const net = recordingFetch();
    let secret = "sk-old";
    const provider = createHttpCredentialProvider({ fetch: net.fetch });
    const handle = provider.shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret }),
    });

    await handle.fetch("/a");
    expect(net.last()?.auth).toBe("Bearer sk-old");

    // Rotate the underlying cell; the same handle must use the new secret.
    secret = "sk-new";
    await handle.fetch("/b");
    expect(net.last()?.auth).toBe("Bearer sk-new");
  });

  test("refuses a cross-origin request and never reaches fetch", async () => {
    const net = recordingFetch();
    const provider = createHttpCredentialProvider({ fetch: net.fetch });
    const handle = provider.shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret: "sk-1" }),
    });

    await expect(
      handle.fetch("https://evil.example.net/steal"),
    ).rejects.toThrow(
      /pinned to https:\/\/api\.example\.com; refusing cross-origin/,
    );
    // The stub was never called, so the token never left for the other origin.
    expect(net.last()).toBeUndefined();
  });

  test("preserves method and body when handed a Request, adding auth", async () => {
    const net = recordingFetch();
    const provider = createHttpCredentialProvider({ fetch: net.fetch });
    const handle = provider.shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret: "sk-1" }),
    });

    await handle.fetch(
      new Request("https://api.example.com/issues", { method: "POST" }),
    );
    expect(net.last()).toEqual({
      url: "https://api.example.com/issues",
      auth: "Bearer sk-1",
    });
  });

  test("a caller-supplied Authorization header is overridden by the credential's", async () => {
    const net = recordingFetch();
    const provider = createHttpCredentialProvider({ fetch: net.fetch });
    const handle = provider.shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret: "sk-real" }),
    });

    await handle.fetch("/x", {
      headers: { authorization: "Bearer sk-attacker" },
    });
    expect(net.last()?.auth).toBe("Bearer sk-real");
  });

  test("dispose is callable on the http handle", () => {
    const provider = createHttpCredentialProvider();
    const handle = provider.shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret: "sk-1" }),
    });
    expect(() => handle.dispose()).not.toThrow();
  });
});

describe("createCredentialProviderRegistry", () => {
  const fake: CredentialProvider = {
    key: "fake",
    shape: () => ({
      kind: "http",
      fetch: async () => new Response(),
      dispose: () => {
        /* the fake holds no resources */
      },
    }),
  };

  test("resolves each registered provider by key", () => {
    const registry = createCredentialProviderRegistry([
      createHttpCredentialProvider(),
      fake,
    ]);
    expect(registry.resolve("http").key).toBe("http");
    expect(registry.resolve("fake")).toBe(fake);
    expect(registry.has("fake")).toBe(true);
    expect(registry.has("nope")).toBe(false);
  });

  test("throws loudly on an unknown provider", () => {
    const registry = createCredentialProviderRegistry(
      builtinCredentialProviders(),
    );
    expect(() => registry.resolve("nope")).toThrow(
      /Unknown credential provider: nope/,
    );
  });

  test("an untrusted key cannot reach an Object.prototype member", () => {
    const registry = createCredentialProviderRegistry(
      builtinCredentialProviders(),
    );
    // "toString" exists on Object.prototype; a Map-backed lookup must not find
    // it and try to invoke it as a provider.
    expect(() => registry.resolve("toString")).toThrow(
      /Unknown credential provider: toString/,
    );
    expect(registry.has("toString")).toBe(false);
  });

  test("a duplicate provider key is a wiring error at construction", () => {
    expect(() => createCredentialProviderRegistry([fake, fake])).toThrow(
      /Duplicate credential provider key: fake/,
    );
  });
});

describe("http credential origin-pinning (adversarial)", () => {
  // Origin pinning is the load-bearing security property: in every refusal
  // case the request must be rejected AND fetch must never be reached, so the
  // bearer token never leaves for the other origin.
  test("a protocol-relative //host is refused", async () => {
    const net = recordingFetch();
    await expect(
      makeHandle(net).fetch("//evil.example.net/steal"),
    ).rejects.toThrow(/refusing cross-origin/);
    expect(net.last()).toBeUndefined();
  });

  test("a suffix-confusion host (api.example.com.evil.com) is refused", async () => {
    const net = recordingFetch();
    await expect(
      makeHandle(net).fetch("https://api.example.com.evil.com/x"),
    ).rejects.toThrow(/refusing cross-origin/);
    expect(net.last()).toBeUndefined();
  });

  test("a userinfo trick (api.example.com@evil.com) is refused", async () => {
    const net = recordingFetch();
    await expect(
      makeHandle(net).fetch("https://api.example.com@evil.com/x"),
    ).rejects.toThrow(/refusing cross-origin/);
    expect(net.last()).toBeUndefined();
  });

  test("a non-default port on the pinned host is a distinct origin, refused", async () => {
    const net = recordingFetch();
    await expect(
      makeHandle(net).fetch("https://api.example.com:8443/x"),
    ).rejects.toThrow(/refusing cross-origin/);
    expect(net.last()).toBeUndefined();
  });

  test("a scheme downgrade (http to a pinned https origin) is refused", async () => {
    const net = recordingFetch();
    await expect(
      makeHandle(net).fetch("http://api.example.com/x"),
    ).rejects.toThrow(/refusing cross-origin/);
    expect(net.last()).toBeUndefined();
  });

  test("a cross-origin URL object is refused", async () => {
    const net = recordingFetch();
    await expect(
      makeHandle(net).fetch(new URL("https://evil.example.net/x")),
    ).rejects.toThrow(/refusing cross-origin/);
    expect(net.last()).toBeUndefined();
  });

  test("a cross-origin Request object is refused (its url is the checked one)", async () => {
    const net = recordingFetch();
    await expect(
      makeHandle(net).fetch(new Request("https://evil.example.net/x")),
    ).rejects.toThrow(/refusing cross-origin/);
    expect(net.last()).toBeUndefined();
  });

  test("the Request branch overrides a mixed-case caller Authorization header", async () => {
    const net = recordingFetch();
    await makeHandle(net, "sk-real").fetch(
      new Request("https://api.example.com/x", {
        headers: { Authorization: "Bearer sk-attacker" },
      }),
    );
    expect(net.last()?.auth).toBe("Bearer sk-real");
  });
});

describe("http credential redirect handling", () => {
  // The handle never follows a redirect: a 3xx is returned to the caller
  // unfollowed, so the bearer is never sent to the redirect target's origin.
  test("returns a 3xx to the caller and calls fetch exactly once", async () => {
    let calls = 0;
    const fetch: FetchLike = async () => {
      calls += 1;
      return new Response(null, {
        status: 302,
        headers: { location: "https://evil.example.net/" },
      });
    };
    const handle = createHttpCredentialProvider({ fetch }).shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret: "sk-1" }),
    });

    const res = await handle.fetch("/x");
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("https://evil.example.net/");
    // Never chased the Location -- the token never left the pinned origin.
    expect(calls).toBe(1);
  });

  test("forces redirect:manual even when a caller Request asks to follow", async () => {
    const seen: (string | undefined)[] = [];
    const fetch: FetchLike = async (input, init) => {
      seen.push(input instanceof Request ? input.redirect : init?.redirect);
      return new Response("ok");
    };
    const handle = createHttpCredentialProvider({ fetch }).shape({
      origin: "https://api.example.com",
      readCurrentMaterial: () => ({ secret: "sk-1" }),
    });

    // Non-Request branch: the init the handle passes forces manual.
    await handle.fetch("/a");
    // Request branch: the caller's redirect:"follow" must be overridden.
    await handle.fetch(
      new Request("https://api.example.com/b", { redirect: "follow" }),
    );

    expect(seen).toEqual(["manual", "manual"]);
  });
});
