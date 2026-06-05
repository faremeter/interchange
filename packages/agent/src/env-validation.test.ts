import { describe, test, expect } from "bun:test";

import { type AgentDefinition, defineAgent } from "./definition";
import { createDefaultDirectorRegistry } from "./director-registry";
import { AgentEnvError, type BaseEnv } from "./env";
import {
  effectiveDirectorRef,
  getRequiredEnvKeys,
  validateEnv,
} from "./env-validation";
import { noopAuditStore } from "./testing/audit-noop";
import { permissiveAuthorize } from "./testing/authorize-allow";
import { defineTool } from "./tool";

const SOURCE = {
  id: "anthropic:claude-3-5-sonnet",
  provider: "anthropic",
  baseURL: "https://api.anthropic.com",
  apiKey: "sk-test",
  model: "claude-3-5-sonnet",
};

// eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; never invoked on the validation path
const STORAGE = {} as unknown as BaseEnv["storage"];

function baseEnv(): BaseEnv {
  return {
    source: SOURCE,
    storage: STORAGE,
    workdir: "/tmp/x",
    audit: noopAuditStore(),
    authorize: permissiveAuthorize(),
    directors: createDefaultDirectorRegistry(),
  };
}

function emptyDef() {
  return defineAgent({
    id: "agent",
    systemPrompt: "test",
    tools: [],
    capabilities: [],
    inference: { sources: [] },
  });
}

describe("validateEnv", () => {
  test("passes when every required key is present", () => {
    expect(() => validateEnv(emptyDef(), baseEnv())).not.toThrow();
  });

  test("does not require env.compactors", () => {
    // The field is optional; an env that simply omits it is valid and a
    // director that never emits `caps.compact(...)` runs unaffected.
    const env = baseEnv();
    expect(env.compactors).toBeUndefined();
    expect(() => validateEnv(emptyDef(), env)).not.toThrow();
  });

  test("accepts env.compactors when supplied", () => {
    const env: BaseEnv = { ...baseEnv(), compactors: {} };
    expect(() => validateEnv(emptyDef(), env)).not.toThrow();
  });

  test("collects missing core BaseEnv keys and blames BaseEnv", () => {
    const env = baseEnv();
    const bad = {
      ...env,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap
      storage: undefined as unknown as BaseEnv["storage"],
    };
    try {
      validateEnv(emptyDef(), bad);
      throw new Error("expected throw");
    } catch (err) {
      if (!(err instanceof AgentEnvError)) throw err;
      expect(err.missing).toContain("storage");
      expect(err.contributors).toContain("BaseEnv");
    }
  });

  test("blames the tool factory that declared the missing key", () => {
    interface MailEnv extends BaseEnv {
      transport: unknown;
    }
    const factory = defineTool<MailEnv>({
      id: "@intx/tools-mail/send",
      requires: ["transport"],
      factory: () => ({
        definitions: [],
        async run(call) {
          return { callId: call.id, content: "" };
        },
      }),
    });

    const def = defineAgent({
      id: "mail",
      systemPrompt: "x",
      tools: [factory],
      capabilities: [],
      inference: { sources: [] },
    });

    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bypass type-level check to exercise runtime presence validation
      validateEnv(def as unknown as AgentDefinition<BaseEnv>, baseEnv());
      throw new Error("expected throw");
    } catch (err) {
      if (!(err instanceof AgentEnvError)) throw err;
      expect(err.missing).toContain("transport");
      expect(err.contributors).toContain("tool:@intx/tools-mail/send");
    }
  });

  test("blames both BaseEnv and a tool factory when each is missing a key", () => {
    interface MailEnv extends BaseEnv {
      transport: unknown;
    }
    const factory = defineTool<MailEnv>({
      id: "@intx/tools-mail/send",
      requires: ["transport"],
      factory: () => ({
        definitions: [],
        async run(call) {
          return { callId: call.id, content: "" };
        },
      }),
    });

    const def = defineAgent({
      id: "mail",
      systemPrompt: "x",
      tools: [factory],
      capabilities: [],
      inference: { sources: [] },
    });

    const env = baseEnv();
    const bad = {
      ...env,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap
      audit: undefined as unknown as BaseEnv["audit"],
    };

    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bypass type-level check to exercise runtime presence validation
      validateEnv(def as unknown as AgentDefinition<BaseEnv>, bad);
      throw new Error("expected throw");
    } catch (err) {
      if (!(err instanceof AgentEnvError)) throw err;
      expect(err.missing).toContain("audit");
      expect(err.missing).toContain("transport");
      expect(err.contributors).toContain("BaseEnv");
      expect(err.contributors).toContain("tool:@intx/tools-mail/send");
    }
  });

  test("does not blame a director when the registry is itself missing", () => {
    const env = baseEnv();
    const bad = {
      ...env,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap
      directors: undefined as unknown as BaseEnv["directors"],
    };
    try {
      validateEnv(emptyDef(), bad);
      throw new Error("expected throw");
    } catch (err) {
      if (!(err instanceof AgentEnvError)) throw err;
      expect(err.missing).toContain("directors");
      // No "director:..." contributor because we cannot resolve a
      // factory without the registry. Only BaseEnv is blamed.
      for (const c of err.contributors) {
        expect(c.startsWith("director:")).toBe(false);
      }
      // The registry is itself missing, not unresolvable; the
      // unresolvedDirectors channel is reserved for the "ref names a
      // director the registry does not contain" case.
      expect(err.unresolvedDirectors).toEqual([]);
    }
  });

  test("surfaces unknown director ids on AgentEnvError.unresolvedDirectors", () => {
    // Use the default registry but reference a director it does not
    // contain. The registry's `resolve` throws; validateEnv funnels
    // the failure through AgentEnvError's separate field so callers
    // can distinguish "missing env key" from "unknown director id".
    const def = defineAgent({
      id: "agent",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: { sources: [] },
      director: { id: "@vendor/pkg/not-registered", config: {} },
    });
    try {
      validateEnv(def, baseEnv());
      throw new Error("expected throw");
    } catch (err) {
      if (!(err instanceof AgentEnvError)) throw err;
      expect(err.missing).toEqual([]);
      expect(err.unresolvedDirectors).toEqual(["@vendor/pkg/not-registered"]);
    }
  });

  test("pairs each contributor with its specific missing keys", () => {
    interface MailEnv extends BaseEnv {
      transport: unknown;
    }
    const factory = defineTool<MailEnv>({
      id: "@intx/tools-mail/send",
      requires: ["transport"],
      factory: () => ({
        definitions: [],
        async run(call) {
          return { callId: call.id, content: "" };
        },
      }),
    });
    const def = defineAgent({
      id: "mail",
      systemPrompt: "x",
      tools: [factory],
      capabilities: [],
      inference: { sources: [] },
    });
    const env = baseEnv();
    const bad = {
      ...env,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- intentional gap
      audit: undefined as unknown as BaseEnv["audit"],
    };
    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- bypass type-level check
      validateEnv(def as unknown as AgentDefinition<BaseEnv>, bad);
      throw new Error("expected throw");
    } catch (err) {
      if (!(err instanceof AgentEnvError)) throw err;
      const baseEnvKeys = err.missingByContributor.get("BaseEnv");
      const toolKeys = err.missingByContributor.get(
        "tool:@intx/tools-mail/send",
      );
      expect(baseEnvKeys).toContain("audit");
      expect(toolKeys).toContain("transport");
      // BaseEnv does not blame `transport` and the tool factory does
      // not blame `audit`; the join is precise per-contributor.
      expect(baseEnvKeys).not.toContain("transport");
      expect(toolKeys).not.toContain("audit");
    }
  });

  test("propagates non-UnknownDirectorIdError faults from a custom registry", () => {
    // A caller-supplied DirectorRegistry that throws something other
    // than UnknownDirectorIdError on resolve must not have its real
    // failure silently relabelled as an unresolved-director id.
    const realError = new TypeError("custom registry exploded");
    const hostileRegistry: BaseEnv["directors"] = {
      resolve() {
        throw realError;
      },
      defaultFactory() {
        throw realError;
      },
      buildDefaultRef() {
        return { id: "@vendor/pkg/anything", config: {} };
      },
    };
    const def = defineAgent({
      id: "agent",
      systemPrompt: "test",
      tools: [],
      capabilities: [],
      inference: { sources: [] },
      director: { id: "@vendor/pkg/anything", config: {} },
    });
    expect(() =>
      validateEnv(def, { ...baseEnv(), directors: hostileRegistry }),
    ).toThrow(realError);
  });
});

describe("getRequiredEnvKeys", () => {
  test("returns the six BaseEnv keys when no tools or director declare more", () => {
    const result = getRequiredEnvKeys(
      emptyDef(),
      createDefaultDirectorRegistry(),
    );
    expect(result.keys).toEqual([
      "source",
      "storage",
      "workdir",
      "audit",
      "authorize",
      "directors",
    ]);
    expect(result.unresolvedDirectorId).toBeNull();
  });

  test("aggregates tool factory requires", () => {
    interface MailEnv extends BaseEnv {
      transport: unknown;
      address: string;
    }
    const factory = defineTool<MailEnv>({
      id: "pkg/mail",
      requires: ["transport", "address"],
      factory: () => ({
        definitions: [],
        async run(call) {
          return { callId: call.id, content: "" };
        },
      }),
    });
    const def = defineAgent({
      id: "x",
      systemPrompt: "x",
      tools: [factory],
      capabilities: [],
      inference: { sources: [] },
    });
    const result = getRequiredEnvKeys(
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- getRequiredEnvKeys is invariant in EnvReq; widen to the BaseEnv-typed parameter to call it
      def as unknown as AgentDefinition<BaseEnv>,
      createDefaultDirectorRegistry(),
    );
    expect(result.keys).toContain("transport");
    expect(result.keys).toContain("address");
    expect(result.unresolvedDirectorId).toBeNull();
  });

  test("surfaces an unresolved director id alongside the partial key set", () => {
    // Pins the contract: when the definition's director ref points
    // at an id the registry does not contain, the helper returns the
    // best partial answer (BaseEnv + tool keys) AND names the
    // unresolved id, so a single call answers both "what env keys
    // must I populate?" and "did the director resolve?".
    interface MailEnv extends BaseEnv {
      transport: unknown;
    }
    const factory = defineTool<MailEnv>({
      id: "pkg/mail",
      requires: ["transport"],
      factory: () => ({
        definitions: [],
        async run(call) {
          return { callId: call.id, content: "" };
        },
      }),
    });
    const def = defineAgent({
      id: "x",
      systemPrompt: "x",
      tools: [factory],
      capabilities: [],
      inference: { sources: [] },
      director: { id: "@vendor/pkg/not-registered", config: {} },
    });
    const result = getRequiredEnvKeys(
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- getRequiredEnvKeys is invariant in EnvReq; widen to the BaseEnv-typed parameter to call it
      def as unknown as AgentDefinition<BaseEnv>,
      createDefaultDirectorRegistry(),
    );
    expect(result.keys).toContain("transport");
    // BaseEnv keys are always present.
    expect(result.keys).toContain("source");
    expect(result.keys).toContain("storage");
    expect(result.unresolvedDirectorId).toBe("@vendor/pkg/not-registered");
  });
});

describe("effectiveDirectorRef", () => {
  test("returns the canonical default ref when def.director is absent", () => {
    const registry = createDefaultDirectorRegistry();
    const ref = effectiveDirectorRef(emptyDef(), registry);
    expect(ref.id).toBe("@intx/agent/default");
  });

  test("returns def.director when set", () => {
    const customRef = { id: "@vendor/a/one", config: { x: 1 } };
    const def = defineAgent({
      id: "x",
      systemPrompt: "x",
      director: customRef,
      tools: [],
      capabilities: [],
      inference: { sources: [] },
    });
    const ref = effectiveDirectorRef(def, createDefaultDirectorRegistry());
    expect(ref).toBe(customRef);
  });
});
