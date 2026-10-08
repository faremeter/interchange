// Deterministic JSON serialization for deploy-hash inputs.
//
// `canonicalizeForHash` produces stable bytes from a value tree: object
// keys NFC-normalized then sorted, strings normalized to NFC, no
// whitespace. Non-JSON values (Date, Map, Set, function, undefined,
// symbol, NaN, +/-Infinity) and cycles are rejected so the hash cannot
// silently absorb a value the JSON receiver could not reproduce.
//
// Key-ordering caveat: `JSON.stringify` emits integer-indexed string
// keys first in numeric order (ECMA-262 OrdinaryOwnPropertyKeys), so
// integer-like keys are not emitted in strict lex order. Deterministic
// across every engine since ES2020, so deploy-hash equality holds; a
// future engine that changed the rule would change the canonical bytes.

type JsonLike =
  | null
  | boolean
  | number
  | string
  | readonly JsonLike[]
  | { readonly [key: string]: JsonLike };

const encoder = new TextEncoder();

export class CanonicalizationError extends Error {
  readonly path: readonly string[];

  constructor(message: string, path: readonly string[]) {
    super(
      path.length === 0
        ? message
        : `${message} (at ${path.length === 1 ? path[0] : path.join(".")})`,
    );
    this.name = "CanonicalizationError";
    this.path = path;
  }
}

function normalize(
  value: unknown,
  path: readonly string[],
  seen: WeakSet<object>,
): JsonLike {
  if (value === null) return null;

  if (typeof value === "boolean") return value;

  if (typeof value === "string") {
    return value.normalize("NFC");
  }

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new CanonicalizationError(
        `non-finite number (${String(value)}) is not valid JSON`,
        path,
      );
    }
    return value;
  }

  if (typeof value === "undefined") {
    throw new CanonicalizationError("undefined is not valid JSON", path);
  }

  if (typeof value === "symbol") {
    throw new CanonicalizationError("symbol is not valid JSON", path);
  }

  if (typeof value === "function") {
    throw new CanonicalizationError("function is not valid JSON", path);
  }

  if (typeof value === "bigint") {
    throw new CanonicalizationError("bigint is not valid JSON", path);
  }

  // Objects: arrays, plain records, or rejected built-ins.
  const obj: object = value;

  if (seen.has(obj)) {
    throw new CanonicalizationError("cycle detected", path);
  }
  seen.add(obj);

  try {
    if (Array.isArray(obj)) {
      const out: JsonLike[] = [];
      for (let i = 0; i < obj.length; i++) {
        out.push(normalize(obj[i], [...path, `[${String(i)}]`], seen));
      }
      return out;
    }

    if (
      obj instanceof Date ||
      obj instanceof Map ||
      obj instanceof Set ||
      obj instanceof RegExp ||
      obj instanceof Promise ||
      obj instanceof Error ||
      obj instanceof ArrayBuffer ||
      ArrayBuffer.isView(obj)
    ) {
      throw new CanonicalizationError(
        `${obj.constructor.name} is not valid JSON`,
        path,
      );
    }

    const proto: unknown = Object.getPrototypeOf(obj);
    if (proto !== Object.prototype && proto !== null) {
      let protoName = "unknown";
      if (
        typeof proto === "object" &&
        proto !== null &&
        "constructor" in proto &&
        typeof proto.constructor === "function"
      ) {
        protoName = proto.constructor.name;
      }
      throw new CanonicalizationError(
        `non-plain object (prototype ${protoName}) is not valid JSON`,
        path,
      );
    }

    // After the proto check, `obj` is a plain Record<string, unknown>.
    // Index it through a generic record type to drop symbol keys.
    const record: Record<string, unknown> = Object.fromEntries(
      Object.entries(obj),
    );
    // Normalize keys to NFC before sorting and indexing: (a) two raw
    // keys normalizing to the same NFC form would silently drop data;
    // (b) sorting raw keys then normalizing would not produce the
    // canonical NFC-sorted order. Normalize first, raise on collision.
    const byNFC = new Map<string, string>();
    for (const rawKey of Object.keys(record)) {
      const nfcKey = rawKey.normalize("NFC");
      const existing = byNFC.get(nfcKey);
      if (existing !== undefined && existing !== rawKey) {
        throw new CanonicalizationError(
          `keys ${JSON.stringify(existing)} and ${JSON.stringify(rawKey)} ` +
            `NFC-normalize to the same value (${JSON.stringify(nfcKey)})`,
          path,
        );
      }
      byNFC.set(nfcKey, rawKey);
    }
    const nfcKeys = [...byNFC.keys()].sort((a, b) =>
      a < b ? -1 : a > b ? 1 : 0,
    );
    const out: Record<string, JsonLike> = {};
    for (const nfcKey of nfcKeys) {
      const rawKey = byNFC.get(nfcKey);
      if (rawKey === undefined) continue;
      out[nfcKey] = normalize(record[rawKey], [...path, rawKey], seen);
    }
    return out;
  } finally {
    seen.delete(obj);
  }
}

/**
 * Produce stable bytes for a value tree: the UTF-8 encoded form of a
 * canonical JSON document with sorted keys, NFC-normalized strings,
 * and no whitespace. Throws `CanonicalizationError` on any non-JSON
 * value or cycle.
 */
export function canonicalizeForHash(value: unknown): Uint8Array {
  const normalized = normalize(value, [], new WeakSet());
  // `normalize` has already resolved key ordering by constructing
  // plain records with sorted keys.
  return encoder.encode(JSON.stringify(normalized));
}
